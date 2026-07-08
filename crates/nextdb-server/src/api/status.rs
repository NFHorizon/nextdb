use std::sync::atomic::Ordering;

use axum::{
    Json,
    body::Body,
    extract::State,
    http::header,
    response::{IntoResponse, Response},
};

use crate::{
    AppState,
    api::{error::ApiError, objects::object_remote_replica_urls_for_shard},
    cluster::ShardRole,
    util::now_ms,
};

pub(crate) async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let room_count = state.actors.room_count().await;
    let actor_kernel = state.actors.kernel_status().await;
    let hot_room_idle_maintenance = state.actors.idle_maintenance_status();
    let actor_split_maintenance = state.actors.split_maintenance_status();
    let actor_reminders = state.actors.reminder_status(64);
    let actor_reminder_maintenance = state.actors.reminder_maintenance_status();
    let mut wal_statuses = Vec::with_capacity(state.wal_shards.len());
    for shard in &state.wal_shards {
        wal_statuses.push(shard.writer.status().await);
    }
    let shard_controls = state
        .shard_controls
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let handoff_workflows = state
        .handoff_workflows
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let topology_overrides = state.topology_overrides.read().await.clone();
    let topology_lease = state.topology_lease.read().await.clone();
    let handoff_controller = state.handoff_controller.read().await.clone();
    let failover_controller = state.failover_controller.read().await.clone();
    let wal_repair_controller = state.wal_repair_controller.read().await.clone();
    let object_repair_controller = state.object_repair_controller.read().await.clone();
    let export_backup_controller = state.export_backup_controller.read().await.clone();
    let peer_health = state.peer_health.read().await.clone();
    let cache_control = state.cache_control.read().await.clone();
    let runtime_drain = state.runtime_drain.read().await.clone();
    let behavior_runtime = state.behaviors.status().await;
    let realtime_status = state.realtime.status().await;
    let record_hot_prewarm = state.record_hot_prewarm.read().await.clone();
    let live_queries = state.live_query_metrics.snapshot(
        state.connections.total_query_subscriptions().await,
        state.realtime_event_batch_max,
    );
    let topology = state.cluster.topology_with_overrides(&topology_overrides);
    let wal_replicas = state
        .wal_shards
        .iter()
        .zip(wal_statuses.iter())
        .map(|(shard, status)| {
            let remote_replica_urls = status
                .remote_replicas
                .iter()
                .map(|replica| replica.url.clone())
                .collect::<Vec<_>>();
            serde_json::json!({
                "shard": shard.index,
                "epoch": state.cluster.epoch_for_shard_with_overrides(shard.index, &topology_overrides),
                "primary": &shard.path,
                "replicas": &shard.replica_paths,
                "remoteReplicas": remote_replica_urls,
                "remoteAckPolicy": shard.remote_ack_policy,
                "remoteRequiredAcks": status.remote_required_acks,
                "remoteStatus": status,
                "owner": state.cluster.owner_for_shard_with_overrides(shard.index, &topology_overrides),
                "role": state.cluster.role_for_shard_with_overrides(shard.index, &topology_overrides),
            })
        })
        .collect::<Vec<_>>();
    let mut object_remote_replica_urls = Vec::new();
    for shard in &state.wal_shards {
        for url in object_remote_replica_urls_for_shard(&state, shard.index).await {
            if !object_remote_replica_urls
                .iter()
                .any(|existing| existing == &url)
            {
                object_remote_replica_urls.push(url);
            }
        }
    }
    Json(serde_json::json!({
        "ok": true,
        "runtimeId": state.runtime_id.clone(),
        "draining": runtime_drain.draining,
        "acceptingWrites": !runtime_drain.draining,
        "runtimeDrain": runtime_drain,
        "runtimeWrites": state.runtime_writes.snapshot(),
        "behaviorRuntime": behavior_runtime,
        "adminAuthEnabled": state.admin_token.is_some(),
        "clientAuthEnabled": state.client_token.is_some() || !state.client_user_tokens.is_empty(),
        "clientUserAuthEnabled": !state.client_user_tokens.is_empty(),
        "roomCount": room_count,
        "hotRoomCount": room_count,
        "actorKernel": actor_kernel,
        "actorShards": state.actors.shard_statuses(),
        "maxHotRooms": state.actors.max_hot_rooms(),
        "hotWindow": state.actors.hot_window(),
        "hotRoomIdleTtlMs": state.actors.hot_room_idle_ttl_ms(),
        "hotRoomMaintenanceIntervalMs": state.hot_room_maintenance_interval_ms,
        "hotRoomIdleMaintenance": hot_room_idle_maintenance,
        "actorSplitMaintenanceIntervalMs": state.actor_split_maintenance_interval_ms,
        "actorSplitMaintenanceLimit": state.actor_split_maintenance_limit,
        "actorSplitMaintenance": actor_split_maintenance,
        "actorReminderMaintenanceIntervalMs": state.actor_reminder_maintenance_interval_ms,
        "actorReminderMaintenanceLimit": state.actor_reminder_maintenance_limit,
        "actorReminders": actor_reminders,
        "actorReminderMaintenance": actor_reminder_maintenance,
        "wal": state.wal_paths.first(),
        "walShardCount": state.wal_shards.len(),
        "walPaths": &state.wal_paths,
        "nodeId": state.cluster.node_id(),
        "clusterEnforceOwnership": state.cluster.enforce_ownership(),
        "clusterTopology": topology,
        "topologyOverrides": topology_overrides,
        "topologyLog": state.topology_log_path,
        "topologyLease": topology_lease,
        "topologyLeaseMs": state.topology_lease_ms,
        "shardControls": shard_controls,
        "handoffWorkflows": handoff_workflows,
        "handoffController": handoff_controller,
        "failoverController": failover_controller,
        "walRepairController": wal_repair_controller,
        "objectRepairController": object_repair_controller,
        "exportBackupController": export_backup_controller,
        "peerHealth": peer_health,
        "walReplicaCount": state.wal_shards.iter().map(|shard| shard.replica_paths.len()).sum::<usize>(),
        "walRemoteReplicaCount": wal_statuses.iter().map(|status| status.remote_replica_count).sum::<usize>(),
        "walReplicas": wal_replicas,
        "currentLsn": state.current_lsn.load(Ordering::Relaxed),
        "lastSnapshotLsn": state.last_snapshot_lsn.load(Ordering::Relaxed),
        "lastCompactionLsn": state.last_compaction_lsn.load(Ordering::Relaxed),
        "startupRecovery": &state.startup_recovery,
        "checkpointEveryLsn": state.checkpoint_every_lsn,
        "checkpointInFlight": state.checkpoint_in_flight.load(Ordering::Acquire),
        "autoCompactWal": state.auto_compact_wal,
        "objectGcGraceMs": state.object_gc_grace_ms,
        "limits": &state.limits,
        "chatLog": "enabled",
        "recordHotCache": state.record_hot.status().await,
        "recordHotMaintenanceIntervalMs": state.record_hot_maintenance_interval_ms,
        "recordHotPrewarmLimit": state.record_hot_prewarm_limit,
        "recordHotPrewarm": record_hot_prewarm,
        "objectStore": "enabled",
        "objectRemoteReplicaCount": object_remote_replica_urls.len(),
        "objectRemoteReplicas": object_remote_replica_urls,
        "realtimeChannels": realtime_status.channel_count,
        "realtimeChannelStates": realtime_status.state_count,
        "realtimeChannelSequences": realtime_status.sequence_count,
        "realtimeMaintenanceIntervalMs": state.realtime_maintenance_interval_ms,
        "realtimeMaintenance": realtime_status.maintenance,
        "connectionCount": state.connections.count().await,
        "connectedUsers": state.connections.user_count().await,
        "liveQueries": live_queries,
        "connectionLayer": {
            "protocol": "nextdb.realtime.v1",
            "frameEncoding": "json",
            "connectPath": "/v1/connect",
            "supportedTransports": ["webSocket", "custom"],
            "defaultTransport": "webSocket",
            "webSocket": {
                "supported": true,
                "connectPath": "/v1/connect",
            },
            "webTransport": {
                "supported": false,
                "connectPath": null,
            },
            "custom": {
                "supported": true,
                "connectPath": "/v1/connect/jsonl",
            },
        },
        "schema": state.schema.path(),
        "clientCache": cache_control,
        "clientCacheControl": state.cache_control_path,
    }))
}

pub(crate) async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    let runtime_drain = state.runtime_drain.read().await.clone();
    let current_lsn = state.current_lsn.load(Ordering::Relaxed);
    let topology_overrides = state.topology_overrides.read().await.clone();
    let wal_statuses = futures_util::future::join_all(
        state
            .wal_shards
            .iter()
            .map(|shard| async move { shard.writer.status().await }),
    )
    .await;
    let local_writable_shards = (0..state.wal_shards.len())
        .filter(|shard| {
            state
                .cluster
                .role_for_shard_with_overrides(*shard, &topology_overrides)
                == ShardRole::Owner
        })
        .count();
    let wal_ready = wal_statuses.len() == state.wal_shards.len() && !state.wal_shards.is_empty();
    let write_ready = wal_ready && !runtime_drain.draining && local_writable_shards > 0;
    let realtime_ready = !runtime_drain.draining;
    let checks = vec![
        serde_json::json!({
            "name": "wal",
            "ok": wal_ready,
            "detail": format!("{} WAL shard worker(s) available", wal_statuses.len()),
        }),
        serde_json::json!({
            "name": "runtimeDrain",
            "ok": !runtime_drain.draining,
            "detail": runtime_drain.reason.clone().unwrap_or_else(|| "runtime accepts new writes and realtime connections".to_string()),
        }),
        serde_json::json!({
            "name": "localShardOwnership",
            "ok": local_writable_shards > 0,
            "detail": format!("{local_writable_shards}/{} local shard(s) writable", state.wal_shards.len()),
        }),
        serde_json::json!({
            "name": "schema",
            "ok": true,
            "detail": state.schema.path().display().to_string(),
        }),
        serde_json::json!({
            "name": "connectionLayer",
            "ok": realtime_ready,
            "detail": if realtime_ready {
                "new WebSocket/custom realtime connections are accepted"
            } else {
                "runtime drain rejects new realtime connections"
            },
        }),
    ];

    Json(serde_json::json!({
        "ok": write_ready && realtime_ready,
        "readReady": wal_ready,
        "writeReady": write_ready,
        "realtimeReady": realtime_ready,
        "acceptingWrites": write_ready,
        "draining": runtime_drain.draining,
        "runtimeDrain": runtime_drain,
        "runtimeWrites": state.runtime_writes.snapshot(),
        "currentLsn": current_lsn,
        "runtimeId": state.runtime_id.clone(),
        "nodeId": state.cluster.node_id(),
        "walShardCount": state.wal_shards.len(),
        "localWritableShards": local_writable_shards,
        "checkedAtMs": now_ms(),
        "checks": checks,
    }))
}

pub(crate) async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let room_count = state.actors.room_count().await;
    let mut wal_statuses = Vec::with_capacity(state.wal_shards.len());
    for shard in &state.wal_shards {
        wal_statuses.push(shard.writer.status().await);
    }
    let runtime_drain = state.runtime_drain.read().await.clone();
    let runtime_writes = state.runtime_writes.snapshot();
    let behavior_runtime = state.behaviors.status().await;
    let hot_room_idle_maintenance = state.actors.idle_maintenance_status();
    let record_hot = state.record_hot.status().await;
    let record_hot_prewarm = state.record_hot_prewarm.read().await.clone();
    let projection_status = state
        .records
        .projection_status()
        .await
        .map_err(ApiError::internal)?;
    let object_count = state
        .objects
        .list_metadata()
        .await
        .map_err(ApiError::internal)?
        .len();
    let export_backup_runs = state.export_backup_runs.read().await.len();
    let export_backup_controller = state.export_backup_controller.read().await.clone();
    let handoff_controller = state.handoff_controller.read().await.clone();
    let failover_controller = state.failover_controller.read().await.clone();
    let wal_repair_controller = state.wal_repair_controller.read().await.clone();
    let object_repair_controller = state.object_repair_controller.read().await.clone();
    let peer_health = state.peer_health.read().await.clone();
    let connection_count = state.connections.count().await;
    let connected_users = state.connections.user_count().await;
    let live_queries = state.live_query_metrics.snapshot(
        state.connections.total_query_subscriptions().await,
        state.realtime_event_batch_max,
    );
    let realtime_status = state.realtime.status().await;

    let mut output = String::new();
    push_metric_help(&mut output, "nextdb_up", "NextDB process health.");
    push_metric_type(&mut output, "nextdb_up", "gauge");
    push_metric(&mut output, "nextdb_up", &[], 1);
    push_metric_help(
        &mut output,
        "nextdb_behavior_pooled_instances",
        "Resident pooled Wasm behavior instances.",
    );
    push_metric_type(&mut output, "nextdb_behavior_pooled_instances", "gauge");
    push_metric(
        &mut output,
        "nextdb_behavior_pooled_instances",
        &[],
        behavior_runtime.pooled_instances as u64,
    );
    push_metric_help(
        &mut output,
        "nextdb_behavior_instance_pool_max",
        "Configured resident Wasm instance pool limit per behavior.",
    );
    push_metric_type(&mut output, "nextdb_behavior_instance_pool_max", "gauge");
    push_metric(
        &mut output,
        "nextdb_behavior_instance_pool_max",
        &[],
        behavior_runtime.instance_pool_max as u64,
    );
    push_metric_help(
        &mut output,
        "nextdb_behavior_fuel_enabled",
        "Whether Wasm fuel instrumentation is enabled for behavior turns.",
    );
    push_metric_type(&mut output, "nextdb_behavior_fuel_enabled", "gauge");
    push_metric(
        &mut output,
        "nextdb_behavior_fuel_enabled",
        &[],
        bool_metric(behavior_runtime.fuel_enabled),
    );
    push_behavior_counter_metrics(&mut output, &behavior_runtime);
    push_metric(
        &mut output,
        "nextdb_accepting_writes",
        &[],
        bool_metric(!runtime_drain.draining),
    );
    push_metric(
        &mut output,
        "nextdb_draining",
        &[],
        bool_metric(runtime_drain.draining),
    );
    push_metric(
        &mut output,
        "nextdb_current_lsn",
        &[],
        state.current_lsn.load(Ordering::Relaxed),
    );
    push_metric(
        &mut output,
        "nextdb_last_snapshot_lsn",
        &[],
        state.last_snapshot_lsn.load(Ordering::Relaxed),
    );
    push_metric(
        &mut output,
        "nextdb_last_compaction_lsn",
        &[],
        state.last_compaction_lsn.load(Ordering::Relaxed),
    );
    push_metric(
        &mut output,
        "nextdb_checkpoint_in_flight",
        &[],
        bool_metric(state.checkpoint_in_flight.load(Ordering::Acquire)),
    );
    push_metric(
        &mut output,
        "nextdb_runtime_writes_in_flight",
        &[],
        runtime_writes.in_flight,
    );
    push_metric(&mut output, "nextdb_rooms_total", &[], room_count as u64);
    push_metric(
        &mut output,
        "nextdb_hot_room_idle_ttl_ms",
        &[],
        state.actors.hot_room_idle_ttl_ms(),
    );
    push_metric(
        &mut output,
        "nextdb_hot_room_maintenance_interval_ms",
        &[],
        state.hot_room_maintenance_interval_ms,
    );
    push_metric(
        &mut output,
        "nextdb_hot_room_idle_last_sweep_at_ms",
        &[],
        hot_room_idle_maintenance.last_sweep_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_hot_room_idle_last_evicted",
        &[],
        hot_room_idle_maintenance.last_evicted as u64,
    );
    push_metric(
        &mut output,
        "nextdb_hot_room_idle_total_evicted",
        &[],
        hot_room_idle_maintenance.total_evicted as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_projection_records",
        &[],
        projection_status.records as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_projection_index_entries",
        &[],
        projection_status.index_entries as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_projection_key_order_entries",
        &[],
        projection_status.key_order_entries as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_projection_recent_entries",
        &[],
        projection_status.recent_entries as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_projection_partition_entries",
        &[],
        projection_status.partition_entries as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_projection_order_entries",
        &[],
        projection_status.order_entries as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_tables",
        &[],
        record_hot.table_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_records",
        &[],
        record_hot.record_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_volatile_records",
        &[],
        record_hot.volatile_records as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_durable_idle_ttl_ms",
        &[],
        record_hot.durable_idle_ttl_ms,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_maintenance_interval_ms",
        &[],
        state.record_hot_maintenance_interval_ms,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_prewarm_enabled",
        &[],
        bool_metric(record_hot_prewarm.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_prewarm_limit",
        &[],
        record_hot_prewarm.limit_per_table as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_prewarm_last_started_at_ms",
        &[],
        record_hot_prewarm.last_started_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_prewarm_last_finished_at_ms",
        &[],
        record_hot_prewarm.last_finished_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_prewarm_total_found",
        &[],
        record_hot_prewarm.total_found as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_prewarm_total_activated",
        &[],
        record_hot_prewarm.total_activated as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_durable_idle_last_sweep_at_ms",
        &[],
        record_hot.durable_idle_last_sweep_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_durable_idle_last_evicted",
        &[],
        record_hot.durable_idle_last_evicted as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_durable_idle_total_evicted",
        &[],
        record_hot.durable_idle_total_evicted as u64,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_get_total",
        &[],
        record_hot.get_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_get_hit_total",
        &[],
        record_hot.get_hit_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_get_miss_total",
        &[],
        record_hot.get_miss_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_list_total",
        &[],
        record_hot.list_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_list_records_total",
        &[],
        record_hot.list_records_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_hydrate_durable_total",
        &[],
        record_hot.hydrate_durable_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_hydrate_durable_skipped_volatile_total",
        &[],
        record_hot.hydrate_durable_skipped_volatile_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_upsert_total",
        &[],
        record_hot.upsert_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_delete_total",
        &[],
        record_hot.delete_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_evict_total",
        &[],
        record_hot.evict_total,
    );
    push_metric(
        &mut output,
        "nextdb_record_hot_lru_evicted_total",
        &[],
        record_hot.lru_evicted_total,
    );
    for table in &record_hot.tables {
        let labels = [("table", table.table.as_str())];
        push_metric(
            &mut output,
            "nextdb_record_hot_table_records",
            &labels,
            table.records as u64,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_volatile_records",
            &labels,
            table.volatile_records as u64,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_get_total",
            &labels,
            table.counters.get_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_get_hit_total",
            &labels,
            table.counters.get_hit_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_get_miss_total",
            &labels,
            table.counters.get_miss_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_list_total",
            &labels,
            table.counters.list_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_list_records_total",
            &labels,
            table.counters.list_records_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_hydrate_durable_total",
            &labels,
            table.counters.hydrate_durable_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_hydrate_durable_skipped_volatile_total",
            &labels,
            table.counters.hydrate_durable_skipped_volatile_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_upsert_total",
            &labels,
            table.counters.upsert_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_delete_total",
            &labels,
            table.counters.delete_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_evict_total",
            &labels,
            table.counters.evict_total,
        );
        push_metric(
            &mut output,
            "nextdb_record_hot_table_lru_evicted_total",
            &labels,
            table.counters.lru_evicted_total,
        );
    }
    push_metric(
        &mut output,
        "nextdb_objects_total",
        &[],
        object_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_connections_total",
        &[],
        connection_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_connected_users",
        &[],
        connected_users as u64,
    );
    push_metric(
        &mut output,
        "nextdb_live_queries_current",
        &[],
        live_queries.current as u64,
    );
    push_metric(
        &mut output,
        "nextdb_live_queries_subscribed_total",
        &[],
        live_queries.subscribed_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_queries_unsubscribed_total",
        &[],
        live_queries.unsubscribed_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_event_batch_max",
        &[],
        live_queries.event_batch_max as u64,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_event_batches_total",
        &[],
        live_queries.event_batches_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_batched_events_total",
        &[],
        live_queries.batched_events_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_refresh_candidates_total",
        &[],
        live_queries.refresh_candidates_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_refresh_total",
        &[],
        live_queries.refresh_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_executions_total",
        &[],
        live_queries.query_executions_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_result_frames_total",
        &[],
        live_queries.result_frames_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_diff_frames_total",
        &[],
        live_queries.diff_frames_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_unchanged_total",
        &[],
        live_queries.unchanged_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_evaluation_cache_hits_total",
        &[],
        live_queries.evaluation_cache_hits_total,
    );
    push_metric(
        &mut output,
        "nextdb_live_query_errors_total",
        &[],
        live_queries.errors_total,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_channels",
        &[],
        realtime_status.channel_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_channel_states",
        &[],
        realtime_status.state_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_channel_sequences",
        &[],
        realtime_status.sequence_count as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_interval_ms",
        &[],
        state.realtime_maintenance_interval_ms,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_last_sweep_at_ms",
        &[],
        realtime_status.maintenance.last_sweep_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_last_orphan_states_removed",
        &[],
        realtime_status.maintenance.last_orphan_states_removed as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_last_stale_members_removed",
        &[],
        realtime_status.maintenance.last_stale_members_removed as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_last_orphan_sequences_removed",
        &[],
        realtime_status.maintenance.last_orphan_sequences_removed as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_total_stale_members_removed",
        &[],
        realtime_status.maintenance.total_stale_members_removed as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_total_orphan_states_removed",
        &[],
        realtime_status.maintenance.total_orphan_states_removed as u64,
    );
    push_metric(
        &mut output,
        "nextdb_realtime_maintenance_total_orphan_sequences_removed",
        &[],
        realtime_status.maintenance.total_orphan_sequences_removed as u64,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_object_bytes",
        &[],
        state.limits.max_object_bytes,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_message_bytes",
        &[],
        state.limits.max_message_bytes,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_user_event_bytes",
        &[],
        state.limits.max_user_event_bytes,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_record_value_bytes",
        &[],
        state.limits.max_record_value_bytes,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_live_queries_per_connection",
        &[],
        state.limits.max_live_queries_per_connection as u64,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_live_queries_per_table_per_connection",
        &[],
        state.limits.max_live_queries_per_table_per_connection as u64,
    );
    push_metric(
        &mut output,
        "nextdb_limit_max_live_queries_per_user",
        &[],
        state.limits.max_live_queries_per_user as u64,
    );
    push_metric(
        &mut output,
        "nextdb_wal_shards",
        &[],
        state.wal_shards.len() as u64,
    );
    push_metric(
        &mut output,
        "nextdb_wal_remote_replicas",
        &[],
        wal_statuses
            .iter()
            .map(|status| status.remote_replica_count as u64)
            .sum::<u64>(),
    );
    for status in &wal_statuses {
        let shard = status.shard.to_string();
        push_metric(
            &mut output,
            "nextdb_wal_batch_max",
            &[("shard", &shard)],
            status.batch_max as u64,
        );
        push_metric(
            &mut output,
            "nextdb_wal_batch_wait_ms",
            &[("shard", &shard)],
            status.batch_wait_ms,
        );
        push_metric(
            &mut output,
            "nextdb_wal_queue_depth",
            &[("shard", &shard)],
            status.queue_depth as u64,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_batches",
            &[("shard", &shard)],
            status.local_batches,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_failed_batches",
            &[("shard", &shard)],
            status.local_failed_batches,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_records",
            &[("shard", &shard)],
            status.local_records,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_bytes",
            &[("shard", &shard)],
            status.local_bytes,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_syncs",
            &[("shard", &shard)],
            status.local_syncs,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_last_batch_records",
            &[("shard", &shard)],
            status.local_last_batch_records as u64,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_last_batch_write_ms",
            &[("shard", &shard)],
            status.local_last_batch_write_ms,
        );
        push_metric(
            &mut output,
            "nextdb_wal_local_last_batch_sync_ms",
            &[("shard", &shard)],
            status.local_last_batch_sync_ms,
        );
        for (index, replica) in status.remote_replicas.iter().enumerate() {
            let replica_index = index.to_string();
            push_metric(
                &mut output,
                "nextdb_wal_remote_replica_ok",
                &[
                    ("shard", &shard),
                    ("replica", &replica_index),
                    ("url", &replica.url),
                ],
                bool_metric(replica.ok),
            );
            push_metric(
                &mut output,
                "nextdb_wal_remote_replica_highest_acked_lsn",
                &[
                    ("shard", &shard),
                    ("replica", &replica_index),
                    ("url", &replica.url),
                ],
                replica.highest_acked_lsn,
            );
        }
    }
    push_metric(
        &mut output,
        "nextdb_backup_runs_total",
        &[],
        export_backup_runs as u64,
    );
    push_metric(
        &mut output,
        "nextdb_backup_controller_enabled",
        &[],
        bool_metric(export_backup_controller.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_backup_controller_interval_ms",
        &[],
        export_backup_controller.interval_ms,
    );
    push_metric(
        &mut output,
        "nextdb_handoff_controller_enabled",
        &[],
        bool_metric(handoff_controller.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_failover_controller_enabled",
        &[],
        bool_metric(failover_controller.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_enabled",
        &[],
        bool_metric(wal_repair_controller.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_interval_ms",
        &[],
        wal_repair_controller.interval_ms,
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_last_run_at_ms",
        &[],
        wal_repair_controller.last_run_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_records_sent",
        &[],
        wal_repair_controller.last_records_sent as u64,
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_repaired_replicas",
        &[],
        wal_repair_controller.last_repaired_replicas as u64,
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_satisfied",
        &[],
        bool_metric(wal_repair_controller.last_satisfied),
    );
    push_metric(
        &mut output,
        "nextdb_wal_repair_controller_last_error",
        &[],
        bool_metric(wal_repair_controller.last_error.is_some()),
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_enabled",
        &[],
        bool_metric(object_repair_controller.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_interval_ms",
        &[],
        object_repair_controller.interval_ms,
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_last_run_at_ms",
        &[],
        object_repair_controller.last_run_at_ms.unwrap_or(0),
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_objects_sent",
        &[],
        object_repair_controller.last_objects_sent as u64,
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_repaired_replicas",
        &[],
        object_repair_controller.last_repaired_replicas as u64,
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_satisfied",
        &[],
        bool_metric(object_repair_controller.last_satisfied),
    );
    push_metric(
        &mut output,
        "nextdb_object_repair_controller_last_error",
        &[],
        bool_metric(object_repair_controller.last_error.is_some()),
    );
    push_metric(
        &mut output,
        "nextdb_peer_monitor_enabled",
        &[],
        bool_metric(peer_health.enabled),
    );
    push_metric(
        &mut output,
        "nextdb_peer_monitor_peers",
        &[],
        peer_health.peers.len() as u64,
    );

    Response::builder()
        .header(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(output))
        .map_err(|err| ApiError::internal(err.into()))
}

fn push_metric_help(output: &mut String, name: &str, help: &str) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
}

fn push_metric_type(output: &mut String, name: &str, kind: &str) {
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(kind);
    output.push('\n');
}

fn push_metric(output: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    output.push_str(name);
    if !labels.is_empty() {
        output.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            output.push_str(key);
            output.push_str("=\"");
            push_metric_label_value(output, value);
            output.push('"');
        }
        output.push('}');
    }
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

fn push_behavior_counter_metrics(
    output: &mut String,
    status: &crate::behavior::BehaviorRuntimeStatus,
) {
    push_metric_help(
        output,
        "nextdb_behavior_invocations_total",
        "Total loaded behavior invocation attempts.",
    );
    push_metric_type(output, "nextdb_behavior_invocations_total", "counter");
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_invocations_total",
        status,
        |counters| counters.invocations,
    );
    push_metric_help(
        output,
        "nextdb_behavior_successes_total",
        "Total behavior invocations that returned valid output and were returned to the resident pool.",
    );
    push_metric_type(output, "nextdb_behavior_successes_total", "counter");
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_successes_total",
        status,
        |counters| counters.successes,
    );
    push_metric_help(
        output,
        "nextdb_behavior_unknown_message_invocations_total",
        "Total behavior invocations routed to on_unknown_message.",
    );
    push_metric_type(
        output,
        "nextdb_behavior_unknown_message_invocations_total",
        "counter",
    );
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_unknown_message_invocations_total",
        status,
        |counters| counters.unknown_message_invocations,
    );
    push_metric_help(
        output,
        "nextdb_behavior_guest_errors_total",
        "Total behavior guest traps, ABI errors, or entrypoint errors.",
    );
    push_metric_type(output, "nextdb_behavior_guest_errors_total", "counter");
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_guest_errors_total",
        status,
        |counters| counters.guest_errors,
    );
    push_metric_help(
        output,
        "nextdb_behavior_command_rejections_total",
        "Total behavior outputs rejected by host command capability checks.",
    );
    push_metric_type(
        output,
        "nextdb_behavior_command_rejections_total",
        "counter",
    );
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_command_rejections_total",
        status,
        |counters| counters.command_rejections,
    );
    push_metric_help(
        output,
        "nextdb_behavior_instances_created_total",
        "Total resident behavior Wasm instances created.",
    );
    push_metric_type(output, "nextdb_behavior_instances_created_total", "counter");
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_instances_created_total",
        status,
        |counters| counters.instances_created,
    );
    push_metric_help(
        output,
        "nextdb_behavior_instances_reused_total",
        "Total behavior Wasm instances reused from the resident pool.",
    );
    push_metric_type(output, "nextdb_behavior_instances_reused_total", "counter");
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_instances_reused_total",
        status,
        |counters| counters.instances_reused,
    );
    push_metric_help(
        output,
        "nextdb_behavior_instances_discarded_total",
        "Total behavior Wasm instances discarded instead of returned to the resident pool.",
    );
    push_metric_type(
        output,
        "nextdb_behavior_instances_discarded_total",
        "counter",
    );
    push_labeled_behavior_counter(
        output,
        "nextdb_behavior_instances_discarded_total",
        status,
        |counters| counters.instances_discarded,
    );
}

fn push_labeled_behavior_counter<F>(
    output: &mut String,
    name: &str,
    status: &crate::behavior::BehaviorRuntimeStatus,
    value: F,
) where
    F: Fn(&crate::behavior::BehaviorRuntimeCounterSnapshot) -> u64,
{
    push_metric(output, name, &[], value(&status.counters));
    for behavior in &status.behaviors {
        push_metric(
            output,
            name,
            &[("behavior", behavior.name.as_str())],
            value(&behavior.counters),
        );
    }
}

fn push_metric_label_value(output: &mut String, value: &str) {
    for char in value.chars() {
        match char {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            _ => output.push(char),
        }
    }
}

fn bool_metric(value: bool) -> u64 {
    if value { 1 } else { 0 }
}
