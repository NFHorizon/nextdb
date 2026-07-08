#![recursion_limit = "256"]
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

mod actor;
mod aggregate;
mod api;
mod behavior;
mod cache_control;
mod chat_log;
mod cluster;
mod config;
mod connection;
mod live_query;
mod model;
mod object_refs;
mod object_store;
mod realtime;
mod realtime_fanout;
mod record_hot;
mod record_projection;
mod record_store;
mod schema;
mod snapshot;
mod tasks;
mod util;
mod wal;

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicBool, AtomicU64},
    },
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, bail};
use axum::{
    Router, middleware,
    routing::{get, post},
};
use tokio::sync::{Mutex, RwLock, broadcast};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    actor::{ActorRuntime, actor_reminders_from_wal_records, actor_states_with_wal_tail},
    aggregate::AggregateRegistry,
    api::audit::{audit_replay, audit_trace, audit_wal},
    api::auth::auth_middleware,
    api::backup::{
        ExportBackupControllerState, ExportBackupPolicy, ExportBackupRunRecord,
        apply_import_bundle_delta, archive_export_bundle_to_object, create_export_bundle,
        export_manifest, get_export_backup_policy, import_bundle_delta_preflight,
        import_bundle_from_object, import_bundle_preflight, list_export_backup_runs,
        list_export_bundles, load_export_backup_policy, load_export_backup_runs,
        restore_import_bundle, restore_import_bundle_chain, retain_export_backups,
        run_export_backup, run_export_backup_policy, set_export_backup_policy,
        spawn_export_backup_controller, verify_export_bundle, verify_export_bundle_chain,
    },
    api::behavior::{
        HostHttpRequestRecord, host_http_request_index_from_wal_records, invoke_behavior,
        list_behaviors, pending_host_http_requests_from_wal_records, reload_behaviors,
        reload_behaviors_internal, replay_pending_host_http_requests,
        validate_behavior_manifest_schema,
    },
    api::cache::{get_cache_profile, invalidate_cache, update_cache_profile},
    api::connections::{
        ConnectionControlMessage, ConnectionEvent, connect_jsonl, connect_ws,
        disconnect_connections, list_connections,
    },
    api::mutation::{CommittedMutation, latest_messages, mutate},
    api::objects::{
        commit_object_delete, commit_object_put, delete_object, gc_objects, get_object_body,
        get_object_metadata, get_object_references, list_objects, put_object,
        run_object_repair_controller_once,
    },
    api::realtime::{
        publish_realtime_member_left, realtime_broadcast, realtime_channel_list,
        realtime_channel_state, realtime_join, realtime_leave, realtime_members, realtime_signal,
        update_realtime_channel_state, update_realtime_presence,
    },
    api::records::{
        delete_nested_record, delete_record, get_nested_record, get_record, list_nested_records,
        list_records, query_nested_records_by_index, query_records_by_index, record_batch,
        record_transaction, schema_indexes_by_table, schema_orders_by_table, upsert_nested_record,
        upsert_record,
    },
    api::runtime::{
        ActorReminderIndexKey, ActorReminderRecord, ProjectionRebuildStatus,
        RecordHotPrewarmStatus, StartupRecoveryReport, WalReplayReport, activate_runtime_actor,
        activate_runtime_records, activate_runtime_room, actor_reminder_index_from_wal_records,
        cancel_actor_reminder, compact_wal, create_snapshot, evict_runtime_records,
        evict_runtime_room, get_runtime_drain, prepare_runtime_restart, projection_rebuild_status,
        projection_status, read_startup_projections_from_wal_paths, rebuild_projections,
        recover_schema_from_wal, restore_missing_wal_from_replicas, run_due_actor_reminders,
        run_due_actor_reminders_once, runtime_activation_status, schedule_actor_reminder,
        set_runtime_drain, shutdown_signal, spawn_record_hot_prewarm,
    },
    api::schema::{
        SchemaProposal, SchemaReplayApplyStatus, abort_schema_proposal, abort_schema_proposal_peer,
        apply_schema, cancel_schema_replay_apply, commit_schema_proposal,
        commit_schema_proposal_peer, get_schema, get_schema_history, get_schema_typescript,
        get_schema_version, list_schema_proposals, load_schema_proposals,
        load_schema_replay_apply_status, preflight_schema, prepare_schema_proposal_peer,
        reload_schema, resume_schema_replay_apply, retry_schema_replay_apply,
        schema_migration_plan, schema_replay_apply_status, schema_storage_policy,
        start_schema_proposal, validate_schema,
    },
    api::status::{health, metrics, readiness},
    api::sync::{sync_pull, sync_wait},
    api::topology::{
        TopologyLease, TopologyProposal, load_handoff_workflows, load_topology_lease,
        load_topology_overrides, load_topology_proposals, run_failover_controller_once,
        run_handoff_controller_once, run_peer_health_monitor_once,
    },
    api::users::{UserProjection, get_user, list_user_events, list_users, upsert_user},
    api::wal::{
        retain_wal_archives, run_wal_repair_controller_once, seal_wal_checksums, wal_integrity,
    },
    behavior::BehaviorRuntime,
    cache_control::{ClientCacheControl, ClientCacheInvalidationEntry, load_cache_control},
    chat_log::ChatLog,
    cluster::{ClusterConfig, ClusterShardOverride},
    config::{
        DEFAULT_AUTO_COMPACT_WAL, DEFAULT_CHECKPOINT_EVERY_LSN, DEFAULT_OBJECT_GC_GRACE_MS,
        DEFAULT_REALTIME_EVENT_BATCH_MAX, DEFAULT_RECORD_HOT_PREWARM_LIMIT,
        DEFAULT_TOPOLOGY_LEASE_MS, DEFAULT_WAL_BATCH_MAX, DEFAULT_WAL_BATCH_WAIT_MS,
        DEFAULT_WAL_SHARD_COUNT, MAX_WAL_SHARDS, RuntimeLimits, effective_actor_runtime_config,
        env_u64, env_usize, parse_bool_env, parse_path_list_env, parse_url_list_value,
        parse_user_token_env, parse_wal_remote_ack_policy, read_secret_env,
    },
    connection::ConnectionRegistry,
    live_query::{LiveQueryEvaluationCache, LiveQueryMetrics},
    model::DeliveryEventBatch,
    object_refs::ObjectRefIndex,
    object_store::ObjectStore,
    realtime::RealtimeChannels,
    realtime_fanout::RealtimeFanoutRegistry,
    record_hot::RecordHotCache,
    record_projection::RecordProjectionApplier,
    record_store::RecordStore,
    schema::SchemaRegistry,
    snapshot::SnapshotStore,
    tasks::{
        FailoverControllerState, HandoffControllerState, HandoffWorkflow,
        ObjectRepairControllerState, PeerHealthMonitorState, RuntimeDrainState,
        RuntimeWriteTracker, ShardControl, WalRepairControllerState,
        spawn_actor_scope_residency_maintenance, spawn_actor_split_maintenance,
        spawn_hot_room_maintenance, spawn_periodic_controller, spawn_periodic_task,
        spawn_realtime_maintenance, spawn_record_hot_maintenance,
    },
    wal::{WalRemoteReplica, WalReplica, WalShard, WalWriter, WalWriterConfig},
};

#[cfg(test)]
use crate::api::connections::drain_realtime_event_batch;
#[cfg(test)]
use crate::api::mutation::client_mutation_index_from_wal_records;
#[cfg(test)]
use crate::api::records::{RecordPredicateOp, RecordPredicateTerm};
#[cfg(test)]
use crate::api::runtime::SchemaWalRecoveryReport;
#[cfg(test)]
use crate::api::wal::{apply_replicated_wal_record, wait_for_replicated_record_projection};
#[cfg(test)]
use crate::model::UserProfileDraft;
#[cfg(test)]
use crate::model::{DbRecord, Durability, Message, WalPayload, WalRecord};
#[cfg(test)]
use crate::model::{MessageDraft, ObjectMetadata, UserEventDraft};
#[cfg(test)]
use crate::realtime::{
    JsonLineFrameSink, JsonLineFrameSource, decode_client_frame, encode_server_frame_json_line,
};
#[cfg(test)]
use crate::record_store::parse_record_order_terms;
#[cfg(test)]
use crate::schema::DatabaseSchema;
#[cfg(test)]
use crate::util::hex_lower;
use crate::util::now_ms;
#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use std::{future::Future, pin::Pin};
#[cfg(test)]
use tokio::fs;

const DEFAULT_BEHAVIOR_WATCH_INTERVAL_MS: u64 = 500;
const MIN_BEHAVIOR_WATCH_INTERVAL_MS: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DevOptions {
    watch_behaviors: bool,
}

impl DevOptions {
    const fn server() -> Self {
        Self {
            watch_behaviors: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) runtime_id: String,
    pub(crate) actors: ActorRuntime,
    pub(crate) wal_shards: Vec<WalShard>,
    pub(crate) wal_paths: Vec<PathBuf>,
    pub(crate) current_lsn: Arc<AtomicU64>,
    pub(crate) next_lsn: Arc<AtomicU64>,
    pub(crate) last_snapshot_lsn: Arc<AtomicU64>,
    pub(crate) last_compaction_lsn: Arc<AtomicU64>,
    pub(crate) checkpoint_in_flight: Arc<AtomicBool>,
    pub(crate) checkpoint_every_lsn: u64,
    pub(crate) auto_compact_wal: bool,
    pub(crate) limits: RuntimeLimits,
    pub(crate) admin_token: Option<String>,
    pub(crate) client_token: Option<String>,
    pub(crate) client_user_tokens: BTreeMap<String, String>,
    pub(crate) wal_replication_token: Option<String>,
    pub(crate) explicit_wal_remote_replica_urls: Option<Vec<String>>,
    explicit_object_remote_replica_urls: Option<Vec<String>>,
    object_replication_token: Option<String>,
    pub(crate) object_gc_grace_ms: u64,
    export_root: PathBuf,
    pub(crate) export_backup_runs: Arc<RwLock<Vec<ExportBackupRunRecord>>>,
    pub(crate) export_backup_runs_path: PathBuf,
    pub(crate) export_backup_policy: Arc<RwLock<ExportBackupPolicy>>,
    pub(crate) export_backup_policy_path: PathBuf,
    pub(crate) export_backup_controller: Arc<RwLock<ExportBackupControllerState>>,
    pub(crate) object_repair_controller: Arc<RwLock<ObjectRepairControllerState>>,
    pub(crate) chat_log: ChatLog,
    pub(crate) records: RecordStore,
    pub(crate) record_projection_applier: RecordProjectionApplier,
    pub(crate) record_hot: RecordHotCache,
    pub(crate) record_hot_durable_idle_ttl_ms: u64,
    pub(crate) record_hot_maintenance_interval_ms: u64,
    pub(crate) record_hot_prewarm_limit: usize,
    pub(crate) record_hot_prewarm: Arc<RwLock<RecordHotPrewarmStatus>>,
    pub(crate) projection_rebuild_status: Arc<RwLock<ProjectionRebuildStatus>>,
    pub(crate) hot_room_maintenance_interval_ms: u64,
    pub(crate) actor_scope_residency_maintenance_interval_ms: u64,
    pub(crate) actor_scope_residency_maintenance_limit: usize,
    pub(crate) actor_split_maintenance_interval_ms: u64,
    pub(crate) actor_split_maintenance_limit: usize,
    pub(crate) actor_reminder_maintenance_interval_ms: u64,
    pub(crate) actor_reminder_maintenance_limit: usize,
    pub(crate) realtime_maintenance_interval_ms: u64,
    pub(crate) realtime_event_batch_max: usize,
    pub(crate) objects: ObjectStore,
    pub(crate) object_refs: ObjectRefIndex,
    pub(crate) users: UserProjection,
    pub(crate) client_mutations: Arc<StdRwLock<BTreeMap<String, CommittedMutation>>>,
    pub(crate) host_http_requests: Arc<StdRwLock<BTreeMap<String, HostHttpRequestRecord>>>,
    pub(crate) actor_reminders:
        Arc<StdRwLock<BTreeMap<ActorReminderIndexKey, ActorReminderRecord>>>,
    snapshots: SnapshotStore,
    pub(crate) behaviors: BehaviorRuntime,
    pub(crate) behavior_root: PathBuf,
    pub(crate) schema: SchemaRegistry,
    pub(crate) schema_apply_lock: Arc<Mutex<()>>,
    pub(crate) schema_replay_apply_status: Arc<RwLock<SchemaReplayApplyStatus>>,
    pub(crate) schema_replay_apply_status_path: PathBuf,
    pub(crate) schema_proposals: Arc<RwLock<BTreeMap<String, SchemaProposal>>>,
    pub(crate) schema_proposals_path: PathBuf,
    pub(crate) cache_control: Arc<RwLock<ClientCacheControl>>,
    pub(crate) cache_control_path: PathBuf,
    pub(crate) connections: ConnectionRegistry,
    pub(crate) connection_controls: broadcast::Sender<ConnectionControlMessage>,
    pub(crate) connection_events: broadcast::Sender<ConnectionEvent>,
    pub(crate) realtime: RealtimeChannels,
    pub(crate) realtime_fanout: RealtimeFanoutRegistry,
    pub(crate) aggregates: AggregateRegistry,
    pub(crate) cluster: ClusterConfig,
    pub(crate) topology_overrides: Arc<RwLock<BTreeMap<usize, ClusterShardOverride>>>,
    topology_overrides_path: PathBuf,
    pub(crate) topology_log_path: PathBuf,
    topology_proposals: Arc<RwLock<BTreeMap<String, TopologyProposal>>>,
    topology_proposals_path: PathBuf,
    pub(crate) topology_lease: Arc<RwLock<TopologyLease>>,
    topology_lease_path: PathBuf,
    pub(crate) topology_lease_ms: u64,
    pub(crate) shard_controls: Arc<RwLock<BTreeMap<usize, ShardControl>>>,
    pub(crate) runtime_drain: Arc<RwLock<RuntimeDrainState>>,
    pub(crate) runtime_writes: RuntimeWriteTracker,
    pub(crate) live_query_metrics: LiveQueryMetrics,
    live_query_evaluation_cache: Arc<Mutex<LiveQueryEvaluationCache>>,
    pub(crate) runtime_write_gate: Arc<RwLock<()>>,
    pub(crate) handoff_workflows: Arc<RwLock<BTreeMap<String, HandoffWorkflow>>>,
    pub(crate) handoff_workflows_path: PathBuf,
    pub(crate) handoff_controller: Arc<RwLock<HandoffControllerState>>,
    pub(crate) failover_controller: Arc<RwLock<FailoverControllerState>>,
    pub(crate) wal_repair_controller: Arc<RwLock<WalRepairControllerState>>,
    pub(crate) peer_health: Arc<RwLock<PeerHealthMonitorState>>,
    pub(crate) startup_recovery: StartupRecoveryReport,
    pub(crate) events: broadcast::Sender<DeliveryEventBatch>,
    pub(crate) cache_invalidations: broadcast::Sender<ClientCacheInvalidationEntry>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nextdb_server=info,tower_http=info".into()),
        )
        .init();

    let dev_options = parse_dev_options_from_args(std::env::args())?;
    let data_dir =
        PathBuf::from(std::env::var("NEXTDB_DATA_DIR").unwrap_or_else(|_| "data".to_string()));
    let addr: SocketAddr = std::env::var("NEXTDB_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3188".to_string())
        .parse()
        .context("NEXTDB_ADDR must be a socket address")?;
    let record_hot_durable_idle_ttl_ms = env_u64("NEXTDB_RECORD_HOT_DURABLE_IDLE_TTL_MS", 0);
    let default_record_hot_maintenance_interval_ms = if record_hot_durable_idle_ttl_ms > 0 {
        record_hot_durable_idle_ttl_ms.clamp(50, 5_000)
    } else {
        0
    };
    let record_hot_maintenance_interval_ms = env_u64(
        "NEXTDB_RECORD_HOT_MAINTENANCE_INTERVAL_MS",
        default_record_hot_maintenance_interval_ms,
    );
    let record_hot_prewarm_limit = env_usize(
        "NEXTDB_RECORD_HOT_PREWARM_LIMIT",
        DEFAULT_RECORD_HOT_PREWARM_LIMIT,
    );
    let realtime_maintenance_interval_ms =
        env_u64("NEXTDB_REALTIME_MAINTENANCE_INTERVAL_MS", 5_000);
    let realtime_event_batch_max = env_usize(
        "NEXTDB_REALTIME_EVENT_BATCH_MAX",
        DEFAULT_REALTIME_EVENT_BATCH_MAX,
    )
    .max(1);

    let wal_shard_count = std::env::var("NEXTDB_WAL_SHARDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WAL_SHARD_COUNT)
        .clamp(1, MAX_WAL_SHARDS);
    let cluster = ClusterConfig::from_env(wal_shard_count);
    let wal_paths: Vec<PathBuf> = (0..wal_shard_count)
        .map(|shard| data_dir.join("wal").join(format!("shard-{shard:04}.jsonl")))
        .collect();
    let wal_replica_roots = if cluster::cluster_enabled() {
        parse_path_list_env("NEXTDB_WAL_REPLICA_DIRS")
    } else {
        Vec::new()
    };
    let wal_replica_paths: Vec<Vec<PathBuf>> = (0..wal_shard_count)
        .map(|shard| {
            wal_replica_roots
                .iter()
                .map(|root| root.join("wal").join(format!("shard-{shard:04}.jsonl")))
                .collect()
        })
        .collect();
    let mut wal_restore_reports = Vec::with_capacity(wal_paths.len());
    for (path, replicas) in wal_paths.iter().zip(wal_replica_paths.iter()) {
        wal_restore_reports.push(restore_missing_wal_from_replicas(path, replicas).await?);
    }
    let schema = SchemaRegistry::load(data_dir.join("schema").join("nextdb.schema.json")).await?;
    let schema_wal_recovery = recover_schema_from_wal(&schema, &wal_paths).await?;
    let (hot_window, max_hot_rooms, hot_room_idle_ttl_ms) =
        effective_actor_runtime_config(&schema.schema());
    let default_hot_room_maintenance_interval_ms = if hot_room_idle_ttl_ms > 0 {
        hot_room_idle_ttl_ms.clamp(50, 5_000)
    } else {
        0
    };
    let hot_room_maintenance_interval_ms = env_u64(
        "NEXTDB_HOT_ROOM_MAINTENANCE_INTERVAL_MS",
        default_hot_room_maintenance_interval_ms,
    );
    let actor_split_maintenance_interval_ms =
        env_u64("NEXTDB_ACTOR_SPLIT_MAINTENANCE_INTERVAL_MS", 0);
    let actor_split_maintenance_limit =
        env_usize("NEXTDB_ACTOR_SPLIT_MAINTENANCE_LIMIT", 64).max(1);
    let actor_scope_residency_maintenance_interval_ms =
        env_u64("NEXTDB_ACTOR_SCOPE_RESIDENCY_MAINTENANCE_INTERVAL_MS", 0);
    let actor_scope_residency_maintenance_limit =
        env_usize("NEXTDB_ACTOR_SCOPE_RESIDENCY_MAINTENANCE_LIMIT", 256).max(1);
    let actor_reminder_maintenance_interval_ms =
        env_u64("NEXTDB_ACTOR_REMINDER_MAINTENANCE_INTERVAL_MS", 0);
    let actor_reminder_maintenance_limit =
        env_usize("NEXTDB_ACTOR_REMINDER_MAINTENANCE_LIMIT", 64).max(1);

    let snapshots = SnapshotStore::new(data_dir.join("snapshots").join("actors.json"));
    let snapshot = snapshots.load().await?;
    let (
        since_lsn,
        replay_rooms,
        replay_actor_states,
        highest_lsn,
        wal_replay_reports,
        record_hot_snapshot,
        (snapshot_loaded, snapshot_schema_version, snapshot_room_count),
    ) = match snapshot {
        Some(snapshot) => {
            let since_lsn = snapshot.lsn;
            let snapshot_schema_version = snapshot.schema_version;
            let snapshot_room_count = snapshot.rooms.len();
            let record_hot_snapshot = snapshot.record_hot.clone();
            let startup_snapshot = (true, Some(snapshot_schema_version), snapshot_room_count);
            let snapshot_runtime_state = snapshot.into_runtime_state();
            let mut replay_rooms = snapshot_runtime_state.rooms;
            let replay_actor_states = snapshot_runtime_state.actor_states;
            let mut highest_lsn = since_lsn;
            let mut wal_replay_reports = Vec::with_capacity(wal_paths.len());
            for (shard, wal_path) in wal_paths.iter().enumerate() {
                let replay = wal::replay_from(wal_path, hot_window, since_lsn, replay_rooms)?;
                replay_rooms = replay.rooms;
                highest_lsn = highest_lsn.max(replay.highest_lsn);
                wal_replay_reports.push(WalReplayReport {
                    shard,
                    path: wal_path.clone(),
                    since_lsn,
                    highest_lsn: replay.highest_lsn,
                    scanned_records: replay.scanned_records,
                    records_after_snapshot: replay.records_after_snapshot,
                    quarantined_wal: replay.quarantined_wal,
                });
            }
            (
                since_lsn,
                replay_rooms,
                replay_actor_states,
                highest_lsn,
                wal_replay_reports,
                record_hot_snapshot,
                startup_snapshot,
            )
        }
        None => {
            let since_lsn = 0;
            let startup_snapshot = (false, None, 0);
            let mut replay_rooms = Default::default();
            let replay_actor_states = Vec::new();
            let mut highest_lsn = since_lsn;
            let mut wal_replay_reports = Vec::with_capacity(wal_paths.len());
            for (shard, wal_path) in wal_paths.iter().enumerate() {
                let replay = wal::replay_from(wal_path, hot_window, since_lsn, replay_rooms)?;
                replay_rooms = replay.rooms;
                highest_lsn = highest_lsn.max(replay.highest_lsn);
                wal_replay_reports.push(WalReplayReport {
                    shard,
                    path: wal_path.clone(),
                    since_lsn,
                    highest_lsn: replay.highest_lsn,
                    scanned_records: replay.scanned_records,
                    records_after_snapshot: replay.records_after_snapshot,
                    quarantined_wal: replay.quarantined_wal,
                });
            }
            (
                since_lsn,
                replay_rooms,
                replay_actor_states,
                highest_lsn,
                wal_replay_reports,
                None,
                startup_snapshot,
            )
        }
    };
    let replay_actor_states = if replay_actor_states.is_empty() {
        replay_actor_states
    } else {
        let wal_tail_records = wal::read_records_from_wal_paths_after_lsn(&wal_paths, since_lsn)?;
        actor_states_with_wal_tail(replay_actor_states, &wal_tail_records)
    };
    let all_wal_records = wal::read_records_from_wal_paths(&wal_paths)?;
    let (all_messages, all_records, users, client_mutations) =
        read_startup_projections_from_wal_paths(&wal_paths)?;
    let actor_reminders = actor_reminders_from_wal_records(&all_wal_records);
    let actor_reminder_index = actor_reminder_index_from_wal_records(&all_wal_records);
    let host_http_requests = host_http_request_index_from_wal_records(&all_wal_records);
    let pending_host_http_requests = pending_host_http_requests_from_wal_records(&all_wal_records);
    let chat_log = ChatLog::new(data_dir.join("chat-log"));
    chat_log.rebuild_from_messages(&all_messages).await?;
    let record_store = RecordStore::new(data_dir.join("records"));
    let active_schema = schema.schema();
    let schema_indexes = schema_indexes_by_table(&active_schema);
    let schema_orders = schema_orders_by_table(&active_schema)?;
    if !record_store.is_projection_bootstrapped() && !all_records.is_empty() {
        record_store
            .force_rebuild_from_records_with_indexes(&all_records, &schema_indexes, &schema_orders)
            .await?;
    }
    let record_hot = RecordHotCache::from_schema_snapshot_and_records(
        &active_schema,
        record_hot_snapshot.as_ref(),
        since_lsn,
        &all_records,
        record_hot_durable_idle_ttl_ms,
    );
    let record_projection_applier =
        RecordProjectionApplier::spawn(record_store.clone(), record_hot.clone());
    let object_refs = ObjectRefIndex::load_for_schema(
        data_dir.join("objects").join("refs.json"),
        &all_messages,
        &all_records,
        &active_schema,
    )
    .await?;
    let rebuilt_object_refs = object_refs.referenced_ids().await.len();
    let startup_recovery = StartupRecoveryReport {
        snapshot_loaded,
        snapshot_lsn: since_lsn,
        snapshot_schema_version,
        snapshot_room_count,
        snapshot_record_hot_table_count: record_hot_snapshot
            .as_ref()
            .map(|snapshot| snapshot.table_count())
            .unwrap_or(0),
        snapshot_record_hot_record_count: record_hot_snapshot
            .as_ref()
            .map(|snapshot| snapshot.record_count())
            .unwrap_or(0),
        schema_wal_recovery: schema_wal_recovery.clone(),
        wal_records_scanned: wal_replay_reports
            .iter()
            .map(|report| report.scanned_records)
            .sum(),
        wal_records_after_snapshot: wal_replay_reports
            .iter()
            .map(|report| report.records_after_snapshot)
            .sum(),
        highest_lsn,
        rebuilt_messages: all_messages.len(),
        rebuilt_records: all_records.len(),
        rebuilt_object_refs,
        wal_restores: wal_restore_reports,
        wal_replay: wal_replay_reports,
    };
    let behavior_root = data_dir.join("behaviors");
    let active_schema_for_behaviors = active_schema.clone();
    let behaviors = BehaviorRuntime::load_checked(behavior_root.clone(), |manifest| {
        validate_behavior_manifest_schema(&active_schema_for_behaviors, manifest)
    })
    .await?;
    let export_root = data_dir.join("exports");
    let export_backup_runs_path = export_root.join("backup-runs.json");
    let export_backup_runs = load_export_backup_runs(&export_backup_runs_path).await?;
    let export_backup_policy_path = export_root.join("backup-policy.json");
    let export_backup_policy = load_export_backup_policy(&export_backup_policy_path).await?;
    let cache_control_path = data_dir.join("cache").join("control.json");
    let cache_control = load_cache_control(&cache_control_path).await?;
    let topology_overrides_path = data_dir.join("cluster").join("topology-overrides.json");
    let topology_log_path = data_dir.join("cluster").join("topology-log.jsonl");
    let topology_overrides =
        load_topology_overrides(&topology_overrides_path, &topology_log_path).await?;
    let topology_proposals_path = data_dir.join("cluster").join("topology-proposals.json");
    let topology_proposals = load_topology_proposals(&topology_proposals_path).await?;
    let schema_proposals_path = data_dir.join("schema").join("schema-proposals.json");
    let schema_proposals = load_schema_proposals(&schema_proposals_path).await?;
    let schema_replay_apply_status_path = data_dir.join("schema").join("schema-replay-status.json");
    let startup_projection_status = record_store.projection_status().await?;
    let loaded_schema_replay_apply_status = load_schema_replay_apply_status(
        &schema_replay_apply_status_path,
        &schema_wal_recovery,
        startup_projection_status,
    )
    .await?;
    let topology_lease_path = data_dir.join("cluster").join("topology-lease.json");
    let topology_lease = load_topology_lease(&topology_lease_path).await?;
    let handoff_workflows_path = data_dir.join("cluster").join("handoff-workflows.json");
    let handoff_workflows = load_handoff_workflows(&handoff_workflows_path).await?;
    let topology_lease_ms = std::env::var("NEXTDB_TOPOLOGY_LEASE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_TOPOLOGY_LEASE_MS);
    let handoff_controller_interval_ms = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_HANDOFF_CONTROLLER_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    let failover_controller_interval_ms = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_FAILOVER_CONTROLLER_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    let wal_repair_controller_interval_ms = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_WAL_REPAIR_CONTROLLER_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    let object_repair_controller_interval_ms = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_OBJECT_REPAIR_CONTROLLER_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    let peer_monitor_interval_ms = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_PEER_MONITOR_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    let checkpoint_every_lsn = std::env::var("NEXTDB_CHECKPOINT_EVERY_LSN")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CHECKPOINT_EVERY_LSN);
    let auto_compact_wal =
        parse_bool_env("NEXTDB_AUTO_COMPACT_WAL").unwrap_or(DEFAULT_AUTO_COMPACT_WAL);
    let wal_writer_config = WalWriterConfig::new(
        env_usize("NEXTDB_WAL_BATCH_MAX", DEFAULT_WAL_BATCH_MAX),
        env_u64("NEXTDB_WAL_BATCH_WAIT_MS", DEFAULT_WAL_BATCH_WAIT_MS),
    );
    let object_gc_grace_ms = std::env::var("NEXTDB_OBJECT_GC_GRACE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_OBJECT_GC_GRACE_MS);
    let limits = RuntimeLimits::from_env();
    let admin_token = read_secret_env("NEXTDB_ADMIN_TOKEN");
    let client_token = read_secret_env("NEXTDB_CLIENT_TOKEN");
    let client_user_tokens = parse_user_token_env("NEXTDB_CLIENT_USER_TOKENS");
    let explicit_wal_remote_replica_urls = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_WAL_REMOTE_REPLICAS")
            .ok()
            .map(|value| parse_url_list_value(&value))
    } else {
        None
    };
    let wal_remote_ack_policy = if cluster::cluster_enabled() {
        parse_wal_remote_ack_policy()
    } else {
        wal::WalRemoteAckPolicy::None
    };
    let wal_replication_token = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_WAL_REPLICATION_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
    } else {
        None
    };
    let explicit_object_remote_replica_urls = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_OBJECT_REMOTE_REPLICAS")
            .ok()
            .map(|value| parse_url_list_value(&value))
            .or_else(|| explicit_wal_remote_replica_urls.clone())
    } else {
        None
    };
    let object_replication_token = if cluster::cluster_enabled() {
        std::env::var("NEXTDB_OBJECT_REPLICATION_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| wal_replication_token.clone())
    } else {
        None
    };

    let wal_shards: Vec<WalShard> = wal_paths
        .iter()
        .zip(wal_replica_paths.iter())
        .enumerate()
        .map(|(shard, (path, replicas))| {
            let remote_replica_urls =
                explicit_wal_remote_replica_urls.clone().unwrap_or_else(|| {
                    cluster
                        .replicas_for_shard_with_overrides(shard, &topology_overrides)
                        .into_iter()
                        .filter(|node_id| node_id != cluster.node_id())
                        .filter_map(|node_id| cluster.node_url_for(&node_id))
                        .collect()
                });
            WalShard {
                index: shard,
                path: path.clone(),
                replica_paths: replicas.clone(),
                remote_ack_policy: wal_remote_ack_policy,
                append_send_lock: Arc::new(Mutex::new(())),
                writer: WalWriter::spawn(
                    path.clone(),
                    shard,
                    replicas
                        .iter()
                        .cloned()
                        .map(|path| WalReplica { path })
                        .collect(),
                    remote_replica_urls
                        .iter()
                        .cloned()
                        .map(|url| WalRemoteReplica {
                            url,
                            token: wal_replication_token.clone(),
                        })
                        .collect(),
                    wal_remote_ack_policy,
                    wal_writer_config,
                ),
            }
        })
        .collect();

    let state = AppState {
        runtime_id: Uuid::now_v7().to_string(),
        actors: ActorRuntime::new_with_actor_states_and_reminders(
            replay_rooms,
            replay_actor_states,
            actor_reminders,
            hot_window,
            max_hot_rooms,
            hot_room_idle_ttl_ms,
        ),
        wal_shards,
        wal_paths,
        current_lsn: Arc::new(AtomicU64::new(highest_lsn)),
        next_lsn: Arc::new(AtomicU64::new(highest_lsn)),
        last_snapshot_lsn: Arc::new(AtomicU64::new(since_lsn)),
        last_compaction_lsn: Arc::new(AtomicU64::new(0)),
        checkpoint_in_flight: Arc::new(AtomicBool::new(false)),
        checkpoint_every_lsn,
        auto_compact_wal,
        limits,
        admin_token,
        client_token,
        client_user_tokens,
        wal_replication_token,
        explicit_wal_remote_replica_urls,
        explicit_object_remote_replica_urls,
        object_replication_token,
        object_gc_grace_ms,
        export_root,
        export_backup_runs: Arc::new(RwLock::new(export_backup_runs)),
        export_backup_runs_path,
        export_backup_policy: Arc::new(RwLock::new(export_backup_policy.clone())),
        export_backup_policy_path,
        export_backup_controller: Arc::new(RwLock::new(ExportBackupControllerState {
            enabled: export_backup_policy.enabled,
            interval_ms: export_backup_policy.interval_ms,
            last_run_at_ms: None,
            last_run_id: None,
            last_error: None,
        })),
        object_repair_controller: Arc::new(RwLock::new(ObjectRepairControllerState {
            enabled: object_repair_controller_interval_ms > 0,
            interval_ms: object_repair_controller_interval_ms,
            last_run_at_ms: None,
            last_shards: Vec::new(),
            last_objects_sent: 0,
            last_repaired_replicas: 0,
            last_satisfied: true,
            last_error: None,
        })),
        chat_log,
        records: record_store,
        record_projection_applier,
        record_hot,
        record_hot_durable_idle_ttl_ms,
        record_hot_maintenance_interval_ms,
        record_hot_prewarm_limit,
        record_hot_prewarm: Arc::new(RwLock::new(RecordHotPrewarmStatus {
            enabled: record_hot_prewarm_limit > 0,
            limit_per_table: record_hot_prewarm_limit,
            ..RecordHotPrewarmStatus::default()
        })),
        projection_rebuild_status: Arc::new(RwLock::new(ProjectionRebuildStatus::default())),
        hot_room_maintenance_interval_ms,
        actor_scope_residency_maintenance_interval_ms,
        actor_scope_residency_maintenance_limit,
        actor_split_maintenance_interval_ms,
        actor_split_maintenance_limit,
        actor_reminder_maintenance_interval_ms,
        actor_reminder_maintenance_limit,
        realtime_maintenance_interval_ms,
        realtime_event_batch_max,
        objects: ObjectStore::new(data_dir.join("objects")),
        object_refs,
        users,
        client_mutations: Arc::new(StdRwLock::new(client_mutations)),
        host_http_requests: Arc::new(StdRwLock::new(host_http_requests)),
        actor_reminders: Arc::new(StdRwLock::new(actor_reminder_index)),
        snapshots,
        behaviors,
        behavior_root,
        schema,
        schema_apply_lock: Arc::new(Mutex::new(())),
        schema_replay_apply_status: Arc::new(RwLock::new(loaded_schema_replay_apply_status)),
        schema_replay_apply_status_path,
        schema_proposals: Arc::new(RwLock::new(schema_proposals)),
        schema_proposals_path,
        cache_control: Arc::new(RwLock::new(cache_control)),
        cache_control_path,
        connections: ConnectionRegistry::default(),
        connection_controls: broadcast::channel(1024).0,
        connection_events: broadcast::channel(4096).0,
        realtime: RealtimeChannels::default(),
        realtime_fanout: RealtimeFanoutRegistry::default(),
        aggregates: AggregateRegistry::default(),
        cluster,
        topology_overrides: Arc::new(RwLock::new(topology_overrides)),
        topology_overrides_path,
        topology_log_path,
        topology_proposals: Arc::new(RwLock::new(topology_proposals)),
        topology_proposals_path,
        topology_lease: Arc::new(RwLock::new(topology_lease)),
        topology_lease_path,
        topology_lease_ms,
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
        handoff_workflows: Arc::new(RwLock::new(handoff_workflows)),
        handoff_workflows_path,
        handoff_controller: Arc::new(RwLock::new(HandoffControllerState {
            enabled: handoff_controller_interval_ms > 0,
            interval_ms: handoff_controller_interval_ms,
            last_run_at_ms: None,
            last_workflow_id: None,
            last_applied_workflow_id: None,
            last_error: None,
        })),
        failover_controller: Arc::new(RwLock::new(FailoverControllerState {
            enabled: failover_controller_interval_ms > 0,
            interval_ms: failover_controller_interval_ms,
            last_run_at_ms: None,
            last_shard: None,
            last_proposal_id: None,
            last_committed_proposal_id: None,
            last_error: None,
        })),
        wal_repair_controller: Arc::new(RwLock::new(WalRepairControllerState {
            enabled: wal_repair_controller_interval_ms > 0,
            interval_ms: wal_repair_controller_interval_ms,
            last_run_at_ms: None,
            last_shards: Vec::new(),
            last_records_sent: 0,
            last_repaired_replicas: 0,
            last_satisfied: true,
            last_error: None,
        })),
        peer_health: Arc::new(RwLock::new(PeerHealthMonitorState {
            enabled: peer_monitor_interval_ms > 0,
            interval_ms: peer_monitor_interval_ms,
            last_run_at_ms: None,
            peers: BTreeMap::new(),
        })),
        startup_recovery,
        events: broadcast::channel(65_536).0,
        cache_invalidations: broadcast::channel(4_096).0,
    };

    let replayed_host_http_requests =
        replay_pending_host_http_requests(&state, pending_host_http_requests);
    if replayed_host_http_requests > 0 {
        info!(
            replayed_host_http_requests,
            "replayed pending host HTTP requests"
        );
    }

    if handoff_controller_interval_ms > 0 {
        spawn_periodic_controller(
            state.clone(),
            handoff_controller_interval_ms,
            "handoff",
            |state| async move { run_handoff_controller_once(&state).await },
        );
    }
    if failover_controller_interval_ms > 0 {
        spawn_periodic_controller(
            state.clone(),
            failover_controller_interval_ms,
            "failover",
            |state| async move { run_failover_controller_once(&state).await },
        );
    }
    if wal_repair_controller_interval_ms > 0 {
        spawn_periodic_controller(
            state.clone(),
            wal_repair_controller_interval_ms,
            "wal_repair",
            |state| async move { run_wal_repair_controller_once(&state).await },
        );
    }
    if object_repair_controller_interval_ms > 0 {
        spawn_periodic_controller(
            state.clone(),
            object_repair_controller_interval_ms,
            "object_repair",
            |state| async move { run_object_repair_controller_once(&state).await },
        );
    }
    if peer_monitor_interval_ms > 0 {
        spawn_periodic_task(
            state.clone(),
            peer_monitor_interval_ms,
            "peer_health",
            |state| async move {
                run_peer_health_monitor_once(&state).await;
            },
        );
    }
    if hot_room_idle_ttl_ms > 0 && hot_room_maintenance_interval_ms > 0 {
        spawn_hot_room_maintenance(state.clone(), hot_room_maintenance_interval_ms);
    }
    if actor_scope_residency_maintenance_interval_ms > 0 {
        spawn_actor_scope_residency_maintenance(
            state.clone(),
            actor_scope_residency_maintenance_interval_ms,
            actor_scope_residency_maintenance_limit,
        );
    }
    if actor_split_maintenance_interval_ms > 0 {
        spawn_actor_split_maintenance(
            state.clone(),
            actor_split_maintenance_interval_ms,
            actor_split_maintenance_limit,
        );
    }
    if actor_reminder_maintenance_interval_ms > 0 {
        spawn_periodic_task(
            state.clone(),
            actor_reminder_maintenance_interval_ms,
            "actor_reminders",
            move |state| async move {
                if let Err(error) = run_due_actor_reminders_once(
                    &state,
                    Some(actor_reminder_maintenance_limit),
                    None,
                )
                .await
                {
                    tracing::warn!(?error, "actor reminder maintenance tick failed");
                }
            },
        );
    }
    if record_hot_durable_idle_ttl_ms > 0 && record_hot_maintenance_interval_ms > 0 {
        spawn_record_hot_maintenance(state.clone(), record_hot_maintenance_interval_ms);
    }
    if record_hot_prewarm_limit > 0 {
        spawn_record_hot_prewarm(state.clone());
    }
    if realtime_maintenance_interval_ms > 0 {
        spawn_realtime_maintenance(
            state.clone(),
            realtime_maintenance_interval_ms,
            |state, leave| async move {
                state.aggregates.publish_presence_update(
                    &leave.channel_id,
                    &leave.remaining,
                    state.current_lsn.load(std::sync::atomic::Ordering::Acquire),
                    now_ms(),
                );
                publish_realtime_member_left(&state, &leave).await;
            },
        );
    }
    spawn_export_backup_controller(state.clone());
    if dev_options.watch_behaviors {
        let interval_ms = env_u64(
            "NEXTDB_BEHAVIOR_WATCH_INTERVAL_MS",
            DEFAULT_BEHAVIOR_WATCH_INTERVAL_MS,
        )
        .max(MIN_BEHAVIOR_WATCH_INTERVAL_MS);
        spawn_behavior_hot_reload_watcher(state.clone(), interval_ms);
    }

    let app = Router::new()
        .route("/v1/ready", get(readiness))
        .route("/v1/health", get(health))
        .route("/v1/metrics", get(metrics))
        .route("/v1/mutate", post(mutate))
        .route("/v1/audit/wal", get(audit_wal))
        .route("/v1/audit/trace", get(audit_trace))
        .route("/v1/audit/replay", get(audit_replay))
        .route("/v1/admin/export/manifest", get(export_manifest))
        .route("/v1/admin/export/bundle", post(create_export_bundle))
        .route("/v1/admin/export/backup/run", post(run_export_backup))
        .route("/v1/admin/export/backup/runs", get(list_export_backup_runs))
        .route(
            "/v1/admin/export/backup/policy",
            get(get_export_backup_policy).post(set_export_backup_policy),
        )
        .route(
            "/v1/admin/export/backup/policy/run",
            post(run_export_backup_policy),
        )
        .route(
            "/v1/admin/export/backup/retention",
            post(retain_export_backups),
        )
        .route("/v1/admin/export/bundles", get(list_export_bundles))
        .route(
            "/v1/admin/export/bundles/verify-chain",
            post(verify_export_bundle_chain),
        )
        .route(
            "/v1/admin/export/bundles/{bundle_id}/verify",
            post(verify_export_bundle),
        )
        .route(
            "/v1/admin/export/bundles/{bundle_id}/archive-object",
            post(archive_export_bundle_to_object),
        )
        .route(
            "/v1/admin/import/bundles/from-object/{object_id}",
            post(import_bundle_from_object),
        )
        .route(
            "/v1/admin/import/bundles/restore-chain",
            post(restore_import_bundle_chain),
        )
        .route(
            "/v1/admin/import/bundles/{bundle_id}/preflight",
            post(import_bundle_preflight),
        )
        .route(
            "/v1/admin/import/bundles/{bundle_id}/preflight-delta",
            post(import_bundle_delta_preflight),
        )
        .route(
            "/v1/admin/import/bundles/{bundle_id}/restore",
            post(restore_import_bundle),
        )
        .route(
            "/v1/admin/import/bundles/{bundle_id}/apply-delta",
            post(apply_import_bundle_delta),
        )
        .route("/v1/sync/pull", get(sync_pull))
        .route("/v1/sync/wait", get(sync_wait))
        .route("/v1/cache/profile", get(get_cache_profile))
        .route("/v1/admin/cache/profile", post(update_cache_profile))
        .route("/v1/admin/cache/invalidate", post(invalidate_cache))
        .route("/v1/admin/connections", get(list_connections))
        .route(
            "/v1/admin/connections/disconnect",
            post(disconnect_connections),
        )
        .merge(cluster_routes())
        .route("/v1/records/transaction", post(record_transaction))
        .route("/v1/records/batch", post(record_batch))
        .route("/v1/records/{table}", get(list_records))
        .route(
            "/v1/records/{table}/indexes/{index_name}",
            get(query_records_by_index),
        )
        .route(
            "/v1/records/{table}/{parent_key}/{nested}",
            get(list_nested_records),
        )
        .route(
            "/v1/records/{table}/{parent_key}/{nested}/indexes/{index_name}",
            get(query_nested_records_by_index),
        )
        .route(
            "/v1/records/{table}/{parent_key}/{nested}/{nested_key}",
            get(get_nested_record)
                .post(upsert_nested_record)
                .delete(delete_nested_record),
        )
        .route(
            "/v1/records/{table}/{key}",
            get(get_record).post(upsert_record).delete(delete_record),
        )
        .route("/v1/realtime/channels", get(realtime_channel_list))
        .route(
            "/v1/realtime/channels/{channel_id}/members",
            get(realtime_members),
        )
        .route(
            "/v1/realtime/channels/{channel_id}/state",
            get(realtime_channel_state).post(update_realtime_channel_state),
        )
        .route(
            "/v1/realtime/channels/{channel_id}/join",
            post(realtime_join),
        )
        .route(
            "/v1/realtime/channels/{channel_id}/leave",
            post(realtime_leave),
        )
        .route(
            "/v1/realtime/channels/{channel_id}/presence",
            post(update_realtime_presence),
        )
        .route(
            "/v1/realtime/channels/{channel_id}/signal",
            post(realtime_signal),
        )
        .route(
            "/v1/realtime/channels/{channel_id}/broadcast",
            post(realtime_broadcast),
        )
        .route("/v1/users/{user_id}/events", get(list_user_events))
        .route("/v1/users/{user_id}", get(get_user).post(upsert_user))
        .route("/v1/admin/users", get(list_users))
        .route("/v1/rooms/{room_id}/messages/latest", get(latest_messages))
        .route("/v1/objects", get(list_objects).post(put_object))
        .route(
            "/v1/objects/{object_id}",
            axum::routing::delete(delete_object),
        )
        .route("/v1/objects/{object_id}/metadata", get(get_object_metadata))
        .route("/v1/objects/{object_id}/body", get(get_object_body))
        .route(
            "/v1/objects/{object_id}/references",
            get(get_object_references),
        )
        .merge(cluster_replication_routes())
        .route("/v1/admin/objects/gc", post(gc_objects))
        .route(
            "/v1/admin/runtime/drain",
            get(get_runtime_drain).post(set_runtime_drain),
        )
        .route(
            "/v1/admin/runtime/prepare-restart",
            post(prepare_runtime_restart),
        )
        .route(
            "/v1/admin/runtime/activation",
            get(runtime_activation_status),
        )
        .route(
            "/v1/admin/runtime/activate-records",
            post(activate_runtime_records),
        )
        .route(
            "/v1/admin/runtime/activate-actor",
            post(activate_runtime_actor),
        )
        .route("/v1/admin/runtime/reminders", post(schedule_actor_reminder))
        .route(
            "/v1/admin/runtime/reminders/cancel",
            post(cancel_actor_reminder),
        )
        .route(
            "/v1/admin/runtime/reminders/run-due",
            post(run_due_actor_reminders),
        )
        .route(
            "/v1/admin/runtime/evict-records",
            post(evict_runtime_records),
        )
        .route(
            "/v1/admin/runtime/activate-room",
            post(activate_runtime_room),
        )
        .route("/v1/admin/runtime/evict-room", post(evict_runtime_room))
        .route("/v1/admin/snapshot", post(create_snapshot))
        .route("/v1/admin/wal/integrity", get(wal_integrity))
        .route("/v1/admin/wal/seal-checksums", post(seal_wal_checksums))
        .route("/v1/admin/wal/compact", post(compact_wal))
        .route("/v1/admin/wal/archive/retention", post(retain_wal_archives))
        .route("/v1/admin/projections/rebuild", post(rebuild_projections))
        .route(
            "/v1/admin/projections/rebuild/status",
            get(projection_rebuild_status),
        )
        .route("/v1/admin/projections/status", get(projection_status))
        .route("/v1/behaviors", get(list_behaviors))
        .route("/v1/admin/behaviors/reload", post(reload_behaviors))
        .route("/v1/behaviors/invoke", post(invoke_behavior))
        .route("/v1/schema", get(get_schema))
        .route("/v1/schema/history", get(get_schema_history))
        .route("/v1/schema/history/{version}", get(get_schema_version))
        .route("/v1/schema/typescript", get(get_schema_typescript))
        .route("/v1/schema/validate", get(validate_schema))
        .route("/v1/schema/migration-plan", get(schema_migration_plan))
        .route("/v1/schema/storage-policy", get(schema_storage_policy))
        .route("/v1/admin/schema/reload", post(reload_schema))
        .route("/v1/admin/schema/preflight", post(preflight_schema))
        .route(
            "/v1/admin/schema/replay/status",
            get(schema_replay_apply_status),
        )
        .route(
            "/v1/admin/schema/replay/retry",
            post(retry_schema_replay_apply),
        )
        .route(
            "/v1/admin/schema/replay/resume",
            post(resume_schema_replay_apply),
        )
        .route(
            "/v1/admin/schema/replay/cancel",
            post(cancel_schema_replay_apply),
        )
        .route(
            "/v1/admin/schema/proposals",
            get(list_schema_proposals).post(start_schema_proposal),
        )
        .route(
            "/v1/admin/schema/proposals/{proposal_id}/commit",
            post(commit_schema_proposal),
        )
        .route(
            "/v1/admin/schema/proposals/{proposal_id}/abort",
            post(abort_schema_proposal),
        )
        .route(
            "/v1/admin/schema/proposals/prepare",
            post(prepare_schema_proposal_peer),
        )
        .route(
            "/v1/admin/schema/proposals/commit",
            post(commit_schema_proposal_peer),
        )
        .route(
            "/v1/admin/schema/proposals/abort",
            post(abort_schema_proposal_peer),
        )
        .route("/v1/admin/schema/apply", post(apply_schema))
        .route("/v1/connect", get(connect_ws))
        .route("/v1/connect/jsonl", post(connect_jsonl))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "nextdb server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state))
        .await?;
    Ok(())
}

fn parse_dev_options_from_args<I, S>(args: I) -> Result<DevOptions>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    let _binary = args.next();
    let mut options = DevOptions::server();
    let rest: Vec<String> = args.map(|arg| arg.as_ref().to_string()).collect();
    if rest.is_empty() {
        return Ok(options);
    }

    let flags = if rest[0] == "dev" {
        &rest[1..]
    } else if rest[0] == "--watch" {
        &rest[..]
    } else {
        bail!("usage: nextdb [dev [--watch]]");
    };

    for flag in flags {
        match flag.as_str() {
            "--watch" => options.watch_behaviors = true,
            _ => bail!("usage: nextdb [dev [--watch]]"),
        }
    }
    Ok(options)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BehaviorFileSignature {
    len: u64,
    modified_ms: u128,
}

fn spawn_behavior_hot_reload_watcher(state: AppState, interval_ms: u64) {
    tokio::spawn(async move {
        let mut previous = match behavior_tree_signature(&state.behavior_root).await {
            Ok(signature) => signature,
            Err(error) => {
                warn!(?error, "failed to scan behavior tree for hot reload");
                BTreeMap::new()
            }
        };
        info!(
            path = %state.behavior_root.display(),
            interval_ms,
            "behavior hot reload watcher started"
        );

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
            let current = match behavior_tree_signature(&state.behavior_root).await {
                Ok(signature) => signature,
                Err(error) => {
                    warn!(?error, "failed to scan behavior tree for hot reload");
                    continue;
                }
            };
            if current == previous {
                continue;
            }
            previous = current;
            match reload_behaviors_internal(&state).await {
                Ok(response) => info!(
                    loaded = response.loaded,
                    epoch = response.epoch,
                    published_lsn = response.published_lsn,
                    "behavior hot reload committed"
                ),
                Err(error) => warn!(?error, "behavior hot reload failed"),
            }
        }
    });
}

async fn behavior_tree_signature(root: &Path) -> Result<BTreeMap<String, BehaviorFileSignature>> {
    let mut signature = BTreeMap::new();
    if !root.exists() {
        return Ok(signature);
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .unwrap_or(path.as_path())
                .to_string_lossy()
                .replace('\\', "/");
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis())
                .unwrap_or(0);
            signature.insert(
                relative,
                BehaviorFileSignature {
                    len: metadata.len(),
                    modified_ms,
                },
            );
        }
    }
    Ok(signature)
}

#[cfg(feature = "cluster")]
fn cluster_routes() -> Router<AppState> {
    use crate::api::topology::{
        abort_handoff_workflow, abort_topology_proposal, abort_topology_proposal_peer,
        apply_handoff_workflow, apply_topology_override, auto_handoff_workflow,
        cleanup_topology_lease, cluster_route, cluster_topology, commit_topology_proposal,
        commit_topology_proposal_peer, failover_plan, freeze_shard, get_topology_log,
        get_topology_overrides, handoff_plan, list_handoff_workflows, list_topology_proposals,
        prepare_topology_proposal_peer, retry_topology_proposal, start_failover_proposal,
        start_handoff_workflow, start_topology_proposal, step_handoff_workflow, unfreeze_shard,
    };

    Router::new()
        .route("/v1/cluster/topology", get(cluster_topology))
        .route("/v1/cluster/route", get(cluster_route))
        .route(
            "/v1/admin/cluster/topology/overrides",
            get(get_topology_overrides).post(apply_topology_override),
        )
        .route("/v1/admin/cluster/topology/log", get(get_topology_log))
        .route(
            "/v1/admin/cluster/topology/proposals",
            get(list_topology_proposals).post(start_topology_proposal),
        )
        .route(
            "/v1/admin/cluster/topology/proposals/{proposal_id}/commit",
            post(commit_topology_proposal),
        )
        .route(
            "/v1/admin/cluster/topology/proposals/{proposal_id}/retry",
            post(retry_topology_proposal),
        )
        .route(
            "/v1/admin/cluster/topology/proposals/{proposal_id}/abort",
            post(abort_topology_proposal),
        )
        .route(
            "/v1/admin/cluster/topology/proposals/prepare",
            post(prepare_topology_proposal_peer),
        )
        .route(
            "/v1/admin/cluster/topology/proposals/commit",
            post(commit_topology_proposal_peer),
        )
        .route(
            "/v1/admin/cluster/topology/proposals/abort",
            post(abort_topology_proposal_peer),
        )
        .route(
            "/v1/admin/cluster/topology/lease/cleanup",
            post(cleanup_topology_lease),
        )
        .route(
            "/v1/admin/cluster/shards/{shard}/freeze",
            post(freeze_shard),
        )
        .route(
            "/v1/admin/cluster/shards/{shard}/unfreeze",
            post(unfreeze_shard),
        )
        .route("/v1/admin/cluster/handoff/plan", post(handoff_plan))
        .route("/v1/admin/cluster/failover/plan", post(failover_plan))
        .route(
            "/v1/admin/cluster/failover/proposals",
            post(start_failover_proposal),
        )
        .route(
            "/v1/admin/cluster/handoff/workflows",
            get(list_handoff_workflows).post(start_handoff_workflow),
        )
        .route(
            "/v1/admin/cluster/handoff/workflows/{workflow_id}/step",
            post(step_handoff_workflow),
        )
        .route(
            "/v1/admin/cluster/handoff/workflows/{workflow_id}/auto",
            post(auto_handoff_workflow),
        )
        .route(
            "/v1/admin/cluster/handoff/workflows/{workflow_id}/abort",
            post(abort_handoff_workflow),
        )
        .route(
            "/v1/admin/cluster/handoff/workflows/{workflow_id}/apply",
            post(apply_handoff_workflow),
        )
}

#[cfg(not(feature = "cluster"))]
fn cluster_routes() -> Router<AppState> {
    Router::new()
}

#[cfg(feature = "cluster")]
fn cluster_replication_routes() -> Router<AppState> {
    use crate::api::{
        objects::{repair_objects, replicate_object},
        wal::{repair_wal_remotes, replicate_wal},
    };

    Router::new()
        .route("/v1/admin/objects/replicate", post(replicate_object))
        .route("/v1/admin/objects/repair", post(repair_objects))
        .route("/v1/admin/wal/replicate/repair", post(repair_wal_remotes))
        .route("/v1/admin/wal/replicate", post(replicate_wal))
}

#[cfg(not(feature = "cluster"))]
fn cluster_replication_routes() -> Router<AppState> {
    Router::new()
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn parse_dev_options_defaults_to_server_mode() {
        assert_eq!(
            parse_dev_options_from_args(["nextdb"]).expect("parse args"),
            DevOptions {
                watch_behaviors: false,
            }
        );
    }

    #[test]
    fn parse_dev_options_accepts_dev_watch() {
        assert_eq!(
            parse_dev_options_from_args(["nextdb", "dev", "--watch"]).expect("parse args"),
            DevOptions {
                watch_behaviors: true,
            }
        );
    }

    #[test]
    fn parse_dev_options_accepts_watch_alias() {
        assert_eq!(
            parse_dev_options_from_args(["nextdb", "--watch"]).expect("parse args"),
            DevOptions {
                watch_behaviors: true,
            }
        );
    }

    #[test]
    fn parse_dev_options_rejects_unknown_args() {
        assert!(parse_dev_options_from_args(["nextdb", "serve"]).is_err());
        assert!(parse_dev_options_from_args(["nextdb", "dev", "--unknown"]).is_err());
    }
}

#[cfg(test)]
mod behavior_watcher_tests {
    use super::*;

    #[tokio::test]
    async fn behavior_tree_signature_tracks_recursive_files_and_content_changes() {
        let root = temp_behavior_watch_root("signature-change");
        let nested = root.join("echo-ts");
        tokio::fs::create_dir_all(&nested)
            .await
            .expect("create behavior dir");
        let manifest = nested.join("nextdb.behavior.json");
        let wasm = nested.join("echo-ts.wasm");
        tokio::fs::write(&manifest, br#"{"name":"echo-ts"}"#)
            .await
            .expect("write manifest");
        tokio::fs::write(&wasm, b"wasm-v1")
            .await
            .expect("write wasm");

        let initial = behavior_tree_signature(&root)
            .await
            .expect("initial signature");
        assert!(initial.contains_key("echo-ts/nextdb.behavior.json"));
        assert_eq!(initial["echo-ts/echo-ts.wasm"].len, 7);

        tokio::fs::write(&wasm, b"wasm-v1-with-more-bytes")
            .await
            .expect("rewrite wasm");
        let changed = behavior_tree_signature(&root)
            .await
            .expect("changed signature");
        assert_ne!(initial, changed);
        assert_eq!(changed["echo-ts/echo-ts.wasm"].len, 23);

        let _ = tokio::fs::remove_dir_all(&root).await;
    }

    #[tokio::test]
    async fn behavior_tree_signature_treats_missing_root_as_empty() {
        let root = temp_behavior_watch_root("missing-root");
        let _ = tokio::fs::remove_dir_all(&root).await;

        let signature = behavior_tree_signature(&root)
            .await
            .expect("missing root signature");
        assert!(signature.is_empty());
    }

    fn temp_behavior_watch_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("nextdb-{label}-{}-{nonce}", std::process::id()))
    }
}

#[cfg(test)]
mod sync_tests;
