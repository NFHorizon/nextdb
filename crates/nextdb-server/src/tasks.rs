use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{AppState, realtime::RealtimeLeave, util::now_ms};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeDrainState {
    pub(crate) draining: bool,
    pub(crate) reason: Option<String>,
    pub(crate) updated_at_ms: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct RuntimeWriteTracker {
    in_flight: Arc<AtomicU64>,
    last_started_at_ms: Arc<AtomicU64>,
    last_finished_at_ms: Arc<AtomicU64>,
}

impl RuntimeWriteTracker {
    pub(crate) fn new() -> Self {
        Self {
            in_flight: Arc::new(AtomicU64::new(0)),
            last_started_at_ms: Arc::new(AtomicU64::new(0)),
            last_finished_at_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn begin(&self) {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        self.last_started_at_ms.store(now_ms(), Ordering::Release);
    }

    fn finish(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.last_finished_at_ms.store(now_ms(), Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> RuntimeWriteState {
        RuntimeWriteState {
            in_flight: self.in_flight.load(Ordering::Acquire),
            last_started_at_ms: nonzero_atomic(self.last_started_at_ms.load(Ordering::Acquire)),
            last_finished_at_ms: nonzero_atomic(self.last_finished_at_ms.load(Ordering::Acquire)),
        }
    }
}

pub(crate) struct RuntimeWriteGuard {
    pub(crate) tracker: RuntimeWriteTracker,
    pub(crate) _gate: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl Drop for RuntimeWriteGuard {
    fn drop(&mut self) {
        self.tracker.finish();
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeWriteState {
    pub(crate) in_flight: u64,
    pub(crate) last_started_at_ms: Option<u64>,
    pub(crate) last_finished_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffControllerState {
    pub(crate) enabled: bool,
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at_ms: Option<u64>,
    pub(crate) last_workflow_id: Option<String>,
    pub(crate) last_applied_workflow_id: Option<String>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailoverControllerState {
    pub(crate) enabled: bool,
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at_ms: Option<u64>,
    pub(crate) last_shard: Option<usize>,
    pub(crate) last_proposal_id: Option<String>,
    pub(crate) last_committed_proposal_id: Option<String>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalRepairControllerState {
    pub(crate) enabled: bool,
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at_ms: Option<u64>,
    pub(crate) last_shards: Vec<usize>,
    pub(crate) last_records_sent: usize,
    pub(crate) last_repaired_replicas: usize,
    pub(crate) last_satisfied: bool,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectRepairControllerState {
    pub(crate) enabled: bool,
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at_ms: Option<u64>,
    pub(crate) last_shards: Vec<usize>,
    pub(crate) last_objects_sent: usize,
    pub(crate) last_repaired_replicas: usize,
    pub(crate) last_satisfied: bool,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeerHealthMonitorState {
    pub(crate) enabled: bool,
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at_ms: Option<u64>,
    pub(crate) peers: BTreeMap<String, PeerHealthStatus>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeerHealthStatus {
    pub(crate) node_id: String,
    pub(crate) url: String,
    pub(crate) ok: bool,
    pub(crate) status: Option<u16>,
    pub(crate) accepting_writes: Option<bool>,
    pub(crate) current_lsn: Option<u64>,
    pub(crate) last_seen_ok_lsn: Option<u64>,
    pub(crate) latency_ms: Option<u64>,
    pub(crate) last_checked_at_ms: u64,
    pub(crate) last_seen_ok_at_ms: Option<u64>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShardControl {
    pub(crate) shard: usize,
    pub(crate) frozen: bool,
    pub(crate) reason: Option<String>,
    pub(crate) frozen_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffWorkflow {
    pub(crate) id: String,
    pub(crate) shard: usize,
    pub(crate) current_owner: String,
    pub(crate) target_owner: String,
    pub(crate) current_epoch: u64,
    pub(crate) next_epoch: u64,
    pub(crate) phase: HandoffWorkflowPhase,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) current_shard_lsn: u64,
    pub(crate) target_acked_lsn: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) required_env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum HandoffWorkflowPhase {
    WaitingForCatchUp,
    ReadyToReconfigure,
    Applied,
    Aborted,
}

fn nonzero_atomic(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

pub(crate) fn spawn_hot_room_maintenance(state: AppState, interval_ms: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let evicted = state.actors.evict_idle_rooms().await;
            if evicted > 0 {
                debug!(evicted, "hot room maintenance evicted idle room actors");
            }
        }
    });
}

pub(crate) fn spawn_actor_split_maintenance(state: AppState, interval_ms: u64, limit: usize) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let processed = state.actors.split_pending_scopes(limit).await;
            if processed > 0 {
                debug!(
                    processed,
                    "actor split maintenance processed pending scopes"
                );
            }
        }
    });
}

pub(crate) fn spawn_actor_scope_residency_maintenance(
    state: AppState,
    interval_ms: u64,
    limit: usize,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let tiered_down = state.actors.tier_down_idle_scopes(limit).await;
            if tiered_down > 0 {
                debug!(
                    tiered_down,
                    "actor scope residency maintenance tiered down idle scopes"
                );
            }
        }
    });
}

pub(crate) fn spawn_periodic_controller<F, Fut, E>(
    state: AppState,
    interval_ms: u64,
    controller: &'static str,
    run_once: F,
) where
    F: Fn(AppState) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Debug,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            if let Err(err) = run_once.clone()(state.clone()).await {
                warn!(?err, controller, "periodic controller tick failed");
            }
        }
    });
}

pub(crate) fn spawn_periodic_task<F, Fut>(
    state: AppState,
    interval_ms: u64,
    _task: &'static str,
    run_once: F,
) where
    F: Fn(AppState) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            run_once.clone()(state.clone()).await;
        }
    });
}

pub(crate) fn spawn_record_hot_maintenance(state: AppState, interval_ms: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let evicted = state.record_hot.evict_idle_durable_records().await;
            if evicted > 0 {
                debug!(
                    evicted,
                    "record hot maintenance evicted idle durable records"
                );
            }
        }
    });
}

pub(crate) fn spawn_realtime_maintenance<F, Fut>(
    state: AppState,
    interval_ms: u64,
    publish_member_left: F,
) where
    F: Fn(AppState, RealtimeLeave) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let active_user_sessions = state.connections.active_user_sessions().await;
            let leaves = state
                .realtime
                .cleanup_inactive_session_members(&active_user_sessions)
                .await;
            let stale_members = leaves
                .iter()
                .map(|leave| leave.removed.len())
                .sum::<usize>();
            for leave in leaves {
                publish_member_left.clone()(state.clone(), leave).await;
            }
            let (states, sequences) = state.realtime.cleanup_orphan_runtime_state().await;
            if stale_members > 0 || states > 0 || sequences > 0 {
                debug!(
                    stale_members,
                    states, sequences, "realtime maintenance removed orphan runtime state"
                );
            }
        }
    });
}
