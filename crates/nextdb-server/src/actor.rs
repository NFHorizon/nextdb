use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    sync::mpsc as std_mpsc,
    sync::{Arc, RwLock as StdRwLock},
    thread,
};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    model::{ActorReminderDraft, DbRecord, DbRecordMutationDraft, Message, WalPayload, WalRecord},
    record_hot::RecordHotSnapshot,
    util::{now_ms, shard_index},
};

static ACTOR_ACCESS_SEQ: AtomicU64 = AtomicU64::new(0);
const DEFAULT_ACTOR_SCOPE_SPLIT_ROWS: usize = 1_024;
const DEFAULT_ACTOR_SCOPE_SPLIT_BYTES: usize = 0;
const MAX_SCOPE_SPLIT_DEPTH: usize = 32;

#[derive(Clone)]
pub struct ActorRuntime {
    shards: Arc<Vec<ActorShard>>,
    resident_rooms: Arc<AtomicUsize>,
    config: Arc<StdRwLock<ActorRuntimeConfig>>,
    idle_maintenance: Arc<StdRwLock<ActorIdleMaintenanceStatus>>,
    scope_residency_maintenance: Arc<StdRwLock<ActorScopeResidencyMaintenanceStatus>>,
    split_maintenance: Arc<StdRwLock<ActorSplitMaintenanceStatus>>,
    reminders: Arc<StdRwLock<ActorReminderWheel>>,
    reminder_maintenance: Arc<StdRwLock<ActorReminderMaintenanceStatus>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorId {
    pub kind: ActorKind,
    pub key: String,
}

impl ActorId {
    pub fn room(room_id: impl Into<String>) -> Self {
        Self {
            kind: ActorKind::Room,
            key: room_id.into(),
        }
    }

    #[allow(dead_code)]
    pub fn scope(scope_key: impl Into<String>) -> Self {
        Self {
            kind: ActorKind::Scope,
            key: scope_key.into(),
        }
    }

    #[allow(dead_code)]
    pub fn table(table_key: impl Into<String>) -> Self {
        Self {
            kind: ActorKind::Table,
            key: table_key.into(),
        }
    }

    #[allow(dead_code)]
    pub fn view(view_key: impl Into<String>) -> Self {
        Self {
            kind: ActorKind::View,
            key: view_key.into(),
        }
    }

    #[allow(dead_code)]
    pub fn aggregate(aggregate_key: impl Into<String>) -> Self {
        Self {
            kind: ActorKind::Aggregate,
            key: aggregate_key.into(),
        }
    }

    fn route_key(&self) -> String {
        format!("{}:{}", self.kind.as_str(), self.key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActorKind {
    Room,
    Scope,
    Table,
    View,
    Aggregate,
}

impl ActorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Room => "room",
            Self::Scope => "scope",
            Self::Table => "table",
            Self::View => "view",
            Self::Aggregate => "aggregate",
        }
    }

    pub(crate) fn from_wal_str(value: &str) -> Option<Self> {
        match value {
            "room" => Some(Self::Room),
            "scope" => Some(Self::Scope),
            "table" => Some(Self::Table),
            "view" => Some(Self::View),
            "aggregate" => Some(Self::Aggregate),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActorKernelMessage {
    Touch,
    ReminderFired {
        reminder_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json::Value>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeRowsActivationResult {
    pub actor_id: ActorId,
    pub table_actor_id: ActorId,
    pub shard_index: usize,
    pub created: bool,
    pub requested: usize,
    pub inserted: usize,
    pub updated: usize,
    pub rows: usize,
    pub bytes: usize,
    pub table_scopes: usize,
    pub table_pending_splits: usize,
    pub scope_split_pending: bool,
    pub scope_split_rows: usize,
    pub scope_split_bytes: usize,
    pub turn_count: u64,
    pub last_accessed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActorResidencyTier {
    L1Index,
    L3Full,
}

impl Default for ActorResidencyTier {
    fn default() -> Self {
        Self::L3Full
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeResidencyResult {
    pub actor_id: ActorId,
    pub table_actor_id: ActorId,
    pub shard_index: usize,
    pub created: bool,
    pub subscription_ref_count: usize,
    pub residency_tier: ActorResidencyTier,
    pub rows: usize,
    pub bytes: usize,
    pub lingering_until_ms: u64,
    pub turn_count: u64,
    pub last_accessed_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorScopeResidencyMaintenanceStatus {
    pub last_sweep_at_ms: Option<u64>,
    pub last_tiered_down: usize,
    pub last_cleared_rows: usize,
    pub total_tiered_down: usize,
    pub total_cleared_rows: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct ScopeTierDownResult {
    tiered_down: usize,
    cleared_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorTurnResult {
    pub actor_id: ActorId,
    pub shard_index: usize,
    pub created: bool,
    pub turn_count: u64,
    pub last_accessed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ActorLiveState {
    Room(RoomLiveState),
    Scope(ScopeActorLiveState),
    Table(TableActorLiveState),
    Kernel(GenericActorLiveState),
}

impl ActorLiveState {
    fn new_for_actor(actor_id: &ActorId) -> Self {
        match actor_id.kind {
            ActorKind::Room => Self::Room(RoomLiveState::new()),
            ActorKind::Scope => Self::Scope(ScopeActorLiveState::new()),
            ActorKind::Table => Self::Table(TableActorLiveState::new()),
            ActorKind::View | ActorKind::Aggregate => Self::Kernel(GenericActorLiveState::new()),
        }
    }

    fn is_room(&self) -> bool {
        matches!(self, Self::Room(_))
    }

    fn last_accessed_ms(&self) -> u64 {
        match self {
            Self::Room(room) => room.last_accessed_ms,
            Self::Scope(scope) => scope.last_accessed_ms,
            Self::Table(table) => table.last_accessed_ms,
            Self::Kernel(actor) => actor.last_accessed_ms,
        }
    }

    fn run_kernel_message(&mut self, message: ActorKernelMessage) -> (u64, u64) {
        match self {
            Self::Room(room) => {
                match message {
                    ActorKernelMessage::Touch | ActorKernelMessage::ReminderFired { .. } => {
                        room.touch()
                    }
                }
                (0, room.last_accessed_ms)
            }
            Self::Scope(scope) => scope.run_message(message),
            Self::Table(table) => table.run_message(message),
            Self::Kernel(actor) => actor.run_message(message),
        }
    }

    fn snapshot_entry(&self, actor_id: &ActorId) -> Option<ActorKernelSnapshotEntry> {
        let state = match self {
            Self::Room(_) => return None,
            Self::Scope(scope) => ActorKernelSnapshotState::Scope {
                rows: scope.rows.values().cloned().collect(),
                turn_count: scope.turn_count,
                last_accessed_ms: scope.last_accessed_ms,
                subscription_ref_count: scope.subscription_ref_count,
                lingering_until_ms: scope.lingering_until_ms,
                residency_tier: scope.residency_tier,
            },
            Self::Table(table) => ActorKernelSnapshotState::Table {
                scopes: table
                    .scopes
                    .values()
                    .map(|scope| TableScopeSnapshotEntry {
                        scope_key: scope.scope_key.clone(),
                        rows: scope.rows,
                        bytes: scope.bytes,
                        last_accessed_ms: scope.last_accessed_ms,
                        split_pending: scope.split_pending,
                        split_rows: scope.split_rows,
                        split_bytes: scope.split_bytes,
                        split_reminder_at_ms: scope.split_reminder_at_ms,
                        child_scopes: scope.child_scopes.clone(),
                    })
                    .collect(),
                turn_count: table.turn_count,
                last_accessed_ms: table.last_accessed_ms,
            },
            Self::Kernel(actor) => ActorKernelSnapshotState::Kernel {
                turn_count: actor.turn_count,
                last_accessed_ms: actor.last_accessed_ms,
            },
        };
        Some(ActorKernelSnapshotEntry {
            actor_id: actor_id.clone(),
            state,
        })
    }

    fn from_snapshot_entry(entry: ActorKernelSnapshotEntry) -> Option<(ActorId, Self)> {
        let actor = match (&entry.actor_id.kind, entry.state) {
            (
                ActorKind::Scope,
                ActorKernelSnapshotState::Scope {
                    rows,
                    turn_count,
                    last_accessed_ms,
                    subscription_ref_count,
                    lingering_until_ms,
                    residency_tier,
                },
            ) => Self::Scope(ScopeActorLiveState {
                rows: rows.into_iter().map(|row| (row.key.clone(), row)).collect(),
                turn_count,
                last_accessed_ms,
                subscription_ref_count,
                lingering_until_ms,
                residency_tier,
            }),
            (
                ActorKind::Table,
                ActorKernelSnapshotState::Table {
                    scopes,
                    turn_count,
                    last_accessed_ms,
                },
            ) => Self::Table(TableActorLiveState {
                scopes: scopes
                    .into_iter()
                    .map(|scope| {
                        (
                            scope.scope_key.clone(),
                            TableScopeDirectoryEntry {
                                scope_key: scope.scope_key,
                                rows: scope.rows,
                                bytes: scope.bytes,
                                last_accessed_ms: scope.last_accessed_ms,
                                split_pending: scope.split_pending,
                                split_rows: scope.split_rows,
                                split_bytes: scope.split_bytes,
                                split_reminder_at_ms: scope.split_reminder_at_ms,
                                child_scopes: scope.child_scopes,
                            },
                        )
                    })
                    .collect(),
                turn_count,
                last_accessed_ms,
            }),
            (
                ActorKind::View | ActorKind::Aggregate,
                ActorKernelSnapshotState::Kernel {
                    turn_count,
                    last_accessed_ms,
                },
            ) => Self::Kernel(GenericActorLiveState {
                turn_count,
                last_accessed_ms,
            }),
            _ => return None,
        };
        Some((entry.actor_id, actor))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeActorLiveState {
    rows: BTreeMap<String, DbRecord>,
    turn_count: u64,
    last_accessed_ms: u64,
    #[serde(default)]
    subscription_ref_count: usize,
    #[serde(default)]
    lingering_until_ms: u64,
    #[serde(default)]
    residency_tier: ActorResidencyTier,
}

impl ScopeActorLiveState {
    fn new() -> Self {
        Self {
            rows: BTreeMap::new(),
            turn_count: 0,
            last_accessed_ms: next_access_stamp(),
            subscription_ref_count: 0,
            lingering_until_ms: 0,
            residency_tier: ActorResidencyTier::L3Full,
        }
    }

    fn run_message(&mut self, message: ActorKernelMessage) -> (u64, u64) {
        match message {
            ActorKernelMessage::Touch | ActorKernelMessage::ReminderFired { .. } => self.touch(),
        }
        (self.turn_count, self.last_accessed_ms)
    }

    fn upsert_rows(&mut self, rows: Vec<DbRecord>) -> (usize, usize) {
        let mut inserted = 0;
        let mut updated = 0;
        for row in rows {
            if self.rows.insert(row.key.clone(), row).is_some() {
                updated += 1;
            } else {
                inserted += 1;
            }
        }
        self.touch();
        (inserted, updated)
    }

    fn retain_subscription(&mut self) {
        self.subscription_ref_count = self.subscription_ref_count.saturating_add(1);
        self.lingering_until_ms = 0;
        if !self.rows.is_empty() {
            self.residency_tier = ActorResidencyTier::L3Full;
        }
        self.touch();
    }

    fn release_subscription(&mut self, linger_ms: u64) {
        self.subscription_ref_count = self.subscription_ref_count.saturating_sub(1);
        self.lingering_until_ms = if self.subscription_ref_count == 0 && linger_ms > 0 {
            now_ms().saturating_add(linger_ms)
        } else {
            0
        };
        self.touch();
    }

    fn tier_down_if_idle(&mut self, now_ms: u64) -> usize {
        if self.subscription_ref_count > 0
            || self.lingering_until_ms == 0
            || self.lingering_until_ms > now_ms
        {
            return 0;
        }
        let cleared_rows = self.rows.len();
        self.rows.clear();
        self.residency_tier = ActorResidencyTier::L1Index;
        self.lingering_until_ms = 0;
        self.touch();
        cleared_rows
    }

    fn touch(&mut self) {
        self.turn_count = self.turn_count.saturating_add(1);
        self.last_accessed_ms = next_access_stamp();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TableActorLiveState {
    scopes: BTreeMap<String, TableScopeDirectoryEntry>,
    turn_count: u64,
    last_accessed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TableScopeDirectoryEntry {
    scope_key: String,
    rows: usize,
    #[serde(default)]
    bytes: usize,
    last_accessed_ms: u64,
    #[serde(default)]
    split_pending: bool,
    #[serde(default)]
    split_rows: usize,
    #[serde(default)]
    split_bytes: usize,
    #[serde(default)]
    split_reminder_at_ms: u64,
    #[serde(default)]
    child_scopes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ScopeSplitPolicy {
    rows: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct TableScopeUpdate {
    table_scopes: usize,
    table_pending_splits: usize,
    scope_split_pending: bool,
    scope_split_rows: usize,
    scope_split_bytes: usize,
}

#[derive(Debug, Clone)]
struct TableScopeChildUpdate {
    scope_key: String,
    rows: usize,
    bytes: usize,
    last_accessed_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct ScopeDrainResult {
    rows: Vec<DbRecord>,
    last_accessed_ms: u64,
}

#[derive(Debug, Clone)]
struct TableScopeSplitCandidate {
    table_actor_id: ActorId,
    scope_key: String,
}

impl TableActorLiveState {
    fn new() -> Self {
        Self {
            scopes: BTreeMap::new(),
            turn_count: 0,
            last_accessed_ms: next_access_stamp(),
        }
    }

    fn run_message(&mut self, message: ActorKernelMessage) -> (u64, u64) {
        match message {
            ActorKernelMessage::Touch | ActorKernelMessage::ReminderFired { .. } => self.touch(),
        }
        (self.turn_count, self.last_accessed_ms)
    }

    fn upsert_scope(
        &mut self,
        scope_key: String,
        rows: usize,
        bytes: usize,
        last_accessed_ms: u64,
        split_policy: ScopeSplitPolicy,
    ) -> TableScopeUpdate {
        let split_pending = scope_exceeds_split_policy(rows, bytes, split_policy);
        let split_reminder_at_ms = if split_pending { now_ms() } else { 0 };
        self.scopes.insert(
            scope_key.clone(),
            TableScopeDirectoryEntry {
                scope_key,
                rows,
                bytes,
                last_accessed_ms,
                split_pending,
                split_rows: split_policy.rows,
                split_bytes: split_policy.bytes,
                split_reminder_at_ms,
                child_scopes: Vec::new(),
            },
        );
        self.touch();
        TableScopeUpdate {
            table_scopes: self.scopes.len(),
            table_pending_splits: self.pending_split_count(),
            scope_split_pending: split_pending,
            scope_split_rows: split_policy.rows,
            scope_split_bytes: split_policy.bytes,
        }
    }

    fn touch(&mut self) {
        self.turn_count = self.turn_count.saturating_add(1);
        self.last_accessed_ms = next_access_stamp();
    }

    fn pending_split_count(&self) -> usize {
        self.scopes
            .values()
            .filter(|scope| scope.split_pending)
            .count()
    }

    fn pending_split_scope_keys(&self, limit: usize, now_ms: u64) -> Vec<String> {
        self.scopes
            .values()
            .filter(|scope| {
                scope.split_pending
                    && (scope.split_reminder_at_ms == 0 || scope.split_reminder_at_ms <= now_ms)
                    && scope.child_scopes.is_empty()
                    && scope_split_depth(&scope.scope_key) < MAX_SCOPE_SPLIT_DEPTH
            })
            .take(limit)
            .map(|scope| scope.scope_key.clone())
            .collect()
    }

    fn route_rows(&self, scope_key: String, rows: Vec<DbRecord>) -> Vec<(String, Vec<DbRecord>)> {
        let mut routed = BTreeMap::<String, Vec<DbRecord>>::new();
        for row in rows {
            let routed_scope_key = self.route_record_scope_key(&scope_key, &row.key);
            routed.entry(routed_scope_key).or_default().push(row);
        }
        routed.into_iter().collect()
    }

    fn route_record_scope_key(&self, scope_key: &str, record_key: &str) -> String {
        let mut current_scope_key = scope_key.to_string();
        for _ in 0..=MAX_SCOPE_SPLIT_DEPTH {
            let Some(scope) = self.scopes.get(&current_scope_key) else {
                return current_scope_key;
            };
            if scope.child_scopes.is_empty() {
                return current_scope_key;
            }
            let child_index =
                split_child_index(&current_scope_key, record_key, scope.child_scopes.len());
            let child_scope_key = scope.child_scopes[child_index].clone();
            if child_scope_key == current_scope_key {
                return current_scope_key;
            }
            current_scope_key = child_scope_key;
        }
        current_scope_key
    }

    fn split_scope(
        &mut self,
        parent_scope_key: String,
        children: Vec<TableScopeChildUpdate>,
        split_policy: ScopeSplitPolicy,
        last_accessed_ms: u64,
    ) -> TableScopeUpdate {
        let child_scopes = children
            .iter()
            .map(|child| child.scope_key.clone())
            .collect::<Vec<_>>();
        self.scopes.insert(
            parent_scope_key.clone(),
            TableScopeDirectoryEntry {
                scope_key: parent_scope_key.clone(),
                rows: 0,
                bytes: 0,
                last_accessed_ms,
                split_pending: false,
                split_rows: split_policy.rows,
                split_bytes: split_policy.bytes,
                split_reminder_at_ms: 0,
                child_scopes: child_scopes.clone(),
            },
        );
        for child in children {
            let child_split_pending =
                scope_exceeds_split_policy(child.rows, child.bytes, split_policy);
            let child_split_reminder_at_ms = if child_split_pending { now_ms() } else { 0 };
            self.scopes.insert(
                child.scope_key.clone(),
                TableScopeDirectoryEntry {
                    scope_key: child.scope_key,
                    rows: child.rows,
                    bytes: child.bytes,
                    last_accessed_ms: child.last_accessed_ms,
                    split_pending: child_split_pending,
                    split_rows: split_policy.rows,
                    split_bytes: split_policy.bytes,
                    split_reminder_at_ms: child_split_reminder_at_ms,
                    child_scopes: Vec::new(),
                },
            );
        }
        self.touch();
        TableScopeUpdate {
            table_scopes: self.scopes.len(),
            table_pending_splits: self.pending_split_count(),
            scope_split_pending: false,
            scope_split_rows: split_policy.rows,
            scope_split_bytes: split_policy.bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenericActorLiveState {
    turn_count: u64,
    last_accessed_ms: u64,
}

impl GenericActorLiveState {
    fn new() -> Self {
        Self {
            turn_count: 0,
            last_accessed_ms: next_access_stamp(),
        }
    }

    fn run_message(&mut self, message: ActorKernelMessage) -> (u64, u64) {
        match message {
            ActorKernelMessage::Touch | ActorKernelMessage::ReminderFired { .. } => self.touch(),
        }
        (self.turn_count, self.last_accessed_ms)
    }

    fn touch(&mut self) {
        self.turn_count = self.turn_count.saturating_add(1);
        self.last_accessed_ms = next_access_stamp();
    }
}

struct ActorShard {
    tx: std_mpsc::Sender<ActorShardCommand>,
    runtime_state: ActorShardRuntimeState,
}

struct ActorShardCommand {
    request: ActorShardRequest,
}

enum ActorShardRequest {
    RunActorTurn {
        actor_id: ActorId,
        message: ActorKernelMessage,
        reply: oneshot::Sender<ActorTurnResult>,
    },
    UpsertScopeRows {
        table_actor_id: ActorId,
        actor_id: ActorId,
        rows: Vec<DbRecord>,
        reply: oneshot::Sender<ScopeRowsActivationResult>,
    },
    RouteScopeRows {
        table_actor_id: ActorId,
        scope_key: String,
        rows: Vec<DbRecord>,
        reply: oneshot::Sender<Vec<(String, Vec<DbRecord>)>>,
    },
    UpsertTableScope {
        table_actor_id: ActorId,
        scope_key: String,
        rows: usize,
        bytes: usize,
        last_accessed_ms: u64,
        split_policy: ScopeSplitPolicy,
        reply: oneshot::Sender<TableScopeUpdate>,
    },
    DrainScopeRows {
        actor_id: ActorId,
        reply: oneshot::Sender<ScopeDrainResult>,
    },
    RetainScope {
        table_actor_id: ActorId,
        actor_id: ActorId,
        reply: oneshot::Sender<ScopeResidencyResult>,
    },
    ReleaseScope {
        table_actor_id: ActorId,
        actor_id: ActorId,
        linger_ms: u64,
        reply: oneshot::Sender<ScopeResidencyResult>,
    },
    TierDownIdleScopes {
        now_ms: u64,
        limit: usize,
        reply: oneshot::Sender<ScopeTierDownResult>,
    },
    SplitTableScope {
        table_actor_id: ActorId,
        parent_scope_key: String,
        children: Vec<TableScopeChildUpdate>,
        last_accessed_ms: u64,
        split_policy: ScopeSplitPolicy,
        reply: oneshot::Sender<TableScopeUpdate>,
    },
    PendingTableSplits {
        limit: usize,
        now_ms: u64,
        reply: oneshot::Sender<Vec<TableScopeSplitCandidate>>,
    },
    KernelStatus {
        reply: oneshot::Sender<ActorKernelStatus>,
    },
    ApplyMessages {
        actor_batches: Vec<(ActorId, Vec<Message>)>,
        hot_window: usize,
        reply: oneshot::Sender<usize>,
    },
    LatestMessages {
        actor_id: ActorId,
        before_lsn: Option<u64>,
        limit: usize,
        reply: oneshot::Sender<Vec<Message>>,
    },
    RoomStatuses {
        reply: oneshot::Sender<Vec<ActorRoomStatus>>,
    },
    HasRoom {
        actor_id: ActorId,
        reply: oneshot::Sender<bool>,
    },
    EvictRoom {
        actor_id: ActorId,
        reply: oneshot::Sender<bool>,
    },
    TruncateAll {
        hot_window: usize,
        reply: oneshot::Sender<()>,
    },
    Snapshot {
        reply: oneshot::Sender<ActorShardSnapshot>,
    },
    EvictIdle {
        hot_room_idle_ttl_ms: u64,
        now_ms: u64,
        reply: oneshot::Sender<usize>,
    },
    LruCandidates {
        reply: oneshot::Sender<Vec<RoomLruCandidate>>,
    },
    EvictRooms {
        actor_ids: Vec<ActorId>,
        reply: oneshot::Sender<usize>,
    },
}

#[derive(Debug, Clone)]
struct RoomLruCandidate {
    shard_index: usize,
    actor_id: ActorId,
    last_accessed_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct ActorShardSnapshot {
    rooms: Vec<(String, RoomSnapshot)>,
    actor_states: Vec<ActorKernelSnapshotEntry>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorKernelStatus {
    pub total_actors: usize,
    pub room_actors: usize,
    pub kernel_actors: usize,
    pub scope_rows: usize,
    pub scope_bytes: usize,
    pub scope_subscription_ref_count: usize,
    pub subscribed_scopes: usize,
    pub lingering_scopes: usize,
    pub l1_scope_actors: usize,
    pub l3_scope_actors: usize,
    pub table_scopes: usize,
    pub table_pending_splits: usize,
    pub kind_counts: BTreeMap<String, usize>,
    pub oldest_accessed_ms: Option<u64>,
    pub newest_accessed_ms: Option<u64>,
}

impl ActorKernelStatus {
    fn from_actors(actors: &HashMap<ActorId, ActorLiveState>) -> Self {
        let mut status = Self::default();
        for (actor_id, actor) in actors {
            status.total_actors += 1;
            if actor.is_room() {
                status.room_actors += 1;
            } else {
                status.kernel_actors += 1;
            }
            if let ActorLiveState::Scope(scope) = actor {
                status.scope_rows += scope.rows.len();
                status.scope_bytes += scope_rows_estimated_bytes(&scope.rows);
                status.scope_subscription_ref_count += scope.subscription_ref_count;
                if scope.subscription_ref_count > 0 {
                    status.subscribed_scopes += 1;
                }
                if scope.lingering_until_ms > 0 {
                    status.lingering_scopes += 1;
                }
                match scope.residency_tier {
                    ActorResidencyTier::L1Index => status.l1_scope_actors += 1,
                    ActorResidencyTier::L3Full => status.l3_scope_actors += 1,
                }
            }
            if let ActorLiveState::Table(table) = actor {
                status.table_scopes += table.scopes.len();
                status.table_pending_splits += table.pending_split_count();
            }
            *status
                .kind_counts
                .entry(actor_id.kind.as_str().to_string())
                .or_default() += 1;
            let last_accessed_ms = actor.last_accessed_ms();
            status.oldest_accessed_ms = Some(
                status
                    .oldest_accessed_ms
                    .map_or(last_accessed_ms, |oldest| oldest.min(last_accessed_ms)),
            );
            status.newest_accessed_ms = Some(
                status
                    .newest_accessed_ms
                    .map_or(last_accessed_ms, |newest| newest.max(last_accessed_ms)),
            );
        }
        status
    }

    fn merge(&mut self, other: Self) {
        self.total_actors += other.total_actors;
        self.room_actors += other.room_actors;
        self.kernel_actors += other.kernel_actors;
        self.scope_rows += other.scope_rows;
        self.scope_bytes += other.scope_bytes;
        self.scope_subscription_ref_count += other.scope_subscription_ref_count;
        self.subscribed_scopes += other.subscribed_scopes;
        self.lingering_scopes += other.lingering_scopes;
        self.l1_scope_actors += other.l1_scope_actors;
        self.l3_scope_actors += other.l3_scope_actors;
        self.table_scopes += other.table_scopes;
        self.table_pending_splits += other.table_pending_splits;
        for (kind, count) in other.kind_counts {
            *self.kind_counts.entry(kind).or_default() += count;
        }
        if let Some(oldest) = other.oldest_accessed_ms {
            self.oldest_accessed_ms = Some(
                self.oldest_accessed_ms
                    .map_or(oldest, |current| current.min(oldest)),
            );
        }
        if let Some(newest) = other.newest_accessed_ms {
            self.newest_accessed_ms = Some(
                self.newest_accessed_ms
                    .map_or(newest, |current| current.max(newest)),
            );
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorShardRuntimeStatus {
    pub shard_index: usize,
    pub thread_name: String,
    pub target_core_id: Option<usize>,
    pub pinning_requested: bool,
    pub pinning_succeeded: bool,
}

#[derive(Clone)]
struct ActorShardRuntimeState {
    shard_index: usize,
    thread_name: String,
    target_core_id: Option<usize>,
    pinning_requested: bool,
    pinning_succeeded: Arc<AtomicBool>,
}

impl ActorShardRuntimeState {
    fn status(&self) -> ActorShardRuntimeStatus {
        ActorShardRuntimeStatus {
            shard_index: self.shard_index,
            thread_name: self.thread_name.clone(),
            target_core_id: self.target_core_id,
            pinning_requested: self.pinning_requested,
            pinning_succeeded: self.pinning_succeeded.load(AtomicOrdering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ActorRuntimeConfig {
    hot_window: usize,
    max_hot_rooms: usize,
    hot_room_idle_ttl_ms: u64,
    scope_split_rows: usize,
    scope_split_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorIdleMaintenanceStatus {
    pub last_sweep_at_ms: Option<u64>,
    pub last_evicted: usize,
    pub total_evicted: usize,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorSplitMaintenanceStatus {
    pub last_sweep_at_ms: Option<u64>,
    pub last_processed: usize,
    pub total_processed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActorReminderEntry {
    pub actor_id: ActorId,
    pub reminder_id: String,
    pub due_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorReminderStatus {
    pub pending: usize,
    pub next_due_at_ms: Option<u64>,
    pub reminders: Vec<ActorReminderEntry>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorReminderMaintenanceStatus {
    pub last_sweep_at_ms: Option<u64>,
    pub last_fired: usize,
    pub total_fired: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ActorReminderKey {
    due_at_ms: u64,
    actor_kind: &'static str,
    actor_key: String,
    reminder_id: String,
}

#[derive(Debug, Clone, Default)]
struct ActorReminderWheel {
    entries: BTreeMap<ActorReminderKey, ActorReminderEntry>,
}

impl ActorReminderWheel {
    fn upsert(&mut self, entry: ActorReminderEntry) {
        self.remove(&entry.actor_id, &entry.reminder_id);
        self.entries
            .insert(ActorReminderKey::from_entry(&entry), entry);
    }

    fn remove(&mut self, actor_id: &ActorId, reminder_id: &str) -> Option<ActorReminderEntry> {
        let key = self.entries.iter().find_map(|(key, entry)| {
            if &entry.actor_id == actor_id && entry.reminder_id == reminder_id {
                Some(key.clone())
            } else {
                None
            }
        })?;
        self.entries.remove(&key)
    }

    fn take_due(&mut self, now_ms: u64, limit: usize) -> Vec<ActorReminderEntry> {
        if limit == 0 {
            return Vec::new();
        }
        let due_keys = self
            .entries
            .iter()
            .take_while(|(key, _)| key.due_at_ms <= now_ms)
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        due_keys
            .into_iter()
            .filter_map(|key| self.entries.remove(&key))
            .collect()
    }

    fn status(&self, limit: usize) -> ActorReminderStatus {
        ActorReminderStatus {
            pending: self.entries.len(),
            next_due_at_ms: self.entries.keys().next().map(|key| key.due_at_ms),
            reminders: self.entries.values().take(limit).cloned().collect(),
        }
    }
}

impl ActorReminderKey {
    fn from_entry(entry: &ActorReminderEntry) -> Self {
        Self {
            due_at_ms: entry.due_at_ms,
            actor_kind: entry.actor_id.kind.as_str(),
            actor_key: entry.actor_id.key.clone(),
            reminder_id: entry.reminder_id.clone(),
        }
    }
}

impl ActorRuntime {
    #[allow(dead_code)]
    pub fn new(
        rooms: HashMap<String, RoomLiveState>,
        hot_window: usize,
        max_hot_rooms: usize,
        hot_room_idle_ttl_ms: u64,
    ) -> Self {
        Self::new_with_shard_count(
            rooms,
            hot_window,
            max_hot_rooms,
            hot_room_idle_ttl_ms,
            default_actor_shard_count(),
        )
    }

    #[allow(dead_code)]
    pub fn new_with_shard_count(
        rooms: HashMap<String, RoomLiveState>,
        hot_window: usize,
        max_hot_rooms: usize,
        hot_room_idle_ttl_ms: u64,
        shard_count: usize,
    ) -> Self {
        Self::new_with_shard_count_and_actor_states(
            rooms,
            Vec::new(),
            Vec::new(),
            hot_window,
            max_hot_rooms,
            hot_room_idle_ttl_ms,
            shard_count,
        )
    }

    #[allow(dead_code)]
    pub fn new_with_actor_states(
        rooms: HashMap<String, RoomLiveState>,
        actor_states: Vec<ActorKernelSnapshotEntry>,
        hot_window: usize,
        max_hot_rooms: usize,
        hot_room_idle_ttl_ms: u64,
    ) -> Self {
        Self::new_with_shard_count_and_actor_states(
            rooms,
            actor_states,
            Vec::new(),
            hot_window,
            max_hot_rooms,
            hot_room_idle_ttl_ms,
            default_actor_shard_count(),
        )
    }

    pub fn new_with_actor_states_and_reminders(
        rooms: HashMap<String, RoomLiveState>,
        actor_states: Vec<ActorKernelSnapshotEntry>,
        reminders: Vec<ActorReminderEntry>,
        hot_window: usize,
        max_hot_rooms: usize,
        hot_room_idle_ttl_ms: u64,
    ) -> Self {
        Self::new_with_shard_count_and_actor_states(
            rooms,
            actor_states,
            reminders,
            hot_window,
            max_hot_rooms,
            hot_room_idle_ttl_ms,
            default_actor_shard_count(),
        )
    }

    pub fn new_with_shard_count_and_actor_states(
        mut rooms: HashMap<String, RoomLiveState>,
        actor_states: Vec<ActorKernelSnapshotEntry>,
        reminders: Vec<ActorReminderEntry>,
        hot_window: usize,
        max_hot_rooms: usize,
        hot_room_idle_ttl_ms: u64,
        shard_count: usize,
    ) -> Self {
        evict_lru_rooms(&mut rooms, max_hot_rooms);
        evict_idle_rooms(&mut rooms, hot_room_idle_ttl_ms, now_ms());
        let shard_count = shard_count.max(1);
        let mut shard_rooms = (0..shard_count)
            .map(|_| HashMap::new())
            .collect::<Vec<HashMap<ActorId, ActorLiveState>>>();
        for (room_id, room) in rooms {
            let actor_id = ActorId::room(room_id);
            let shard_index = actor_shard_index(&actor_id, shard_count);
            shard_rooms[shard_index].insert(actor_id, ActorLiveState::Room(room));
        }
        for entry in actor_states {
            if let Some((actor_id, actor)) = ActorLiveState::from_snapshot_entry(entry) {
                let shard_index = actor_shard_index(&actor_id, shard_count);
                shard_rooms[shard_index].insert(actor_id, actor);
            }
        }
        let resident_rooms = shard_rooms
            .iter()
            .flat_map(HashMap::values)
            .filter(|actor| actor.is_room())
            .count();
        let pinning_requested = actor_thread_pinning_requested();
        let core_ids = if pinning_requested {
            core_affinity::get_core_ids().unwrap_or_default()
        } else {
            Vec::new()
        };
        let shards = shard_rooms
            .into_iter()
            .enumerate()
            .map(|(shard_index, rooms)| {
                let (tx, rx) = std_mpsc::channel();
                let thread_name = format!("nextdb-actor-shard-{shard_index}");
                let target_core_id = core_ids
                    .get(shard_index % core_ids.len().max(1))
                    .map(|core_id| core_id.id);
                let runtime_state = ActorShardRuntimeState {
                    shard_index,
                    thread_name: thread_name.clone(),
                    target_core_id,
                    pinning_requested,
                    pinning_succeeded: Arc::new(AtomicBool::new(false)),
                };
                let thread_state = runtime_state.clone();
                match thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || run_actor_shard(thread_state, rooms, rx))
                {
                    Ok(_handle) => {}
                    Err(error) => panic!("spawn actor shard thread {shard_index}: {error}"),
                }
                ActorShard { tx, runtime_state }
            })
            .collect();
        Self {
            shards: Arc::new(shards),
            resident_rooms: Arc::new(AtomicUsize::new(resident_rooms)),
            config: Arc::new(StdRwLock::new(ActorRuntimeConfig {
                hot_window,
                max_hot_rooms,
                hot_room_idle_ttl_ms,
                scope_split_rows: actor_scope_split_rows(),
                scope_split_bytes: actor_scope_split_bytes(),
            })),
            idle_maintenance: Arc::new(StdRwLock::new(ActorIdleMaintenanceStatus::default())),
            scope_residency_maintenance: Arc::new(StdRwLock::new(
                ActorScopeResidencyMaintenanceStatus::default(),
            )),
            split_maintenance: Arc::new(StdRwLock::new(ActorSplitMaintenanceStatus::default())),
            reminders: Arc::new(StdRwLock::new(reminders.into_iter().fold(
                ActorReminderWheel::default(),
                |mut wheel, reminder| {
                    wheel.upsert(reminder);
                    wheel
                },
            ))),
            reminder_maintenance: Arc::new(StdRwLock::new(
                ActorReminderMaintenanceStatus::default(),
            )),
        }
    }

    pub async fn apply_message(&self, message: Message) {
        let config = self.config();
        let actor_id = ActorId::room(message.room_id.clone());
        let shard = self.shard_for_actor(&actor_id);
        let new_rooms = shard
            .apply_messages(vec![(actor_id, vec![message])], config.hot_window)
            .await;
        self.resident_rooms
            .fetch_add(new_rooms, AtomicOrdering::Relaxed);
        self.enforce_capacity(config.max_hot_rooms).await;
    }

    pub async fn apply_messages(&self, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }
        let config = self.config();
        let mut by_actor: HashMap<ActorId, Vec<Message>> = HashMap::new();
        for message in messages {
            by_actor
                .entry(ActorId::room(message.room_id.clone()))
                .or_default()
                .push(message);
        }
        let mut by_shard: HashMap<usize, Vec<(ActorId, Vec<Message>)>> = HashMap::new();
        let shard_count = self.shards.len();
        for (actor_id, messages) in by_actor {
            by_shard
                .entry(actor_shard_index(&actor_id, shard_count))
                .or_default()
                .push((actor_id, messages));
        }
        for (shard_index, room_batches) in by_shard {
            let new_rooms = self.shards[shard_index]
                .apply_messages(room_batches, config.hot_window)
                .await;
            self.resident_rooms
                .fetch_add(new_rooms, AtomicOrdering::Relaxed);
        }
        self.enforce_capacity(config.max_hot_rooms).await;
    }

    pub async fn run_actor_turn(
        &self,
        actor_id: ActorId,
        message: ActorKernelMessage,
    ) -> ActorTurnResult {
        let is_room = actor_id.kind == ActorKind::Room;
        let config = self.config();
        let result = self
            .shard_for_actor(&actor_id)
            .run_actor_turn(actor_id, message)
            .await;
        if is_room && result.created {
            self.resident_rooms.fetch_add(1, AtomicOrdering::Relaxed);
            self.enforce_capacity(config.max_hot_rooms).await;
        }
        result
    }

    pub async fn kernel_status(&self) -> ActorKernelStatus {
        let mut status = ActorKernelStatus::default();
        for shard in self.shards.iter() {
            status.merge(shard.kernel_status().await);
        }
        status
    }

    pub async fn upsert_scope_rows(
        &self,
        table_key: impl Into<String>,
        scope_key: impl Into<String>,
        rows: Vec<DbRecord>,
    ) -> ScopeRowsActivationResult {
        let table_actor_id = ActorId::table(table_key);
        let base_scope_key = scope_key.into();
        let split_policy = self.split_policy();
        let routed = self
            .shard_for_actor(&table_actor_id)
            .route_scope_rows(table_actor_id.clone(), base_scope_key.clone(), rows)
            .await;
        let mut aggregate = ScopeRowsActivationResult {
            actor_id: ActorId::scope(base_scope_key),
            table_actor_id: table_actor_id.clone(),
            shard_index: self
                .shard_for_actor(&table_actor_id)
                .runtime_state
                .shard_index,
            created: false,
            requested: 0,
            inserted: 0,
            updated: 0,
            rows: 0,
            bytes: 0,
            table_scopes: 0,
            table_pending_splits: 0,
            scope_split_pending: false,
            scope_split_rows: split_policy.rows,
            scope_split_bytes: split_policy.bytes,
            turn_count: 0,
            last_accessed_ms: 0,
        };
        for (scope_key, rows) in routed {
            let actor_id = ActorId::scope(scope_key);
            let mut result = self
                .shard_for_actor(&actor_id)
                .upsert_scope_rows(table_actor_id.clone(), actor_id.clone(), rows)
                .await;
            let table_update = self
                .shard_for_actor(&table_actor_id)
                .upsert_table_scope(
                    table_actor_id.clone(),
                    actor_id.key.clone(),
                    result.rows,
                    result.bytes,
                    result.last_accessed_ms,
                    split_policy,
                )
                .await;
            result.table_scopes = table_update.table_scopes;
            result.table_pending_splits = table_update.table_pending_splits;
            result.scope_split_pending = table_update.scope_split_pending;
            result.scope_split_rows = table_update.scope_split_rows;
            result.scope_split_bytes = table_update.scope_split_bytes;
            if result.scope_split_pending {
                let split_update = self
                    .split_scope_rows(table_actor_id.clone(), actor_id.clone(), split_policy)
                    .await;
                result.table_scopes = split_update.table_scopes;
                result.table_pending_splits = split_update.table_pending_splits;
                result.scope_split_pending = split_update.scope_split_pending;
                result.scope_split_rows = split_update.scope_split_rows;
                result.scope_split_bytes = split_update.scope_split_bytes;
            }
            merge_scope_activation_result(&mut aggregate, result);
        }
        aggregate
    }

    pub async fn retain_scope_subscription(
        &self,
        table_key: impl Into<String>,
        scope_key: impl Into<String>,
    ) -> ScopeResidencyResult {
        let table_actor_id = ActorId::table(table_key);
        let actor_id = ActorId::scope(scope_key);
        self.shard_for_actor(&actor_id)
            .retain_scope(table_actor_id, actor_id)
            .await
    }

    pub async fn release_scope_subscription(
        &self,
        table_key: impl Into<String>,
        scope_key: impl Into<String>,
        linger_ms: u64,
    ) -> ScopeResidencyResult {
        let table_actor_id = ActorId::table(table_key);
        let actor_id = ActorId::scope(scope_key);
        self.shard_for_actor(&actor_id)
            .release_scope(table_actor_id, actor_id, linger_ms)
            .await
    }

    pub async fn tier_down_idle_scopes(&self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let swept_at_ms = now_ms();
        let mut remaining = limit;
        let mut aggregate = ScopeTierDownResult::default();
        for shard in self.shards.iter() {
            if remaining == 0 {
                break;
            }
            let result = shard.tier_down_idle_scopes(swept_at_ms, remaining).await;
            remaining = remaining.saturating_sub(result.tiered_down);
            aggregate.tiered_down = aggregate.tiered_down.saturating_add(result.tiered_down);
            aggregate.cleared_rows = aggregate.cleared_rows.saturating_add(result.cleared_rows);
        }
        self.record_scope_residency_sweep(swept_at_ms, aggregate);
        aggregate.tiered_down
    }

    async fn split_scope_rows(
        &self,
        table_actor_id: ActorId,
        actor_id: ActorId,
        split_policy: ScopeSplitPolicy,
    ) -> TableScopeUpdate {
        let mut pending = VecDeque::from([(actor_id, 0usize)]);
        let mut last_update = TableScopeUpdate {
            scope_split_rows: split_policy.rows,
            scope_split_bytes: split_policy.bytes,
            ..TableScopeUpdate::default()
        };
        while let Some((actor_id, depth)) = pending.pop_front() {
            let drained = self
                .shard_for_actor(&actor_id)
                .drain_scope_rows(actor_id.clone())
                .await;
            let drained_bytes = records_estimated_bytes(&drained.rows);
            if drained.rows.is_empty()
                || depth >= MAX_SCOPE_SPLIT_DEPTH
                || !scope_exceeds_split_policy(drained.rows.len(), drained_bytes, split_policy)
            {
                let row_count = drained.rows.len();
                let restored = self
                    .shard_for_actor(&actor_id)
                    .upsert_scope_rows(table_actor_id.clone(), actor_id.clone(), drained.rows)
                    .await;
                last_update = self
                    .shard_for_actor(&table_actor_id)
                    .upsert_table_scope(
                        table_actor_id.clone(),
                        actor_id.key,
                        row_count,
                        restored.bytes,
                        restored.last_accessed_ms,
                        split_policy,
                    )
                    .await;
                continue;
            }

            let mut child_rows = (0..2)
                .map(|child_index| {
                    (
                        format!("{}/child:{child_index:02x}", actor_id.key),
                        Vec::new(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            for row in drained.rows {
                child_rows
                    .entry(split_child_scope_key(&actor_id.key, &row.key, 2))
                    .or_default()
                    .push(row);
            }

            let mut children = Vec::with_capacity(child_rows.len());
            let mut next_pending = Vec::new();
            for (child_scope_key, rows) in child_rows {
                let child_actor_id = ActorId::scope(child_scope_key.clone());
                let child_result = if rows.is_empty() {
                    ScopeRowsActivationResult {
                        actor_id: child_actor_id.clone(),
                        table_actor_id: table_actor_id.clone(),
                        shard_index: self
                            .shard_for_actor(&child_actor_id)
                            .runtime_state
                            .shard_index,
                        created: false,
                        requested: 0,
                        inserted: 0,
                        updated: 0,
                        rows: 0,
                        table_scopes: 0,
                        table_pending_splits: 0,
                        scope_split_pending: false,
                        scope_split_rows: split_policy.rows,
                        scope_split_bytes: split_policy.bytes,
                        turn_count: 0,
                        last_accessed_ms: drained.last_accessed_ms,
                        bytes: 0,
                    }
                } else {
                    self.shard_for_actor(&child_actor_id)
                        .upsert_scope_rows(table_actor_id.clone(), child_actor_id.clone(), rows)
                        .await
                };
                if depth + 1 < MAX_SCOPE_SPLIT_DEPTH
                    && scope_exceeds_split_policy(
                        child_result.rows,
                        child_result.bytes,
                        split_policy,
                    )
                {
                    next_pending.push(child_actor_id);
                }
                children.push(TableScopeChildUpdate {
                    scope_key: child_scope_key,
                    rows: child_result.rows,
                    bytes: child_result.bytes,
                    last_accessed_ms: child_result.last_accessed_ms,
                });
            }
            last_update = self
                .shard_for_actor(&table_actor_id)
                .split_table_scope(
                    table_actor_id.clone(),
                    actor_id.key,
                    children,
                    drained.last_accessed_ms,
                    split_policy,
                )
                .await;
            for child_actor_id in next_pending {
                pending.push_back((child_actor_id, depth + 1));
            }
        }
        last_update
    }

    async fn enforce_capacity(&self, max_hot_rooms: usize) -> usize {
        if self.resident_rooms.load(AtomicOrdering::Relaxed) <= max_hot_rooms {
            return 0;
        }
        let evicted = evict_lru_sharded_rooms(&self.shards, max_hot_rooms).await;
        self.decrement_resident_rooms(evicted);
        evicted
    }

    async fn evict_with_config(&self, config: ActorRuntimeConfig, now: u64) -> usize {
        let lru_evicted = evict_lru_sharded_rooms(&self.shards, config.max_hot_rooms).await;
        let idle_evicted =
            evict_idle_sharded_rooms(&self.shards, config.hot_room_idle_ttl_ms, now).await;
        let evicted = lru_evicted + idle_evicted;
        self.decrement_resident_rooms(evicted);
        evicted
    }

    pub async fn latest_messages(
        &self,
        room_id: &str,
        before_lsn: Option<u64>,
        limit: usize,
    ) -> Vec<Message> {
        self.evict_idle_rooms().await;
        let actor_id = ActorId::room(room_id);
        self.shard_for_actor(&actor_id)
            .latest_messages(actor_id, before_lsn, limit)
            .await
    }

    pub async fn room_count(&self) -> usize {
        self.resident_rooms.load(AtomicOrdering::Relaxed)
    }

    pub async fn room_statuses(&self) -> Vec<ActorRoomStatus> {
        let mut statuses = Vec::new();
        for shard in self.shards.iter() {
            statuses.extend(shard.room_statuses().await);
        }
        statuses.sort_by(|left, right| {
            right
                .last_accessed_ms
                .cmp(&left.last_accessed_ms)
                .then_with(|| left.room_id.cmp(&right.room_id))
        });
        statuses
    }

    pub async fn has_room(&self, room_id: &str) -> bool {
        let actor_id = ActorId::room(room_id);
        self.shard_for_actor(&actor_id).has_room(actor_id).await
    }

    pub async fn evict_room(&self, room_id: &str) -> bool {
        let actor_id = ActorId::room(room_id);
        let evicted = self.shard_for_actor(&actor_id).evict_room(actor_id).await;
        if evicted {
            self.decrement_resident_rooms(1);
        }
        evicted
    }

    pub fn max_hot_rooms(&self) -> usize {
        self.config().max_hot_rooms
    }

    pub fn hot_window(&self) -> usize {
        self.config().hot_window
    }

    pub fn hot_room_idle_ttl_ms(&self) -> u64 {
        self.config().hot_room_idle_ttl_ms
    }

    pub fn shard_statuses(&self) -> Vec<ActorShardRuntimeStatus> {
        self.shards
            .iter()
            .map(|shard| shard.runtime_state.status())
            .collect()
    }

    pub fn idle_maintenance_status(&self) -> ActorIdleMaintenanceStatus {
        *self
            .idle_maintenance
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn scope_residency_maintenance_status(&self) -> ActorScopeResidencyMaintenanceStatus {
        *self
            .scope_residency_maintenance
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn split_maintenance_status(&self) -> ActorSplitMaintenanceStatus {
        *self
            .split_maintenance
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn reminder_maintenance_status(&self) -> ActorReminderMaintenanceStatus {
        *self
            .reminder_maintenance
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn reminder_status(&self, limit: usize) -> ActorReminderStatus {
        self.reminders
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .status(limit)
    }

    pub fn schedule_reminder(&self, entry: ActorReminderEntry) {
        self.reminders
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .upsert(entry);
    }

    pub fn cancel_reminder(&self, actor_id: &ActorId, reminder_id: &str) -> bool {
        self.reminders
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(actor_id, reminder_id)
            .is_some()
    }

    pub fn take_due_reminders(&self, now_ms: u64, limit: usize) -> Vec<ActorReminderEntry> {
        self.reminders
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take_due(now_ms, limit)
    }

    pub fn requeue_reminders(&self, reminders: Vec<ActorReminderEntry>) {
        if reminders.is_empty() {
            return;
        }
        let mut wheel = self
            .reminders
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for reminder in reminders {
            wheel.upsert(reminder);
        }
    }

    pub async fn split_pending_scopes(&self, limit: usize) -> usize {
        self.split_pending_scopes_with_policy(limit, self.split_policy())
            .await
    }

    async fn split_pending_scopes_with_policy(
        &self,
        limit: usize,
        split_policy: ScopeSplitPolicy,
    ) -> usize {
        if limit == 0 {
            return 0;
        }
        let swept_at_ms = now_ms();
        let mut candidates = Vec::new();
        for shard in self.shards.iter() {
            let remaining = limit.saturating_sub(candidates.len());
            if remaining == 0 {
                break;
            }
            candidates.extend(shard.pending_table_splits(remaining, swept_at_ms).await);
        }
        candidates.sort_by(|left, right| {
            left.table_actor_id
                .route_key()
                .cmp(&right.table_actor_id.route_key())
                .then_with(|| left.scope_key.cmp(&right.scope_key))
        });
        candidates.truncate(limit);
        let mut processed = 0usize;
        for candidate in candidates {
            self.split_scope_rows(
                candidate.table_actor_id,
                ActorId::scope(candidate.scope_key),
                split_policy,
            )
            .await;
            processed += 1;
        }
        self.record_split_sweep(swept_at_ms, processed);
        processed
    }

    pub async fn evict_idle_rooms(&self) -> usize {
        let ttl_ms = self.config().hot_room_idle_ttl_ms;
        if ttl_ms == 0 {
            return 0;
        }
        let now = now_ms();
        let evicted = evict_idle_sharded_rooms(&self.shards, ttl_ms, now).await;
        self.decrement_resident_rooms(evicted);
        self.record_idle_sweep(now, evicted);
        evicted
    }

    pub async fn reconfigure(
        &self,
        hot_window: usize,
        max_hot_rooms: usize,
        hot_room_idle_ttl_ms: u64,
    ) {
        {
            let mut config = self
                .config
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            config.hot_window = hot_window;
            config.max_hot_rooms = max_hot_rooms;
            config.hot_room_idle_ttl_ms = hot_room_idle_ttl_ms;
            config.scope_split_rows = actor_scope_split_rows();
            config.scope_split_bytes = actor_scope_split_bytes();
        }
        for shard in self.shards.iter() {
            shard.truncate_all(hot_window).await;
        }
        let now = now_ms();
        let evicted = self.evict_with_config(self.config(), now).await;
        if hot_room_idle_ttl_ms == 0 {
            self.reset_idle_sweep();
        } else {
            self.record_idle_sweep(now, evicted);
        }
    }

    pub async fn snapshot_with_schema(&self, lsn: u64, schema_version: u32) -> ActorSnapshot {
        self.evict_idle_rooms().await;
        let mut snapshot_rooms = HashMap::new();
        let mut actor_states = Vec::new();
        for shard in self.shards.iter() {
            let snapshot = shard.snapshot().await;
            snapshot_rooms.reserve(snapshot.rooms.len());
            actor_states.reserve(snapshot.actor_states.len());
            for (room_id, room) in snapshot.rooms {
                snapshot_rooms.insert(room_id, room);
            }
            actor_states.extend(snapshot.actor_states);
        }
        ActorSnapshot {
            lsn,
            schema_version,
            record_hot: None,
            rooms: snapshot_rooms,
            actor_states,
        }
    }

    fn config(&self) -> ActorRuntimeConfig {
        *self
            .config
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn split_policy(&self) -> ScopeSplitPolicy {
        let config = self.config();
        ScopeSplitPolicy {
            rows: config.scope_split_rows,
            bytes: config.scope_split_bytes,
        }
    }

    fn record_idle_sweep(&self, swept_at_ms: u64, evicted: usize) {
        let mut status = self
            .idle_maintenance
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.last_sweep_at_ms = Some(swept_at_ms);
        status.last_evicted = evicted;
        status.total_evicted = status.total_evicted.saturating_add(evicted);
    }

    fn record_scope_residency_sweep(&self, swept_at_ms: u64, result: ScopeTierDownResult) {
        let mut status = self
            .scope_residency_maintenance
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.last_sweep_at_ms = Some(swept_at_ms);
        status.last_tiered_down = result.tiered_down;
        status.last_cleared_rows = result.cleared_rows;
        status.total_tiered_down = status.total_tiered_down.saturating_add(result.tiered_down);
        status.total_cleared_rows = status
            .total_cleared_rows
            .saturating_add(result.cleared_rows);
    }

    fn record_split_sweep(&self, swept_at_ms: u64, processed: usize) {
        let mut status = self
            .split_maintenance
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.last_sweep_at_ms = Some(swept_at_ms);
        status.last_processed = processed;
        status.total_processed = status.total_processed.saturating_add(processed);
    }

    pub fn record_reminder_sweep(&self, swept_at_ms: u64, fired: usize) {
        let mut status = self
            .reminder_maintenance
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.last_sweep_at_ms = Some(swept_at_ms);
        status.last_fired = fired;
        status.total_fired = status.total_fired.saturating_add(fired);
    }

    fn reset_idle_sweep(&self) {
        let mut status = self
            .idle_maintenance
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *status = ActorIdleMaintenanceStatus::default();
    }

    fn decrement_resident_rooms(&self, evicted: usize) {
        if evicted == 0 {
            return;
        }
        let _ = self.resident_rooms.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |current| Some(current.saturating_sub(evicted)),
        );
    }

    fn shard_for_actor(&self, actor_id: &ActorId) -> &ActorShard {
        &self.shards[actor_shard_index(actor_id, self.shards.len())]
    }
}

impl ActorShard {
    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> ActorShardRequest,
        fallback: T,
    ) -> T {
        let (reply, receive) = oneshot::channel();
        let command = ActorShardCommand {
            request: build(reply),
        };
        if self.tx.send(command).is_err() {
            return fallback;
        }
        receive.await.unwrap_or(fallback)
    }

    async fn apply_messages(
        &self,
        actor_batches: Vec<(ActorId, Vec<Message>)>,
        hot_window: usize,
    ) -> usize {
        self.request(
            |reply| ActorShardRequest::ApplyMessages {
                actor_batches,
                hot_window,
                reply,
            },
            0,
        )
        .await
    }

    async fn run_actor_turn(
        &self,
        actor_id: ActorId,
        message: ActorKernelMessage,
    ) -> ActorTurnResult {
        let fallback = ActorTurnResult {
            actor_id: actor_id.clone(),
            shard_index: self.runtime_state.shard_index,
            created: false,
            turn_count: 0,
            last_accessed_ms: 0,
        };
        self.request(
            |reply| ActorShardRequest::RunActorTurn {
                actor_id,
                message,
                reply,
            },
            fallback,
        )
        .await
    }

    async fn kernel_status(&self) -> ActorKernelStatus {
        self.request(
            |reply| ActorShardRequest::KernelStatus { reply },
            ActorKernelStatus::default(),
        )
        .await
    }

    async fn upsert_scope_rows(
        &self,
        table_actor_id: ActorId,
        actor_id: ActorId,
        rows: Vec<DbRecord>,
    ) -> ScopeRowsActivationResult {
        let fallback = ScopeRowsActivationResult {
            actor_id: actor_id.clone(),
            table_actor_id: table_actor_id.clone(),
            shard_index: self.runtime_state.shard_index,
            created: false,
            requested: rows.len(),
            inserted: 0,
            updated: 0,
            rows: 0,
            bytes: 0,
            table_scopes: 0,
            table_pending_splits: 0,
            scope_split_pending: false,
            scope_split_rows: 0,
            scope_split_bytes: 0,
            turn_count: 0,
            last_accessed_ms: 0,
        };
        self.request(
            |reply| ActorShardRequest::UpsertScopeRows {
                table_actor_id,
                actor_id,
                rows,
                reply,
            },
            fallback,
        )
        .await
    }

    async fn route_scope_rows(
        &self,
        table_actor_id: ActorId,
        scope_key: String,
        rows: Vec<DbRecord>,
    ) -> Vec<(String, Vec<DbRecord>)> {
        self.request(
            |reply| ActorShardRequest::RouteScopeRows {
                table_actor_id,
                scope_key,
                rows,
                reply,
            },
            Vec::new(),
        )
        .await
    }

    async fn upsert_table_scope(
        &self,
        table_actor_id: ActorId,
        scope_key: String,
        rows: usize,
        bytes: usize,
        last_accessed_ms: u64,
        split_policy: ScopeSplitPolicy,
    ) -> TableScopeUpdate {
        self.request(
            |reply| ActorShardRequest::UpsertTableScope {
                table_actor_id,
                scope_key,
                rows,
                bytes,
                last_accessed_ms,
                split_policy,
                reply,
            },
            TableScopeUpdate::default(),
        )
        .await
    }

    async fn drain_scope_rows(&self, actor_id: ActorId) -> ScopeDrainResult {
        self.request(
            |reply| ActorShardRequest::DrainScopeRows { actor_id, reply },
            ScopeDrainResult::default(),
        )
        .await
    }

    async fn retain_scope(
        &self,
        table_actor_id: ActorId,
        actor_id: ActorId,
    ) -> ScopeResidencyResult {
        let fallback = scope_residency_fallback(
            table_actor_id.clone(),
            actor_id.clone(),
            self.runtime_state.shard_index,
        );
        self.request(
            |reply| ActorShardRequest::RetainScope {
                table_actor_id,
                actor_id,
                reply,
            },
            fallback,
        )
        .await
    }

    async fn release_scope(
        &self,
        table_actor_id: ActorId,
        actor_id: ActorId,
        linger_ms: u64,
    ) -> ScopeResidencyResult {
        let fallback = scope_residency_fallback(
            table_actor_id.clone(),
            actor_id.clone(),
            self.runtime_state.shard_index,
        );
        self.request(
            |reply| ActorShardRequest::ReleaseScope {
                table_actor_id,
                actor_id,
                linger_ms,
                reply,
            },
            fallback,
        )
        .await
    }

    async fn tier_down_idle_scopes(&self, now_ms: u64, limit: usize) -> ScopeTierDownResult {
        self.request(
            |reply| ActorShardRequest::TierDownIdleScopes {
                now_ms,
                limit,
                reply,
            },
            ScopeTierDownResult::default(),
        )
        .await
    }

    async fn split_table_scope(
        &self,
        table_actor_id: ActorId,
        parent_scope_key: String,
        children: Vec<TableScopeChildUpdate>,
        last_accessed_ms: u64,
        split_policy: ScopeSplitPolicy,
    ) -> TableScopeUpdate {
        self.request(
            |reply| ActorShardRequest::SplitTableScope {
                table_actor_id,
                parent_scope_key,
                children,
                last_accessed_ms,
                split_policy,
                reply,
            },
            TableScopeUpdate::default(),
        )
        .await
    }

    async fn pending_table_splits(
        &self,
        limit: usize,
        now_ms: u64,
    ) -> Vec<TableScopeSplitCandidate> {
        self.request(
            |reply| ActorShardRequest::PendingTableSplits {
                limit,
                now_ms,
                reply,
            },
            Vec::new(),
        )
        .await
    }

    async fn latest_messages(
        &self,
        actor_id: ActorId,
        before_lsn: Option<u64>,
        limit: usize,
    ) -> Vec<Message> {
        self.request(
            |reply| ActorShardRequest::LatestMessages {
                actor_id,
                before_lsn,
                limit,
                reply,
            },
            Vec::new(),
        )
        .await
    }

    async fn room_statuses(&self) -> Vec<ActorRoomStatus> {
        self.request(
            |reply| ActorShardRequest::RoomStatuses { reply },
            Vec::new(),
        )
        .await
    }

    async fn has_room(&self, actor_id: ActorId) -> bool {
        self.request(
            |reply| ActorShardRequest::HasRoom { actor_id, reply },
            false,
        )
        .await
    }

    async fn evict_room(&self, actor_id: ActorId) -> bool {
        self.request(
            |reply| ActorShardRequest::EvictRoom { actor_id, reply },
            false,
        )
        .await
    }

    async fn truncate_all(&self, hot_window: usize) {
        self.request(
            |reply| ActorShardRequest::TruncateAll { hot_window, reply },
            (),
        )
        .await
    }

    async fn snapshot(&self) -> ActorShardSnapshot {
        self.request(
            |reply| ActorShardRequest::Snapshot { reply },
            ActorShardSnapshot::default(),
        )
        .await
    }

    async fn evict_idle(&self, hot_room_idle_ttl_ms: u64, now_ms: u64) -> usize {
        self.request(
            |reply| ActorShardRequest::EvictIdle {
                hot_room_idle_ttl_ms,
                now_ms,
                reply,
            },
            0,
        )
        .await
    }

    async fn lru_candidates(&self) -> Vec<RoomLruCandidate> {
        self.request(
            |reply| ActorShardRequest::LruCandidates { reply },
            Vec::new(),
        )
        .await
    }

    async fn evict_rooms(&self, actor_ids: Vec<ActorId>) -> usize {
        self.request(
            |reply| ActorShardRequest::EvictRooms { actor_ids, reply },
            0,
        )
        .await
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorRoomStatus {
    pub room_id: String,
    pub messages: usize,
    pub oldest_lsn: Option<u64>,
    pub newest_lsn: Option<u64>,
    pub last_accessed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomLiveState {
    messages: VecDeque<Message>,
    #[serde(default)]
    last_accessed_ms: u64,
}

impl RoomLiveState {
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            last_accessed_ms: next_access_stamp(),
        }
    }

    pub fn apply_message(&mut self, message: Message, hot_window: usize) {
        self.apply_messages(vec![message], hot_window);
    }

    pub fn apply_messages(&mut self, messages: Vec<Message>, hot_window: usize) {
        if messages.is_empty() {
            return;
        }
        self.touch();
        let can_append_in_order = messages_are_sorted_by_order(&messages)
            && self
                .messages
                .back()
                .zip(messages.first())
                .is_none_or(|(last, first)| compare_message_order(last, first).is_le());
        self.messages.extend(messages);
        if !can_append_in_order {
            self.messages
                .make_contiguous()
                .sort_by(compare_message_order);
        }
        self.truncate_to(hot_window);
    }

    fn truncate_to(&mut self, hot_window: usize) {
        while self.messages.len() > hot_window {
            self.messages.pop_front();
        }
    }

    fn latest(&mut self, limit: usize) -> Vec<Message> {
        self.touch();
        self.messages.iter().rev().take(limit).cloned().collect()
    }

    fn before(&mut self, before_lsn: u64, limit: usize) -> Vec<Message> {
        self.touch();
        self.messages
            .iter()
            .rev()
            .filter(|message| message.lsn > 0 && message.lsn < before_lsn)
            .take(limit)
            .cloned()
            .collect()
    }

    fn touch(&mut self) {
        self.last_accessed_ms = next_access_stamp();
    }

    fn status(&self, room_id: &str) -> ActorRoomStatus {
        let mut oldest_lsn = None::<u64>;
        let mut newest_lsn = None::<u64>;
        for message in &self.messages {
            if message.lsn == 0 {
                continue;
            }
            oldest_lsn = Some(oldest_lsn.map_or(message.lsn, |oldest| oldest.min(message.lsn)));
            newest_lsn = Some(newest_lsn.map_or(message.lsn, |newest| newest.max(message.lsn)));
        }
        ActorRoomStatus {
            room_id: room_id.to_string(),
            messages: self.messages.len(),
            oldest_lsn,
            newest_lsn,
            last_accessed_ms: self.last_accessed_ms,
        }
    }
}

fn next_access_stamp() -> u64 {
    let now = now_ms();
    let mut current = ACTOR_ACCESS_SEQ.load(AtomicOrdering::Relaxed);
    loop {
        let next = now.max(current.saturating_add(1));
        match ACTOR_ACCESS_SEQ.compare_exchange(
            current,
            next,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => return next,
            Err(observed) => current = observed,
        }
    }
}

fn compare_message_order(left: &Message, right: &Message) -> Ordering {
    match (left.lsn, right.lsn) {
        (left_lsn, right_lsn) if left_lsn > 0 && right_lsn > 0 => left_lsn.cmp(&right_lsn),
        _ => left
            .created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.id.cmp(&right.id)),
    }
}

fn messages_are_sorted_by_order(messages: &[Message]) -> bool {
    messages
        .windows(2)
        .all(|pair| compare_message_order(&pair[0], &pair[1]).is_le())
}

fn default_actor_shard_count() -> usize {
    std::env::var("NEXTDB_ACTOR_SHARDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1)
}

fn actor_thread_pinning_requested() -> bool {
    std::env::var("NEXTDB_ACTOR_PIN_THREADS")
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(true)
}

fn actor_scope_split_rows() -> usize {
    std::env::var("NEXTDB_ACTOR_SCOPE_SPLIT_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_ACTOR_SCOPE_SPLIT_ROWS)
}

fn actor_scope_split_bytes() -> usize {
    std::env::var("NEXTDB_ACTOR_SCOPE_SPLIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_ACTOR_SCOPE_SPLIT_BYTES)
}

fn actor_scope_split_policy() -> ScopeSplitPolicy {
    ScopeSplitPolicy {
        rows: actor_scope_split_rows(),
        bytes: actor_scope_split_bytes(),
    }
}

fn scope_exceeds_split_policy(rows: usize, bytes: usize, policy: ScopeSplitPolicy) -> bool {
    rows > 1
        && ((policy.rows > 0 && rows > policy.rows) || (policy.bytes > 0 && bytes > policy.bytes))
}

fn scope_rows_estimated_bytes(rows: &BTreeMap<String, DbRecord>) -> usize {
    rows.values().map(record_estimated_bytes).sum()
}

fn records_estimated_bytes(rows: &[DbRecord]) -> usize {
    rows.iter().map(record_estimated_bytes).sum()
}

fn record_estimated_bytes(record: &DbRecord) -> usize {
    record.table.len()
        + record.key.len()
        + record.path.len()
        + record.value.to_string().len()
        + std::mem::size_of_val(&record.updated_at_ms)
        + std::mem::size_of_val(&record.lsn)
}

fn actor_shard_index(actor_id: &ActorId, shard_count: usize) -> usize {
    shard_index(&actor_id.route_key(), shard_count.max(1))
}

pub fn record_actor_table_key(logical_table: &str) -> String {
    format!("table:{logical_table}")
}

pub const RECORD_ACTOR_SCOPE_BUCKET_COUNT: usize = 256;

pub fn record_actor_scope_key(logical_table: &str, record_key: &str) -> String {
    if let Some((parent_key, _)) = record_key.split_once(':')
        && logical_table.contains('.')
    {
        return format!("table:{logical_table}/parent:{parent_key}");
    }
    let bucket = shard_index(record_key, RECORD_ACTOR_SCOPE_BUCKET_COUNT);
    record_actor_scope_bucket_key(logical_table, bucket)
}

pub fn record_actor_scope_bucket_key(logical_table: &str, bucket: usize) -> String {
    let bucket = bucket % RECORD_ACTOR_SCOPE_BUCKET_COUNT;
    format!("table:{logical_table}/bucket:{bucket:02x}")
}

fn split_child_scope_key(parent_scope_key: &str, record_key: &str, child_count: usize) -> String {
    let child_count = child_count.max(1);
    let child_index = split_child_index(parent_scope_key, record_key, child_count);
    format!("{parent_scope_key}/child:{child_index:02x}")
}

fn split_child_index(parent_scope_key: &str, record_key: &str, child_count: usize) -> usize {
    shard_index(
        &format!("{parent_scope_key}:{record_key}"),
        child_count.max(1),
    )
}

fn scope_split_depth(scope_key: &str) -> usize {
    scope_key.matches("/child:").count()
}

fn merge_scope_activation_result(
    aggregate: &mut ScopeRowsActivationResult,
    result: ScopeRowsActivationResult,
) {
    aggregate.created |= result.created;
    aggregate.requested = aggregate.requested.saturating_add(result.requested);
    aggregate.inserted = aggregate.inserted.saturating_add(result.inserted);
    aggregate.updated = aggregate.updated.saturating_add(result.updated);
    aggregate.rows = aggregate.rows.saturating_add(result.rows);
    aggregate.bytes = aggregate.bytes.saturating_add(result.bytes);
    aggregate.table_scopes = result.table_scopes;
    aggregate.table_pending_splits = result.table_pending_splits;
    aggregate.scope_split_pending |= result.scope_split_pending;
    aggregate.scope_split_rows = result.scope_split_rows;
    aggregate.scope_split_bytes = result.scope_split_bytes;
    aggregate.turn_count = aggregate.turn_count.max(result.turn_count);
    aggregate.last_accessed_ms = aggregate.last_accessed_ms.max(result.last_accessed_ms);
}

fn scope_residency_result(
    table_actor_id: ActorId,
    actor_id: ActorId,
    shard_index: usize,
    created: bool,
    scope: &ScopeActorLiveState,
) -> ScopeResidencyResult {
    ScopeResidencyResult {
        actor_id,
        table_actor_id,
        shard_index,
        created,
        subscription_ref_count: scope.subscription_ref_count,
        residency_tier: scope.residency_tier,
        rows: scope.rows.len(),
        bytes: scope_rows_estimated_bytes(&scope.rows),
        lingering_until_ms: scope.lingering_until_ms,
        turn_count: scope.turn_count,
        last_accessed_ms: scope.last_accessed_ms,
    }
}

fn scope_residency_fallback(
    table_actor_id: ActorId,
    actor_id: ActorId,
    shard_index: usize,
) -> ScopeResidencyResult {
    ScopeResidencyResult {
        actor_id,
        table_actor_id,
        shard_index,
        created: false,
        subscription_ref_count: 0,
        residency_tier: ActorResidencyTier::L1Index,
        rows: 0,
        bytes: 0,
        lingering_until_ms: 0,
        turn_count: 0,
        last_accessed_ms: 0,
    }
}

pub fn actor_states_with_wal_tail(
    actor_states: Vec<ActorKernelSnapshotEntry>,
    wal_records: &[WalRecord],
) -> Vec<ActorKernelSnapshotEntry> {
    let mut actors = actor_states
        .into_iter()
        .filter_map(ActorLiveState::from_snapshot_entry)
        .collect::<HashMap<_, _>>();
    apply_wal_tail_to_actor_map(&mut actors, wal_records);
    actors
        .into_iter()
        .filter_map(|(actor_id, actor)| actor.snapshot_entry(&actor_id))
        .collect()
}

pub fn actor_reminders_from_wal_records(wal_records: &[WalRecord]) -> Vec<ActorReminderEntry> {
    let mut ordered = wal_records.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|record| record.lsn);
    let mut wheel = ActorReminderWheel::default();
    for wal_record in ordered {
        match &wal_record.payload {
            WalPayload::ActorReminderScheduled { reminder } => {
                if let Some(entry) = actor_reminder_entry_from_draft(reminder) {
                    wheel.upsert(entry);
                }
            }
            WalPayload::ActorReminderCancelled {
                actor_kind,
                actor_key,
                reminder_id,
                ..
            }
            | WalPayload::ActorReminderFired {
                actor_kind,
                actor_key,
                reminder_id,
                ..
            } => {
                if let Some(actor_id) = actor_id_from_wal_parts(actor_kind, actor_key) {
                    wheel.remove(&actor_id, reminder_id);
                }
            }
            WalPayload::MessageCreated { .. }
            | WalPayload::UserEventPublished { .. }
            | WalPayload::UserUpserted { .. }
            | WalPayload::ObjectCommitted { .. }
            | WalPayload::ObjectDeleted { .. }
            | WalPayload::RecordUpserted { .. }
            | WalPayload::RecordDeleted { .. }
            | WalPayload::RecordTransactionCommitted { .. }
            | WalPayload::SchemaApplied { .. }
            | WalPayload::BehaviorPublished { .. }
            | WalPayload::HostHttpRequested { .. }
            | WalPayload::HostHttpCompleted { .. }
            | WalPayload::ClientMutationRecorded { .. } => {}
        }
    }
    wheel.entries.into_values().collect()
}

pub fn actor_reminder_entry_from_draft(draft: &ActorReminderDraft) -> Option<ActorReminderEntry> {
    Some(ActorReminderEntry {
        actor_id: actor_id_from_wal_parts(&draft.actor_kind, &draft.actor_key)?,
        reminder_id: draft.reminder_id.clone(),
        due_at_ms: draft.due_at_ms,
        payload: draft.payload.clone(),
    })
}

fn actor_id_from_wal_parts(actor_kind: &str, actor_key: &str) -> Option<ActorId> {
    let actor_kind = ActorKind::from_wal_str(actor_kind.trim())?;
    let actor_key = actor_key.trim();
    if actor_key.is_empty() {
        return None;
    }
    Some(ActorId {
        kind: actor_kind,
        key: actor_key.to_string(),
    })
}

fn apply_wal_tail_to_actor_map(
    actors: &mut HashMap<ActorId, ActorLiveState>,
    wal_records: &[WalRecord],
) {
    let mut by_path = BTreeMap::<String, RecordActorWalChange>::new();
    for wal_record in wal_records {
        match &wal_record.payload {
            WalPayload::RecordUpserted { record } => {
                by_path.insert(
                    record.path.clone(),
                    RecordActorWalChange::Upsert(record.clone().into_record(wal_record.lsn)),
                );
            }
            WalPayload::RecordDeleted { record } => {
                by_path.insert(
                    record.path.clone(),
                    RecordActorWalChange::Delete {
                        table: record.table.clone(),
                        key: record.key.clone(),
                    },
                );
            }
            WalPayload::RecordTransactionCommitted { operations, .. } => {
                for operation in operations {
                    match operation {
                        DbRecordMutationDraft::Upsert { record } => {
                            by_path.insert(
                                record.path.clone(),
                                RecordActorWalChange::Upsert(
                                    record.clone().into_record(wal_record.lsn),
                                ),
                            );
                        }
                        DbRecordMutationDraft::Delete { record } => {
                            by_path.insert(
                                record.path.clone(),
                                RecordActorWalChange::Delete {
                                    table: record.table.clone(),
                                    key: record.key.clone(),
                                },
                            );
                        }
                    }
                }
            }
            WalPayload::MessageCreated { .. }
            | WalPayload::UserEventPublished { .. }
            | WalPayload::UserUpserted { .. }
            | WalPayload::ObjectCommitted { .. }
            | WalPayload::ObjectDeleted { .. }
            | WalPayload::SchemaApplied { .. }
            | WalPayload::BehaviorPublished { .. }
            | WalPayload::ActorReminderScheduled { .. }
            | WalPayload::ActorReminderCancelled { .. }
            | WalPayload::ActorReminderFired { .. }
            | WalPayload::HostHttpRequested { .. }
            | WalPayload::HostHttpCompleted { .. }
            | WalPayload::ClientMutationRecorded { .. } => {}
        }
    }
    for change in by_path.into_values() {
        match change {
            RecordActorWalChange::Upsert(record) => {
                apply_record_upsert_to_actor_map(actors, record)
            }
            RecordActorWalChange::Delete { table, key } => {
                apply_record_delete_to_actor_map(actors, &table, &key)
            }
        }
    }
}

enum RecordActorWalChange {
    Upsert(DbRecord),
    Delete { table: String, key: String },
}

fn apply_record_upsert_to_actor_map(
    actors: &mut HashMap<ActorId, ActorLiveState>,
    record: DbRecord,
) {
    let table_actor_id = ActorId::table(record_actor_table_key(&record.table));
    let base_scope_key = record_actor_scope_key(&record.table, &record.key);
    let scope_key = routed_record_scope_key(actors, &table_actor_id, &base_scope_key, &record.key);
    let scope_actor_id = ActorId::scope(scope_key.clone());
    if !actors.contains_key(&table_actor_id)
        && !actors.contains_key(&scope_actor_id)
        && !actors.contains_key(&ActorId::scope(base_scope_key))
    {
        return;
    }
    let (rows, bytes, last_accessed_ms) = {
        let actor = actors
            .entry(scope_actor_id.clone())
            .or_insert_with(|| ActorLiveState::new_for_actor(&scope_actor_id));
        match actor {
            ActorLiveState::Scope(scope) => {
                scope.rows.insert(record.key.clone(), record);
                scope.touch();
                (
                    scope.rows.len(),
                    scope_rows_estimated_bytes(&scope.rows),
                    scope.last_accessed_ms,
                )
            }
            ActorLiveState::Room(_) | ActorLiveState::Table(_) | ActorLiveState::Kernel(_) => {
                return;
            }
        }
    };
    let actor = actors
        .entry(table_actor_id.clone())
        .or_insert_with(|| ActorLiveState::new_for_actor(&table_actor_id));
    if let ActorLiveState::Table(table) = actor {
        table.upsert_scope(
            scope_key,
            rows,
            bytes,
            last_accessed_ms,
            actor_scope_split_policy(),
        );
    }
}

fn apply_record_delete_to_actor_map(
    actors: &mut HashMap<ActorId, ActorLiveState>,
    table: &str,
    key: &str,
) {
    let table_actor_id = ActorId::table(record_actor_table_key(table));
    let base_scope_key = record_actor_scope_key(table, key);
    let scope_key = routed_record_scope_key(actors, &table_actor_id, &base_scope_key, key);
    let scope_actor_id = ActorId::scope(scope_key.clone());
    if !actors.contains_key(&table_actor_id)
        && !actors.contains_key(&scope_actor_id)
        && !actors.contains_key(&ActorId::scope(base_scope_key))
    {
        return;
    }
    let Some(actor) = actors.get_mut(&scope_actor_id) else {
        return;
    };
    let (rows, bytes, last_accessed_ms) = match actor {
        ActorLiveState::Scope(scope) => {
            if scope.rows.remove(key).is_none() {
                return;
            }
            scope.touch();
            (
                scope.rows.len(),
                scope_rows_estimated_bytes(&scope.rows),
                scope.last_accessed_ms,
            )
        }
        ActorLiveState::Room(_) | ActorLiveState::Table(_) | ActorLiveState::Kernel(_) => return,
    };
    let actor = actors
        .entry(table_actor_id.clone())
        .or_insert_with(|| ActorLiveState::new_for_actor(&table_actor_id));
    if let ActorLiveState::Table(table) = actor {
        table.upsert_scope(
            scope_key,
            rows,
            bytes,
            last_accessed_ms,
            actor_scope_split_policy(),
        );
    }
}

fn routed_record_scope_key(
    actors: &HashMap<ActorId, ActorLiveState>,
    table_actor_id: &ActorId,
    base_scope_key: &str,
    record_key: &str,
) -> String {
    actors.get(table_actor_id).map_or_else(
        || base_scope_key.to_string(),
        |actor| match actor {
            ActorLiveState::Table(table) => {
                table.route_record_scope_key(base_scope_key, record_key)
            }
            ActorLiveState::Room(_) | ActorLiveState::Scope(_) | ActorLiveState::Kernel(_) => {
                base_scope_key.to_string()
            }
        },
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorSnapshot {
    pub lsn: u64,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_hot: Option<RecordHotSnapshot>,
    pub rooms: HashMap<String, RoomSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actor_states: Vec<ActorKernelSnapshotEntry>,
}

fn default_schema_version() -> u32 {
    1
}

#[derive(Debug, Clone)]
pub struct ActorSnapshotRuntimeState {
    pub rooms: HashMap<String, RoomLiveState>,
    pub actor_states: Vec<ActorKernelSnapshotEntry>,
}

impl ActorSnapshot {
    #[allow(dead_code)]
    pub fn into_rooms(self) -> HashMap<String, RoomLiveState> {
        self.into_runtime_state().rooms
    }

    pub fn into_runtime_state(self) -> ActorSnapshotRuntimeState {
        let restored_at = now_ms();
        let rooms = self
            .rooms
            .into_iter()
            .map(|(room_id, room)| {
                (
                    room_id,
                    RoomLiveState {
                        messages: room.messages.into(),
                        last_accessed_ms: if room.last_accessed_ms == 0 {
                            restored_at
                        } else {
                            room.last_accessed_ms
                        },
                    },
                )
            })
            .collect();
        ActorSnapshotRuntimeState {
            rooms,
            actor_states: self.actor_states,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorKernelSnapshotEntry {
    pub actor_id: ActorId,
    pub state: ActorKernelSnapshotState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stateKind", rename_all = "camelCase")]
pub enum ActorKernelSnapshotState {
    Scope {
        rows: Vec<DbRecord>,
        turn_count: u64,
        last_accessed_ms: u64,
        #[serde(default)]
        subscription_ref_count: usize,
        #[serde(default)]
        lingering_until_ms: u64,
        #[serde(default)]
        residency_tier: ActorResidencyTier,
    },
    Table {
        scopes: Vec<TableScopeSnapshotEntry>,
        turn_count: u64,
        last_accessed_ms: u64,
    },
    Kernel {
        turn_count: u64,
        last_accessed_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableScopeSnapshotEntry {
    pub scope_key: String,
    pub rows: usize,
    #[serde(default)]
    pub bytes: usize,
    pub last_accessed_ms: u64,
    #[serde(default)]
    pub split_pending: bool,
    #[serde(default)]
    pub split_rows: usize,
    #[serde(default)]
    pub split_bytes: usize,
    #[serde(default)]
    pub split_reminder_at_ms: u64,
    #[serde(default)]
    pub child_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomSnapshot {
    pub messages: Vec<Message>,
    #[serde(default)]
    pub last_accessed_ms: u64,
}

fn run_actor_shard(
    runtime_state: ActorShardRuntimeState,
    mut actors: HashMap<ActorId, ActorLiveState>,
    rx: std_mpsc::Receiver<ActorShardCommand>,
) {
    if runtime_state.pinning_requested
        && let Some(core_id) = runtime_state
            .target_core_id
            .map(|id| core_affinity::CoreId { id })
    {
        runtime_state.pinning_succeeded.store(
            core_affinity::set_for_current(core_id),
            AtomicOrdering::Relaxed,
        );
    }

    while let Ok(command) = rx.recv() {
        match command.request {
            ActorShardRequest::RunActorTurn {
                actor_id,
                message,
                reply,
            } => {
                let created = !actors.contains_key(&actor_id);
                let actor = actors
                    .entry(actor_id.clone())
                    .or_insert_with(|| ActorLiveState::new_for_actor(&actor_id));
                let (turn_count, last_accessed_ms) = actor.run_kernel_message(message);
                let _ = reply.send(ActorTurnResult {
                    actor_id,
                    shard_index: runtime_state.shard_index,
                    created,
                    turn_count,
                    last_accessed_ms,
                });
            }
            ActorShardRequest::UpsertScopeRows {
                table_actor_id,
                actor_id,
                rows,
                reply,
            } => {
                let requested = rows.len();
                let created = !actors.contains_key(&actor_id);
                let (inserted, updated, row_count, byte_count, turn_count, last_accessed_ms) = {
                    let actor = actors
                        .entry(actor_id.clone())
                        .or_insert_with(|| ActorLiveState::new_for_actor(&actor_id));
                    match actor {
                        ActorLiveState::Scope(scope) => {
                            let (inserted, updated) = scope.upsert_rows(rows);
                            (
                                inserted,
                                updated,
                                scope.rows.len(),
                                scope_rows_estimated_bytes(&scope.rows),
                                scope.turn_count,
                                scope.last_accessed_ms,
                            )
                        }
                        ActorLiveState::Room(_)
                        | ActorLiveState::Table(_)
                        | ActorLiveState::Kernel(_) => (0, 0, 0, 0, 0, 0),
                    }
                };
                let _ = reply.send(ScopeRowsActivationResult {
                    actor_id,
                    table_actor_id,
                    shard_index: runtime_state.shard_index,
                    created,
                    requested,
                    inserted,
                    updated,
                    rows: row_count,
                    bytes: byte_count,
                    table_scopes: 0,
                    table_pending_splits: 0,
                    scope_split_pending: false,
                    scope_split_rows: 0,
                    scope_split_bytes: 0,
                    turn_count,
                    last_accessed_ms,
                });
            }
            ActorShardRequest::RouteScopeRows {
                table_actor_id,
                scope_key,
                rows,
                reply,
            } => {
                let routed = match actors.get(&table_actor_id) {
                    Some(ActorLiveState::Table(table)) => table.route_rows(scope_key, rows),
                    Some(
                        ActorLiveState::Room(_)
                        | ActorLiveState::Scope(_)
                        | ActorLiveState::Kernel(_),
                    )
                    | None => vec![(scope_key, rows)],
                };
                let _ = reply.send(routed);
            }
            ActorShardRequest::UpsertTableScope {
                table_actor_id,
                scope_key,
                rows,
                bytes,
                last_accessed_ms,
                split_policy,
                reply,
            } => {
                let actor = actors
                    .entry(table_actor_id.clone())
                    .or_insert_with(|| ActorLiveState::new_for_actor(&table_actor_id));
                let update = match actor {
                    ActorLiveState::Table(table) => {
                        table.upsert_scope(scope_key, rows, bytes, last_accessed_ms, split_policy)
                    }
                    ActorLiveState::Room(_)
                    | ActorLiveState::Scope(_)
                    | ActorLiveState::Kernel(_) => TableScopeUpdate::default(),
                };
                let _ = reply.send(update);
            }
            ActorShardRequest::DrainScopeRows { actor_id, reply } => {
                let drained =
                    actors
                        .get_mut(&actor_id)
                        .map_or_else(ScopeDrainResult::default, |actor| match actor {
                            ActorLiveState::Scope(scope) => {
                                let rows = std::mem::take(&mut scope.rows)
                                    .into_values()
                                    .collect::<Vec<_>>();
                                scope.touch();
                                ScopeDrainResult {
                                    rows,
                                    last_accessed_ms: scope.last_accessed_ms,
                                }
                            }
                            ActorLiveState::Room(_)
                            | ActorLiveState::Table(_)
                            | ActorLiveState::Kernel(_) => ScopeDrainResult::default(),
                        });
                let _ = reply.send(drained);
            }
            ActorShardRequest::RetainScope {
                table_actor_id,
                actor_id,
                reply,
            } => {
                let created = !actors.contains_key(&actor_id);
                let result = {
                    let actor = actors
                        .entry(actor_id.clone())
                        .or_insert_with(|| ActorLiveState::new_for_actor(&actor_id));
                    match actor {
                        ActorLiveState::Scope(scope) => {
                            scope.retain_subscription();
                            scope_residency_result(
                                table_actor_id.clone(),
                                actor_id.clone(),
                                runtime_state.shard_index,
                                created,
                                scope,
                            )
                        }
                        ActorLiveState::Room(_)
                        | ActorLiveState::Table(_)
                        | ActorLiveState::Kernel(_) => scope_residency_fallback(
                            table_actor_id.clone(),
                            actor_id.clone(),
                            runtime_state.shard_index,
                        ),
                    }
                };
                let _ = reply.send(result);
            }
            ActorShardRequest::ReleaseScope {
                table_actor_id,
                actor_id,
                linger_ms,
                reply,
            } => {
                let result = actors.get_mut(&actor_id).map_or_else(
                    || {
                        scope_residency_fallback(
                            table_actor_id.clone(),
                            actor_id.clone(),
                            runtime_state.shard_index,
                        )
                    },
                    |actor| match actor {
                        ActorLiveState::Scope(scope) => {
                            scope.release_subscription(linger_ms);
                            scope_residency_result(
                                table_actor_id.clone(),
                                actor_id.clone(),
                                runtime_state.shard_index,
                                false,
                                scope,
                            )
                        }
                        ActorLiveState::Room(_)
                        | ActorLiveState::Table(_)
                        | ActorLiveState::Kernel(_) => scope_residency_fallback(
                            table_actor_id.clone(),
                            actor_id.clone(),
                            runtime_state.shard_index,
                        ),
                    },
                );
                let _ = reply.send(result);
            }
            ActorShardRequest::TierDownIdleScopes {
                now_ms,
                limit,
                reply,
            } => {
                let mut result = ScopeTierDownResult::default();
                if limit > 0 {
                    let mut scope_actor_ids = actors
                        .iter()
                        .filter_map(|(actor_id, actor)| match actor {
                            ActorLiveState::Scope(scope)
                                if scope.subscription_ref_count == 0
                                    && scope.lingering_until_ms > 0
                                    && scope.lingering_until_ms <= now_ms =>
                            {
                                Some(actor_id.clone())
                            }
                            ActorLiveState::Scope(_)
                            | ActorLiveState::Room(_)
                            | ActorLiveState::Table(_)
                            | ActorLiveState::Kernel(_) => None,
                        })
                        .collect::<Vec<_>>();
                    scope_actor_ids.sort_by_key(|actor_id| actor_id.route_key());
                    for actor_id in scope_actor_ids.into_iter().take(limit) {
                        let Some(ActorLiveState::Scope(scope)) = actors.get_mut(&actor_id) else {
                            continue;
                        };
                        let cleared_rows = scope.tier_down_if_idle(now_ms);
                        result.tiered_down = result.tiered_down.saturating_add(1);
                        result.cleared_rows = result.cleared_rows.saturating_add(cleared_rows);
                    }
                }
                let _ = reply.send(result);
            }
            ActorShardRequest::SplitTableScope {
                table_actor_id,
                parent_scope_key,
                children,
                last_accessed_ms,
                split_policy,
                reply,
            } => {
                let actor = actors
                    .entry(table_actor_id.clone())
                    .or_insert_with(|| ActorLiveState::new_for_actor(&table_actor_id));
                let update = match actor {
                    ActorLiveState::Table(table) => table.split_scope(
                        parent_scope_key,
                        children,
                        split_policy,
                        last_accessed_ms,
                    ),
                    ActorLiveState::Room(_)
                    | ActorLiveState::Scope(_)
                    | ActorLiveState::Kernel(_) => TableScopeUpdate::default(),
                };
                let _ = reply.send(update);
            }
            ActorShardRequest::PendingTableSplits {
                limit,
                now_ms,
                reply,
            } => {
                let mut candidates = Vec::new();
                if limit > 0 {
                    let mut table_actor_ids = actors
                        .iter()
                        .filter_map(|(actor_id, actor)| match actor {
                            ActorLiveState::Table(_) => Some(actor_id.clone()),
                            ActorLiveState::Room(_)
                            | ActorLiveState::Scope(_)
                            | ActorLiveState::Kernel(_) => None,
                        })
                        .collect::<Vec<_>>();
                    table_actor_ids.sort_by_key(|actor_id| actor_id.route_key());
                    for table_actor_id in table_actor_ids {
                        if candidates.len() >= limit {
                            break;
                        }
                        let Some(ActorLiveState::Table(table)) = actors.get(&table_actor_id) else {
                            continue;
                        };
                        for scope_key in table.pending_split_scope_keys(
                            limit.saturating_sub(candidates.len()),
                            now_ms,
                        ) {
                            candidates.push(TableScopeSplitCandidate {
                                table_actor_id: table_actor_id.clone(),
                                scope_key,
                            });
                        }
                    }
                }
                let _ = reply.send(candidates);
            }
            ActorShardRequest::KernelStatus { reply } => {
                let _ = reply.send(ActorKernelStatus::from_actors(&actors));
            }
            ActorShardRequest::ApplyMessages {
                actor_batches,
                hot_window,
                reply,
            } => {
                let mut new_rooms = 0;
                for (actor_id, messages) in actor_batches {
                    if !actors.contains_key(&actor_id) && actor_id.kind == ActorKind::Room {
                        new_rooms += 1;
                    }
                    match actors
                        .entry(actor_id.clone())
                        .or_insert_with(|| ActorLiveState::new_for_actor(&actor_id))
                    {
                        ActorLiveState::Room(room) => room.apply_messages(messages, hot_window),
                        ActorLiveState::Scope(_) => {}
                        ActorLiveState::Table(_) => {}
                        ActorLiveState::Kernel(_) => {}
                    }
                }
                let _ = reply.send(new_rooms);
            }
            ActorShardRequest::LatestMessages {
                actor_id,
                before_lsn,
                limit,
                reply,
            } => {
                let messages =
                    actors
                        .get_mut(&actor_id)
                        .map_or_else(Vec::new, |actor| match actor {
                            ActorLiveState::Room(room) => match before_lsn {
                                Some(before_lsn) => room.before(before_lsn, limit),
                                None => room.latest(limit),
                            },
                            ActorLiveState::Scope(_) => Vec::new(),
                            ActorLiveState::Table(_) => Vec::new(),
                            ActorLiveState::Kernel(_) => Vec::new(),
                        });
                let _ = reply.send(messages);
            }
            ActorShardRequest::RoomStatuses { reply } => {
                let statuses = actors
                    .iter()
                    .filter_map(|(actor_id, actor)| match actor {
                        ActorLiveState::Room(room) => Some(room.status(&actor_id.key)),
                        ActorLiveState::Scope(_) => None,
                        ActorLiveState::Table(_) => None,
                        ActorLiveState::Kernel(_) => None,
                    })
                    .collect();
                let _ = reply.send(statuses);
            }
            ActorShardRequest::HasRoom { actor_id, reply } => {
                let _ = reply.send(actors.contains_key(&actor_id));
            }
            ActorShardRequest::EvictRoom { actor_id, reply } => {
                let _ = reply.send(actors.remove(&actor_id).is_some());
            }
            ActorShardRequest::TruncateAll { hot_window, reply } => {
                for actor in actors.values_mut() {
                    match actor {
                        ActorLiveState::Room(room) => room.truncate_to(hot_window),
                        ActorLiveState::Scope(_) => {}
                        ActorLiveState::Table(_) => {}
                        ActorLiveState::Kernel(_) => {}
                    }
                }
                let _ = reply.send(());
            }
            ActorShardRequest::Snapshot { reply } => {
                let mut snapshot = ActorShardSnapshot::default();
                for (actor_id, actor) in &actors {
                    match actor {
                        ActorLiveState::Room(room) => {
                            snapshot.rooms.push((
                                actor_id.key.clone(),
                                RoomSnapshot {
                                    messages: room.messages.iter().cloned().collect(),
                                    last_accessed_ms: room.last_accessed_ms,
                                },
                            ));
                        }
                        ActorLiveState::Scope(_)
                        | ActorLiveState::Table(_)
                        | ActorLiveState::Kernel(_) => {
                            if let Some(entry) = actor.snapshot_entry(actor_id) {
                                snapshot.actor_states.push(entry);
                            }
                        }
                    }
                }
                let _ = reply.send(snapshot);
            }
            ActorShardRequest::EvictIdle {
                hot_room_idle_ttl_ms,
                now_ms,
                reply,
            } => {
                let before = actors.len();
                actors.retain(|_, actor| {
                    !actor.is_room()
                        || now_ms.saturating_sub(actor.last_accessed_ms()) <= hot_room_idle_ttl_ms
                });
                let evicted = before - actors.len();
                let _ = reply.send(evicted);
            }
            ActorShardRequest::LruCandidates { reply } => {
                let candidates = actors
                    .iter()
                    .filter_map(|(actor_id, actor)| match actor {
                        ActorLiveState::Room(_) => Some(RoomLruCandidate {
                            shard_index: runtime_state.shard_index,
                            actor_id: actor_id.clone(),
                            last_accessed_ms: actor.last_accessed_ms(),
                        }),
                        ActorLiveState::Scope(_) => None,
                        ActorLiveState::Table(_) => None,
                        ActorLiveState::Kernel(_) => None,
                    })
                    .collect();
                let _ = reply.send(candidates);
            }
            ActorShardRequest::EvictRooms { actor_ids, reply } => {
                let mut evicted = 0;
                for actor_id in actor_ids {
                    if actors.remove(&actor_id).is_some() {
                        evicted += 1;
                    }
                }
                let _ = reply.send(evicted);
            }
        }
    }
}
fn evict_lru_rooms(rooms: &mut HashMap<String, RoomLiveState>, max_hot_rooms: usize) {
    let overflow = rooms.len().saturating_sub(max_hot_rooms);
    if overflow == 0 {
        return;
    }
    if max_hot_rooms == 0 {
        rooms.clear();
        return;
    }

    let mut candidates = rooms
        .iter()
        .map(|(room_id, room)| (room.last_accessed_ms, room_id.clone()))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    for (_, room_id) in candidates.into_iter().take(overflow) {
        rooms.remove(&room_id);
    }
}

async fn evict_lru_sharded_rooms(shards: &[ActorShard], max_hot_rooms: usize) -> usize {
    let mut candidates = Vec::new();
    for shard in shards {
        candidates.extend(shard.lru_candidates().await);
    }

    let overflow = candidates.len().saturating_sub(max_hot_rooms);
    if overflow == 0 {
        return 0;
    }
    if max_hot_rooms == 0 {
        let total_rooms = candidates.len();
        for shard in shards {
            let actor_ids = shard
                .lru_candidates()
                .await
                .into_iter()
                .map(|candidate| candidate.actor_id)
                .collect();
            shard.evict_rooms(actor_ids).await;
        }
        return total_rooms;
    }

    candidates.sort_by(|left, right| {
        left.last_accessed_ms
            .cmp(&right.last_accessed_ms)
            .then_with(|| left.actor_id.key.cmp(&right.actor_id.key))
            .then_with(|| left.shard_index.cmp(&right.shard_index))
    });
    let mut by_shard: HashMap<usize, Vec<ActorId>> = HashMap::new();
    for candidate in candidates.into_iter().take(overflow) {
        by_shard
            .entry(candidate.shard_index)
            .or_default()
            .push(candidate.actor_id);
    }
    let mut evicted = 0;
    for (shard_index, actor_ids) in by_shard {
        evicted += shards[shard_index].evict_rooms(actor_ids).await;
    }
    evicted
}

fn evict_idle_rooms(
    rooms: &mut HashMap<String, RoomLiveState>,
    hot_room_idle_ttl_ms: u64,
    now_ms: u64,
) -> usize {
    if hot_room_idle_ttl_ms == 0 {
        return 0;
    }
    let before = rooms.len();
    rooms.retain(|_, room| now_ms.saturating_sub(room.last_accessed_ms) <= hot_room_idle_ttl_ms);
    before - rooms.len()
}

async fn evict_idle_sharded_rooms(
    shards: &[ActorShard],
    hot_room_idle_ttl_ms: u64,
    now_ms: u64,
) -> usize {
    if hot_room_idle_ttl_ms == 0 {
        return 0;
    }
    let mut evicted = 0;
    for shard in shards {
        evicted += shard.evict_idle(hot_room_idle_ttl_ms, now_ms).await;
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_batch_apply_sorts_by_lsn_and_keeps_hot_window() {
        let mut room = RoomLiveState::new();
        room.apply_messages(
            vec![
                message("room-a", "message-3", 3),
                message("room-a", "message-1", 1),
                message("room-a", "message-2", 2),
            ],
            2,
        );

        assert_eq!(
            room.latest(3)
                .into_iter()
                .map(|message| message.body)
                .collect::<Vec<_>>(),
            vec!["message-3", "message-2"]
        );
    }

    #[test]
    fn room_batch_apply_appends_ordered_messages_without_reordering_window() {
        let mut room = RoomLiveState::new();
        room.apply_messages(
            vec![
                message("room-a", "message-1", 1),
                message("room-a", "message-2", 2),
            ],
            4,
        );
        room.apply_messages(
            vec![
                message("room-a", "message-3", 3),
                message("room-a", "message-4", 4),
            ],
            4,
        );

        assert_eq!(
            room.latest(4)
                .into_iter()
                .map(|message| message.body)
                .collect::<Vec<_>>(),
            vec!["message-4", "message-3", "message-2", "message-1"]
        );
    }

    #[test]
    fn room_batch_apply_falls_back_to_sort_for_backfilled_messages() {
        let mut room = RoomLiveState::new();
        room.apply_messages(
            vec![
                message("room-a", "message-3", 3),
                message("room-a", "message-4", 4),
            ],
            4,
        );
        room.apply_messages(vec![message("room-a", "message-2", 2)], 4);

        assert_eq!(
            room.latest(4)
                .into_iter()
                .map(|message| message.body)
                .collect::<Vec<_>>(),
            vec!["message-4", "message-3", "message-2"]
        );
    }

    #[test]
    fn room_status_computes_durable_lsn_bounds_without_volatile_messages() {
        let mut room = RoomLiveState::new();
        let mut volatile = message("room-a", "volatile", 0);
        volatile.created_at_ms = 5;
        room.apply_messages(
            vec![
                message("room-a", "message-3", 3),
                volatile,
                message("room-a", "message-1", 1),
            ],
            10,
        );

        let status = room.status("room-a");

        assert_eq!(status.messages, 3);
        assert_eq!(status.oldest_lsn, Some(1));
        assert_eq!(status.newest_lsn, Some(3));
    }

    #[tokio::test]
    async fn runtime_batch_apply_updates_rooms_with_one_lru_pass() {
        let runtime = ActorRuntime::new(HashMap::new(), 4, 1, 0);
        runtime
            .apply_messages(vec![
                message("room-a", "a-1", 1),
                message("room-a", "a-2", 2),
            ])
            .await;
        assert_eq!(runtime.room_count().await, 1);

        runtime
            .apply_messages(vec![
                message("room-b", "b-1", 3),
                message("room-b", "b-2", 4),
            ])
            .await;
        assert_eq!(runtime.room_count().await, 1);
        assert_eq!(
            runtime
                .latest_messages("room-b", None, 2)
                .await
                .into_iter()
                .map(|message| message.body)
                .collect::<Vec<_>>(),
            vec!["b-2", "b-1"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn runtime_concurrent_room_activation_keeps_rooms_independent() {
        let runtime = ActorRuntime::new(HashMap::new(), 16, 64, 0);
        let mut tasks = Vec::new();
        for room_index in 0..16_u64 {
            for message_index in 0..8_u64 {
                let runtime = runtime.clone();
                tasks.push(tokio::spawn(async move {
                    let room_id = format!("room-{room_index}");
                    let lsn = room_index * 100 + message_index + 1;
                    runtime
                        .apply_message(message(&room_id, &format!("message-{message_index}"), lsn))
                        .await;
                }));
            }
        }
        for task in tasks {
            task.await.expect("actor task");
        }

        assert_eq!(runtime.room_count().await, 16);
        for room_index in 0..16_u64 {
            let room_id = format!("room-{room_index}");
            let latest = runtime.latest_messages(&room_id, None, 8).await;
            assert_eq!(
                latest
                    .into_iter()
                    .map(|message| message.body)
                    .collect::<Vec<_>>(),
                (0..8_u64)
                    .rev()
                    .map(|message_index| format!("message-{message_index}"))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn runtime_routes_rooms_to_independent_shards() {
        let shard_count = 4;
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, shard_count);
        let mut rooms_by_shard = HashMap::<usize, String>::new();
        let mut candidate = 0;
        while rooms_by_shard.len() < shard_count {
            let room_id = format!("room-{candidate}");
            rooms_by_shard
                .entry(actor_shard_index(&ActorId::room(&room_id), shard_count))
                .or_insert(room_id);
            candidate += 1;
        }

        for (shard_index, room_id) in rooms_by_shard.values().enumerate() {
            runtime
                .apply_message(message(room_id, &format!("message-{shard_index}"), 1))
                .await;
        }

        assert_eq!(runtime.room_count().await, shard_count);
        for room_id in rooms_by_shard.values() {
            assert_eq!(runtime.latest_messages(room_id, None, 1).await.len(), 1);
        }
    }

    #[test]
    fn runtime_starts_shards_without_tokio_runtime() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        assert_eq!(runtime.resident_rooms.load(AtomicOrdering::Relaxed), 0);
        let shards = runtime.shard_statuses();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].thread_name, "nextdb-actor-shard-0");
        assert_eq!(shards[1].thread_name, "nextdb-actor-shard-1");
    }

    #[test]
    fn actor_id_route_key_includes_kind_and_key() {
        let actor_id = ActorId::room("general");
        assert_eq!(actor_id.route_key(), "room:general");
        assert_eq!(
            actor_shard_index(&actor_id, 8),
            shard_index("room:general", 8)
        );
    }

    #[test]
    fn actor_id_route_key_separates_actor_kinds() {
        assert_eq!(ActorId::scope("general").route_key(), "scope:general");
        assert_eq!(ActorId::table("general").route_key(), "table:general");
        assert_eq!(ActorId::view("general").route_key(), "view:general");
        assert_eq!(
            ActorId::aggregate("general").route_key(),
            "aggregate:general"
        );
        assert_ne!(
            ActorId::room("general").route_key(),
            ActorId::scope("general").route_key()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn runtime_generic_actor_turns_are_serial_and_do_not_count_as_rooms() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 1, 1, 4);
        let actor_id = ActorId::scope("rooms/general");
        let shard_index = actor_shard_index(&actor_id, 4);
        let mut tasks = Vec::new();

        for _ in 0..16 {
            let runtime = runtime.clone();
            let actor_id = actor_id.clone();
            tasks.push(tokio::spawn(async move {
                runtime
                    .run_actor_turn(actor_id, ActorKernelMessage::Touch)
                    .await
            }));
        }

        let mut turn_counts = Vec::new();
        for task in tasks {
            let result = task.await.expect("actor turn task");
            assert_eq!(result.actor_id, actor_id);
            assert_eq!(result.shard_index, shard_index);
            assert!(result.last_accessed_ms > 0);
            turn_counts.push(result.turn_count);
        }
        turn_counts.sort_unstable();

        assert_eq!(turn_counts, (1..=16).collect::<Vec<_>>());
        assert_eq!(runtime.room_count().await, 0);
        assert_eq!(runtime.evict_idle_rooms().await, 0);
        assert_eq!(runtime.room_count().await, 0);
    }

    #[tokio::test]
    async fn runtime_kernel_status_counts_actor_kinds() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        runtime.apply_message(message("general", "hello", 1)).await;
        runtime
            .run_actor_turn(ActorId::scope("rooms/general"), ActorKernelMessage::Touch)
            .await;
        runtime
            .run_actor_turn(ActorId::view("rooms/recent"), ActorKernelMessage::Touch)
            .await;

        let status = runtime.kernel_status().await;

        assert_eq!(status.total_actors, 3);
        assert_eq!(status.room_actors, 1);
        assert_eq!(status.kernel_actors, 2);
        assert_eq!(status.kind_counts.get("room"), Some(&1));
        assert_eq!(status.kind_counts.get("scope"), Some(&1));
        assert_eq!(status.kind_counts.get("view"), Some(&1));
        assert!(status.oldest_accessed_ms.is_some());
        assert!(status.newest_accessed_ms.is_some());
    }

    #[tokio::test]
    async fn runtime_scope_actor_owns_rows_on_shard_thread() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);

        let first = runtime
            .upsert_scope_rows(
                "table:rooms",
                "table:rooms",
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                ],
            )
            .await;
        let second = runtime
            .upsert_scope_rows(
                "table:rooms",
                "table:rooms",
                vec![record("rooms", "room-a", "Room A+", 3)],
            )
            .await;
        let status = runtime.kernel_status().await;

        assert!(first.created);
        assert_eq!(first.actor_id, ActorId::scope("table:rooms"));
        assert_eq!(first.table_actor_id, ActorId::table("table:rooms"));
        assert_eq!(first.requested, 2);
        assert_eq!(first.inserted, 2);
        assert_eq!(first.updated, 0);
        assert_eq!(first.rows, 2);
        assert_eq!(first.table_scopes, 1);
        assert!(!second.created);
        assert_eq!(second.inserted, 0);
        assert_eq!(second.updated, 1);
        assert_eq!(second.rows, 2);
        assert_eq!(second.table_scopes, 1);
        assert_eq!(second.turn_count, 2);
        assert_eq!(status.kind_counts.get("table"), Some(&1));
        assert_eq!(status.kind_counts.get("scope"), Some(&1));
        assert_eq!(status.kernel_actors, 2);
        assert_eq!(status.scope_rows, 2);
        assert_eq!(status.table_scopes, 1);
        assert_eq!(runtime.room_count().await, 0);
    }

    #[test]
    fn table_actor_marks_scope_split_pending_when_threshold_is_exceeded() {
        let mut table = TableActorLiveState::new();
        let update = table.upsert_scope(
            "table:rooms/bucket:00".to_string(),
            2,
            20,
            10,
            ScopeSplitPolicy { rows: 1, bytes: 0 },
        );

        assert_eq!(update.table_scopes, 1);
        assert_eq!(update.table_pending_splits, 1);
        assert!(update.scope_split_pending);
        assert_eq!(update.scope_split_rows, 1);
        assert_eq!(update.scope_split_bytes, 0);
        assert_eq!(table.pending_split_count(), 1);
        assert!(table.scopes["table:rooms/bucket:00"].split_pending);
    }

    #[test]
    fn table_actor_marks_scope_split_pending_when_byte_threshold_is_exceeded() {
        let mut table = TableActorLiveState::new();
        let update = table.upsert_scope(
            "table:rooms/bucket:00".to_string(),
            2,
            512,
            10,
            ScopeSplitPolicy {
                rows: 0,
                bytes: 128,
            },
        );

        assert_eq!(update.table_scopes, 1);
        assert_eq!(update.table_pending_splits, 1);
        assert!(update.scope_split_pending);
        assert_eq!(update.scope_split_rows, 0);
        assert_eq!(update.scope_split_bytes, 128);
        assert_eq!(table.scopes["table:rooms/bucket:00"].bytes, 512);
        assert_eq!(table.scopes["table:rooms/bucket:00"].split_bytes, 128);
    }

    #[tokio::test]
    async fn runtime_snapshot_restores_scope_and_table_actor_state() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        runtime
            .upsert_scope_rows(
                "table:rooms",
                "table:rooms/bucket:00",
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                ],
            )
            .await;
        runtime
            .run_actor_turn(ActorId::view("rooms/recent"), ActorKernelMessage::Touch)
            .await;

        let snapshot = runtime.snapshot_with_schema(2, 1).await;
        assert_eq!(snapshot.rooms.len(), 0);
        assert_eq!(snapshot.actor_states.len(), 3);
        let restored = snapshot.into_runtime_state();
        let restored_runtime = ActorRuntime::new_with_shard_count_and_actor_states(
            restored.rooms,
            restored.actor_states,
            Vec::new(),
            4,
            64,
            0,
            2,
        );
        let status = restored_runtime.kernel_status().await;

        assert_eq!(status.room_actors, 0);
        assert_eq!(status.kernel_actors, 3);
        assert_eq!(status.kind_counts.get("scope"), Some(&1));
        assert_eq!(status.kind_counts.get("table"), Some(&1));
        assert_eq!(status.kind_counts.get("view"), Some(&1));
        assert_eq!(status.scope_rows, 2);
        assert_eq!(status.table_scopes, 1);
        assert_eq!(restored_runtime.room_count().await, 0);
    }

    #[tokio::test]
    async fn scope_subscription_refcount_survives_snapshot_restore() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        runtime
            .upsert_scope_rows(
                "table:rooms",
                "table:rooms/bucket:00",
                vec![record("rooms", "room-a", "Room A", 1)],
            )
            .await;

        let retained = runtime
            .retain_scope_subscription("table:rooms", "table:rooms/bucket:00")
            .await;
        assert_eq!(retained.subscription_ref_count, 1);
        assert_eq!(retained.residency_tier, ActorResidencyTier::L3Full);

        let snapshot = runtime.snapshot_with_schema(1, 1).await;
        let restored = snapshot.into_runtime_state();
        let restored_runtime = ActorRuntime::new_with_shard_count_and_actor_states(
            restored.rooms,
            restored.actor_states,
            Vec::new(),
            4,
            64,
            0,
            2,
        );
        let status = restored_runtime.kernel_status().await;

        assert_eq!(status.scope_subscription_ref_count, 1);
        assert_eq!(status.subscribed_scopes, 1);
        assert_eq!(status.l3_scope_actors, 1);
        assert_eq!(status.scope_rows, 1);
    }

    #[tokio::test]
    async fn released_scope_lingers_then_tiers_down_to_index_residency() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        runtime
            .upsert_scope_rows(
                "table:rooms",
                "table:rooms/bucket:00",
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                ],
            )
            .await;
        runtime
            .retain_scope_subscription("table:rooms", "table:rooms/bucket:00")
            .await;

        let released = runtime
            .release_scope_subscription("table:rooms", "table:rooms/bucket:00", 1)
            .await;
        assert_eq!(released.subscription_ref_count, 0);
        assert!(released.lingering_until_ms > 0);
        assert_eq!(released.rows, 2);

        let tiered_down = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let tiered_down = runtime.tier_down_idle_scopes(8).await;
                if tiered_down == 1 {
                    break tiered_down;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("scope should become eligible for tier-down");

        assert_eq!(tiered_down, 1);
        let status = runtime.kernel_status().await;
        assert_eq!(status.scope_rows, 0);
        assert_eq!(status.lingering_scopes, 0);
        assert_eq!(status.l1_scope_actors, 1);
        assert_eq!(status.l3_scope_actors, 0);
        let maintenance = runtime.scope_residency_maintenance_status();
        assert_eq!(maintenance.last_tiered_down, 1);
        assert_eq!(maintenance.last_cleared_rows, 2);
        assert_eq!(maintenance.total_tiered_down, 1);
        assert_eq!(maintenance.total_cleared_rows, 2);
    }

    #[tokio::test]
    async fn runtime_snapshot_restores_table_pending_split_metadata() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let scope_key = "table:rooms/bucket:00".to_string();
        runtime
            .shard_for_actor(&table_actor_id)
            .upsert_table_scope(
                table_actor_id.clone(),
                scope_key,
                2,
                20,
                10,
                ScopeSplitPolicy { rows: 1, bytes: 0 },
            )
            .await;

        let snapshot = runtime.snapshot_with_schema(1, 1).await;
        let table_entry = snapshot
            .actor_states
            .iter()
            .find(|entry| entry.actor_id == table_actor_id)
            .expect("table actor snapshot");
        match &table_entry.state {
            ActorKernelSnapshotState::Table { scopes, .. } => {
                assert_eq!(scopes.len(), 1);
                assert!(scopes[0].split_pending);
                assert_eq!(scopes[0].split_rows, 1);
                assert!(scopes[0].split_reminder_at_ms > 0);
            }
            _ => panic!("expected table actor state"),
        }
        let restored = snapshot.into_runtime_state();
        let restored_runtime = ActorRuntime::new_with_shard_count_and_actor_states(
            restored.rooms,
            restored.actor_states,
            Vec::new(),
            4,
            64,
            0,
            2,
        );
        let status = restored_runtime.kernel_status().await;

        assert_eq!(status.table_scopes, 1);
        assert_eq!(status.table_pending_splits, 1);
    }

    #[tokio::test]
    async fn runtime_split_maintenance_processes_restored_split_reminder() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let parent_scope_key = "table:rooms/bucket:00";
        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                ],
            )
            .await;
        runtime
            .shard_for_actor(&table_actor_id)
            .upsert_table_scope(
                table_actor_id,
                parent_scope_key.to_string(),
                2,
                20,
                10,
                ScopeSplitPolicy { rows: 1, bytes: 0 },
            )
            .await;
        let snapshot = runtime.snapshot_with_schema(2, 1).await;
        let restored = snapshot.into_runtime_state();
        let restored_runtime = ActorRuntime::new_with_shard_count_and_actor_states(
            restored.rooms,
            restored.actor_states,
            Vec::new(),
            4,
            64,
            0,
            2,
        );

        assert_eq!(
            restored_runtime.kernel_status().await.table_pending_splits,
            1
        );
        let processed = restored_runtime
            .split_pending_scopes_with_policy(8, ScopeSplitPolicy { rows: 1, bytes: 0 })
            .await;
        let status = restored_runtime.kernel_status().await;

        assert_eq!(processed, 1);
        assert_eq!(status.scope_rows, 2);
        assert_eq!(status.table_pending_splits, 0);
        assert!(status.table_scopes >= 3);
    }

    #[tokio::test]
    async fn runtime_split_maintenance_processes_pending_table_scopes() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let parent_scope_key = "table:rooms/bucket:00";
        let parent_actor_id = ActorId::scope(parent_scope_key);
        let split_policy = ScopeSplitPolicy { rows: 1, bytes: 0 };
        let scope_result = runtime
            .shard_for_actor(&parent_actor_id)
            .upsert_scope_rows(
                table_actor_id.clone(),
                parent_actor_id.clone(),
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                ],
            )
            .await;
        runtime
            .shard_for_actor(&table_actor_id)
            .upsert_table_scope(
                table_actor_id,
                parent_scope_key.to_string(),
                scope_result.rows,
                scope_result.bytes,
                scope_result.last_accessed_ms,
                split_policy,
            )
            .await;
        assert_eq!(runtime.kernel_status().await.table_pending_splits, 1);

        let processed = runtime
            .split_pending_scopes_with_policy(8, split_policy)
            .await;
        let status = runtime.kernel_status().await;
        let maintenance = runtime.split_maintenance_status();

        assert_eq!(processed, 1);
        assert_eq!(maintenance.last_processed, 1);
        assert_eq!(maintenance.total_processed, 1);
        assert!(maintenance.last_sweep_at_ms.is_some());
        assert_eq!(status.scope_rows, 2);
        assert_eq!(status.table_pending_splits, 0);
        assert!(status.table_scopes >= 3);
    }

    #[tokio::test]
    async fn runtime_executes_scope_split_and_routes_future_rows_to_children() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let parent_scope_key = "table:rooms/bucket:00";
        let parent_actor_id = ActorId::scope(parent_scope_key);
        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                    record("rooms", "room-c", "Room C", 3),
                ],
            )
            .await;

        let split = runtime
            .split_scope_rows(
                table_actor_id.clone(),
                parent_actor_id,
                ScopeSplitPolicy { rows: 2, bytes: 0 },
            )
            .await;
        let split_snapshot = runtime.snapshot_with_schema(3, 1).await;
        let table_entry = split_snapshot
            .actor_states
            .iter()
            .find(|entry| entry.actor_id == table_actor_id)
            .expect("table actor snapshot");

        assert_eq!(split.table_scopes, 3);
        assert!(scope_snapshot_rows(&split_snapshot, parent_scope_key).is_empty());
        match &table_entry.state {
            ActorKernelSnapshotState::Table { scopes, .. } => {
                let parent = scopes
                    .iter()
                    .find(|scope| scope.scope_key == parent_scope_key)
                    .expect("parent scope directory entry");
                assert_eq!(parent.rows, 0);
                assert_eq!(
                    parent.child_scopes,
                    vec![
                        "table:rooms/bucket:00/child:00".to_string(),
                        "table:rooms/bucket:00/child:01".to_string(),
                    ]
                );
            }
            _ => panic!("expected table actor state"),
        }

        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![record("rooms", "room-d", "Room D", 4)],
            )
            .await;
        let status = runtime.kernel_status().await;
        let routed_scope_key = split_child_scope_key(parent_scope_key, "room-d", 2);
        let routed_snapshot = runtime.snapshot_with_schema(4, 1).await;

        assert_eq!(status.scope_rows, 4);
        assert_eq!(status.table_scopes, 3);
        assert!(scope_snapshot_rows(&routed_snapshot, parent_scope_key).is_empty());
        assert!(
            scope_snapshot_rows(&routed_snapshot, &routed_scope_key)
                .iter()
                .any(|row| row.key == "room-d")
        );
    }

    #[tokio::test]
    async fn runtime_executes_scope_split_when_byte_threshold_is_exceeded() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let parent_scope_key = "table:rooms/bucket:00";
        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![
                    record("rooms", "room-large-a", &"x".repeat(256), 1),
                    record("rooms", "room-large-b", &"y".repeat(256), 2),
                ],
            )
            .await;

        let split = runtime
            .split_scope_rows(
                table_actor_id.clone(),
                ActorId::scope(parent_scope_key),
                ScopeSplitPolicy {
                    rows: 0,
                    bytes: 128,
                },
            )
            .await;
        let snapshot = runtime.snapshot_with_schema(1, 1).await;
        let table_entry = snapshot
            .actor_states
            .iter()
            .find(|entry| entry.actor_id == table_actor_id)
            .expect("table actor snapshot");
        let status = runtime.kernel_status().await;

        assert_eq!(split.table_scopes, 5);
        assert_eq!(status.scope_rows, 2);
        assert!(status.scope_bytes > 128);
        assert!(scope_snapshot_rows(&snapshot, parent_scope_key).is_empty());
        match &table_entry.state {
            ActorKernelSnapshotState::Table { scopes, .. } => {
                let parent = scopes
                    .iter()
                    .find(|scope| scope.scope_key == parent_scope_key)
                    .expect("parent scope directory entry");
                assert_eq!(parent.bytes, 0);
                assert_eq!(parent.split_rows, 0);
                assert_eq!(parent.split_bytes, 128);
                assert_eq!(parent.child_scopes.len(), 2);
            }
            _ => panic!("expected table actor state"),
        }
    }

    #[tokio::test]
    async fn runtime_recursively_splits_child_scopes_until_policy_is_satisfied() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let parent_scope_key = "table:rooms/bucket:00";
        let (left_key, right_key, overloaded_child_key) =
            recursive_split_test_keys(parent_scope_key);
        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![
                    record("rooms", &left_key, "Room Left", 1),
                    record("rooms", &right_key, "Room Right", 2),
                ],
            )
            .await;
        let split = runtime
            .split_scope_rows(
                ActorId::table("table:rooms"),
                ActorId::scope(parent_scope_key),
                ScopeSplitPolicy { rows: 1, bytes: 0 },
            )
            .await;
        let status = runtime.kernel_status().await;
        let snapshot = runtime.snapshot_with_schema(2, 1).await;
        let left_leaf = split_child_scope_key(&overloaded_child_key, &left_key, 2);
        let right_leaf = split_child_scope_key(&overloaded_child_key, &right_key, 2);

        assert_eq!(split.table_pending_splits, 0);
        assert!(!split.scope_split_pending);
        assert_eq!(status.scope_rows, 2);
        assert_eq!(status.table_pending_splits, 0);
        assert_eq!(status.table_scopes, 5);
        assert!(scope_snapshot_rows(&snapshot, parent_scope_key).is_empty());
        assert!(scope_snapshot_rows(&snapshot, &overloaded_child_key).is_empty());
        assert!(
            scope_snapshot_rows(&snapshot, &left_leaf)
                .iter()
                .any(|row| row.key == left_key)
        );
        assert!(
            scope_snapshot_rows(&snapshot, &right_leaf)
                .iter()
                .any(|row| row.key == right_key)
        );

        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![record("rooms", &left_key, "Room Left+", 3)],
            )
            .await;
        let routed_snapshot = runtime.snapshot_with_schema(3, 1).await;
        assert!(
            scope_snapshot_rows(&routed_snapshot, &left_leaf)
                .iter()
                .any(|row| row.key == left_key && row.value["title"] == "Room Left+")
        );
        assert!(scope_snapshot_rows(&routed_snapshot, &overloaded_child_key).is_empty());
    }

    #[tokio::test]
    async fn runtime_snapshot_restores_split_child_routing() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let parent_scope_key = "table:rooms/bucket:00";
        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                    record("rooms", "room-c", "Room C", 3),
                ],
            )
            .await;
        runtime
            .split_scope_rows(
                table_actor_id,
                ActorId::scope(parent_scope_key),
                ScopeSplitPolicy { rows: 2, bytes: 0 },
            )
            .await;

        let snapshot = runtime.snapshot_with_schema(3, 1).await;
        let restored = snapshot.into_runtime_state();
        let restored_runtime = ActorRuntime::new_with_shard_count_and_actor_states(
            restored.rooms,
            restored.actor_states,
            Vec::new(),
            4,
            64,
            0,
            2,
        );
        restored_runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key,
                vec![record("rooms", "room-z", "Room Z", 4)],
            )
            .await;
        let status = restored_runtime.kernel_status().await;
        let restored_snapshot = restored_runtime.snapshot_with_schema(4, 1).await;
        let routed_scope_key = split_child_scope_key(parent_scope_key, "room-z", 2);

        assert_eq!(status.scope_rows, 4);
        assert_eq!(status.table_scopes, 3);
        assert!(scope_snapshot_rows(&restored_snapshot, parent_scope_key).is_empty());
        assert!(
            scope_snapshot_rows(&restored_snapshot, &routed_scope_key)
                .iter()
                .any(|row| row.key == "room-z")
        );
    }

    #[tokio::test]
    async fn actor_states_with_wal_tail_updates_existing_scope_and_table_state() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let base_scope_key = record_actor_scope_key("rooms", "room-a");
        runtime
            .upsert_scope_rows(
                "table:rooms",
                base_scope_key,
                vec![record("rooms", "room-a", "Room A", 1)],
            )
            .await;
        let snapshot = runtime.snapshot_with_schema(1, 1).await;
        let updated = actor_states_with_wal_tail(
            snapshot.actor_states,
            &[
                wal_record_upsert(2, record("rooms", "room-a", "Room A+", 2)),
                wal_record_upsert(3, record("rooms", "room-b", "Room B", 3)),
            ],
        );
        let restored = ActorRuntime::new_with_shard_count_and_actor_states(
            HashMap::new(),
            updated,
            Vec::new(),
            4,
            64,
            0,
            2,
        );
        let status = restored.kernel_status().await;

        assert_eq!(status.kind_counts.get("scope"), Some(&2));
        assert_eq!(status.kind_counts.get("table"), Some(&1));
        assert_eq!(status.scope_rows, 2);
        assert_eq!(status.table_scopes, 2);
    }

    #[tokio::test]
    async fn actor_states_with_wal_tail_deletes_existing_scope_row() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        runtime
            .upsert_scope_rows(
                "table:rooms",
                record_actor_scope_key("rooms", "room-a"),
                vec![record("rooms", "room-a", "Room A", 1)],
            )
            .await;
        let snapshot = runtime.snapshot_with_schema(1, 1).await;
        let updated = actor_states_with_wal_tail(
            snapshot.actor_states,
            &[wal_record_delete(2, "rooms", "room-a")],
        );
        let restored = ActorRuntime::new_with_shard_count_and_actor_states(
            HashMap::new(),
            updated,
            Vec::new(),
            4,
            64,
            0,
            2,
        );
        let status = restored.kernel_status().await;

        assert_eq!(status.scope_rows, 0);
        assert_eq!(status.table_scopes, 1);
    }

    #[tokio::test]
    async fn actor_states_with_wal_tail_routes_split_scope_updates_to_children() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 4, 64, 0, 2);
        let table_actor_id = ActorId::table("table:rooms");
        let parent_scope_key = record_actor_scope_key("rooms", "room-a");
        runtime
            .upsert_scope_rows(
                "table:rooms",
                parent_scope_key.clone(),
                vec![
                    record("rooms", "room-a", "Room A", 1),
                    record("rooms", "room-b", "Room B", 2),
                    record("rooms", "room-c", "Room C", 3),
                ],
            )
            .await;
        runtime
            .split_scope_rows(
                table_actor_id,
                ActorId::scope(parent_scope_key.clone()),
                ScopeSplitPolicy { rows: 1, bytes: 0 },
            )
            .await;
        let snapshot = runtime.snapshot_with_schema(3, 1).await;
        let updated = actor_states_with_wal_tail(
            snapshot.actor_states,
            &[wal_record_upsert(
                4,
                record("rooms", "room-a", "Room A+", 4),
            )],
        );
        let routed_scope_key = split_child_scope_key(&parent_scope_key, "room-a", 2);
        let updated_snapshot = ActorSnapshot {
            lsn: 4,
            schema_version: 1,
            record_hot: None,
            rooms: HashMap::new(),
            actor_states: updated,
        };

        assert!(scope_snapshot_rows(&updated_snapshot, &parent_scope_key).is_empty());
        assert!(
            scope_snapshot_rows(&updated_snapshot, &routed_scope_key)
                .iter()
                .any(|row| row.key == "room-a" && row.value["title"] == "Room A+")
        );
    }

    #[test]
    fn actor_states_with_wal_tail_does_not_activate_cold_records() {
        let updated = actor_states_with_wal_tail(
            Vec::new(),
            &[wal_record_upsert(1, record("rooms", "room-a", "Room A", 1))],
        );

        assert!(updated.is_empty());
    }

    #[test]
    fn actor_reminders_from_wal_records_restores_pending_only() {
        let scheduled = wal_record(
            1,
            WalPayload::ActorReminderScheduled {
                reminder: ActorReminderDraft {
                    actor_kind: "view".to_string(),
                    actor_key: "rooms/recent".to_string(),
                    reminder_id: "refresh".to_string(),
                    due_at_ms: 42,
                    payload: Some(serde_json::json!({ "reason": "test" })),
                },
            },
        );
        let cancelled = wal_record(
            2,
            WalPayload::ActorReminderCancelled {
                actor_kind: "view".to_string(),
                actor_key: "rooms/recent".to_string(),
                reminder_id: "refresh".to_string(),
                cancelled_at_ms: 41,
            },
        );
        let pending = wal_record(
            3,
            WalPayload::ActorReminderScheduled {
                reminder: ActorReminderDraft {
                    actor_kind: "aggregate".to_string(),
                    actor_key: "presence".to_string(),
                    reminder_id: "tick".to_string(),
                    due_at_ms: 100,
                    payload: None,
                },
            },
        );

        let reminders = actor_reminders_from_wal_records(&[pending, cancelled, scheduled]);

        assert_eq!(reminders.len(), 1);
        assert_eq!(reminders[0].actor_id, ActorId::aggregate("presence"));
        assert_eq!(reminders[0].reminder_id, "tick");
        assert_eq!(reminders[0].due_at_ms, 100);
    }

    #[tokio::test]
    async fn runtime_due_reminder_runs_actor_turn_and_drains_wheel() {
        let runtime = ActorRuntime::new_with_actor_states_and_reminders(
            HashMap::new(),
            Vec::new(),
            vec![ActorReminderEntry {
                actor_id: ActorId::view("rooms/recent"),
                reminder_id: "refresh".to_string(),
                due_at_ms: 10,
                payload: Some(serde_json::json!({ "reason": "test" })),
            }],
            4,
            64,
            0,
        );

        let due = runtime.take_due_reminders(10, 8);
        assert_eq!(due.len(), 1);
        let turn = runtime
            .run_actor_turn(
                due[0].actor_id.clone(),
                ActorKernelMessage::ReminderFired {
                    reminder_id: due[0].reminder_id.clone(),
                    payload: due[0].payload.clone(),
                },
            )
            .await;

        assert!(turn.created);
        assert_eq!(turn.turn_count, 1);
        assert_eq!(runtime.reminder_status(8).pending, 0);
        assert_eq!(
            runtime.kernel_status().await.kind_counts.get("view"),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn runtime_room_api_uses_actor_id_directory() {
        let runtime = ActorRuntime::new_with_shard_count(HashMap::new(), 8, 64, 0, 2);
        runtime.apply_message(message("general", "hello", 1)).await;

        assert!(runtime.has_room("general").await);
        assert_eq!(runtime.room_count().await, 1);
        assert_eq!(
            runtime
                .latest_messages("general", None, 1)
                .await
                .into_iter()
                .map(|message| message.body)
                .collect::<Vec<_>>(),
            vec!["hello"]
        );
    }

    #[tokio::test]
    async fn runtime_apply_does_not_scan_idle_rooms_on_write_path() {
        let runtime = ActorRuntime::new(HashMap::new(), 4, 10, 50);
        runtime.apply_message(message("room-a", "a-1", 1)).await;

        let status = runtime.idle_maintenance_status();
        assert_eq!(status.last_sweep_at_ms, None);
        assert_eq!(status.last_evicted, 0);
        assert_eq!(status.total_evicted, 0);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        runtime.apply_message(message("room-b", "b-1", 2)).await;

        let status = runtime.idle_maintenance_status();
        assert_eq!(status.last_sweep_at_ms, None);
        assert_eq!(status.last_evicted, 0);
        assert_eq!(status.total_evicted, 0);
        assert_eq!(runtime.room_count().await, 2);

        let evicted = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let evicted = runtime.evict_idle_rooms().await;
                if evicted == 1 {
                    break evicted;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("room should become idle within timeout");
        assert_eq!(evicted, 1);
        assert_eq!(runtime.room_count().await, 1);
    }

    #[test]
    fn idle_room_eviction_removes_only_rooms_past_ttl() {
        let mut rooms = HashMap::from([
            (
                "room-a".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 100,
                },
            ),
            (
                "room-b".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 40,
                },
            ),
        ]);

        assert_eq!(evict_idle_rooms(&mut rooms, 50, 100), 1);
        assert!(rooms.contains_key("room-a"));
        assert!(!rooms.contains_key("room-b"));
    }

    #[test]
    fn lru_room_eviction_removes_overflow_in_one_ordered_pass() {
        let mut rooms = HashMap::from([
            (
                "room-c".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 3,
                },
            ),
            (
                "room-a".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 1,
                },
            ),
            (
                "room-b".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 2,
                },
            ),
            (
                "room-d".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 4,
                },
            ),
        ]);

        evict_lru_rooms(&mut rooms, 2);

        assert_eq!(rooms.len(), 2);
        assert!(!rooms.contains_key("room-a"));
        assert!(!rooms.contains_key("room-b"));
        assert!(rooms.contains_key("room-c"));
        assert!(rooms.contains_key("room-d"));
    }

    #[test]
    fn lru_room_eviction_uses_room_id_as_stable_tie_breaker() {
        let mut rooms = HashMap::from([
            (
                "room-b".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 1,
                },
            ),
            (
                "room-a".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 1,
                },
            ),
            (
                "room-c".to_string(),
                RoomLiveState {
                    messages: VecDeque::new(),
                    last_accessed_ms: 2,
                },
            ),
        ]);

        evict_lru_rooms(&mut rooms, 2);

        assert!(!rooms.contains_key("room-a"));
        assert!(rooms.contains_key("room-b"));
        assert!(rooms.contains_key("room-c"));
    }

    #[test]
    fn lru_room_eviction_clears_when_max_hot_rooms_is_zero() {
        let mut rooms = HashMap::from([(
            "room-a".to_string(),
            RoomLiveState {
                messages: VecDeque::new(),
                last_accessed_ms: 1,
            },
        )]);

        evict_lru_rooms(&mut rooms, 0);

        assert!(rooms.is_empty());
    }

    #[tokio::test]
    async fn runtime_room_count_observes_without_evicting_idle_rooms() {
        let runtime = ActorRuntime::new(HashMap::new(), 4, 10, 50);
        runtime.apply_message(message("room-a", "a-1", 1)).await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(runtime.room_count().await, 1);
        assert_eq!(runtime.idle_maintenance_status().total_evicted, 0);

        let evicted = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let evicted = runtime.evict_idle_rooms().await;
                if evicted == 1 {
                    break evicted;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("room should become idle within timeout");
        assert_eq!(evicted, 1);
        assert_eq!(runtime.room_count().await, 0);
        let status = runtime.idle_maintenance_status();
        assert!(status.last_sweep_at_ms.is_some());
        assert_eq!(status.last_evicted, 1);
        assert_eq!(status.total_evicted, 1);
    }

    fn scope_snapshot_rows(snapshot: &ActorSnapshot, scope_key: &str) -> Vec<DbRecord> {
        snapshot
            .actor_states
            .iter()
            .find_map(|entry| {
                if entry.actor_id != ActorId::scope(scope_key) {
                    return None;
                }
                match &entry.state {
                    ActorKernelSnapshotState::Scope { rows, .. } => Some(rows.clone()),
                    ActorKernelSnapshotState::Table { .. }
                    | ActorKernelSnapshotState::Kernel { .. } => None,
                }
            })
            .unwrap_or_default()
    }

    fn recursive_split_test_keys(parent_scope_key: &str) -> (String, String, String) {
        for left_index in 0..512 {
            let left_key = format!("room-recursive-{left_index}");
            let left_child_index = split_child_index(parent_scope_key, &left_key, 2);
            let child_scope_key = split_child_scope_key(parent_scope_key, &left_key, 2);
            for right_index in (left_index + 1)..512 {
                let right_key = format!("room-recursive-{right_index}");
                if split_child_index(parent_scope_key, &right_key, 2) != left_child_index {
                    continue;
                }
                if split_child_index(&child_scope_key, &left_key, 2)
                    != split_child_index(&child_scope_key, &right_key, 2)
                {
                    return (left_key, right_key, child_scope_key);
                }
            }
        }
        panic!("expected to find recursive split test keys");
    }

    fn message(room_id: &str, body: &str, lsn: u64) -> Message {
        Message {
            id: format!("message-{lsn}"),
            room_id: room_id.to_string(),
            sender_id: "user-a".to_string(),
            body: body.to_string(),
            attachments: Vec::new(),
            created_at_ms: lsn,
            lsn,
            path: format!("rooms/{room_id}/messages/message-{lsn}"),
        }
    }

    fn record(table: &str, key: &str, title: &str, lsn: u64) -> DbRecord {
        DbRecord {
            table: table.to_string(),
            key: key.to_string(),
            value: serde_json::json!({
                "id": key,
                "title": title,
            }),
            updated_at_ms: lsn,
            lsn,
            path: format!("tables/{table}/{key}"),
        }
    }

    fn wal_record_upsert(lsn: u64, record: DbRecord) -> WalRecord {
        wal_record(
            lsn,
            WalPayload::RecordUpserted {
                record: crate::model::DbRecordDraft {
                    table: record.table,
                    key: record.key,
                    value: record.value,
                    updated_at_ms: record.updated_at_ms,
                    path: record.path,
                    client_mutation_id: None,
                },
            },
        )
    }

    fn wal_record_delete(lsn: u64, table: &str, key: &str) -> WalRecord {
        wal_record(
            lsn,
            WalPayload::RecordDeleted {
                record: crate::model::DbRecordDeleteDraft {
                    table: table.to_string(),
                    key: key.to_string(),
                    deleted_at_ms: lsn,
                    path: format!("tables/{table}/{key}"),
                    client_mutation_id: None,
                },
            },
        )
    }

    fn wal_record(lsn: u64, payload: WalPayload) -> WalRecord {
        WalRecord {
            lsn,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test-node".to_string(),
            timestamp_ms: lsn,
            schema_version: 1,
            durability: crate::model::Durability::Strict,
            payload,
            checksum: None,
        }
    }
}
