#![cfg_attr(not(feature = "cluster"), allow(dead_code))]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
};
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

use crate::{
    AppState,
    api::error::ApiError,
    api::guards::ensure_shard_index,
    api::wal::{cluster_epoch_for_shard, cluster_owner_for_shard},
    api::wal::{
        latest_lsn_for_shard, refresh_wal_remote_replicas_for_shard, remote_ack_lsn_for_url,
    },
    cluster::{ClusterShardOverride, ClusterTopology, ShardRoute},
    tasks::{HandoffWorkflow, HandoffWorkflowPhase, PeerHealthStatus, ShardControl},
    util::{now_ms, shard_index},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterRouteQuery {
    pub(crate) key: Option<String>,
    pub(crate) room_id: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) record_key: Option<String>,
    pub(crate) object_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShardFreezeRequest {
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShardControlResponse {
    pub(crate) control: ShardControl,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffPlanRequest {
    pub(crate) shard: usize,
    pub(crate) target_owner: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffPlanResponse {
    pub(crate) shard: usize,
    pub(crate) current_owner: String,
    pub(crate) target_owner: String,
    pub(crate) target_owner_url: Option<String>,
    pub(crate) current_epoch: u64,
    pub(crate) next_epoch: u64,
    pub(crate) current_shard_lsn: u64,
    pub(crate) target_acked_lsn: u64,
    pub(crate) target_caught_up: bool,
    pub(crate) frozen: bool,
    pub(crate) ready: bool,
    pub(crate) required_env: BTreeMap<String, String>,
    pub(crate) steps: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailoverPlanRequest {
    pub(crate) shard: usize,
    pub(crate) target_owner: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailoverPlanResponse {
    pub(crate) shard: usize,
    pub(crate) current_owner: String,
    pub(crate) target_owner: String,
    pub(crate) target_owner_url: Option<String>,
    pub(crate) current_epoch: u64,
    pub(crate) next_epoch: u64,
    pub(crate) current_shard_lsn: u64,
    pub(crate) local_lsn: u64,
    pub(crate) owner_last_seen_ok_lsn: Option<u64>,
    pub(crate) owner_healthy: bool,
    pub(crate) target_is_local: bool,
    pub(crate) target_is_replica: bool,
    pub(crate) target_caught_up: bool,
    pub(crate) ready: bool,
    pub(crate) reason: Option<String>,
    pub(crate) required_override: ApplyTopologyOverrideRequest,
    pub(crate) required_acks: usize,
    pub(crate) owner_peer: Option<PeerHealthStatus>,
    pub(crate) steps: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailoverProposalResponse {
    pub(crate) plan: FailoverPlanResponse,
    pub(crate) proposal: TopologyProposal,
    pub(crate) topology: ClusterTopology,
    pub(crate) overrides: BTreeMap<usize, ClusterShardOverride>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffWorkflowResponse {
    pub(crate) workflow: HandoffWorkflow,
    pub(crate) plan: HandoffPlanResponse,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffWorkflowListResponse {
    pub(crate) workflows: Vec<HandoffWorkflow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApplyTopologyOverrideRequest {
    pub(crate) shard: usize,
    pub(crate) owner: Option<String>,
    pub(crate) epoch: Option<u64>,
    pub(crate) replicas: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyOverrideResponse {
    pub(crate) overrides: BTreeMap<usize, ClusterShardOverride>,
    pub(crate) topology: ClusterTopology,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyLogEntry {
    pub(crate) id: String,
    pub(crate) timestamp_ms: u64,
    pub(crate) node_id: String,
    pub(crate) reason: String,
    pub(crate) request: ApplyTopologyOverrideRequest,
    pub(crate) overrides: BTreeMap<usize, ClusterShardOverride>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyLogResponse {
    pub(crate) entries: Vec<TopologyLogEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyLease {
    pub(crate) current_term: u64,
    pub(crate) holder_node_id: Option<String>,
    pub(crate) proposal_id: Option<String>,
    pub(crate) expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum TopologyProposalPhase {
    Prepared,
    Committed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyProposal {
    pub(crate) id: String,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) proposed_by: String,
    pub(crate) term: u64,
    pub(crate) lease_expires_at_ms: u64,
    pub(crate) reason: String,
    pub(crate) phase: TopologyProposalPhase,
    pub(crate) request: ApplyTopologyOverrideRequest,
    pub(crate) prepare_acks: Vec<TopologyPropagationResult>,
    pub(crate) commit_results: Vec<TopologyPropagationResult>,
    pub(crate) required_acks: usize,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyProposalRequest {
    pub(crate) shard: usize,
    pub(crate) owner: Option<String>,
    pub(crate) epoch: Option<u64>,
    pub(crate) replicas: Option<Vec<String>>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyProposalResponse {
    pub(crate) proposal: TopologyProposal,
    pub(crate) topology: ClusterTopology,
    pub(crate) overrides: BTreeMap<usize, ClusterShardOverride>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyProposalListResponse {
    pub(crate) proposals: Vec<TopologyProposal>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyProposalCommitRequest {
    pub(crate) proposal_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyProposalAbortRequest {
    pub(crate) proposal_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyLeaseCleanupResponse {
    pub(crate) cleared: bool,
    pub(crate) lease: TopologyLease,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffApplyResponse {
    pub(crate) workflow: HandoffWorkflow,
    pub(crate) topology: ClusterTopology,
    pub(crate) overrides: BTreeMap<usize, ClusterShardOverride>,
    pub(crate) propagation: Vec<TopologyPropagationResult>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandoffAutoResponse {
    pub(crate) workflow: HandoffWorkflow,
    pub(crate) plan: HandoffPlanResponse,
    pub(crate) applied: bool,
    pub(crate) apply: Option<HandoffApplyResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopologyPropagationResult {
    pub(crate) node_id: String,
    pub(crate) url: String,
    pub(crate) applied: bool,
    pub(crate) status: Option<u16>,
    pub(crate) error: Option<String>,
}

pub(crate) async fn load_topology_proposals(
    path: &PathBuf,
) -> Result<BTreeMap<String, TopologyProposal>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn load_topology_lease(path: &PathBuf) -> Result<TopologyLease> {
    if !path.exists() {
        return Ok(TopologyLease::default());
    }
    let bytes = fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn load_handoff_workflows(
    path: &PathBuf,
) -> Result<BTreeMap<String, HandoffWorkflow>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn load_topology_overrides(
    snapshot_path: &PathBuf,
    log_path: &PathBuf,
) -> Result<BTreeMap<usize, ClusterShardOverride>> {
    let mut overrides = if snapshot_path.exists() {
        let bytes = fs::read(snapshot_path).await?;
        serde_json::from_slice(&bytes)?
    } else {
        BTreeMap::new()
    };
    for entry in read_topology_log(log_path).await? {
        overrides = entry.overrides;
    }
    Ok(overrides)
}

pub(crate) async fn read_topology_log(path: &PathBuf) -> Result<Vec<TopologyLogEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = fs::read_to_string(path).await?;
    let mut entries = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry = serde_json::from_str::<TopologyLogEntry>(line)
            .with_context(|| format!("parse topology log line {}", index + 1))?;
        entries.push(entry);
    }
    Ok(entries)
}

pub(crate) async fn write_topology_log_entries(
    path: &Path,
    entries: &[TopologyLogEntry],
) -> Result<(), ApiError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let mut bytes = Vec::new();
    for entry in entries {
        let mut encoded =
            serde_json::to_vec(entry).map_err(|err| ApiError::internal(err.into()))?;
        encoded.push(b'\n');
        bytes.extend(encoded);
    }
    fs::write(path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn persist_topology_overrides(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.topology_overrides_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let overrides = state.topology_overrides.read().await;
    let bytes =
        serde_json::to_vec_pretty(&*overrides).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.topology_overrides_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn append_topology_log_entry(
    state: &AppState,
    request: ApplyTopologyOverrideRequest,
    overrides: BTreeMap<usize, ClusterShardOverride>,
    reason: impl Into<String>,
) -> Result<TopologyLogEntry, ApiError> {
    if let Some(parent) = state.topology_log_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let entry = TopologyLogEntry {
        id: Uuid::now_v7().to_string(),
        timestamp_ms: now_ms(),
        node_id: state.cluster.node_id().to_string(),
        reason: reason.into(),
        request,
        overrides,
    };
    let mut encoded = serde_json::to_vec(&entry).map_err(|err| ApiError::internal(err.into()))?;
    encoded.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&state.topology_log_path)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    file.write_all(&encoded)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    file.sync_data()
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(entry)
}

pub(crate) async fn persist_topology_proposals(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.topology_proposals_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let proposals = state.topology_proposals.read().await;
    let bytes =
        serde_json::to_vec_pretty(&*proposals).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.topology_proposals_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn persist_topology_lease(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.topology_lease_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let lease = state.topology_lease.read().await;
    let bytes = serde_json::to_vec_pretty(&*lease).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.topology_lease_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn persist_handoff_workflows(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.handoff_workflows_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let workflows = state.handoff_workflows.read().await;
    let bytes =
        serde_json::to_vec_pretty(&*workflows).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.handoff_workflows_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn run_peer_health_monitor_once(state: &AppState) {
    let peers = topology_peer_nodes(state).await;
    let previous = state.peer_health.read().await.peers.clone();
    let http = reqwest::Client::new();
    let mut next = BTreeMap::new();
    for (node_id, url) in peers {
        let checked_at = now_ms();
        let started = std::time::Instant::now();
        let endpoint = format!("{}/v1/health", url.trim_end_matches('/'));
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(2_000),
            http.get(&endpoint).send(),
        )
        .await;
        let status = match result {
            Ok(Ok(response)) => {
                let status = response.status();
                if status.is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(body) => {
                            let ok = body
                                .get("ok")
                                .and_then(serde_json::Value::as_bool)
                                .unwrap_or(false);
                            PeerHealthStatus {
                                node_id: node_id.clone(),
                                url: url.clone(),
                                ok,
                                status: Some(status.as_u16()),
                                accepting_writes: body
                                    .get("acceptingWrites")
                                    .and_then(serde_json::Value::as_bool),
                                current_lsn: body
                                    .get("currentLsn")
                                    .and_then(serde_json::Value::as_u64),
                                last_seen_ok_lsn: if ok {
                                    body.get("currentLsn").and_then(serde_json::Value::as_u64)
                                } else {
                                    previous
                                        .get(&node_id)
                                        .and_then(|peer| peer.last_seen_ok_lsn)
                                },
                                latency_ms: Some(started.elapsed().as_millis() as u64),
                                last_checked_at_ms: checked_at,
                                last_seen_ok_at_ms: if ok {
                                    Some(checked_at)
                                } else {
                                    previous
                                        .get(&node_id)
                                        .and_then(|peer| peer.last_seen_ok_at_ms)
                                },
                                error: if ok {
                                    None
                                } else {
                                    Some("peer health returned ok=false".to_string())
                                },
                            }
                        }
                        Err(err) => peer_health_error(
                            &node_id,
                            &url,
                            Some(status.as_u16()),
                            checked_at,
                            &previous,
                            format!("decode health failed: {err}"),
                        ),
                    }
                } else {
                    peer_health_error(
                        &node_id,
                        &url,
                        Some(status.as_u16()),
                        checked_at,
                        &previous,
                        format!("health returned {status}"),
                    )
                }
            }
            Ok(Err(err)) => peer_health_error(
                &node_id,
                &url,
                None,
                checked_at,
                &previous,
                format!("health request failed: {err}"),
            ),
            Err(_) => peer_health_error(
                &node_id,
                &url,
                None,
                checked_at,
                &previous,
                "health request timed out".to_string(),
            ),
        };
        next.insert(node_id, status);
    }

    let mut monitor = state.peer_health.write().await;
    monitor.last_run_at_ms = Some(now_ms());
    monitor.peers = next;
}

fn peer_health_error(
    node_id: &str,
    url: &str,
    status: Option<u16>,
    checked_at: u64,
    previous: &BTreeMap<String, PeerHealthStatus>,
    error: String,
) -> PeerHealthStatus {
    PeerHealthStatus {
        node_id: node_id.to_string(),
        url: url.to_string(),
        ok: false,
        status,
        accepting_writes: None,
        current_lsn: None,
        last_seen_ok_lsn: previous.get(node_id).and_then(|peer| peer.last_seen_ok_lsn),
        latency_ms: None,
        last_checked_at_ms: checked_at,
        last_seen_ok_at_ms: previous
            .get(node_id)
            .and_then(|peer| peer.last_seen_ok_at_ms),
        error: Some(error),
    }
}

pub(crate) async fn topology_peer_nodes(state: &AppState) -> Vec<(String, String)> {
    let topology = {
        let overrides = state.topology_overrides.read().await;
        state.cluster.topology_with_overrides(&overrides)
    };
    topology
        .nodes
        .into_iter()
        .filter(|node| node.id != state.cluster.node_id())
        .filter_map(|node| node.url.map(|url| (node.id, url)))
        .collect()
}

pub(crate) async fn cluster_topology(State(state): State<AppState>) -> Json<ClusterTopology> {
    let overrides = state.topology_overrides.read().await;
    Json(state.cluster.topology_with_overrides(&overrides))
}

pub(crate) async fn cluster_route(
    State(state): State<AppState>,
    Query(query): Query<ClusterRouteQuery>,
) -> Result<Json<ShardRoute>, ApiError> {
    let key = cluster_route_key(query)?;
    let shard = shard_index(&key, state.cluster.shard_count());
    let overrides = state.topology_overrides.read().await;
    Ok(Json(
        state
            .cluster
            .route_for_key_with_overrides(key, shard, &overrides),
    ))
}

fn cluster_route_key(query: ClusterRouteQuery) -> Result<String, ApiError> {
    if let Some(key) = query.key.filter(|value| !value.trim().is_empty()) {
        return Ok(key);
    }
    if let Some(room_id) = query.room_id.filter(|value| !value.trim().is_empty()) {
        return Ok(room_id);
    }
    if let Some(object_id) = query.object_id.filter(|value| !value.trim().is_empty()) {
        return Ok(object_id);
    }
    match (query.table, query.record_key) {
        (Some(table), Some(key)) if !table.trim().is_empty() && !key.trim().is_empty() => {
            Ok(format!("{table}:{key}"))
        }
        _ => Err(ApiError::bad_request(
            "provide key, roomId, objectId, or table plus recordKey",
        )),
    }
}

pub(crate) async fn freeze_shard(
    State(state): State<AppState>,
    AxumPath(shard): AxumPath<usize>,
    Json(request): Json<ShardFreezeRequest>,
) -> Result<Json<ShardControlResponse>, ApiError> {
    ensure_shard_index(&state, shard)?;
    let control = ShardControl {
        shard,
        frozen: true,
        reason: request.reason.filter(|reason| !reason.trim().is_empty()),
        frozen_at_ms: Some(now_ms()),
    };
    state
        .shard_controls
        .write()
        .await
        .insert(shard, control.clone());
    Ok(Json(ShardControlResponse { control }))
}

pub(crate) async fn unfreeze_shard(
    State(state): State<AppState>,
    AxumPath(shard): AxumPath<usize>,
) -> Result<Json<ShardControlResponse>, ApiError> {
    ensure_shard_index(&state, shard)?;
    let control = ShardControl {
        shard,
        frozen: false,
        reason: None,
        frozen_at_ms: None,
    };
    state.shard_controls.write().await.remove(&shard);
    Ok(Json(ShardControlResponse { control }))
}

pub(crate) async fn get_topology_overrides(
    State(state): State<AppState>,
) -> Json<TopologyOverrideResponse> {
    let overrides = state.topology_overrides.read().await.clone();
    Json(TopologyOverrideResponse {
        topology: state.cluster.topology_with_overrides(&overrides),
        overrides,
    })
}

pub(crate) async fn get_topology_log(
    State(state): State<AppState>,
) -> Result<Json<TopologyLogResponse>, ApiError> {
    let entries = read_topology_log(&state.topology_log_path)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(TopologyLogResponse { entries }))
}

pub(crate) async fn apply_topology_override(
    State(state): State<AppState>,
    Json(request): Json<ApplyTopologyOverrideRequest>,
) -> Result<Json<TopologyOverrideResponse>, ApiError> {
    apply_topology_override_inner(&state, request, "operator topology override").await?;
    let overrides = state.topology_overrides.read().await.clone();
    Ok(Json(TopologyOverrideResponse {
        topology: state.cluster.topology_with_overrides(&overrides),
        overrides,
    }))
}

pub(crate) async fn list_topology_proposals(
    State(state): State<AppState>,
) -> Json<TopologyProposalListResponse> {
    let proposals = state
        .topology_proposals
        .read()
        .await
        .values()
        .cloned()
        .collect();
    Json(TopologyProposalListResponse { proposals })
}

pub(crate) async fn start_topology_proposal(
    State(state): State<AppState>,
    Json(request): Json<TopologyProposalRequest>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    let override_request = ApplyTopologyOverrideRequest {
        shard: request.shard,
        owner: request.owner,
        epoch: request.epoch,
        replicas: request.replicas,
    };
    let proposal = prepare_topology_proposal_inner(
        &state,
        override_request,
        request
            .reason
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| "operator topology proposal".to_string()),
    )
    .await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

pub(crate) async fn commit_topology_proposal(
    State(state): State<AppState>,
    AxumPath(proposal_id): AxumPath<String>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    let proposal = commit_topology_proposal_inner(&state, &proposal_id).await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

pub(crate) async fn retry_topology_proposal(
    State(state): State<AppState>,
    AxumPath(proposal_id): AxumPath<String>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    let proposal = retry_topology_proposal_inner(&state, &proposal_id).await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

pub(crate) async fn abort_topology_proposal(
    State(state): State<AppState>,
    AxumPath(proposal_id): AxumPath<String>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    let proposal = abort_topology_proposal_inner(&state, &proposal_id, true).await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

pub(crate) async fn cleanup_topology_lease(
    State(state): State<AppState>,
) -> Result<Json<TopologyLeaseCleanupResponse>, ApiError> {
    let cleared = cleanup_expired_topology_lease_inner(&state).await?;
    let lease = state.topology_lease.read().await.clone();
    Ok(Json(TopologyLeaseCleanupResponse { cleared, lease }))
}

pub(crate) async fn prepare_topology_proposal_peer(
    State(state): State<AppState>,
    Json(mut proposal): Json<TopologyProposal>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    proposal.request = normalize_topology_override_request(&state, proposal.request).await?;
    accept_topology_lease_for_proposal(&state, &proposal).await?;
    proposal.phase = TopologyProposalPhase::Prepared;
    proposal.updated_at_ms = now_ms();
    proposal.last_error = None;
    state
        .topology_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_topology_proposals(&state).await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

pub(crate) async fn commit_topology_proposal_peer(
    State(state): State<AppState>,
    Json(request): Json<TopologyProposalCommitRequest>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    let proposal = state
        .topology_proposals
        .read()
        .await
        .get(&request.proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("topology proposal not found"))?;
    ensure_topology_lease_matches(&state, &proposal).await?;
    let proposal = apply_committed_topology_proposal(&state, proposal, Vec::new()).await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

pub(crate) async fn abort_topology_proposal_peer(
    State(state): State<AppState>,
    Json(request): Json<TopologyProposalAbortRequest>,
) -> Result<Json<TopologyProposalResponse>, ApiError> {
    let proposal = abort_topology_proposal_inner(&state, &request.proposal_id, false).await?;
    Ok(Json(topology_proposal_response(&state, proposal).await))
}

async fn topology_proposal_response(
    state: &AppState,
    proposal: TopologyProposal,
) -> TopologyProposalResponse {
    let overrides = state.topology_overrides.read().await.clone();
    TopologyProposalResponse {
        proposal,
        topology: state.cluster.topology_with_overrides(&overrides),
        overrides,
    }
}

async fn apply_topology_override_inner(
    state: &AppState,
    request: ApplyTopologyOverrideRequest,
    reason: impl Into<String>,
) -> Result<(), ApiError> {
    let request = normalize_topology_override_request(state, request).await?;
    let next_overrides = build_topology_overrides_after_request(state, &request).await;
    append_topology_log_entry(state, request.clone(), next_overrides.clone(), reason).await?;
    *state.topology_overrides.write().await = next_overrides;
    persist_topology_overrides(state).await?;
    refresh_wal_remote_replicas_for_shard(state, request.shard).await
}

async fn normalize_topology_override_request(
    state: &AppState,
    mut request: ApplyTopologyOverrideRequest,
) -> Result<ApplyTopologyOverrideRequest, ApiError> {
    ensure_shard_index(state, request.shard)?;
    let current_epoch = cluster_epoch_for_shard(state, request.shard).await;
    if let Some(epoch) = request.epoch
        && epoch < current_epoch
    {
        return Err(ApiError::conflict(format!(
            "topology override epoch {epoch} is lower than current epoch {current_epoch}"
        )));
    }
    if let Some(owner) = &request.owner {
        if owner.trim().is_empty() {
            return Err(ApiError::bad_request("owner cannot be empty"));
        }
        request.owner = Some(owner.trim().to_string());
    }
    if let Some(replicas) = request.replicas.take() {
        let mut normalized = Vec::new();
        for replica in replicas {
            let replica = replica.trim();
            if replica.is_empty() {
                continue;
            }
            if !normalized.iter().any(|existing| existing == replica) {
                normalized.push(replica.to_string());
            }
        }
        request.replicas = Some(normalized);
    }

    Ok(request)
}

async fn build_topology_overrides_after_request(
    state: &AppState,
    request: &ApplyTopologyOverrideRequest,
) -> BTreeMap<usize, ClusterShardOverride> {
    let mut next_overrides = state.topology_overrides.read().await.clone();
    {
        let entry = next_overrides.entry(request.shard).or_default();
        if let Some(owner) = &request.owner {
            entry.owner = Some(owner.clone());
        }
        if let Some(epoch) = request.epoch {
            entry.epoch = Some(epoch.max(1));
        }
        if let Some(replicas) = &request.replicas {
            entry.replicas = Some(replicas.clone());
        }
    }
    next_overrides
}

pub(crate) async fn prepare_topology_proposal_inner(
    state: &AppState,
    request: ApplyTopologyOverrideRequest,
    reason: String,
) -> Result<TopologyProposal, ApiError> {
    prepare_topology_proposal_inner_with_policy(state, request, reason, false).await
}

pub(crate) async fn prepare_topology_proposal_inner_with_policy(
    state: &AppState,
    request: ApplyTopologyOverrideRequest,
    reason: String,
    return_failed: bool,
) -> Result<TopologyProposal, ApiError> {
    cleanup_expired_topology_lease_inner(state).await?;
    let request = normalize_topology_override_request(state, request).await?;
    let now = now_ms();
    let mut proposal = TopologyProposal {
        id: Uuid::now_v7().to_string(),
        created_at_ms: now,
        updated_at_ms: now,
        proposed_by: state.cluster.node_id().to_string(),
        term: 0,
        lease_expires_at_ms: 0,
        reason,
        phase: TopologyProposalPhase::Prepared,
        request,
        prepare_acks: vec![self_topology_result(state, "prepare")],
        commit_results: Vec::new(),
        required_acks: required_topology_acks(state).await,
        last_error: None,
    };
    acquire_topology_lease_for_proposal(state, &mut proposal).await?;
    state
        .topology_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_topology_proposals(state).await?;

    let peer_results = prepare_topology_proposal_on_peers(state, &proposal).await;
    proposal.prepare_acks.extend(peer_results);
    let acked = proposal
        .prepare_acks
        .iter()
        .filter(|result| result.applied)
        .count();
    if acked < proposal.required_acks {
        proposal.phase = TopologyProposalPhase::Failed;
        proposal.last_error = Some(format!(
            "topology proposal prepare requires {} acks, got {acked}",
            proposal.required_acks
        ));
        release_topology_lease(state, &proposal).await?;
    }
    proposal.updated_at_ms = now_ms();
    state
        .topology_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_topology_proposals(state).await?;

    if proposal.phase == TopologyProposalPhase::Failed && !return_failed {
        return Err(ApiError::conflict(
            proposal
                .last_error
                .clone()
                .unwrap_or_else(|| "topology proposal prepare failed".to_string()),
        ));
    }

    Ok(proposal)
}

async fn acquire_topology_lease_for_proposal(
    state: &AppState,
    proposal: &mut TopologyProposal,
) -> Result<(), ApiError> {
    let now = now_ms();
    let expires_at = now + state.topology_lease_ms;
    {
        let mut lease = state.topology_lease.write().await;
        if lease
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms > now)
            && lease
                .holder_node_id
                .as_deref()
                .is_some_and(|holder| holder != state.cluster.node_id())
        {
            return Err(ApiError::conflict(format!(
                "topology lease is held by {} until {}",
                lease.holder_node_id.clone().unwrap_or_default(),
                lease.expires_at_ms.unwrap_or_default()
            )));
        }
        lease.current_term = lease.current_term.saturating_add(1).max(1);
        lease.holder_node_id = Some(state.cluster.node_id().to_string());
        lease.proposal_id = Some(proposal.id.clone());
        lease.expires_at_ms = Some(expires_at);
        proposal.term = lease.current_term;
        proposal.lease_expires_at_ms = expires_at;
    }
    persist_topology_lease(state).await
}

async fn accept_topology_lease_for_proposal(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Result<(), ApiError> {
    let now = now_ms();
    {
        let mut lease = state.topology_lease.write().await;
        if proposal.term < lease.current_term {
            return Err(ApiError::conflict(format!(
                "topology proposal term {} is lower than current term {}",
                proposal.term, lease.current_term
            )));
        }
        let same_lease = lease.holder_node_id.as_deref() == Some(proposal.proposed_by.as_str())
            && lease.proposal_id.as_deref() == Some(proposal.id.as_str());
        if proposal.term == lease.current_term
            && !same_lease
            && lease
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms > now)
        {
            return Err(ApiError::conflict(format!(
                "topology lease term {} is held by {}",
                lease.current_term,
                lease.holder_node_id.clone().unwrap_or_default()
            )));
        }
        lease.current_term = proposal.term;
        lease.holder_node_id = Some(proposal.proposed_by.clone());
        lease.proposal_id = Some(proposal.id.clone());
        lease.expires_at_ms = Some(proposal.lease_expires_at_ms);
    }
    persist_topology_lease(state).await
}

async fn ensure_topology_lease_matches(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Result<(), ApiError> {
    let now = now_ms();
    let lease = state.topology_lease.read().await;
    if proposal.term != lease.current_term {
        return Err(ApiError::conflict(format!(
            "topology proposal term {} does not match current term {}",
            proposal.term, lease.current_term
        )));
    }
    if lease.holder_node_id.as_deref() != Some(proposal.proposed_by.as_str())
        || lease.proposal_id.as_deref() != Some(proposal.id.as_str())
    {
        return Err(ApiError::conflict(
            "topology lease holder does not match proposal",
        ));
    }
    if lease
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms < now)
    {
        return Err(ApiError::conflict("topology lease has expired"));
    }
    Ok(())
}

async fn release_topology_lease(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Result<(), ApiError> {
    {
        let mut lease = state.topology_lease.write().await;
        if lease.current_term == proposal.term
            && lease.holder_node_id.as_deref() == Some(proposal.proposed_by.as_str())
            && lease.proposal_id.as_deref() == Some(proposal.id.as_str())
        {
            lease.holder_node_id = None;
            lease.proposal_id = None;
            lease.expires_at_ms = None;
        }
    }
    persist_topology_lease(state).await
}

async fn cleanup_expired_topology_lease_inner(state: &AppState) -> Result<bool, ApiError> {
    let now = now_ms();
    let mut cleared = false;
    {
        let mut lease = state.topology_lease.write().await;
        if lease.holder_node_id.is_some()
            && lease
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms <= now)
        {
            lease.holder_node_id = None;
            lease.proposal_id = None;
            lease.expires_at_ms = None;
            cleared = true;
        }
    }
    if cleared {
        persist_topology_lease(state).await?;
    }
    Ok(cleared)
}

async fn abort_topology_proposal_inner(
    state: &AppState,
    proposal_id: &str,
    propagate: bool,
) -> Result<TopologyProposal, ApiError> {
    let mut proposal = state
        .topology_proposals
        .read()
        .await
        .get(proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("topology proposal not found"))?;
    if proposal.phase == TopologyProposalPhase::Committed {
        return Err(ApiError::conflict(
            "committed topology proposal cannot be aborted",
        ));
    }

    if propagate {
        let _ = abort_topology_proposal_on_peers(state, &proposal).await;
    }
    release_topology_lease(state, &proposal).await?;
    proposal.phase = TopologyProposalPhase::Aborted;
    proposal.updated_at_ms = now_ms();
    proposal.last_error = Some("aborted by operator".to_string());
    state
        .topology_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_topology_proposals(state).await?;
    Ok(proposal)
}

async fn retry_topology_proposal_inner(
    state: &AppState,
    proposal_id: &str,
) -> Result<TopologyProposal, ApiError> {
    cleanup_expired_topology_lease_inner(state).await?;
    let proposal = state
        .topology_proposals
        .read()
        .await
        .get(proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("topology proposal not found"))?;
    if proposal.phase == TopologyProposalPhase::Committed {
        return Err(ApiError::conflict(
            "committed topology proposal cannot be retried",
        ));
    }
    if proposal.phase == TopologyProposalPhase::Prepared && proposal.lease_expires_at_ms > now_ms()
    {
        return Err(ApiError::conflict(
            "prepared topology proposal still holds an active lease",
        ));
    }
    prepare_topology_proposal_inner(
        state,
        proposal.request.clone(),
        format!("retry {}", proposal.reason),
    )
    .await
}

pub(crate) async fn commit_topology_proposal_inner(
    state: &AppState,
    proposal_id: &str,
) -> Result<TopologyProposal, ApiError> {
    let proposal = state
        .topology_proposals
        .read()
        .await
        .get(proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("topology proposal not found"))?;
    if proposal.phase == TopologyProposalPhase::Committed {
        return Ok(proposal);
    }
    if proposal.phase == TopologyProposalPhase::Failed {
        return Err(ApiError::conflict("topology proposal has failed"));
    }
    let prepared = proposal
        .prepare_acks
        .iter()
        .filter(|result| result.applied)
        .count();
    if prepared < proposal.required_acks {
        return Err(ApiError::conflict(format!(
            "topology proposal requires {} prepare acks, got {prepared}",
            proposal.required_acks
        )));
    }
    ensure_topology_lease_matches(state, &proposal).await?;

    let peer_results = commit_topology_proposal_on_peers(state, &proposal).await;
    let peer_successes = peer_results.iter().filter(|result| result.applied).count();
    if peer_successes + 1 < proposal.required_acks {
        let mut failed = proposal;
        release_topology_lease(state, &failed).await?;
        failed.phase = TopologyProposalPhase::Failed;
        failed.updated_at_ms = now_ms();
        failed.commit_results = peer_results;
        failed.last_error = Some(format!(
            "topology proposal commit requires {} acks, got {}",
            failed.required_acks,
            peer_successes + 1
        ));
        state
            .topology_proposals
            .write()
            .await
            .insert(failed.id.clone(), failed.clone());
        persist_topology_proposals(state).await?;
        return Err(ApiError::conflict(
            failed
                .last_error
                .clone()
                .unwrap_or_else(|| "topology proposal commit failed".to_string()),
        ));
    }

    apply_committed_topology_proposal(state, proposal, peer_results).await
}

async fn apply_committed_topology_proposal(
    state: &AppState,
    mut proposal: TopologyProposal,
    mut peer_results: Vec<TopologyPropagationResult>,
) -> Result<TopologyProposal, ApiError> {
    ensure_topology_lease_matches(state, &proposal).await?;
    apply_topology_override_inner(
        state,
        proposal.request.clone(),
        format!("topology proposal {}", proposal.id),
    )
    .await?;
    release_topology_lease(state, &proposal).await?;
    peer_results.push(self_topology_result(state, "commit"));
    proposal.phase = TopologyProposalPhase::Committed;
    proposal.updated_at_ms = now_ms();
    proposal.commit_results = peer_results;
    proposal.last_error = None;
    state
        .topology_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_topology_proposals(state).await?;
    Ok(proposal)
}

async fn prepare_topology_proposal_on_peers(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Vec<TopologyPropagationResult> {
    let http = reqwest::Client::new();
    let mut results = Vec::new();
    for (node_id, url) in topology_peer_nodes(state).await {
        let endpoint = topology_proposal_prepare_endpoint(&url);
        let response = http.post(&endpoint).json(proposal).send().await;
        results.push(topology_http_result(node_id, endpoint, response, "prepare").await);
    }
    results
}

async fn commit_topology_proposal_on_peers(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Vec<TopologyPropagationResult> {
    let http = reqwest::Client::new();
    let mut results = Vec::new();
    for (node_id, url) in topology_peer_nodes(state).await {
        let endpoint = topology_proposal_commit_endpoint(&url);
        let request = TopologyProposalCommitRequest {
            proposal_id: proposal.id.clone(),
        };
        let response = http.post(&endpoint).json(&request).send().await;
        results.push(topology_http_result(node_id, endpoint, response, "commit").await);
    }
    results
}

async fn abort_topology_proposal_on_peers(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Vec<TopologyPropagationResult> {
    let http = reqwest::Client::new();
    let mut results = Vec::new();
    for (node_id, url) in topology_peer_nodes(state).await {
        let endpoint = topology_proposal_abort_endpoint(&url);
        let request = TopologyProposalAbortRequest {
            proposal_id: proposal.id.clone(),
        };
        let response = http.post(&endpoint).json(&request).send().await;
        results.push(topology_http_result(node_id, endpoint, response, "abort").await);
    }
    results
}

pub(crate) async fn required_topology_acks(state: &AppState) -> usize {
    let topology = {
        let overrides = state.topology_overrides.read().await;
        state.cluster.topology_with_overrides(&overrides)
    };
    (topology.nodes.len() / 2) + 1
}

pub(crate) fn self_topology_result(state: &AppState, operation: &str) -> TopologyPropagationResult {
    TopologyPropagationResult {
        node_id: state.cluster.node_id().to_string(),
        url: state
            .cluster
            .node_url_for(state.cluster.node_id())
            .unwrap_or_else(|| "local".to_string()),
        applied: true,
        status: Some(200),
        error: Some(format!("local {operation}")),
    }
}

pub(crate) async fn topology_http_result(
    node_id: String,
    endpoint: String,
    response: Result<reqwest::Response, reqwest::Error>,
    operation: &str,
) -> TopologyPropagationResult {
    match response {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                TopologyPropagationResult {
                    node_id,
                    url: endpoint,
                    applied: true,
                    status: Some(status.as_u16()),
                    error: None,
                }
            } else {
                let text = response.text().await.unwrap_or_default();
                TopologyPropagationResult {
                    node_id,
                    url: endpoint,
                    applied: false,
                    status: Some(status.as_u16()),
                    error: Some(text),
                }
            }
        }
        Err(err) => TopologyPropagationResult {
            node_id,
            url: endpoint,
            applied: false,
            status: None,
            error: Some(format!("{operation} failed: {err}")),
        },
    }
}

fn topology_proposal_prepare_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/cluster/topology/proposals/prepare") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/cluster/topology/proposals/prepare")
    }
}

fn topology_proposal_commit_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/cluster/topology/proposals/commit") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/cluster/topology/proposals/commit")
    }
}

fn topology_proposal_abort_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/cluster/topology/proposals/abort") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/cluster/topology/proposals/abort")
    }
}

pub(crate) async fn commit_failover_proposal_if_ready(
    state: &AppState,
    proposal: &TopologyProposal,
) -> Result<Option<TopologyProposal>, ApiError> {
    if proposal.phase != TopologyProposalPhase::Prepared {
        return Ok(None);
    }
    Ok(Some(
        commit_topology_proposal_inner(state, &proposal.id).await?,
    ))
}

pub(crate) async fn existing_failover_proposal_for_shard(
    state: &AppState,
    shard: usize,
) -> Option<TopologyProposal> {
    state
        .topology_proposals
        .read()
        .await
        .values()
        .filter(|proposal| proposal.request.shard == shard)
        .filter(|proposal| proposal.reason.starts_with("failover shard "))
        .filter(|proposal| {
            matches!(
                proposal.phase,
                TopologyProposalPhase::Prepared | TopologyProposalPhase::Failed
            )
        })
        .max_by_key(|proposal| proposal.updated_at_ms)
        .cloned()
}

pub(crate) async fn handoff_plan(
    State(state): State<AppState>,
    Json(request): Json<HandoffPlanRequest>,
) -> Result<Json<HandoffPlanResponse>, ApiError> {
    Ok(Json(compute_handoff_plan(&state, request).await?))
}

async fn compute_handoff_plan(
    state: &AppState,
    request: HandoffPlanRequest,
) -> Result<HandoffPlanResponse, ApiError> {
    ensure_shard_index(state, request.shard)?;
    if request.target_owner.trim().is_empty() {
        return Err(ApiError::bad_request("targetOwner is required"));
    }

    let current_owner = cluster_owner_for_shard(state, request.shard).await;
    if current_owner == request.target_owner {
        return Err(ApiError::bad_request("targetOwner is already the owner"));
    }

    let current_epoch = cluster_epoch_for_shard(state, request.shard).await;
    let next_epoch = current_epoch + 1;
    let current_shard_lsn = latest_lsn_for_shard(state, request.shard)?;
    let target_owner_url = state.cluster.node_url_for(&request.target_owner);
    let target_acked_lsn = target_owner_url
        .as_deref()
        .and_then(|url| remote_ack_lsn_for_url(state, request.shard, url))
        .unwrap_or(0);
    let frozen = state
        .shard_controls
        .read()
        .await
        .get(&request.shard)
        .is_some_and(|control| control.frozen);
    let target_caught_up = target_acked_lsn >= current_shard_lsn;
    let ready = frozen && target_caught_up;

    let mut required_env = BTreeMap::new();
    required_env.insert(
        "NEXTDB_SHARD_OWNERS".to_string(),
        format!("{}={}", request.shard, request.target_owner),
    );
    required_env.insert(
        "NEXTDB_SHARD_EPOCHS".to_string(),
        format!("{}={next_epoch}", request.shard),
    );
    let target_replicas =
        handoff_target_replicas(state, request.shard, &current_owner, &request.target_owner).await;
    required_env.insert(
        "NEXTDB_SHARD_REPLICAS".to_string(),
        format!("{}={}", request.shard, target_replicas.join("|")),
    );

    let steps = vec![
        format!(
            "Freeze shard {} on current owner {current_owner}.",
            request.shard
        ),
        format!(
            "Wait until target owner {} has acknowledged LSN {current_shard_lsn}.",
            request.target_owner
        ),
        format!(
            "Restart or reconfigure every node with shard {} owner={} epoch={next_epoch} replicas={}.",
            request.shard,
            request.target_owner,
            target_replicas.join("|")
        ),
        format!("Unfreeze shard {} on the new owner.", request.shard),
    ];

    Ok(HandoffPlanResponse {
        shard: request.shard,
        current_owner,
        target_owner: request.target_owner,
        target_owner_url,
        current_epoch,
        next_epoch,
        current_shard_lsn,
        target_acked_lsn,
        target_caught_up,
        frozen,
        ready,
        required_env,
        steps,
    })
}

async fn handoff_target_replicas(
    state: &AppState,
    shard: usize,
    current_owner: &str,
    target_owner: &str,
) -> Vec<String> {
    let overrides = state.topology_overrides.read().await;
    let mut replicas = Vec::new();
    for replica in state
        .cluster
        .replicas_for_shard_with_overrides(shard, &overrides)
    {
        if replica != target_owner && !replicas.iter().any(|existing| existing == &replica) {
            replicas.push(replica);
        }
    }
    if !current_owner.is_empty()
        && !replicas
            .iter()
            .any(|replica| replica.as_str() == current_owner)
    {
        replicas.push(current_owner.to_string());
    }
    replicas
}

pub(crate) async fn failover_plan(
    State(state): State<AppState>,
    Json(request): Json<FailoverPlanRequest>,
) -> Result<Json<FailoverPlanResponse>, ApiError> {
    Ok(Json(compute_failover_plan(&state, request).await?))
}

pub(crate) async fn start_failover_proposal(
    State(state): State<AppState>,
    Json(request): Json<FailoverPlanRequest>,
) -> Result<Json<FailoverProposalResponse>, ApiError> {
    let plan = compute_failover_plan(&state, request).await?;
    if !plan.ready {
        return Err(ApiError::conflict(
            plan.reason
                .clone()
                .unwrap_or_else(|| "failover plan is not ready".to_string()),
        ));
    }
    let proposal = prepare_topology_proposal_inner_with_policy(
        &state,
        plan.required_override.clone(),
        format!(
            "failover shard {} from {} to {}",
            plan.shard, plan.current_owner, plan.target_owner
        ),
        true,
    )
    .await?;
    let overrides = state.topology_overrides.read().await.clone();
    Ok(Json(FailoverProposalResponse {
        plan,
        topology: state.cluster.topology_with_overrides(&overrides),
        overrides,
        proposal,
    }))
}

pub(crate) async fn compute_failover_plan(
    state: &AppState,
    request: FailoverPlanRequest,
) -> Result<FailoverPlanResponse, ApiError> {
    ensure_shard_index(state, request.shard)?;
    let target_owner = request
        .target_owner
        .as_deref()
        .unwrap_or_else(|| state.cluster.node_id())
        .trim()
        .to_string();
    if target_owner.is_empty() {
        return Err(ApiError::bad_request("targetOwner cannot be empty"));
    }

    let current_owner = cluster_owner_for_shard(state, request.shard).await;
    let current_epoch = cluster_epoch_for_shard(state, request.shard).await;
    let next_epoch = current_epoch + 1;
    let current_shard_lsn = latest_lsn_for_shard(state, request.shard)?;
    let local_lsn = current_shard_lsn;
    let target_owner_url = state.cluster.node_url_for(&target_owner);
    let target_is_local = target_owner == state.cluster.node_id();
    let replicas = {
        let overrides = state.topology_overrides.read().await;
        state
            .cluster
            .replicas_for_shard_with_overrides(request.shard, &overrides)
    };
    let target_is_replica = replicas.iter().any(|replica| replica == &target_owner);
    let peer_health = state.peer_health.read().await.clone();
    let owner_peer = if current_owner == state.cluster.node_id() {
        None
    } else {
        peer_health.peers.get(&current_owner).cloned()
    };
    let owner_healthy = if current_owner == state.cluster.node_id() {
        true
    } else {
        owner_peer.as_ref().is_some_and(|peer| peer.ok)
    };
    let owner_last_seen_ok_lsn = owner_peer
        .as_ref()
        .and_then(|peer| peer.last_seen_ok_lsn.or(peer.current_lsn));
    let target_caught_up = owner_last_seen_ok_lsn.is_some_and(|owner_lsn| local_lsn >= owner_lsn);
    let target_replicas =
        handoff_target_replicas(state, request.shard, &current_owner, &target_owner).await;
    let required_override = ApplyTopologyOverrideRequest {
        shard: request.shard,
        owner: Some(target_owner.clone()),
        epoch: Some(next_epoch),
        replicas: Some(target_replicas.clone()),
    };
    let required_acks = required_topology_acks(state).await;

    let reason = if current_owner == state.cluster.node_id() {
        Some("current owner is local; use handoff instead of failover".to_string())
    } else if !peer_health.enabled {
        Some("owner health is unknown; enable NEXTDB_PEER_MONITOR_INTERVAL_MS".to_string())
    } else if owner_peer.is_none() {
        Some(format!(
            "owner {current_owner} has not been observed by peer monitor"
        ))
    } else if owner_healthy {
        Some(format!("owner {current_owner} is still healthy"))
    } else if !target_is_local {
        Some("failover can only be promoted from the local target node".to_string())
    } else if !target_is_replica {
        Some(format!(
            "target owner {target_owner} is not a replica for shard {}",
            request.shard
        ))
    } else if owner_last_seen_ok_lsn.is_none() {
        Some(format!(
            "owner {current_owner} has no last healthy LSN; wait for peer monitor to observe it"
        ))
    } else if !target_caught_up {
        Some(format!(
            "local LSN {local_lsn} has not caught up to owner last healthy LSN {}",
            owner_last_seen_ok_lsn.unwrap_or(0)
        ))
    } else {
        None
    };
    let ready = reason.is_none();

    let steps = vec![
        format!("Peer monitor marks current owner {current_owner} unhealthy."),
        format!("Confirm local target {target_owner} is a replica and has WAL LSN {local_lsn}."),
        format!(
            "Prepare topology proposal: shard {} owner={} epoch={next_epoch} replicas={}.",
            request.shard,
            target_owner,
            target_replicas.join("|")
        ),
        format!(
            "Commit only after topology quorum accepts the proposal ({required_acks} required ack(s))."
        ),
    ];

    Ok(FailoverPlanResponse {
        shard: request.shard,
        current_owner,
        target_owner,
        target_owner_url,
        current_epoch,
        next_epoch,
        current_shard_lsn,
        local_lsn,
        owner_last_seen_ok_lsn,
        owner_healthy,
        target_is_local,
        target_is_replica,
        target_caught_up,
        ready,
        reason,
        required_override,
        required_acks,
        owner_peer,
        steps,
    })
}

pub(crate) async fn list_handoff_workflows(
    State(state): State<AppState>,
) -> Json<HandoffWorkflowListResponse> {
    let workflows = state
        .handoff_workflows
        .read()
        .await
        .values()
        .cloned()
        .collect();
    Json(HandoffWorkflowListResponse { workflows })
}

pub(crate) async fn start_handoff_workflow(
    State(state): State<AppState>,
    Json(request): Json<HandoffPlanRequest>,
) -> Result<Json<HandoffWorkflowResponse>, ApiError> {
    let plan = compute_handoff_plan(&state, request.clone()).await?;
    let control = ShardControl {
        shard: request.shard,
        frozen: true,
        reason: Some(format!("handoff workflow to {}", request.target_owner)),
        frozen_at_ms: Some(now_ms()),
    };
    state
        .shard_controls
        .write()
        .await
        .insert(request.shard, control);

    let now = now_ms();
    let phase = if plan.target_caught_up {
        HandoffWorkflowPhase::ReadyToReconfigure
    } else {
        HandoffWorkflowPhase::WaitingForCatchUp
    };
    let workflow = HandoffWorkflow {
        id: Uuid::now_v7().to_string(),
        shard: plan.shard,
        current_owner: plan.current_owner.clone(),
        target_owner: plan.target_owner.clone(),
        current_epoch: plan.current_epoch,
        next_epoch: plan.next_epoch,
        phase,
        created_at_ms: now,
        updated_at_ms: now,
        current_shard_lsn: plan.current_shard_lsn,
        target_acked_lsn: plan.target_acked_lsn,
        last_error: None,
        required_env: plan.required_env.clone(),
    };

    state
        .handoff_workflows
        .write()
        .await
        .insert(workflow.id.clone(), workflow.clone());
    persist_handoff_workflows(&state).await?;

    let plan = compute_handoff_plan(&state, request).await?;
    Ok(Json(HandoffWorkflowResponse { workflow, plan }))
}

pub(crate) async fn step_handoff_workflow(
    State(state): State<AppState>,
    axum::extract::Path(workflow_id): axum::extract::Path<String>,
) -> Result<Json<HandoffWorkflowResponse>, ApiError> {
    Ok(Json(
        step_handoff_workflow_inner(&state, &workflow_id).await?,
    ))
}

async fn step_handoff_workflow_inner(
    state: &AppState,
    workflow_id: &str,
) -> Result<HandoffWorkflowResponse, ApiError> {
    let existing = state
        .handoff_workflows
        .read()
        .await
        .get(workflow_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("handoff workflow not found"))?;
    if existing.phase == HandoffWorkflowPhase::Aborted {
        return Err(ApiError::conflict("handoff workflow is aborted"));
    }
    if existing.phase == HandoffWorkflowPhase::Applied {
        return Err(ApiError::conflict("handoff workflow is already applied"));
    }

    let request = HandoffPlanRequest {
        shard: existing.shard,
        target_owner: existing.target_owner.clone(),
    };
    let plan = compute_handoff_plan(state, request.clone()).await?;
    let mut workflow = existing;
    workflow.current_shard_lsn = plan.current_shard_lsn;
    workflow.target_acked_lsn = plan.target_acked_lsn;
    workflow.required_env = plan.required_env.clone();
    workflow.updated_at_ms = now_ms();
    workflow.last_error = None;
    workflow.phase = if plan.ready {
        HandoffWorkflowPhase::ReadyToReconfigure
    } else {
        HandoffWorkflowPhase::WaitingForCatchUp
    };

    state
        .handoff_workflows
        .write()
        .await
        .insert(workflow.id.clone(), workflow.clone());
    persist_handoff_workflows(state).await?;

    Ok(HandoffWorkflowResponse { workflow, plan })
}

pub(crate) async fn auto_handoff_workflow(
    State(state): State<AppState>,
    axum::extract::Path(workflow_id): axum::extract::Path<String>,
) -> Result<Json<HandoffAutoResponse>, ApiError> {
    Ok(Json(
        auto_handoff_workflow_inner(&state, &workflow_id).await?,
    ))
}

pub(crate) async fn auto_handoff_workflow_inner(
    state: &AppState,
    workflow_id: &str,
) -> Result<HandoffAutoResponse, ApiError> {
    let stepped = step_handoff_workflow_inner(state, workflow_id).await?;
    if stepped.workflow.phase != HandoffWorkflowPhase::ReadyToReconfigure {
        return Ok(HandoffAutoResponse {
            workflow: stepped.workflow,
            plan: stepped.plan,
            applied: false,
            apply: None,
        });
    }

    let apply = apply_handoff_workflow_inner(state, workflow_id).await?;
    Ok(HandoffAutoResponse {
        workflow: apply.workflow.clone(),
        plan: stepped.plan,
        applied: true,
        apply: Some(apply),
    })
}

pub(crate) async fn abort_handoff_workflow(
    State(state): State<AppState>,
    axum::extract::Path(workflow_id): axum::extract::Path<String>,
) -> Result<Json<HandoffWorkflowResponse>, ApiError> {
    let existing = state
        .handoff_workflows
        .read()
        .await
        .get(&workflow_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("handoff workflow not found"))?;
    let request = HandoffPlanRequest {
        shard: existing.shard,
        target_owner: existing.target_owner.clone(),
    };
    state.shard_controls.write().await.remove(&existing.shard);
    let plan = compute_handoff_plan(&state, request).await?;

    let mut workflow = existing;
    workflow.phase = HandoffWorkflowPhase::Aborted;
    workflow.updated_at_ms = now_ms();
    workflow.last_error = Some("aborted by operator".to_string());
    workflow.current_shard_lsn = plan.current_shard_lsn;
    workflow.target_acked_lsn = plan.target_acked_lsn;

    state
        .handoff_workflows
        .write()
        .await
        .insert(workflow.id.clone(), workflow.clone());
    persist_handoff_workflows(&state).await?;

    Ok(Json(HandoffWorkflowResponse { workflow, plan }))
}

pub(crate) async fn apply_handoff_workflow(
    State(state): State<AppState>,
    axum::extract::Path(workflow_id): axum::extract::Path<String>,
) -> Result<Json<HandoffApplyResponse>, ApiError> {
    Ok(Json(
        apply_handoff_workflow_inner(&state, &workflow_id).await?,
    ))
}

async fn apply_handoff_workflow_inner(
    state: &AppState,
    workflow_id: &str,
) -> Result<HandoffApplyResponse, ApiError> {
    let existing = state
        .handoff_workflows
        .read()
        .await
        .get(workflow_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("handoff workflow not found"))?;
    if existing.phase != HandoffWorkflowPhase::ReadyToReconfigure {
        return Err(ApiError::conflict(
            "handoff workflow must be readyToReconfigure before apply",
        ));
    }

    let override_request = ApplyTopologyOverrideRequest {
        shard: existing.shard,
        owner: Some(existing.target_owner.clone()),
        epoch: Some(existing.next_epoch),
        replicas: Some(handoff_replica_nodes(state, &existing).await),
    };
    let proposal = prepare_topology_proposal_inner(
        state,
        override_request,
        format!("handoff workflow {}", existing.id),
    )
    .await?;
    let proposal = commit_topology_proposal_inner(state, &proposal.id).await?;
    let propagation = proposal.commit_results.clone();
    state.shard_controls.write().await.remove(&existing.shard);

    let mut workflow = existing;
    workflow.phase = HandoffWorkflowPhase::Applied;
    workflow.updated_at_ms = now_ms();
    workflow.last_error = None;
    state
        .handoff_workflows
        .write()
        .await
        .insert(workflow.id.clone(), workflow.clone());
    persist_handoff_workflows(state).await?;

    let overrides = state.topology_overrides.read().await.clone();
    Ok(HandoffApplyResponse {
        workflow,
        topology: state.cluster.topology_with_overrides(&overrides),
        overrides,
        propagation,
    })
}

async fn handoff_replica_nodes(state: &AppState, workflow: &HandoffWorkflow) -> Vec<String> {
    let overrides = state.topology_overrides.read().await;
    let mut replicas = Vec::new();
    for replica in state
        .cluster
        .replicas_for_shard_with_overrides(workflow.shard, &overrides)
    {
        if replica != workflow.target_owner && !replicas.iter().any(|existing| existing == &replica)
        {
            replicas.push(replica);
        }
    }
    if !replicas
        .iter()
        .any(|replica| replica == &workflow.current_owner)
    {
        replicas.push(workflow.current_owner.clone());
    }
    replicas
}

pub(crate) async fn run_failover_controller_once(state: &AppState) -> Result<(), ApiError> {
    let mut last_error = None;
    for shard in 0..state.cluster.shard_count() {
        let plan = compute_failover_plan(
            state,
            FailoverPlanRequest {
                shard,
                target_owner: None,
            },
        )
        .await?;
        if !plan.ready {
            if last_error.is_none() {
                last_error = plan.reason.clone();
            }
            continue;
        }

        if let Some(existing) = existing_failover_proposal_for_shard(state, shard).await {
            let committed = commit_failover_proposal_if_ready(state, &existing).await?;
            let mut controller = state.failover_controller.write().await;
            controller.last_run_at_ms = Some(now_ms());
            controller.last_shard = Some(shard);
            controller.last_proposal_id = Some(existing.id.clone());
            if let Some(committed) = committed {
                controller.last_committed_proposal_id = Some(committed.id);
                controller.last_error = None;
            } else {
                controller.last_error = existing.last_error;
            }
            return Ok(());
        }

        let proposal = prepare_topology_proposal_inner_with_policy(
            state,
            plan.required_override.clone(),
            format!(
                "failover shard {} from {} to {}",
                plan.shard, plan.current_owner, plan.target_owner
            ),
            true,
        )
        .await?;
        let committed = commit_failover_proposal_if_ready(state, &proposal).await?;
        let mut controller = state.failover_controller.write().await;
        controller.last_run_at_ms = Some(now_ms());
        controller.last_shard = Some(shard);
        controller.last_proposal_id = Some(proposal.id.clone());
        if let Some(committed) = committed {
            controller.last_committed_proposal_id = Some(committed.id);
            controller.last_error = None;
        } else {
            controller.last_error = proposal.last_error;
        }
        return Ok(());
    }

    let mut controller = state.failover_controller.write().await;
    controller.last_run_at_ms = Some(now_ms());
    controller.last_shard = None;
    controller.last_proposal_id = None;
    controller.last_error = last_error;
    Ok(())
}

pub(crate) async fn run_handoff_controller_once(state: &AppState) -> Result<(), ApiError> {
    let workflow = {
        let workflows = state.handoff_workflows.read().await;
        workflows
            .values()
            .filter(|workflow| {
                matches!(
                    workflow.phase,
                    HandoffWorkflowPhase::WaitingForCatchUp
                        | HandoffWorkflowPhase::ReadyToReconfigure
                )
            })
            .min_by_key(|workflow| workflow.updated_at_ms)
            .cloned()
    };
    let Some(workflow) = workflow else {
        let mut controller = state.handoff_controller.write().await;
        controller.last_run_at_ms = Some(now_ms());
        controller.last_workflow_id = None;
        controller.last_error = None;
        return Ok(());
    };

    let response = auto_handoff_workflow_inner(state, &workflow.id).await;
    let mut controller = state.handoff_controller.write().await;
    controller.last_run_at_ms = Some(now_ms());
    controller.last_workflow_id = Some(workflow.id.clone());
    match response {
        Ok(response) => {
            controller.last_error = None;
            if response.applied {
                controller.last_applied_workflow_id = Some(response.workflow.id);
            }
            Ok(())
        }
        Err(err) => {
            controller.last_error = Some(err.message.clone());
            Err(err)
        }
    }
}
