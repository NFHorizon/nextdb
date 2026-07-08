use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Result;
use axum::{
    Json,
    extract::{Path as AxumPath, State},
};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::api::wal::{
    append_ordered_wal_record, ensure_shard_not_frozen, writable_wal_shard_for_index,
};
use crate::{
    AppState,
    api::{
        behavior::validate_loaded_behavior_manifests_schema,
        error::ApiError,
        objects::validate_records_object_refs_against_schema,
        records::{schema_indexes_by_table, schema_orders_by_table},
        runtime::{
            RuntimeStoragePolicyResponse, SchemaWalRecoveryReport,
            read_startup_projections_from_wal_paths,
        },
        topology::{
            TopologyPropagationResult, TopologyProposalAbortRequest, self_topology_result,
            topology_http_result,
        },
    },
    config::effective_actor_runtime_config,
    model::{Durability, WalPayload, WalRecord},
    object_refs::RefState,
    record_store::RecordProjectionStatus,
    schema::{DatabaseSchema, SchemaHistoryEntry, SchemaMigrationPlan, SchemaValidationReport},
    util::now_ms,
    wal,
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaTypescriptResponse {
    pub(crate) typescript: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaHistoryResponse {
    pub(crate) entries: Vec<SchemaHistoryEntry>,
}

pub(crate) async fn get_schema(State(state): State<AppState>) -> Json<DatabaseSchema> {
    Json(state.schema.schema())
}

pub(crate) async fn get_schema_history(
    State(state): State<AppState>,
) -> Result<Json<SchemaHistoryResponse>, ApiError> {
    let entries = state.schema.history().await.map_err(ApiError::internal)?;
    Ok(Json(SchemaHistoryResponse { entries }))
}

pub(crate) async fn get_schema_version(
    State(state): State<AppState>,
    AxumPath(version): AxumPath<u32>,
) -> Result<Json<DatabaseSchema>, ApiError> {
    let schema = state
        .schema
        .schema_version(version)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("schema version {version} not found")))?;
    Ok(Json(schema))
}

pub(crate) async fn get_schema_typescript(
    State(state): State<AppState>,
) -> Json<SchemaTypescriptResponse> {
    Json(SchemaTypescriptResponse {
        typescript: state.schema.typescript(),
    })
}

pub(crate) async fn validate_schema(State(state): State<AppState>) -> Json<SchemaValidationReport> {
    Json(state.schema.validation_report())
}

pub(crate) async fn schema_storage_policy(
    State(state): State<AppState>,
) -> Json<RuntimeStoragePolicyResponse> {
    Json(RuntimeStoragePolicyResponse {
        hot_window: state.actors.hot_window(),
        max_hot_rooms: state.actors.max_hot_rooms(),
        schema: state.schema.storage_policy_report(),
    })
}

pub(crate) async fn schema_migration_plan(
    State(state): State<AppState>,
) -> Result<Json<SchemaMigrationPlan>, ApiError> {
    let plan = state
        .schema
        .migration_plan_from_disk()
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    Ok(Json(plan))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaReloadResponse {
    pub(crate) name: String,
    pub(crate) version: u32,
    pub(crate) report: SchemaValidationReport,
    pub(crate) migration: SchemaMigrationPlan,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaApplyRequest {
    pub(crate) schema: DatabaseSchema,
    #[serde(default)]
    pub(crate) dry_run: bool,
    #[serde(default)]
    pub(crate) allow_breaking_replay: bool,
    #[serde(default)]
    pub(crate) background_replay: bool,
    pub(crate) expected_version: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaApplyResponse {
    pub(crate) name: String,
    pub(crate) version: u32,
    pub(crate) report: SchemaValidationReport,
    pub(crate) migration: SchemaMigrationPlan,
    pub(crate) applied: bool,
    pub(crate) persisted: bool,
    pub(crate) replay_rebuild: bool,
    pub(crate) breaking_replay_allowed: bool,
    pub(crate) projection_rebuilt: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) background_replay_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) background_replay_phase: Option<SchemaReplayApplyPhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schema_audit_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) peer_preflight: Option<SchemaPeerPreflightReport>,
    pub(crate) projection_status: RecordProjectionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SchemaReplayApplyPhase {
    Idle,
    Running,
    Committing,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaReplayApplyStatus {
    pub(crate) phase: SchemaReplayApplyPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resumed_from_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expected_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schema: Option<DatabaseSchema>,
    pub(crate) allow_breaking_replay: bool,
    pub(crate) replay_rebuild: bool,
    pub(crate) projection_rebuild: bool,
    #[serde(default)]
    pub(crate) resume_eligible: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) resume_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schema_audit_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projection_status: Option<RecordProjectionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

impl Default for SchemaReplayApplyStatus {
    fn default() -> Self {
        Self {
            phase: SchemaReplayApplyPhase::Idle,
            run_id: None,
            resumed_from_run_id: None,
            target_version: None,
            expected_version: None,
            schema: None,
            allow_breaking_replay: false,
            replay_rebuild: false,
            projection_rebuild: false,
            resume_eligible: false,
            resume_reason: None,
            started_at_ms: None,
            finished_at_ms: None,
            schema_audit_lsn: None,
            projection_status: None,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaPeerPreflightReport {
    pub(crate) required_acks: usize,
    pub(crate) acked: usize,
    pub(crate) replicas: Vec<SchemaPeerPreflightResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaPeerPreflightResult {
    pub(crate) node_id: Option<String>,
    pub(crate) url: String,
    pub(crate) ok: bool,
    pub(crate) status: Option<u16>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SchemaProposalPhase {
    Prepared,
    Committed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaProposal {
    pub(crate) id: String,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) proposed_by: String,
    pub(crate) reason: String,
    pub(crate) phase: SchemaProposalPhase,
    pub(crate) expected_version: Option<u32>,
    #[serde(default)]
    pub(crate) allow_breaking_replay: bool,
    pub(crate) schema: DatabaseSchema,
    pub(crate) report: SchemaValidationReport,
    pub(crate) migration: SchemaMigrationPlan,
    #[serde(default)]
    pub(crate) projection_rebuilt: bool,
    pub(crate) projection_status: RecordProjectionStatus,
    #[serde(default)]
    pub(crate) prepare_acks: Vec<TopologyPropagationResult>,
    #[serde(default)]
    pub(crate) commit_acks: Vec<TopologyPropagationResult>,
    #[serde(default)]
    pub(crate) required_acks: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) schema_audit_lsn: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) peer_preflight: Option<SchemaPeerPreflightReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaProposalRequest {
    pub(crate) schema: DatabaseSchema,
    pub(crate) expected_version: Option<u32>,
    #[serde(default)]
    pub(crate) allow_breaking_replay: bool,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaProposalResponse {
    pub(crate) proposal: SchemaProposal,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaProposalListResponse {
    pub(crate) proposals: Vec<SchemaProposal>,
}

pub(crate) async fn load_schema_proposals(
    path: &PathBuf,
) -> Result<BTreeMap<String, SchemaProposal>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn load_schema_replay_apply_status(
    path: &PathBuf,
    schema_wal_recovery: &SchemaWalRecoveryReport,
    projection_status: RecordProjectionStatus,
) -> Result<SchemaReplayApplyStatus> {
    if !path.exists() {
        return Ok(SchemaReplayApplyStatus::default());
    }
    let bytes = fs::read(path).await?;
    let mut status: SchemaReplayApplyStatus = serde_json::from_slice(&bytes)?;
    match status.phase {
        SchemaReplayApplyPhase::Committing
            if status.target_version == schema_wal_recovery.latest_version =>
        {
            status.phase = SchemaReplayApplyPhase::Succeeded;
            status.finished_at_ms = Some(now_ms());
            status.schema_audit_lsn = schema_wal_recovery.latest_lsn;
            status.projection_status = Some(projection_status);
            status.error = None;
            status.resume_eligible = false;
            status.resume_reason = None;
            persist_schema_replay_apply_status_path(path, &status).await?;
        }
        SchemaReplayApplyPhase::Running | SchemaReplayApplyPhase::Committing => {
            status.phase = SchemaReplayApplyPhase::Failed;
            status.finished_at_ms = Some(now_ms());
            status.error = Some(
                "schema replay apply was running during previous shutdown; rerun backgroundReplay to resume"
                    .to_string(),
            );
            mark_schema_replay_resume_eligible(
                &mut status,
                "interrupted before SchemaApplied commit; call resumeSchemaReplayApply() to restart",
            );
            persist_schema_replay_apply_status_path(path, &status).await?;
        }
        _ => {
            reconcile_schema_replay_resume_fields(&mut status);
        }
    }
    Ok(status)
}

pub(crate) async fn persist_schema_proposals(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.schema_proposals_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let proposals = state.schema_proposals.read().await;
    let bytes =
        serde_json::to_vec_pretty(&*proposals).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.schema_proposals_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

async fn persist_schema_replay_apply_status(state: &AppState) -> Result<(), ApiError> {
    let status = state.schema_replay_apply_status.read().await.clone();
    persist_schema_replay_apply_status_path(&state.schema_replay_apply_status_path, &status)
        .await
        .map_err(ApiError::internal)
}

fn mark_schema_replay_resume_eligible(status: &mut SchemaReplayApplyStatus, reason: &str) {
    status.resume_eligible = true;
    status.resume_reason = Some(reason.to_string());
}

fn reconcile_schema_replay_resume_fields(status: &mut SchemaReplayApplyStatus) {
    if status.phase == SchemaReplayApplyPhase::Failed
        && status.schema.is_some()
        && (status.replay_rebuild || status.projection_rebuild)
    {
        if !status.resume_eligible {
            mark_schema_replay_resume_eligible(
                status,
                "failed replay status includes a retryable schema; call resumeSchemaReplayApply() to restart",
            );
        }
    } else {
        status.resume_eligible = false;
        status.resume_reason = None;
    }
}

async fn persist_schema_replay_apply_status_path(
    path: &PathBuf,
    status: &SchemaReplayApplyStatus,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("schema-replay-status.json");
    let tmp = path.with_file_name(format!(".{file_name}.tmp"));
    fs::write(&tmp, serde_json::to_vec_pretty(status)?).await?;
    fs::rename(tmp, path).await?;
    Ok(())
}

pub(crate) async fn reload_schema(
    State(state): State<AppState>,
) -> Result<Json<SchemaReloadResponse>, ApiError> {
    let schema = state
        .schema
        .candidate_from_disk()
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let response = apply_schema_candidate(&state, schema, false, false, None, false, None).await?;
    Ok(Json(SchemaReloadResponse {
        name: response.name,
        version: response.version,
        report: response.report,
        migration: response.migration,
    }))
}

pub(crate) async fn apply_schema(
    State(state): State<AppState>,
    Json(request): Json<SchemaApplyRequest>,
) -> Result<Json<SchemaApplyResponse>, ApiError> {
    if request.background_replay && request.dry_run {
        return Err(ApiError::bad_request(
            "backgroundReplay cannot be combined with dryRun",
        ));
    }
    if request.dry_run {
        return apply_schema_candidate(
            &state,
            request.schema,
            false,
            true,
            request.expected_version,
            request.allow_breaking_replay,
            None,
        )
        .await
        .map(Json);
    }
    if request.background_replay {
        let preflight = apply_schema_candidate(
            &state,
            request.schema.clone(),
            false,
            true,
            request.expected_version,
            request.allow_breaking_replay,
            None,
        )
        .await?;
        if !preflight.replay_rebuild && !preflight.migration.projection_rebuild_required {
            return Err(ApiError::bad_request(
                "backgroundReplay requires a replay or projection-shape rebuild",
            ));
        }
        let run_id = start_schema_replay_apply(
            &state,
            request.schema,
            request.expected_version,
            request.allow_breaking_replay,
            preflight.replay_rebuild,
            preflight.migration.projection_rebuild_required,
            None,
        )
        .await?;
        let mut response = preflight;
        response.background_replay_run_id = Some(run_id);
        response.background_replay_phase = Some(SchemaReplayApplyPhase::Running);
        return Ok(Json(response));
    }
    let _guard = state.schema_apply_lock.lock().await;
    apply_schema_candidate(
        &state,
        request.schema,
        true,
        true,
        request.expected_version,
        request.allow_breaking_replay,
        None,
    )
    .await
    .map(Json)
}

pub(crate) async fn schema_replay_apply_status(
    State(state): State<AppState>,
) -> Json<SchemaReplayApplyStatus> {
    Json(state.schema_replay_apply_status.read().await.clone())
}

pub(crate) async fn retry_schema_replay_apply(
    State(state): State<AppState>,
) -> Result<Json<SchemaApplyResponse>, ApiError> {
    restart_schema_replay_apply(State(state)).await
}

pub(crate) async fn resume_schema_replay_apply(
    State(state): State<AppState>,
) -> Result<Json<SchemaApplyResponse>, ApiError> {
    restart_schema_replay_apply(State(state)).await
}

async fn restart_schema_replay_apply(
    State(state): State<AppState>,
) -> Result<Json<SchemaApplyResponse>, ApiError> {
    let status = state.schema_replay_apply_status.read().await.clone();
    if matches!(
        status.phase,
        SchemaReplayApplyPhase::Running | SchemaReplayApplyPhase::Committing
    ) {
        return Err(ApiError::conflict("schema replay apply already running"));
    }
    if status.phase != SchemaReplayApplyPhase::Failed {
        return Err(ApiError::conflict(
            "schema replay retry requires a failed replay status",
        ));
    }
    let schema = status.schema.clone().ok_or_else(|| {
        ApiError::conflict("failed schema replay status does not include a retryable schema")
    })?;
    let preflight = apply_schema_candidate(
        &state,
        schema.clone(),
        false,
        true,
        status.expected_version,
        status.allow_breaking_replay,
        None,
    )
    .await?;
    if !preflight.replay_rebuild && !preflight.migration.projection_rebuild_required {
        return Err(ApiError::bad_request(
            "schema replay retry requires a replay or projection-shape rebuild",
        ));
    }
    let resumed_from_run_id = status.run_id.clone();
    let run_id = start_schema_replay_apply(
        &state,
        schema,
        status.expected_version,
        status.allow_breaking_replay,
        preflight.replay_rebuild,
        preflight.migration.projection_rebuild_required,
        resumed_from_run_id,
    )
    .await?;
    let mut response = preflight;
    response.background_replay_run_id = Some(run_id);
    response.background_replay_phase = Some(SchemaReplayApplyPhase::Running);
    Ok(Json(response))
}

pub(crate) async fn cancel_schema_replay_apply(
    State(state): State<AppState>,
) -> Result<Json<SchemaReplayApplyStatus>, ApiError> {
    {
        let mut status = state.schema_replay_apply_status.write().await;
        match status.phase {
            SchemaReplayApplyPhase::Running => {
                status.phase = SchemaReplayApplyPhase::Cancelled;
                status.finished_at_ms = Some(now_ms());
                status.error =
                    Some("cancelled by operator before SchemaApplied commit".to_string());
                status.resume_eligible = false;
                status.resume_reason = None;
            }
            SchemaReplayApplyPhase::Committing => {
                return Err(ApiError::conflict(
                    "schema replay apply is already committing",
                ));
            }
            _ => {
                return Err(ApiError::conflict(
                    "schema replay cancel requires a running replay status",
                ));
            }
        }
    }
    persist_schema_replay_apply_status(&state).await?;
    Ok(Json(state.schema_replay_apply_status.read().await.clone()))
}

pub(crate) async fn preflight_schema(
    State(state): State<AppState>,
    Json(request): Json<SchemaApplyRequest>,
) -> Result<Json<SchemaApplyResponse>, ApiError> {
    apply_schema_candidate(
        &state,
        request.schema,
        false,
        false,
        request.expected_version,
        request.allow_breaking_replay,
        None,
    )
    .await
    .map(Json)
}

pub(crate) async fn list_schema_proposals(
    State(state): State<AppState>,
) -> Json<SchemaProposalListResponse> {
    let proposals = state
        .schema_proposals
        .read()
        .await
        .values()
        .cloned()
        .collect();
    Json(SchemaProposalListResponse { proposals })
}

pub(crate) async fn start_schema_proposal(
    State(state): State<AppState>,
    Json(request): Json<SchemaProposalRequest>,
) -> Result<Json<SchemaProposalResponse>, ApiError> {
    let shard = writable_wal_shard_for_index(&state, 0).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;
    let response = apply_schema_candidate(
        &state,
        request.schema.clone(),
        false,
        true,
        request.expected_version,
        request.allow_breaking_replay,
        None,
    )
    .await?;
    let now = now_ms();
    let mut proposal = SchemaProposal {
        id: Uuid::now_v7().to_string(),
        created_at_ms: now,
        updated_at_ms: now,
        proposed_by: state.cluster.node_id().to_string(),
        reason: request
            .reason
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| "operator schema proposal".to_string()),
        phase: SchemaProposalPhase::Prepared,
        expected_version: request.expected_version,
        allow_breaking_replay: request.allow_breaking_replay,
        schema: request.schema,
        report: response.report,
        migration: response.migration,
        projection_rebuilt: response.projection_rebuilt,
        projection_status: response.projection_status,
        prepare_acks: vec![self_topology_result(&state, "schema prepare")],
        commit_acks: Vec::new(),
        required_acks: required_schema_proposal_acks(&state).await,
        schema_audit_lsn: None,
        peer_preflight: None,
        last_error: None,
    };
    let peer_results = prepare_schema_proposal_on_peers(&state, &proposal).await;
    proposal.prepare_acks.extend(peer_results);
    let acked = proposal
        .prepare_acks
        .iter()
        .filter(|result| result.applied)
        .count();
    if acked < proposal.required_acks {
        proposal.phase = SchemaProposalPhase::Failed;
        proposal.last_error = Some(format!(
            "schema proposal prepare requires {} acks, got {acked}",
            proposal.required_acks
        ));
    }
    proposal.updated_at_ms = now_ms();
    state
        .schema_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_schema_proposals(&state).await?;
    if proposal.phase == SchemaProposalPhase::Failed {
        return Err(ApiError::conflict(
            proposal
                .last_error
                .clone()
                .unwrap_or_else(|| "schema proposal prepare failed".to_string()),
        ));
    }
    Ok(Json(SchemaProposalResponse { proposal }))
}

pub(crate) async fn commit_schema_proposal(
    State(state): State<AppState>,
    AxumPath(proposal_id): AxumPath<String>,
) -> Result<Json<SchemaProposalResponse>, ApiError> {
    let shard = writable_wal_shard_for_index(&state, 0).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;
    let proposal = state
        .schema_proposals
        .read()
        .await
        .get(&proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("schema proposal not found"))?;
    match proposal.phase {
        SchemaProposalPhase::Committed => return Ok(Json(SchemaProposalResponse { proposal })),
        SchemaProposalPhase::Aborted => {
            return Err(ApiError::conflict(
                "aborted schema proposal cannot be committed",
            ));
        }
        SchemaProposalPhase::Failed => {
            return Err(ApiError::conflict(
                "failed schema proposal cannot be committed",
            ));
        }
        SchemaProposalPhase::Prepared => {}
    }

    let _guard = state.schema_apply_lock.lock().await;
    let apply = apply_schema_candidate(
        &state,
        proposal.schema.clone(),
        true,
        true,
        proposal.expected_version,
        proposal.allow_breaking_replay,
        None,
    )
    .await;
    let mut next = proposal;
    match apply {
        Ok(apply) => {
            next.phase = SchemaProposalPhase::Committed;
            next.updated_at_ms = now_ms();
            next.report = apply.report;
            next.migration = apply.migration;
            next.projection_rebuilt = apply.projection_rebuilt;
            next.projection_status = apply.projection_status;
            next.schema_audit_lsn = apply.schema_audit_lsn;
            next.peer_preflight = apply.peer_preflight;
            next.commit_acks = vec![self_topology_result(&state, "schema commit")];
            let peer_results = commit_schema_proposal_on_peers(&state, &next).await;
            next.commit_acks.extend(peer_results);
            let commit_acked = next
                .commit_acks
                .iter()
                .filter(|result| result.applied)
                .count();
            next.last_error = if commit_acked < next.required_acks {
                Some(format!(
                    "schema proposal commit requires {} acks, got {commit_acked}",
                    next.required_acks
                ))
            } else {
                None
            };
            state
                .schema_proposals
                .write()
                .await
                .insert(next.id.clone(), next.clone());
            persist_schema_proposals(&state).await?;
            Ok(Json(SchemaProposalResponse { proposal: next }))
        }
        Err(error) => {
            next.phase = SchemaProposalPhase::Failed;
            next.updated_at_ms = now_ms();
            next.last_error = Some(error.message.clone());
            state
                .schema_proposals
                .write()
                .await
                .insert(next.id.clone(), next);
            persist_schema_proposals(&state).await?;
            Err(error)
        }
    }
}

pub(crate) async fn abort_schema_proposal(
    State(state): State<AppState>,
    AxumPath(proposal_id): AxumPath<String>,
) -> Result<Json<SchemaProposalResponse>, ApiError> {
    let shard = writable_wal_shard_for_index(&state, 0).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;
    let mut proposal = state
        .schema_proposals
        .read()
        .await
        .get(&proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("schema proposal not found"))?;
    if proposal.phase == SchemaProposalPhase::Committed {
        return Err(ApiError::conflict(
            "committed schema proposal cannot be aborted",
        ));
    }
    proposal.phase = SchemaProposalPhase::Aborted;
    proposal.updated_at_ms = now_ms();
    proposal.last_error = Some("aborted by operator".to_string());
    let _ = abort_schema_proposal_on_peers(&state, &proposal).await;
    state
        .schema_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_schema_proposals(&state).await?;
    Ok(Json(SchemaProposalResponse { proposal }))
}

async fn required_schema_proposal_acks(state: &AppState) -> usize {
    let Some(shard) = state.wal_shards.first() else {
        return 1;
    };
    let replica_count = schema_preflight_targets_for_shard(state, 0).await.len();
    wal::required_remote_acks(shard.remote_ack_policy, replica_count) + 1
}

async fn prepare_schema_proposal_on_peers(
    state: &AppState,
    proposal: &SchemaProposal,
) -> Vec<TopologyPropagationResult> {
    let http = reqwest::Client::new();
    let mut results = Vec::new();
    for (node_id, url) in schema_preflight_targets_for_shard(state, 0).await {
        let node_id = node_id.unwrap_or_else(|| url.clone());
        let endpoint = schema_proposal_prepare_endpoint(&url);
        let response = http.post(&endpoint).json(proposal).send().await;
        results.push(topology_http_result(node_id, endpoint, response, "schema prepare").await);
    }
    results
}

async fn abort_schema_proposal_on_peers(
    state: &AppState,
    proposal: &SchemaProposal,
) -> Vec<TopologyPropagationResult> {
    let http = reqwest::Client::new();
    let mut results = Vec::new();
    for (node_id, url) in schema_preflight_targets_for_shard(state, 0).await {
        let node_id = node_id.unwrap_or_else(|| url.clone());
        let endpoint = schema_proposal_abort_endpoint(&url);
        let request = TopologyProposalAbortRequest {
            proposal_id: proposal.id.clone(),
        };
        let response = http.post(&endpoint).json(&request).send().await;
        results.push(topology_http_result(node_id, endpoint, response, "schema abort").await);
    }
    results
}

async fn commit_schema_proposal_on_peers(
    state: &AppState,
    proposal: &SchemaProposal,
) -> Vec<TopologyPropagationResult> {
    let http = reqwest::Client::new();
    let mut results = Vec::new();
    for (node_id, url) in schema_preflight_targets_for_shard(state, 0).await {
        let node_id = node_id.unwrap_or_else(|| url.clone());
        let endpoint = schema_proposal_commit_endpoint(&url);
        let response = http.post(&endpoint).json(proposal).send().await;
        results.push(topology_http_result(node_id, endpoint, response, "schema commit").await);
    }
    results
}

fn schema_proposal_prepare_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/schema/proposals/prepare") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/schema/proposals/prepare")
    }
}

fn schema_proposal_commit_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/schema/proposals/commit") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/schema/proposals/commit")
    }
}

fn schema_proposal_abort_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/schema/proposals/abort") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/schema/proposals/abort")
    }
}

pub(crate) async fn commit_schema_proposal_peer(
    State(state): State<AppState>,
    Json(mut proposal): Json<SchemaProposal>,
) -> Result<Json<SchemaProposalResponse>, ApiError> {
    let existing = state
        .schema_proposals
        .read()
        .await
        .get(&proposal.id)
        .cloned();
    if let Some(existing) = existing
        && existing.phase == SchemaProposalPhase::Aborted
    {
        return Err(ApiError::conflict(
            "aborted schema proposal cannot be committed",
        ));
    }
    proposal.phase = SchemaProposalPhase::Committed;
    proposal.updated_at_ms = now_ms();
    proposal.last_error = None;
    state
        .schema_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_schema_proposals(&state).await?;
    Ok(Json(SchemaProposalResponse { proposal }))
}

pub(crate) async fn prepare_schema_proposal_peer(
    State(state): State<AppState>,
    Json(mut proposal): Json<SchemaProposal>,
) -> Result<Json<SchemaProposalResponse>, ApiError> {
    let response = apply_schema_candidate(
        &state,
        proposal.schema.clone(),
        false,
        false,
        proposal.expected_version,
        proposal.allow_breaking_replay,
        None,
    )
    .await?;
    proposal.phase = SchemaProposalPhase::Prepared;
    proposal.updated_at_ms = now_ms();
    proposal.report = response.report;
    proposal.migration = response.migration;
    proposal.projection_status = response.projection_status;
    proposal.last_error = None;
    state
        .schema_proposals
        .write()
        .await
        .insert(proposal.id.clone(), proposal.clone());
    persist_schema_proposals(&state).await?;
    Ok(Json(SchemaProposalResponse { proposal }))
}

pub(crate) async fn abort_schema_proposal_peer(
    State(state): State<AppState>,
    Json(request): Json<TopologyProposalAbortRequest>,
) -> Result<Json<SchemaProposalResponse>, ApiError> {
    let mut proposal = state
        .schema_proposals
        .read()
        .await
        .get(&request.proposal_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("schema proposal not found"))?;
    if proposal.phase != SchemaProposalPhase::Committed {
        proposal.phase = SchemaProposalPhase::Aborted;
        proposal.updated_at_ms = now_ms();
        proposal.last_error = Some("aborted by coordinator".to_string());
        state
            .schema_proposals
            .write()
            .await
            .insert(proposal.id.clone(), proposal.clone());
        persist_schema_proposals(&state).await?;
    }
    Ok(Json(SchemaProposalResponse { proposal }))
}

async fn apply_schema_candidate(
    state: &AppState,
    schema: DatabaseSchema,
    apply: bool,
    persist: bool,
    expected_version: Option<u32>,
    allow_breaking_replay: bool,
    schema_replay_run_id: Option<&str>,
) -> Result<SchemaApplyResponse, ApiError> {
    if let Some(expected_version) = expected_version {
        let active_version = state.schema.schema().version;
        if active_version != expected_version {
            return Err(ApiError::conflict_with_details(
                format!(
                    "schema version conflict: expected active version {expected_version}, got {active_version}"
                ),
                serde_json::json!({
                    "schemaVersionConflict": true,
                    "expectedVersion": expected_version,
                    "activeVersion": active_version,
                }),
            ));
        }
    }
    schema
        .validation_report()
        .into_result()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let migration = state.schema.migration_plan_for(&schema);
    let replay_rebuild = allow_breaking_replay && migration.can_replay_rebuild();
    let projection_rebuild = replay_rebuild || migration.projection_rebuild_required;
    if !migration.compatible && !replay_rebuild {
        migration
            .clone()
            .into_result()
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    }
    let (messages, records, _, _) =
        read_startup_projections_from_wal_paths(&state.wal_paths).map_err(ApiError::internal)?;
    validate_loaded_behavior_manifests_schema(state, &schema).await?;
    validate_records_object_refs_against_schema(state, &schema, &records).await?;
    let schema_indexes = schema_indexes_by_table(&schema);
    let schema_orders =
        schema_orders_by_table(&schema).map_err(|err| ApiError::bad_request(err.to_string()))?;
    if projection_rebuild {
        state
            .records
            .validate_rebuild_from_records_with_indexes(&records, &schema_indexes, &schema_orders)
            .await
            .map_err(ApiError::internal)?;
    }
    if apply && persist {
        let shard = writable_wal_shard_for_index(state, 0).await?;
        ensure_shard_not_frozen(state, shard.index).await?;
    }
    let peer_preflight = if apply && persist {
        Some(preflight_schema_on_remote_replicas(state, &schema, allow_breaking_replay).await?)
    } else {
        None
    };
    let staged_projection_rebuild = if apply && projection_rebuild {
        Some(
            state
                .records
                .stage_rebuild_from_records_with_indexes(&records, &schema_indexes, &schema_orders)
                .await
                .map_err(ApiError::internal)?,
        )
    } else {
        None
    };
    let rebuilt_object_refs = if apply {
        Some(
            RefState::from_messages_and_records_for_schema(&messages, &records, &schema)
                .map_err(ApiError::internal)?,
        )
    } else {
        None
    };
    if apply && persist {
        if let Some(run_id) = schema_replay_run_id {
            mark_schema_replay_committing(state, run_id).await?;
        }
    }
    let schema_audit_lsn = if apply && persist {
        Some(
            append_schema_applied_wal_record(state, schema.clone(), migration.clone())
                .await?
                .lsn,
        )
    } else {
        None
    };
    if apply {
        if persist {
            state
                .schema
                .persist_candidate(&schema)
                .await
                .map_err(ApiError::internal)?;
        }
        if let Some(staged_projection_rebuild) = staged_projection_rebuild {
            staged_projection_rebuild
                .commit()
                .await
                .map_err(ApiError::internal)?;
        }
        if let Some(rebuilt_object_refs) = rebuilt_object_refs {
            state
                .object_refs
                .replace_with(rebuilt_object_refs)
                .await
                .map_err(ApiError::internal)?;
        }
        state
            .record_hot
            .reconfigure(&schema, &records, state.record_hot_durable_idle_ttl_ms)
            .await;
        let (hot_window, max_hot_rooms, hot_room_idle_ttl_ms) =
            effective_actor_runtime_config(&schema);
        state
            .actors
            .reconfigure(hot_window, max_hot_rooms, hot_room_idle_ttl_ms)
            .await;
        state.schema.apply(schema.clone());
    }
    let projection_status = state
        .records
        .projection_status()
        .await
        .map_err(ApiError::internal)?;
    let report = schema.validation_report();
    Ok(SchemaApplyResponse {
        name: schema.name,
        version: schema.version,
        report,
        migration,
        applied: apply,
        persisted: apply && persist,
        replay_rebuild,
        breaking_replay_allowed: allow_breaking_replay,
        projection_rebuilt: apply && projection_rebuild,
        background_replay_run_id: None,
        background_replay_phase: None,
        schema_audit_lsn,
        peer_preflight,
        projection_status,
    })
}

async fn start_schema_replay_apply(
    state: &AppState,
    schema: DatabaseSchema,
    expected_version: Option<u32>,
    allow_breaking_replay: bool,
    replay_rebuild: bool,
    projection_rebuild: bool,
    resumed_from_run_id: Option<String>,
) -> Result<String, ApiError> {
    let run_id = Uuid::now_v7().to_string();
    let next_status = SchemaReplayApplyStatus {
        phase: SchemaReplayApplyPhase::Running,
        run_id: Some(run_id.clone()),
        resumed_from_run_id,
        target_version: Some(schema.version),
        expected_version,
        schema: Some(schema.clone()),
        allow_breaking_replay,
        replay_rebuild,
        projection_rebuild,
        resume_eligible: false,
        resume_reason: None,
        started_at_ms: Some(now_ms()),
        finished_at_ms: None,
        schema_audit_lsn: None,
        projection_status: None,
        error: None,
    };
    {
        let mut status = state.schema_replay_apply_status.write().await;
        if matches!(
            status.phase,
            SchemaReplayApplyPhase::Running | SchemaReplayApplyPhase::Committing
        ) {
            return Err(ApiError::conflict("schema replay apply already running"));
        }
        *status = next_status;
    }
    if let Err(err) = persist_schema_replay_apply_status(state).await {
        *state.schema_replay_apply_status.write().await = SchemaReplayApplyStatus::default();
        return Err(err);
    }

    let task_state = state.clone();
    let task_run_id = run_id.clone();
    tokio::spawn(async move {
        let _guard = task_state.schema_apply_lock.lock().await;
        let result = apply_schema_candidate(
            &task_state,
            schema,
            true,
            true,
            expected_version,
            allow_breaking_replay,
            Some(&task_run_id),
        )
        .await;
        finish_schema_replay_apply(&task_state, &task_run_id, result).await;
    });
    Ok(run_id)
}

async fn mark_schema_replay_committing(state: &AppState, run_id: &str) -> Result<(), ApiError> {
    {
        let mut status = state.schema_replay_apply_status.write().await;
        if status.run_id.as_deref() != Some(run_id) {
            return Err(ApiError::conflict(
                "schema replay apply run changed before commit",
            ));
        }
        match status.phase {
            SchemaReplayApplyPhase::Running => {
                status.phase = SchemaReplayApplyPhase::Committing;
                status.error = None;
                status.resume_eligible = false;
                status.resume_reason = None;
            }
            SchemaReplayApplyPhase::Cancelled => {
                return Err(ApiError::conflict(
                    "schema replay apply cancelled before commit",
                ));
            }
            SchemaReplayApplyPhase::Committing => {}
            _ => {
                return Err(ApiError::conflict(
                    "schema replay apply is no longer running",
                ));
            }
        }
    }
    persist_schema_replay_apply_status(state).await
}

async fn finish_schema_replay_apply(
    state: &AppState,
    run_id: &str,
    result: Result<SchemaApplyResponse, ApiError>,
) {
    let mut status = state.schema_replay_apply_status.write().await;
    if status.run_id.as_deref() != Some(run_id) || status.phase == SchemaReplayApplyPhase::Cancelled
    {
        return;
    }
    status.finished_at_ms = Some(now_ms());
    match result {
        Ok(response) => {
            status.phase = SchemaReplayApplyPhase::Succeeded;
            status.schema_audit_lsn = response.schema_audit_lsn;
            status.projection_status = Some(response.projection_status);
            status.replay_rebuild = response.replay_rebuild;
            status.projection_rebuild = response.projection_rebuilt;
            status.error = None;
            status.resume_eligible = false;
            status.resume_reason = None;
        }
        Err(err) => {
            status.phase = SchemaReplayApplyPhase::Failed;
            status.error = Some(err.message);
            if status.schema.is_some() && (status.replay_rebuild || status.projection_rebuild) {
                mark_schema_replay_resume_eligible(
                    &mut status,
                    "failed replay status includes a retryable schema; call resumeSchemaReplayApply() to restart",
                );
            } else {
                status.resume_eligible = false;
                status.resume_reason = None;
            }
        }
    }
    drop(status);
    let _ = persist_schema_replay_apply_status(state).await;
}

async fn preflight_schema_on_remote_replicas(
    state: &AppState,
    schema: &DatabaseSchema,
    allow_breaking_replay: bool,
) -> Result<SchemaPeerPreflightReport, ApiError> {
    let Some(shard) = state.wal_shards.first() else {
        return Ok(SchemaPeerPreflightReport {
            required_acks: 0,
            acked: 0,
            replicas: Vec::new(),
        });
    };
    let replicas = schema_preflight_targets_for_shard(state, 0).await;
    let required_acks = wal::required_remote_acks(shard.remote_ack_policy, replicas.len());
    if replicas.is_empty() || required_acks == 0 {
        return Ok(SchemaPeerPreflightReport {
            required_acks,
            acked: 0,
            replicas: Vec::new(),
        });
    }

    let http = reqwest::Client::new();
    let mut results = Vec::with_capacity(replicas.len());
    for (node_id, url) in replicas {
        let endpoint = schema_preflight_endpoint(&url);
        let response = http
            .post(&endpoint)
            .json(&SchemaApplyRequest {
                schema: schema.clone(),
                dry_run: true,
                allow_breaking_replay,
                background_replay: false,
                expected_version: None,
            })
            .send()
            .await;
        results.push(schema_preflight_http_result(node_id, endpoint, response).await);
    }
    let acked = results.iter().filter(|result| result.ok).count();
    let report = SchemaPeerPreflightReport {
        required_acks,
        acked,
        replicas: results,
    };
    if acked < required_acks {
        return Err(ApiError::conflict_with_details(
            format!("schema peer preflight requires {required_acks} acks, got {acked}"),
            serde_json::json!({
                "peerPreflight": report,
            }),
        ));
    }
    Ok(report)
}

async fn schema_preflight_targets_for_shard(
    state: &AppState,
    shard: usize,
) -> Vec<(Option<String>, String)> {
    if let Some(urls) = &state.explicit_wal_remote_replica_urls {
        return urls.iter().cloned().map(|url| (None, url)).collect();
    }

    let overrides = state.topology_overrides.read().await;
    state
        .cluster
        .replicas_for_shard_with_overrides(shard, &overrides)
        .into_iter()
        .filter(|node_id| node_id != state.cluster.node_id())
        .filter_map(|node_id| {
            state
                .cluster
                .node_url_for(&node_id)
                .map(|url| (Some(node_id), url))
        })
        .collect()
}

async fn schema_preflight_http_result(
    node_id: Option<String>,
    endpoint: String,
    response: Result<reqwest::Response, reqwest::Error>,
) -> SchemaPeerPreflightResult {
    match response {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                SchemaPeerPreflightResult {
                    node_id,
                    url: endpoint,
                    ok: true,
                    status: Some(status.as_u16()),
                    error: None,
                }
            } else {
                let text = response.text().await.unwrap_or_default();
                SchemaPeerPreflightResult {
                    node_id,
                    url: endpoint,
                    ok: false,
                    status: Some(status.as_u16()),
                    error: Some(text),
                }
            }
        }
        Err(err) => SchemaPeerPreflightResult {
            node_id,
            url: endpoint,
            ok: false,
            status: None,
            error: Some(format!("preflight failed: {err}")),
        },
    }
}

fn schema_preflight_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/schema/preflight") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/schema/preflight")
    }
}

async fn append_schema_applied_wal_record(
    state: &AppState,
    schema: DatabaseSchema,
    migration: SchemaMigrationPlan,
) -> Result<WalRecord, ApiError> {
    let shard = writable_wal_shard_for_index(state, 0).await?;
    ensure_shard_not_frozen(state, shard.index).await?;
    let record = append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        schema.version,
        WalPayload::SchemaApplied { schema, migration },
    )
    .await?;
    Ok(record)
}
