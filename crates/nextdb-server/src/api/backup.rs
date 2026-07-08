use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    path::{Component, Path, PathBuf},
    sync::atomic::Ordering,
};

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::Result;
use axum::{
    Json,
    body::Bytes,
    extract::{Query, State},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{fs, io::AsyncWriteExt};
use tracing::warn;
use uuid::Uuid;

use crate::{
    AppState,
    api::audit::wal_payload_type,
    api::error::ApiError,
    api::records::{records_from_wal_records, schema_indexes_by_table, schema_orders_by_table},
    api::schema::{SchemaProposal, persist_schema_proposals},
    api::topology::{
        TopologyLease, TopologyLogEntry, TopologyProposal, persist_handoff_workflows,
        persist_topology_lease, persist_topology_overrides, persist_topology_proposals,
        read_topology_log, write_topology_log_entries,
    },
    api::wal::{
        apply_replicated_wal_record, maybe_checkpoint, refresh_wal_remote_replicas_for_shard,
        wait_for_replicated_record_projection,
    },
    cluster::ClusterShardOverride,
    commit_object_delete, commit_object_put,
    config::{EXPORT_BUNDLE_ARCHIVE_CONTENT_TYPE, EXPORT_BUNDLE_ARCHIVE_FORMAT, env_bool},
    model::{
        DbRecord, DbRecordMutationDraft, ObjectMetadata, WalChecksumStatus, WalPayload, WalRecord,
    },
    object_store::ensure_safe_object_id,
    schema::DatabaseSchema,
    tasks::HandoffWorkflow,
    util::{hex_lower, now_ms},
    wal,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportManifestQuery {
    pub(crate) include_samples: Option<bool>,
    pub(crate) sample_limit: Option<usize>,
    pub(crate) base_lsn: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleCreateRequest {
    pub(crate) encryption_key: Option<String>,
    pub(crate) base_lsn: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupRunRequest {
    pub(crate) encryption_key: Option<String>,
    pub(crate) force_full: Option<bool>,
    pub(crate) archive_object: Option<bool>,
    pub(crate) object_id: Option<String>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupRetentionRequest {
    pub(crate) dry_run: Option<bool>,
    pub(crate) keep_last: Option<usize>,
    pub(crate) before_timestamp_ms: Option<u64>,
    pub(crate) delete_bundles: Option<bool>,
    pub(crate) delete_archive_objects: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupPolicy {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) interval_ms: u64,
    #[serde(default = "default_true")]
    pub(crate) archive_object: bool,
    #[serde(default)]
    pub(crate) retention_keep_last: Option<usize>,
    #[serde(default = "default_true")]
    pub(crate) retention_delete_bundles: bool,
    #[serde(default)]
    pub(crate) retention_delete_archive_objects: bool,
}

impl Default for ExportBackupPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_ms: 0,
            archive_object: true,
            retention_keep_last: None,
            retention_delete_bundles: true,
            retention_delete_archive_objects: false,
        }
    }
}

fn default_true() -> bool {
    true
}

pub(crate) fn default_export_backup_policy() -> ExportBackupPolicy {
    ExportBackupPolicy {
        enabled: env_bool("NEXTDB_BACKUP_ENABLED", false),
        interval_ms: std::env::var("NEXTDB_BACKUP_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        archive_object: env_bool("NEXTDB_BACKUP_ARCHIVE_OBJECT", true),
        retention_keep_last: std::env::var("NEXTDB_BACKUP_KEEP_LAST")
            .ok()
            .and_then(|value| value.parse::<usize>().ok()),
        retention_delete_bundles: env_bool("NEXTDB_BACKUP_RETENTION_DELETE_BUNDLES", true),
        retention_delete_archive_objects: env_bool(
            "NEXTDB_BACKUP_RETENTION_DELETE_ARCHIVE_OBJECTS",
            false,
        ),
    }
}

pub(crate) async fn load_export_backup_runs(path: &PathBuf) -> Result<Vec<ExportBackupRunRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn load_export_backup_policy(path: &PathBuf) -> Result<ExportBackupPolicy> {
    if !path.exists() {
        return Ok(default_export_backup_policy());
    }
    let bytes = fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn persist_export_backup_runs(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.export_backup_runs_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let runs = state.export_backup_runs.read().await;
    let bytes = serde_json::to_vec_pretty(&*runs).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.export_backup_runs_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn persist_export_backup_policy(state: &AppState) -> Result<(), ApiError> {
    if let Some(parent) = state.export_backup_policy_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    let policy = state.export_backup_policy.read().await;
    let bytes =
        serde_json::to_vec_pretty(&*policy).map_err(|err| ApiError::internal(err.into()))?;
    fs::write(&state.export_backup_policy_path, bytes)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) fn validate_export_backup_policy(policy: &ExportBackupPolicy) -> Result<(), ApiError> {
    if policy.enabled && policy.interval_ms == 0 {
        return Err(ApiError::bad_request(
            "intervalMs must be greater than 0 when backup policy is enabled",
        ));
    }
    Ok(())
}

pub(crate) fn build_export_backup_run_record(
    mode: String,
    base_lsn: u64,
    current_lsn: u64,
    no_op: bool,
    bundle: Option<&ExportBundleResponse>,
    archived: Option<&ExportBundleArchiveObjectResponse>,
    chain: Option<&ExportBundleChainVerifyResponse>,
) -> ExportBackupRunRecord {
    ExportBackupRunRecord {
        id: Uuid::now_v7().to_string(),
        created_at_ms: now_ms(),
        mode,
        base_lsn,
        current_lsn,
        no_op,
        bundle_id: bundle.map(|bundle| bundle.id.clone()),
        object_id: archived.map(|archived| archived.object.id.clone()),
        chain_bundle_ids: chain
            .map(|chain| chain.bundles.iter().map(|entry| entry.id.clone()).collect())
            .unwrap_or_default(),
        chain_ok: chain.map(|chain| chain.ok),
        bundle_wal_records: bundle.map(|bundle| bundle.wal_records),
        bundle_objects: bundle.map(|bundle| bundle.objects),
        bundle_object_bytes: bundle.map(|bundle| bundle.object_bytes),
        archive_bytes: archived.map(|archived| archived.bytes),
    }
}

pub(crate) async fn append_export_backup_run(
    state: &AppState,
    record: ExportBackupRunRecord,
) -> Result<ExportBackupRunRecord, ApiError> {
    {
        let mut runs = state.export_backup_runs.write().await;
        runs.push(record.clone());
    }
    persist_export_backup_runs(state).await?;
    Ok(record)
}

pub(crate) async fn export_manifest(
    State(state): State<AppState>,
    Query(query): Query<ExportManifestQuery>,
) -> Result<Json<ExportManifestResponse>, ApiError> {
    Ok(Json(build_export_manifest(&state, query).await?))
}

pub(crate) async fn create_export_bundle(
    State(state): State<AppState>,
    request: Option<Json<ExportBundleCreateRequest>>,
) -> Result<Json<ExportBundleResponse>, ApiError> {
    let request = request.map(|Json(request)| request).unwrap_or_default();
    create_export_bundle_internal(&state, request)
        .await
        .map(Json)
}

pub(crate) async fn list_export_bundles(
    State(state): State<AppState>,
) -> Result<Json<ExportBundleListResponse>, ApiError> {
    Ok(Json(ExportBundleListResponse {
        bundles: read_export_bundle_entries(&state).await?,
    }))
}

pub(crate) async fn verify_export_bundle(
    State(state): State<AppState>,
    axum::extract::Path(bundle_id): axum::extract::Path<String>,
    request: Option<Json<ExportBundleAccessRequest>>,
) -> Result<Json<ExportBundleVerifyResponse>, ApiError> {
    verify_export_bundle_internal(
        &state,
        bundle_id,
        bundle_encryption_key(request.and_then(|Json(request)| request.encryption_key)),
    )
    .await
    .map(Json)
}

pub(crate) async fn verify_export_bundle_chain(
    State(state): State<AppState>,
    request: Option<Json<ExportBundleChainVerifyRequest>>,
) -> Result<Json<ExportBundleChainVerifyResponse>, ApiError> {
    let request = request.map(|Json(request)| request).unwrap_or_default();
    verify_export_bundle_chain_internal(
        &state,
        request.bundle_ids,
        bundle_encryption_key(request.encryption_key),
    )
    .await
    .map(Json)
}

pub(crate) async fn list_export_backup_runs(
    State(state): State<AppState>,
) -> Result<Json<ExportBackupRunListResponse>, ApiError> {
    let mut runs = state.export_backup_runs.read().await.clone();
    runs.sort_by_key(|run| std::cmp::Reverse(run.created_at_ms));
    Ok(Json(ExportBackupRunListResponse { runs }))
}

pub(crate) async fn get_export_backup_policy(
    State(state): State<AppState>,
) -> Result<Json<ExportBackupPolicyResponse>, ApiError> {
    Ok(Json(ExportBackupPolicyResponse {
        policy: state.export_backup_policy.read().await.clone(),
        controller: state.export_backup_controller.read().await.clone(),
    }))
}

pub(crate) async fn set_export_backup_policy(
    State(state): State<AppState>,
    Json(policy): Json<ExportBackupPolicy>,
) -> Result<Json<ExportBackupPolicyResponse>, ApiError> {
    validate_export_backup_policy(&policy)?;
    {
        let mut current = state.export_backup_policy.write().await;
        *current = policy.clone();
    }
    persist_export_backup_policy(&state).await?;
    {
        let mut controller = state.export_backup_controller.write().await;
        controller.enabled = policy.enabled;
        controller.interval_ms = policy.interval_ms;
    }
    Ok(Json(ExportBackupPolicyResponse {
        policy,
        controller: state.export_backup_controller.read().await.clone(),
    }))
}

pub(crate) async fn run_export_backup_policy(
    State(state): State<AppState>,
) -> Result<Json<ExportBackupPolicyRunResponse>, ApiError> {
    let policy = state.export_backup_policy.read().await.clone();
    let ran_at = now_ms();
    match run_export_backup_policy_internal(&state, policy.clone()).await {
        Ok(response) => {
            {
                let mut controller = state.export_backup_controller.write().await;
                controller.enabled = policy.enabled;
                controller.interval_ms = policy.interval_ms;
                controller.last_run_at_ms = Some(ran_at);
                controller.last_run_id = Some(response.backup.run.id.clone());
                controller.last_error = None;
            }
            Ok(Json(response))
        }
        Err(err) => {
            let mut controller = state.export_backup_controller.write().await;
            controller.enabled = policy.enabled;
            controller.interval_ms = policy.interval_ms;
            controller.last_run_at_ms = Some(ran_at);
            controller.last_error = Some(err.message.clone());
            Err(err)
        }
    }
}

pub(crate) async fn run_export_backup_policy_internal(
    state: &AppState,
    policy: ExportBackupPolicy,
) -> Result<ExportBackupPolicyRunResponse, ApiError> {
    validate_export_backup_policy(&policy)?;
    let backup = run_export_backup_internal(
        state,
        ExportBackupRunRequest {
            encryption_key: None,
            force_full: Some(false),
            archive_object: Some(policy.archive_object),
            object_id: None,
            client_mutation_id: None,
        },
    )
    .await?;
    let retention = if policy.retention_keep_last.is_some() {
        Some(
            retain_export_backups_internal(
                state,
                ExportBackupRetentionRequest {
                    dry_run: Some(false),
                    keep_last: policy.retention_keep_last,
                    before_timestamp_ms: None,
                    delete_bundles: Some(policy.retention_delete_bundles),
                    delete_archive_objects: Some(policy.retention_delete_archive_objects),
                },
            )
            .await?,
        )
    } else {
        None
    };
    Ok(ExportBackupPolicyRunResponse {
        policy,
        backup,
        retention,
    })
}

pub(crate) async fn retain_export_backups(
    State(state): State<AppState>,
    request: Option<Json<ExportBackupRetentionRequest>>,
) -> Result<Json<ExportBackupRetentionResponse>, ApiError> {
    retain_export_backups_internal(
        &state,
        request.map(|Json(request)| request).unwrap_or_default(),
    )
    .await
    .map(Json)
}

async fn retain_export_backups_internal(
    state: &AppState,
    request: ExportBackupRetentionRequest,
) -> Result<ExportBackupRetentionResponse, ApiError> {
    let keep_last = request.keep_last;
    let before_timestamp_ms = request.before_timestamp_ms;
    if keep_last.is_none() && before_timestamp_ms.is_none() {
        return Err(ApiError::bad_request(
            "provide keepLast or beforeTimestampMs for backup retention",
        ));
    }

    let dry_run = request.dry_run.unwrap_or(true);
    let delete_bundles = request.delete_bundles.unwrap_or(true);
    let delete_archive_objects = request.delete_archive_objects.unwrap_or(false);
    let mut runs = state.export_backup_runs.read().await.clone();
    runs.sort_by_key(|run| std::cmp::Reverse(run.created_at_ms));

    let keep_count = keep_last.unwrap_or(0);
    let mut retained_runs = Vec::new();
    let mut candidate_runs = Vec::new();
    for (index, run) in runs.iter().cloned().enumerate() {
        let protected_by_count = keep_last.is_some_and(|_| index < keep_count);
        let protected_by_time =
            before_timestamp_ms.is_some_and(|before| run.created_at_ms > before);
        if protected_by_count || protected_by_time {
            retained_runs.push(run);
        } else {
            candidate_runs.push(run);
        }
    }

    let protected_bundles: BTreeSet<String> = retained_runs
        .iter()
        .flat_map(|run| {
            run.bundle_id
                .iter()
                .chain(run.chain_bundle_ids.iter())
                .cloned()
        })
        .collect();
    let protected_archive_objects: BTreeSet<String> = retained_runs
        .iter()
        .filter_map(|run| run.object_id.clone())
        .collect();

    let mut deleted_runs = Vec::new();
    let mut deleted_bundles = Vec::new();
    let mut deleted_archive_objects = Vec::new();
    let mut skipped_bundles = BTreeSet::new();
    let mut skipped_archive_objects = BTreeSet::new();

    for run in &candidate_runs {
        deleted_runs.push(run.id.clone());
        if delete_bundles && let Some(bundle_id) = &run.bundle_id {
            if protected_bundles.contains(bundle_id) {
                skipped_bundles.insert(bundle_id.clone());
            } else if !deleted_bundles.contains(bundle_id) {
                if !dry_run {
                    remove_export_bundle_dir(state, bundle_id).await?;
                }
                deleted_bundles.push(bundle_id.clone());
            }
        }
        if delete_archive_objects && let Some(object_id) = &run.object_id {
            if protected_archive_objects.contains(object_id) {
                skipped_archive_objects.insert(object_id.clone());
            } else if !deleted_archive_objects.contains(object_id) {
                if !dry_run {
                    commit_object_delete(
                        state,
                        object_id.clone(),
                        true,
                        Some(format!("export-backup-retention-{object_id}")),
                    )
                    .await?;
                }
                deleted_archive_objects.push(object_id.clone());
            }
        }
    }

    if !dry_run && !candidate_runs.is_empty() {
        let deleted_run_ids: HashSet<&str> = deleted_runs.iter().map(String::as_str).collect();
        {
            let mut catalog = state.export_backup_runs.write().await;
            catalog.retain(|run| !deleted_run_ids.contains(run.id.as_str()));
        }
        persist_export_backup_runs(state).await?;
    }

    Ok(ExportBackupRetentionResponse {
        dry_run,
        keep_last,
        before_timestamp_ms,
        candidates: candidate_runs.len(),
        retained: retained_runs.len(),
        deleted_runs,
        deleted_bundles,
        deleted_archive_objects,
        protected_bundles: skipped_bundles.into_iter().collect(),
        protected_archive_objects: skipped_archive_objects.into_iter().collect(),
    })
}

pub(crate) async fn run_export_backup(
    State(state): State<AppState>,
    request: Option<Json<ExportBackupRunRequest>>,
) -> Result<Json<ExportBackupRunResponse>, ApiError> {
    let request = request.map(|Json(request)| request).unwrap_or_default();
    run_export_backup_internal(&state, request).await.map(Json)
}

async fn run_export_backup_internal(
    state: &AppState,
    request: ExportBackupRunRequest,
) -> Result<ExportBackupRunResponse, ApiError> {
    let entries = read_export_bundle_entries(state).await?;
    let existing_chain_ids = latest_export_bundle_chain_ids(&entries);
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    let force_full = request.force_full.unwrap_or(false);
    let base_lsn = if force_full {
        0
    } else {
        existing_chain_ids
            .as_ref()
            .and_then(|chain| chain.last_current_lsn)
            .unwrap_or(0)
    };
    let mode = if base_lsn > 0 { "incremental" } else { "full" }.to_string();
    if !force_full && base_lsn == current_lsn {
        let chain = if let Some(chain) = existing_chain_ids {
            Some(
                verify_export_bundle_chain_internal(
                    state,
                    chain.bundle_ids,
                    bundle_encryption_key(request.encryption_key),
                )
                .await?,
            )
        } else {
            None
        };
        let run = append_export_backup_run(
            state,
            build_export_backup_run_record(
                mode.clone(),
                base_lsn,
                current_lsn,
                true,
                None,
                None,
                chain.as_ref(),
            ),
        )
        .await?;
        return Ok(ExportBackupRunResponse {
            run,
            mode,
            base_lsn,
            current_lsn,
            no_op: true,
            bundle: None,
            archived: None,
            chain,
        });
    }

    let encryption_key = bundle_encryption_key(request.encryption_key);
    let bundle = create_export_bundle_internal(
        state,
        ExportBundleCreateRequest {
            encryption_key: encryption_key.clone(),
            base_lsn: Some(base_lsn),
        },
    )
    .await?;
    let archived = if request.archive_object.unwrap_or(true) {
        Some(
            archive_export_bundle_to_object_internal(
                state,
                &bundle.id,
                ExportBundleArchiveRequest {
                    object_id: request.object_id,
                    client_mutation_id: request.client_mutation_id,
                },
            )
            .await?,
        )
    } else {
        None
    };

    let mut chain_ids = existing_chain_ids
        .map(|chain| chain.bundle_ids)
        .unwrap_or_default();
    if base_lsn == 0 {
        chain_ids.clear();
    }
    chain_ids.push(bundle.id.clone());
    let chain = verify_export_bundle_chain_internal(state, chain_ids, encryption_key).await?;
    let run = append_export_backup_run(
        state,
        build_export_backup_run_record(
            mode.clone(),
            base_lsn,
            current_lsn,
            false,
            Some(&bundle),
            archived.as_ref(),
            Some(&chain),
        ),
    )
    .await?;
    Ok(ExportBackupRunResponse {
        run,
        mode,
        base_lsn,
        current_lsn,
        no_op: false,
        bundle: Some(bundle),
        archived,
        chain: Some(chain),
    })
}

pub(crate) async fn archive_export_bundle_to_object(
    State(state): State<AppState>,
    axum::extract::Path(bundle_id): axum::extract::Path<String>,
    request: Option<Json<ExportBundleArchiveRequest>>,
) -> Result<Json<ExportBundleArchiveObjectResponse>, ApiError> {
    let request = request.map(|Json(request)| request).unwrap_or_default();
    archive_export_bundle_to_object_internal(&state, &bundle_id, request)
        .await
        .map(Json)
}

async fn archive_export_bundle_to_object_internal(
    state: &AppState,
    bundle_id: &str,
    request: ExportBundleArchiveRequest,
) -> Result<ExportBundleArchiveObjectResponse, ApiError> {
    if !ensure_safe_export_bundle_id(bundle_id) {
        return Err(ApiError::bad_request("invalid export bundle id"));
    }
    let root = state.export_root.join(bundle_id);
    if !root.exists() {
        return Err(ApiError::not_found("export bundle not found"));
    }
    let object_id = request
        .object_id
        .unwrap_or_else(|| format!("export-bundle-{bundle_id}"));
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid objectId"));
    }
    let archive = build_export_bundle_archive(&root, bundle_id).await?;
    let bytes = serde_json::to_vec(&archive).map_err(|err| ApiError::internal(err.into()))?;
    let metadata = commit_object_put(
        state,
        Some(object_id),
        EXPORT_BUNDLE_ARCHIVE_CONTENT_TYPE.to_string(),
        Bytes::from(bytes),
        request.client_mutation_id,
    )
    .await?;
    Ok(ExportBundleArchiveObjectResponse {
        bundle_id: bundle_id.to_string(),
        object: metadata,
        files: archive.files.len(),
        bytes: archive.total_bytes,
    })
}

pub(crate) async fn import_bundle_from_object(
    State(state): State<AppState>,
    axum::extract::Path(object_id): axum::extract::Path<String>,
    request: Option<Json<ImportBundleFromObjectRequest>>,
) -> Result<Json<ImportBundleFromObjectResponse>, ApiError> {
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid objectId"));
    }
    let request = request.map(|Json(request)| request).unwrap_or_default();
    let (object, body) = state
        .objects
        .body(&object_id)
        .await
        .map_err(ApiError::internal)?;
    if object.content_type != EXPORT_BUNDLE_ARCHIVE_CONTENT_TYPE {
        return Err(ApiError::bad_request(format!(
            "object contentType must be {EXPORT_BUNDLE_ARCHIVE_CONTENT_TYPE}"
        )));
    }
    let archive: ExportBundleArchive =
        serde_json::from_slice(&body).map_err(|err| ApiError::bad_request(err.to_string()))?;
    validate_export_bundle_archive(&archive)?;
    let bundle_id = request
        .bundle_id
        .unwrap_or_else(|| archive.bundle_id.clone());
    if !ensure_safe_export_bundle_id(&bundle_id) {
        return Err(ApiError::bad_request("invalid bundleId"));
    }
    let target = state.export_root.join(&bundle_id);
    let overwritten = target.exists();
    if overwritten {
        if request.overwrite.unwrap_or(false) {
            fs::remove_dir_all(&target)
                .await
                .map_err(|err| ApiError::internal(err.into()))?;
        } else {
            return Err(ApiError::conflict("export bundle already exists"));
        }
    }
    materialize_export_bundle_archive(&archive, &target).await?;
    let bundle = read_export_bundle_list_entry(target, bundle_id).await;
    Ok(Json(ImportBundleFromObjectResponse {
        bundle,
        object,
        files: archive.files.len(),
        bytes: archive.total_bytes,
        overwritten,
    }))
}

pub(crate) async fn import_bundle_preflight(
    State(state): State<AppState>,
    axum::extract::Path(bundle_id): axum::extract::Path<String>,
    request: Option<Json<ExportBundleAccessRequest>>,
) -> Result<Json<ImportBundlePreflightResponse>, ApiError> {
    import_bundle_preflight_internal(
        &state,
        bundle_id,
        bundle_encryption_key(request.and_then(|Json(request)| request.encryption_key)),
    )
    .await
    .map(Json)
}

pub(crate) async fn restore_import_bundle(
    State(state): State<AppState>,
    axum::extract::Path(bundle_id): axum::extract::Path<String>,
    request: Option<Json<ExportBundleAccessRequest>>,
) -> Result<Json<ImportBundleRestoreResponse>, ApiError> {
    let encryption_key =
        bundle_encryption_key(request.and_then(|Json(request)| request.encryption_key));
    restore_import_bundle_internal(&state, bundle_id, encryption_key)
        .await
        .map(Json)
}

async fn restore_import_bundle_internal(
    state: &AppState,
    bundle_id: String,
    encryption_key: Option<String>,
) -> Result<ImportBundleRestoreResponse, ApiError> {
    let preflight =
        import_bundle_preflight_internal(state, bundle_id, encryption_key.clone()).await?;
    if !preflight.ok {
        return Err(ApiError::conflict(format!(
            "import bundle preflight failed: {}",
            preflight.problems.join("; ")
        )));
    }

    let original_root = PathBuf::from(&preflight.path);
    let prepared = prepare_export_bundle_read_root(
        state,
        &original_root,
        preflight.manifest.as_ref(),
        encryption_key.as_deref(),
        &mut Vec::new(),
    )
    .await?;
    let root = prepared.root.clone();
    let schema = read_export_bundle_schema_strict(&root).await?;
    let schema_history = read_export_bundle_schema_history(&root, &mut Vec::new()).await;
    let mut schema_proposal_problems = Vec::new();
    let schema_proposals =
        read_export_bundle_schema_proposals(&root, &mut schema_proposal_problems).await;
    if !schema_proposal_problems.is_empty() {
        return Err(ApiError::conflict(schema_proposal_problems.join("; ")));
    }
    let mut cluster_control_problems = Vec::new();
    let cluster_control =
        read_export_bundle_cluster_control(&root, &mut cluster_control_problems).await;
    if !cluster_control_problems.is_empty() {
        return Err(ApiError::conflict(cluster_control_problems.join("; ")));
    }
    let records_by_shard = read_export_bundle_wal_by_shard(&root, state)?;

    apply_schema_for_empty_restore(state, schema.clone()).await?;
    for historical_schema in &schema_history {
        state
            .schema
            .persist_history_schema(historical_schema)
            .await
            .map_err(ApiError::internal)?;
    }
    let schema_proposal_count = schema_proposals.len();
    *state.schema_proposals.write().await = schema_proposals;
    persist_schema_proposals(state).await?;
    let cluster_control_summary = cluster_control.summary();
    restore_export_bundle_cluster_control(state, cluster_control).await?;
    let (objects, object_bytes) = restore_export_bundle_objects(
        state,
        &root.join("objects").join("metadata"),
        &root.join("objects").join("blobs"),
    )
    .await?;

    let mut wal_records = 0usize;
    let mut latest_projection_lsn = None;
    for (shard_index, records) in records_by_shard {
        let shard = &state.wal_shards[shard_index];
        wal_records += shard
            .writer
            .replicate(records.clone(), true)
            .await
            .map_err(ApiError::internal)?;
        for record in records {
            if let Some(lsn) =
                apply_replicated_wal_record(state, record, latest_projection_lsn).await?
            {
                latest_projection_lsn = Some(lsn);
            }
        }
    }
    wait_for_replicated_record_projection(state, latest_projection_lsn).await?;

    if let Err(err) = maybe_checkpoint(state).await {
        warn!(
            error = %err.message,
            "checkpoint failed after import bundle restore"
        );
    }

    let response = ImportBundleRestoreResponse {
        id: preflight.id,
        path: preflight.path,
        restored: true,
        restored_at_ms: now_ms(),
        wal_records,
        schema_version: Some(schema.version),
        schema_history_versions: schema_history
            .into_iter()
            .map(|schema| schema.version)
            .collect(),
        schema_proposals: schema_proposal_count,
        cluster_control: cluster_control_summary,
        objects,
        object_bytes,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
        encrypted: preflight.bundle_encrypted,
        manifest: preflight.manifest,
    };
    cleanup_prepared_export_bundle(prepared).await;
    Ok(response)
}

pub(crate) async fn apply_import_bundle_delta(
    State(state): State<AppState>,
    axum::extract::Path(bundle_id): axum::extract::Path<String>,
    request: Option<Json<ExportBundleAccessRequest>>,
) -> Result<Json<ImportBundleDeltaApplyResponse>, ApiError> {
    let encryption_key =
        bundle_encryption_key(request.and_then(|Json(request)| request.encryption_key));
    apply_import_bundle_delta_internal(&state, bundle_id, encryption_key)
        .await
        .map(Json)
}

async fn apply_import_bundle_delta_internal(
    state: &AppState,
    bundle_id: String,
    encryption_key: Option<String>,
) -> Result<ImportBundleDeltaApplyResponse, ApiError> {
    let preflight =
        import_bundle_delta_preflight_internal(state, bundle_id, encryption_key.clone()).await?;
    if !preflight.ok {
        return Err(ApiError::conflict(format!(
            "import bundle delta preflight failed: {}",
            preflight.problems.join("; ")
        )));
    }

    let original_root = PathBuf::from(&preflight.path);
    let prepared = prepare_export_bundle_read_root(
        state,
        &original_root,
        preflight.manifest.as_ref(),
        encryption_key.as_deref(),
        &mut Vec::new(),
    )
    .await?;
    let root = prepared.root.clone();
    let records_by_shard = read_export_bundle_wal_by_shard(&root, state)?;
    let (objects, object_bytes) = restore_export_bundle_objects(
        state,
        &root.join("objects").join("metadata"),
        &root.join("objects").join("blobs"),
    )
    .await?;

    let mut wal_records = 0usize;
    let mut latest_projection_lsn = None;
    for (shard_index, records) in records_by_shard {
        let shard = &state.wal_shards[shard_index];
        wal_records += shard
            .writer
            .replicate(records.clone(), true)
            .await
            .map_err(ApiError::internal)?;
        for record in records {
            if let Some(lsn) =
                apply_replicated_wal_record(state, record, latest_projection_lsn).await?
            {
                latest_projection_lsn = Some(lsn);
            }
        }
    }
    wait_for_replicated_record_projection(state, latest_projection_lsn).await?;

    if let Err(err) = maybe_checkpoint(state).await {
        warn!(
            error = %err.message,
            "checkpoint failed after import bundle delta apply"
        );
    }

    let response = ImportBundleDeltaApplyResponse {
        id: preflight.id,
        path: preflight.path,
        applied: true,
        applied_at_ms: now_ms(),
        base_lsn: preflight.base_lsn,
        wal_records,
        objects,
        object_bytes,
        encrypted: preflight.bundle_encrypted,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
        manifest: preflight.manifest,
    };
    cleanup_prepared_export_bundle(prepared).await;
    Ok(response)
}

pub(crate) async fn restore_import_bundle_chain(
    State(state): State<AppState>,
    Json(request): Json<ImportBundleChainRestoreRequest>,
) -> Result<Json<ImportBundleChainRestoreResponse>, ApiError> {
    if request.bundle_ids.is_empty() {
        return Err(ApiError::bad_request("bundleIds must not be empty"));
    }
    let chain = verify_export_bundle_chain_internal(
        &state,
        request.bundle_ids.clone(),
        request.encryption_key.clone(),
    )
    .await?;
    if !chain.ok {
        return Err(ApiError::conflict(format!(
            "import bundle chain verification failed: {}",
            chain.problems.join("; ")
        )));
    }

    let mut bundle_ids = request.bundle_ids.into_iter();
    let base_id = bundle_ids
        .next()
        .ok_or_else(|| ApiError::bad_request("bundleIds must not be empty"))?;
    let base =
        restore_import_bundle_internal(&state, base_id, request.encryption_key.clone()).await?;
    let mut deltas = Vec::new();
    for bundle_id in bundle_ids {
        deltas.push(
            apply_import_bundle_delta_internal(&state, bundle_id, request.encryption_key.clone())
                .await?,
        );
    }

    let wal_records =
        base.wal_records + deltas.iter().map(|delta| delta.wal_records).sum::<usize>();
    let objects = base.objects + deltas.iter().map(|delta| delta.objects).sum::<usize>();
    let object_bytes =
        base.object_bytes + deltas.iter().map(|delta| delta.object_bytes).sum::<u64>();
    let current_lsn = deltas
        .last()
        .map(|delta| delta.current_lsn)
        .unwrap_or(base.current_lsn);

    Ok(Json(ImportBundleChainRestoreResponse {
        restored: true,
        restored_at_ms: now_ms(),
        chain,
        base,
        deltas,
        wal_records,
        objects,
        object_bytes,
        current_lsn,
    }))
}

pub(crate) async fn import_bundle_delta_preflight(
    State(state): State<AppState>,
    axum::extract::Path(bundle_id): axum::extract::Path<String>,
    request: Option<Json<ExportBundleAccessRequest>>,
) -> Result<Json<ImportBundleDeltaPreflightResponse>, ApiError> {
    import_bundle_delta_preflight_internal(
        &state,
        bundle_id,
        bundle_encryption_key(request.and_then(|Json(request)| request.encryption_key)),
    )
    .await
    .map(Json)
}

pub(crate) fn spawn_export_backup_controller(state: AppState) {
    tokio::spawn(async move {
        loop {
            let policy = state.export_backup_policy.read().await.clone();
            {
                let mut controller = state.export_backup_controller.write().await;
                controller.enabled = policy.enabled;
                controller.interval_ms = policy.interval_ms;
            }
            if !policy.enabled || policy.interval_ms == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
                continue;
            }
            tokio::time::sleep(std::time::Duration::from_millis(policy.interval_ms)).await;
            let latest_policy = state.export_backup_policy.read().await.clone();
            if !latest_policy.enabled || latest_policy.interval_ms == 0 {
                continue;
            }
            let ran_at = now_ms();
            match run_export_backup_policy_internal(&state, latest_policy.clone()).await {
                Ok(response) => {
                    let mut controller = state.export_backup_controller.write().await;
                    controller.enabled = latest_policy.enabled;
                    controller.interval_ms = latest_policy.interval_ms;
                    controller.last_run_at_ms = Some(ran_at);
                    controller.last_run_id = Some(response.backup.run.id);
                    controller.last_error = None;
                }
                Err(err) => {
                    let mut controller = state.export_backup_controller.write().await;
                    controller.enabled = latest_policy.enabled;
                    controller.interval_ms = latest_policy.interval_ms;
                    controller.last_run_at_ms = Some(ran_at);
                    controller.last_error = Some(err.message);
                }
            }
        }
    });
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupControllerState {
    pub(crate) enabled: bool,
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at_ms: Option<u64>,
    pub(crate) last_run_id: Option<String>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleAccessRequest {
    pub(crate) encryption_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleChainVerifyRequest {
    pub(crate) bundle_ids: Vec<String>,
    pub(crate) encryption_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleChainRestoreRequest {
    pub(crate) bundle_ids: Vec<String>,
    pub(crate) encryption_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleArchiveRequest {
    pub(crate) object_id: Option<String>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleFromObjectRequest {
    pub(crate) bundle_id: Option<String>,
    pub(crate) overwrite: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleArchiveObjectResponse {
    pub(crate) bundle_id: String,
    pub(crate) object: ObjectMetadata,
    pub(crate) files: usize,
    pub(crate) bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleFromObjectResponse {
    pub(crate) bundle: ExportBundleListEntry,
    pub(crate) object: ObjectMetadata,
    pub(crate) files: usize,
    pub(crate) bytes: u64,
    pub(crate) overwritten: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleArchive {
    pub(crate) format: String,
    pub(crate) bundle_id: String,
    pub(crate) created_at_ms: u64,
    pub(crate) files: Vec<ExportBundleArchiveFile>,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleArchiveFile {
    pub(crate) path: String,
    pub(crate) byte_size: u64,
    pub(crate) sha256: String,
    pub(crate) data_base64: String,
}

pub(crate) async fn build_export_bundle_archive(
    root: &Path,
    bundle_id: &str,
) -> Result<ExportBundleArchive, ApiError> {
    let mut files = Vec::new();
    let mut total_bytes = 0u64;
    for (path, rel_path) in bundle_data_files(root).await? {
        let bytes = fs::read(&path)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        let byte_size = bytes.len() as u64;
        total_bytes += byte_size;
        files.push(ExportBundleArchiveFile {
            path: export_bundle_rel_label(&rel_path),
            byte_size,
            sha256: hex_lower(&Sha256::digest(&bytes)),
            data_base64: BASE64_STANDARD.encode(&bytes),
        });
    }
    Ok(ExportBundleArchive {
        format: EXPORT_BUNDLE_ARCHIVE_FORMAT.to_string(),
        bundle_id: bundle_id.to_string(),
        created_at_ms: now_ms(),
        files,
        total_bytes,
    })
}

pub(crate) fn validate_export_bundle_archive(
    archive: &ExportBundleArchive,
) -> Result<(), ApiError> {
    if archive.format != EXPORT_BUNDLE_ARCHIVE_FORMAT {
        return Err(ApiError::bad_request(format!(
            "archive format must be {EXPORT_BUNDLE_ARCHIVE_FORMAT}"
        )));
    }
    if !ensure_safe_export_bundle_id(&archive.bundle_id) {
        return Err(ApiError::bad_request("invalid archive bundleId"));
    }

    let mut seen = BTreeSet::new();
    let mut total_bytes = 0u64;
    let mut has_manifest = false;
    for file in &archive.files {
        if !ensure_safe_archive_path(&file.path) {
            return Err(ApiError::bad_request(format!(
                "invalid archive path {}",
                file.path
            )));
        }
        if !seen.insert(file.path.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate archive path {}",
                file.path
            )));
        }
        if file.path == "manifest.json" {
            has_manifest = true;
        }
        let bytes = BASE64_STANDARD
            .decode(&file.data_base64)
            .map_err(|err| ApiError::bad_request(format!("{} is not base64: {err}", file.path)))?;
        if bytes.len() as u64 != file.byte_size {
            return Err(ApiError::bad_request(format!(
                "{} byteSize does not match data",
                file.path
            )));
        }
        let sha256 = hex_lower(&Sha256::digest(&bytes));
        if sha256 != file.sha256 {
            return Err(ApiError::bad_request(format!(
                "{} sha256 does not match data",
                file.path
            )));
        }
        total_bytes += file.byte_size;
    }
    if !has_manifest {
        return Err(ApiError::bad_request("archive is missing manifest.json"));
    }
    if total_bytes != archive.total_bytes {
        return Err(ApiError::bad_request(
            "archive totalBytes does not match files",
        ));
    }
    Ok(())
}

pub(crate) async fn materialize_export_bundle_archive(
    archive: &ExportBundleArchive,
    target: &Path,
) -> Result<(), ApiError> {
    let parent = target
        .parent()
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("export bundle target has no parent")))?;
    fs::create_dir_all(parent)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    let target_name = target
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("bundle");
    let temp = parent.join(format!(".{target_name}.import-{}", Uuid::now_v7()));
    fs::create_dir_all(&temp)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;

    let write_result = async {
        for file in &archive.files {
            let bytes = BASE64_STANDARD.decode(&file.data_base64).map_err(|err| {
                ApiError::bad_request(format!("{} is not base64: {err}", file.path))
            })?;
            let path = temp.join(&file.path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|err| ApiError::internal(err.into()))?;
            }
            fs::write(path, bytes)
                .await
                .map_err(|err| ApiError::internal(err.into()))?;
        }
        fs::rename(&temp, target)
            .await
            .map_err(|err| ApiError::internal(err.into()))
    }
    .await;

    if write_result.is_err() {
        let _ = fs::remove_dir_all(&temp).await;
    }
    write_result
}

fn ensure_safe_archive_path(path: &str) -> bool {
    if path.is_empty() || path.contains('\\') {
        return false;
    }
    let path = Path::new(path);
    if path.is_absolute() {
        return false;
    }
    let mut components = path.components().peekable();
    if components.peek().is_none() {
        return false;
    }
    components.all(|component| matches!(component, Component::Normal(part) if !part.is_empty()))
}

pub(crate) async fn bundle_data_files(root: &Path) -> Result<Vec<(PathBuf, PathBuf)>, ApiError> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|err| ApiError::internal(err.into()))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|err| ApiError::internal(err.into()))?;
            if file_type.is_dir() {
                if path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .map(|name| name.starts_with(".decrypted-"))
                    .unwrap_or(false)
                {
                    continue;
                }
                stack.push(path);
            } else if file_type.is_file() {
                let rel_path = path
                    .strip_prefix(root)
                    .map_err(|err| ApiError::internal(anyhow::anyhow!(err)))?
                    .to_path_buf();
                files.push((path, rel_path));
            }
        }
    }
    files.sort_by(|left, right| left.1.cmp(&right.1));
    Ok(files)
}

pub(crate) fn export_bundle_rel_label(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn ensure_safe_export_bundle_id(id: &str) -> bool {
    Path::new(id).components().count() == 1
        && id
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '_')
}

pub(crate) async fn remove_export_bundle_dir(
    state: &AppState,
    bundle_id: &str,
) -> Result<(), ApiError> {
    if !ensure_safe_export_bundle_id(bundle_id) {
        return Err(ApiError::bad_request("invalid export bundle id"));
    }
    let target = state.export_root.join(bundle_id);
    match fs::remove_dir_all(target).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ApiError::internal(err.into())),
    }
}

pub(crate) fn latest_export_bundle_chain_ids(
    entries: &[ExportBundleListEntry],
) -> Option<LatestExportBundleChain> {
    let mut best_ids = Vec::new();
    let mut best_lsn = None;
    for entry in entries {
        let Some(manifest) = entry.manifest.as_ref() else {
            continue;
        };
        if !entry.ok || manifest.incremental || manifest.base_lsn != 0 {
            continue;
        }
        let mut ids = vec![entry.id.clone()];
        let mut current_lsn = manifest.current_lsn;
        loop {
            let next = entries
                .iter()
                .filter_map(|candidate| {
                    let manifest = candidate.manifest.as_ref()?;
                    if !candidate.ok
                        || !manifest.incremental
                        || manifest.base_lsn != current_lsn
                        || manifest.current_lsn <= current_lsn
                    {
                        return None;
                    }
                    Some((candidate, manifest.current_lsn))
                })
                .max_by_key(|(_, current_lsn)| *current_lsn);
            let Some((next, next_lsn)) = next else {
                break;
            };
            ids.push(next.id.clone());
            current_lsn = next_lsn;
        }
        if best_lsn.is_none_or(|lsn| current_lsn > lsn) {
            best_lsn = Some(current_lsn);
            best_ids = ids;
        }
    }
    best_lsn.map(|last_current_lsn| LatestExportBundleChain {
        bundle_ids: best_ids,
        last_current_lsn: Some(last_current_lsn),
    })
}

pub(crate) async fn read_export_bundle_entries(
    state: &AppState,
) -> Result<Vec<ExportBundleListEntry>, ApiError> {
    if !state.export_root.exists() {
        return Ok(Vec::new());
    }
    let mut bundles = Vec::new();
    let mut entries = fs::read_dir(&state.export_root)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|err| ApiError::internal(err.into()))?
    {
        if !entry
            .file_type()
            .await
            .map_err(|err| ApiError::internal(err.into()))?
            .is_dir()
        {
            continue;
        }
        let id = entry.file_name().to_string_lossy().to_string();
        if !ensure_safe_export_bundle_id(&id) {
            continue;
        }
        bundles.push(read_export_bundle_list_entry(entry.path(), id).await);
    }
    bundles.sort_by(|left, right| right.id.cmp(&left.id));
    Ok(bundles)
}

pub(crate) async fn read_export_bundle_list_entry(
    root: PathBuf,
    id: String,
) -> ExportBundleListEntry {
    let mut problems = Vec::new();
    let manifest = match fs::read(root.join("manifest.json")).await {
        Ok(bytes) => match serde_json::from_slice::<ExportManifestResponse>(&bytes) {
            Ok(manifest) => Some(manifest),
            Err(err) => {
                problems.push(format!("manifest.json is not valid: {err}"));
                None
            }
        },
        Err(err) => {
            problems.push(format!("manifest.json could not be read: {err}"));
            None
        }
    };
    if manifest
        .as_ref()
        .map(|manifest| manifest.encryption.encrypted)
        .unwrap_or(false)
    {
        let manifest_ref = manifest.as_ref();
        return ExportBundleListEntry {
            id,
            path: root.display().to_string(),
            ok: manifest_ref.is_some(),
            schema_version: manifest_ref.map(|manifest| manifest.schema_version),
            schema_history_versions: manifest_ref
                .map(|manifest| manifest.schema_history_versions.clone())
                .unwrap_or_default(),
            schema_proposals: manifest_ref
                .map(|manifest| manifest.schema_proposals)
                .unwrap_or_default(),
            cluster_control: manifest_ref
                .map(|manifest| manifest.cluster_control.clone())
                .unwrap_or_default(),
            wal_records: manifest_ref.map(|manifest| manifest.wal.records),
            highest_lsn: manifest_ref.map(|manifest| manifest.wal.highest_lsn),
            objects: manifest_ref.map(|manifest| manifest.objects.live),
            object_bytes: manifest_ref.map(|manifest| manifest.objects.live_bytes),
            encrypted: true,
            problems,
            manifest,
        };
    }
    let schema_version = read_export_bundle_schema(&root, &mut problems)
        .await
        .map(|schema| schema.version);
    let schema_history = read_export_bundle_schema_history(&root, &mut problems).await;
    let schema_history_versions: Vec<u32> =
        schema_history.iter().map(|schema| schema.version).collect();
    let schema_proposals = read_export_bundle_schema_proposals(&root, &mut problems)
        .await
        .len();
    let cluster_control = read_export_bundle_cluster_control(&root, &mut problems)
        .await
        .summary();
    if let (Some(manifest), Some(schema_version)) = (&manifest, schema_version)
        && manifest.schema_version != schema_version
    {
        problems.push(format!(
            "manifest schema version {} does not match schema.json version {}",
            manifest.schema_version, schema_version
        ));
    }
    if let Some(manifest) = &manifest {
        if !manifest.schema_history_versions.is_empty()
            && manifest.schema_history_versions != schema_history_versions
        {
            problems.push(format!(
                "manifest schema history versions {:?} do not match schema/history versions {:?}",
                manifest.schema_history_versions, schema_history_versions
            ));
        }
        if manifest.schema_proposals != schema_proposals {
            problems.push(format!(
                "manifest schema proposal count {} does not match schema/proposals.json count {}",
                manifest.schema_proposals, schema_proposals
            ));
        }
        if manifest.cluster_control != cluster_control {
            problems.push(format!(
                "manifest cluster control summary {:?} does not match cluster bundle summary {:?}",
                manifest.cluster_control, cluster_control
            ));
        }
    }

    ExportBundleListEntry {
        id,
        path: root.display().to_string(),
        ok: problems.is_empty(),
        schema_version,
        schema_history_versions,
        schema_proposals,
        cluster_control,
        wal_records: manifest.as_ref().map(|manifest| manifest.wal.records),
        highest_lsn: manifest.as_ref().map(|manifest| manifest.wal.highest_lsn),
        objects: manifest.as_ref().map(|manifest| manifest.objects.live),
        object_bytes: manifest
            .as_ref()
            .map(|manifest| manifest.objects.live_bytes),
        encrypted: false,
        problems,
        manifest,
    }
}

pub(crate) async fn read_export_bundle_schema(
    root: &Path,
    problems: &mut Vec<String>,
) -> Option<DatabaseSchema> {
    let path = root.join("schema.json");
    let schema = match fs::read(&path).await {
        Ok(bytes) => match serde_json::from_slice::<DatabaseSchema>(&bytes) {
            Ok(schema) => schema,
            Err(err) => {
                problems.push(format!("schema.json is not valid: {err}"));
                return None;
            }
        },
        Err(err) => {
            problems.push(format!("schema.json could not be read: {err}"));
            return None;
        }
    };
    if let Err(err) = schema.validation_report().into_result() {
        problems.push(format!("schema.json validation failed: {err}"));
        return None;
    }
    Some(schema)
}

pub(crate) async fn read_export_bundle_schema_history(
    root: &Path,
    problems: &mut Vec<String>,
) -> Vec<DatabaseSchema> {
    let history_dir = root.join("schema").join("history");
    if !history_dir.exists() {
        return Vec::new();
    }

    let mut schemas = BTreeMap::<u32, DatabaseSchema>::new();
    let mut entries = match fs::read_dir(&history_dir).await {
        Ok(entries) => entries,
        Err(err) => {
            problems.push(format!("schema/history could not be read: {err}"));
            return Vec::new();
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(err) => {
                problems.push(format!("schema/history entry could not be read: {err}"));
                break;
            }
        };
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let schema = match fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<DatabaseSchema>(&bytes) {
                Ok(schema) => schema,
                Err(err) => {
                    problems.push(format!(
                        "{} is not valid schema history: {err}",
                        path.display()
                    ));
                    continue;
                }
            },
            Err(err) => {
                problems.push(format!("{} could not be read: {err}", path.display()));
                continue;
            }
        };
        if let Err(err) = schema.validation_report().into_result() {
            problems.push(format!(
                "{} schema history validation failed: {err}",
                path.display()
            ));
            continue;
        }
        let expected_file_name = format!("v{}.json", schema.version);
        if path.file_name().and_then(|value| value.to_str()) != Some(expected_file_name.as_str()) {
            problems.push(format!(
                "{} does not match contained schema version {}",
                path.display(),
                schema.version
            ));
        }
        if schemas.insert(schema.version, schema).is_some() {
            problems.push("schema/history contains duplicate schema versions".to_string());
        }
    }

    schemas.into_values().collect()
}

pub(crate) async fn read_export_bundle_schema_proposals(
    root: &Path,
    problems: &mut Vec<String>,
) -> BTreeMap<String, SchemaProposal> {
    let path = root.join("schema").join("proposals.json");
    if !path.exists() {
        return BTreeMap::new();
    }
    let proposals = match fs::read(&path).await {
        Ok(bytes) => match serde_json::from_slice::<BTreeMap<String, SchemaProposal>>(&bytes) {
            Ok(proposals) => proposals,
            Err(err) => {
                problems.push(format!("schema/proposals.json is not valid: {err}"));
                return BTreeMap::new();
            }
        },
        Err(err) => {
            problems.push(format!("schema/proposals.json could not be read: {err}"));
            return BTreeMap::new();
        }
    };

    for (id, proposal) in &proposals {
        if id != &proposal.id {
            problems.push(format!(
                "schema/proposals.json key {} does not match contained proposal id {}",
                id, proposal.id
            ));
        }
        if let Err(err) = proposal.schema.validation_report().into_result() {
            problems.push(format!(
                "schema proposal {} candidate schema validation failed: {err}",
                proposal.id
            ));
        }
    }

    proposals
}

pub(crate) async fn read_export_bundle_cluster_control(
    root: &Path,
    problems: &mut Vec<String>,
) -> ExportClusterControlBundle {
    let dir = root.join("cluster");
    if !dir.exists() {
        return ExportClusterControlBundle {
            topology_overrides: BTreeMap::new(),
            topology_log: Vec::new(),
            topology_proposals: BTreeMap::new(),
            topology_lease: TopologyLease::default(),
            handoff_workflows: BTreeMap::new(),
        };
    }

    let topology_overrides = read_bundle_json_file::<BTreeMap<usize, ClusterShardOverride>>(
        &dir.join("topology-overrides.json"),
        "cluster/topology-overrides.json",
        problems,
    )
    .await
    .unwrap_or_default();
    let topology_log = match read_topology_log(&dir.join("topology-log.jsonl")).await {
        Ok(entries) => entries,
        Err(err) => {
            problems.push(format!("cluster/topology-log.jsonl is not valid: {err}"));
            Vec::new()
        }
    };
    let topology_proposals = read_bundle_json_file::<BTreeMap<String, TopologyProposal>>(
        &dir.join("topology-proposals.json"),
        "cluster/topology-proposals.json",
        problems,
    )
    .await
    .unwrap_or_default();
    let topology_lease = read_bundle_json_file::<TopologyLease>(
        &dir.join("topology-lease.json"),
        "cluster/topology-lease.json",
        problems,
    )
    .await
    .unwrap_or_default();
    let handoff_workflows = read_bundle_json_file::<BTreeMap<String, HandoffWorkflow>>(
        &dir.join("handoff-workflows.json"),
        "cluster/handoff-workflows.json",
        problems,
    )
    .await
    .unwrap_or_default();

    for (id, proposal) in &topology_proposals {
        if id != &proposal.id {
            problems.push(format!(
                "cluster/topology-proposals.json key {} does not match contained proposal id {}",
                id, proposal.id
            ));
        }
    }
    for (id, workflow) in &handoff_workflows {
        if id != &workflow.id {
            problems.push(format!(
                "cluster/handoff-workflows.json key {} does not match contained workflow id {}",
                id, workflow.id
            ));
        }
    }
    if let Some(proposal_id) = &topology_lease.proposal_id
        && !topology_proposals.contains_key(proposal_id)
    {
        problems.push(format!(
            "cluster/topology-lease.json references missing proposal {}",
            proposal_id
        ));
    }

    ExportClusterControlBundle {
        topology_overrides,
        topology_log,
        topology_proposals,
        topology_lease,
        handoff_workflows,
    }
}

async fn read_bundle_json_file<T>(path: &Path, label: &str, problems: &mut Vec<String>) -> Option<T>
where
    T: serde::de::DeserializeOwned,
{
    if !path.exists() {
        return None;
    }
    match fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice::<T>(&bytes) {
            Ok(value) => Some(value),
            Err(err) => {
                problems.push(format!("{label} is not valid: {err}"));
                None
            }
        },
        Err(err) => {
            problems.push(format!("{label} could not be read: {err}"));
            None
        }
    }
}

pub(crate) async fn read_export_bundle_schema_strict(
    root: &Path,
) -> Result<DatabaseSchema, ApiError> {
    let mut problems = Vec::new();
    let Some(schema) = read_export_bundle_schema(root, &mut problems).await else {
        return Err(ApiError::conflict(problems.join("; ")));
    };
    Ok(schema)
}

pub(crate) async fn write_export_bundle_schema_history(
    state: &AppState,
    history_dir: &Path,
) -> Result<Vec<u32>, ApiError> {
    let entries = state.schema.history().await.map_err(ApiError::internal)?;
    let mut versions = Vec::new();
    for entry in entries {
        let Some(schema) = state
            .schema
            .schema_version(entry.version)
            .await
            .map_err(ApiError::internal)?
        else {
            continue;
        };
        fs::write(
            history_dir.join(format!("v{}.json", schema.version)),
            serde_json::to_vec_pretty(&schema).map_err(|err| ApiError::internal(err.into()))?,
        )
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
        versions.push(schema.version);
    }
    versions.sort_unstable();
    Ok(versions)
}

pub(crate) async fn write_export_bundle_schema_proposals(
    state: &AppState,
    path: &Path,
) -> Result<usize, ApiError> {
    let proposals = state.schema_proposals.read().await;
    fs::write(
        path,
        serde_json::to_vec_pretty(&*proposals).map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;
    Ok(proposals.len())
}

pub(crate) async fn write_export_bundle_cluster_control(
    state: &AppState,
    dir: &Path,
) -> Result<ExportClusterControlSummary, ApiError> {
    let topology_overrides = state.topology_overrides.read().await.clone();
    let topology_log = read_topology_log(&state.topology_log_path)
        .await
        .map_err(ApiError::internal)?;
    let topology_proposals = state.topology_proposals.read().await.clone();
    let topology_lease = state.topology_lease.read().await.clone();
    let handoff_workflows = state.handoff_workflows.read().await.clone();

    fs::write(
        dir.join("topology-overrides.json"),
        serde_json::to_vec_pretty(&topology_overrides)
            .map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;
    write_topology_log_entries(&dir.join("topology-log.jsonl"), &topology_log).await?;
    fs::write(
        dir.join("topology-proposals.json"),
        serde_json::to_vec_pretty(&topology_proposals)
            .map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;
    fs::write(
        dir.join("topology-lease.json"),
        serde_json::to_vec_pretty(&topology_lease).map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;
    fs::write(
        dir.join("handoff-workflows.json"),
        serde_json::to_vec_pretty(&handoff_workflows)
            .map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;

    Ok(ExportClusterControlSummary {
        topology_overrides: topology_overrides.len(),
        topology_log_entries: topology_log.len(),
        topology_proposals: topology_proposals.len(),
        handoff_workflows: handoff_workflows.len(),
        topology_lease_term: topology_lease.current_term,
    })
}

pub(crate) async fn verify_export_bundle_objects(
    metadata_dir: &Path,
    blob_dir: &Path,
    problems: &mut Vec<String>,
) -> (usize, u64) {
    if !metadata_dir.exists() {
        problems.push("objects/metadata directory is missing".to_string());
        return (0, 0);
    }
    if !blob_dir.exists() {
        problems.push("objects/blobs directory is missing".to_string());
        return (0, 0);
    }

    let mut count = 0usize;
    let mut bytes = 0u64;
    let mut entries = match fs::read_dir(metadata_dir).await {
        Ok(entries) => entries,
        Err(err) => {
            problems.push(format!("objects/metadata could not be read: {err}"));
            return (0, 0);
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(err) => {
                problems.push(format!("objects/metadata entry could not be read: {err}"));
                break;
            }
        };
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let metadata = match fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<ObjectMetadata>(&bytes) {
                Ok(metadata) => metadata,
                Err(err) => {
                    problems.push(format!(
                        "{} is not valid object metadata: {err}",
                        path.display()
                    ));
                    continue;
                }
            },
            Err(err) => {
                problems.push(format!("{} could not be read: {err}", path.display()));
                continue;
            }
        };
        if !ensure_safe_object_id(&metadata.id) {
            problems.push(format!("object metadata has unsafe id {}", metadata.id));
            continue;
        }
        let blob_path = blob_dir.join(format!("{}.bin", metadata.id));
        let blob = match fs::read(&blob_path).await {
            Ok(blob) => blob,
            Err(err) => {
                problems.push(format!("{} could not be read: {err}", blob_path.display()));
                continue;
            }
        };
        if blob.len() as u64 != metadata.byte_size {
            problems.push(format!(
                "object {} byte size {} does not match blob size {}",
                metadata.id,
                metadata.byte_size,
                blob.len()
            ));
            continue;
        }
        let sha256 = hex_lower(&Sha256::digest(&blob));
        if sha256 != metadata.sha256 {
            problems.push(format!("object {} sha256 mismatch", metadata.id));
            continue;
        }
        count += 1;
        bytes += metadata.byte_size;
    }

    (count, bytes)
}

pub(crate) async fn restore_export_bundle_objects(
    state: &AppState,
    metadata_dir: &Path,
    blob_dir: &Path,
) -> Result<(usize, u64), ApiError> {
    let mut count = 0usize;
    let mut bytes = 0u64;
    let mut entries = fs::read_dir(metadata_dir)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(err) => return Err(ApiError::internal(err.into())),
        };
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let metadata = fs::read(&path)
            .await
            .map_err(|err| ApiError::internal(err.into()))
            .and_then(|bytes| {
                serde_json::from_slice::<ObjectMetadata>(&bytes)
                    .map_err(|err| ApiError::internal(err.into()))
            })?;
        let blob = fs::read(blob_dir.join(format!("{}.bin", metadata.id)))
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        state
            .objects
            .put_replicated(metadata.clone(), Bytes::from(blob))
            .await
            .map_err(ApiError::internal)?;
        count += 1;
        bytes += metadata.byte_size;
    }

    Ok((count, bytes))
}

pub(crate) fn read_export_bundle_wal_by_shard(
    root: &Path,
    state: &AppState,
) -> Result<BTreeMap<usize, Vec<WalRecord>>, ApiError> {
    let records =
        wal::read_records_file(&root.join("wal-records.jsonl")).map_err(ApiError::internal)?;
    let mut records_by_shard = BTreeMap::<usize, Vec<WalRecord>>::new();
    for record in records {
        records_by_shard
            .entry(record.shard)
            .or_default()
            .push(record);
    }
    for shard_index in records_by_shard.keys() {
        if *shard_index >= state.wal_shards.len() {
            return Err(ApiError::conflict(format!(
                "bundle references shard {shard_index}, but local node only has {} shards",
                state.wal_shards.len()
            )));
        }
    }
    Ok(records_by_shard)
}

pub(crate) async fn encrypt_export_bundle_files(
    root: &Path,
    bundle_id: &str,
    encryption_key: &str,
) -> Result<usize, ApiError> {
    let key = derive_export_bundle_key(encryption_key);
    let mut count = 0usize;
    for (path, rel_path) in bundle_data_files(root).await? {
        if rel_path == Path::new("manifest.json") {
            continue;
        }
        let rel_label = export_bundle_rel_label(&rel_path);
        let plaintext = fs::read(&path)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        let ciphertext = encrypt_export_bundle_file(&key, bundle_id, &rel_label, &plaintext)?;
        fs::write(&path, ciphertext)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        count += 1;
    }
    Ok(count)
}

pub(crate) fn derive_export_bundle_key(encryption_key: &str) -> [u8; 32] {
    Sha256::digest(encryption_key.as_bytes()).into()
}

fn encrypt_export_bundle_file(
    key: &[u8; 32],
    bundle_id: &str,
    rel_path: &str,
    plaintext: &[u8],
) -> Result<Vec<u8>, ApiError> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|err| ApiError::internal(err.into()))?;
    let nonce_bytes = export_bundle_nonce(bundle_id, rel_path);
    cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: plaintext,
                aad: rel_path.as_bytes(),
            },
        )
        .map_err(|err| ApiError::internal(anyhow::anyhow!("encrypt export bundle file: {err}")))
}

pub(crate) fn decrypt_export_bundle_file(
    key: &[u8; 32],
    bundle_id: &str,
    rel_path: &str,
    ciphertext: &[u8],
) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce_bytes = export_bundle_nonce(bundle_id, rel_path);
    cipher
        .decrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: ciphertext,
                aad: rel_path.as_bytes(),
            },
        )
        .map_err(|err| anyhow::anyhow!("AES-GCM authentication failed: {err}"))
}

fn export_bundle_nonce(bundle_id: &str, rel_path: &str) -> [u8; 12] {
    let mut hasher = Sha256::new();
    hasher.update(b"nextdb-export-bundle-file-v1");
    hasher.update(bundle_id.as_bytes());
    hasher.update([0]);
    hasher.update(rel_path.as_bytes());
    let digest = hasher.finalize();
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&digest[..12]);
    nonce
}

pub(crate) async fn prepare_export_bundle_read_root(
    state: &AppState,
    root: &Path,
    manifest: Option<&ExportManifestResponse>,
    encryption_key: Option<&str>,
    problems: &mut Vec<String>,
) -> Result<PreparedExportBundleReadRoot, ApiError> {
    if !manifest
        .map(|manifest| manifest.encryption.encrypted)
        .unwrap_or(false)
    {
        return Ok(PreparedExportBundleReadRoot {
            root: root.to_path_buf(),
            cleanup_root: None,
        });
    }
    let Some(encryption_key) = encryption_key else {
        problems.push("bundle is encrypted; encryptionKey is required".to_string());
        return Ok(PreparedExportBundleReadRoot {
            root: root.to_path_buf(),
            cleanup_root: None,
        });
    };
    let bundle_id = root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("bundle");
    let temp_root = state
        .export_root
        .join(format!(".decrypted-{}-{}", now_ms(), Uuid::now_v7()));
    fs::create_dir_all(&temp_root)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    let key = derive_export_bundle_key(encryption_key);
    for (path, rel_path) in bundle_data_files(root).await? {
        let target_path = temp_root.join(&rel_path);
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|err| ApiError::internal(err.into()))?;
        }
        if rel_path == Path::new("manifest.json") {
            fs::copy(&path, &target_path)
                .await
                .map_err(|err| ApiError::internal(err.into()))?;
            continue;
        }
        let rel_label = export_bundle_rel_label(&rel_path);
        let ciphertext = fs::read(&path)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        match decrypt_export_bundle_file(&key, bundle_id, &rel_label, &ciphertext) {
            Ok(plaintext) => {
                fs::write(&target_path, plaintext)
                    .await
                    .map_err(|err| ApiError::internal(err.into()))?;
            }
            Err(err) => {
                problems.push(format!("{} could not be decrypted: {err}", rel_label));
            }
        }
    }
    Ok(PreparedExportBundleReadRoot {
        root: temp_root.clone(),
        cleanup_root: Some(temp_root),
    })
}

pub(crate) async fn cleanup_prepared_export_bundle(root: PreparedExportBundleReadRoot) {
    if let Some(cleanup_root) = root.cleanup_root {
        let _ = fs::remove_dir_all(cleanup_root).await;
    }
}

pub(crate) async fn export_bundle_object_ids(
    state: &AppState,
    records: &[WalRecord],
    base_lsn: u64,
) -> Result<Vec<String>, ApiError> {
    if base_lsn == 0 {
        let objects = state
            .objects
            .list_metadata()
            .await
            .map_err(ApiError::internal)?;
        return Ok(objects.into_iter().map(|metadata| metadata.id).collect());
    }

    let mut ids = BTreeSet::new();
    for record in records {
        match &record.payload {
            WalPayload::ObjectCommitted { object, .. } => {
                ids.insert(object.id.clone());
            }
            WalPayload::ObjectDeleted { object_id, .. } => {
                ids.remove(object_id);
            }
            _ => {}
        }
    }
    Ok(ids.into_iter().collect())
}

pub(crate) async fn build_export_cluster_control_summary(
    state: &AppState,
) -> Result<ExportClusterControlSummary> {
    Ok(ExportClusterControlSummary {
        topology_overrides: state.topology_overrides.read().await.len(),
        topology_log_entries: read_topology_log(&state.topology_log_path).await?.len(),
        topology_proposals: state.topology_proposals.read().await.len(),
        handoff_workflows: state.handoff_workflows.read().await.len(),
        topology_lease_term: state.topology_lease.read().await.current_term,
    })
}

pub(crate) async fn build_export_manifest(
    state: &AppState,
    query: ExportManifestQuery,
) -> Result<ExportManifestResponse, ApiError> {
    let base_lsn = query.base_lsn.unwrap_or(0);
    let records: Vec<WalRecord> = wal::read_records_from_wal_paths(&state.wal_paths)
        .map_err(ApiError::internal)?
        .into_iter()
        .filter(|record| record.lsn > base_lsn)
        .collect();
    let mut payloads = BTreeMap::<String, usize>::new();
    let mut tables = BTreeMap::<String, usize>::new();
    let mut rooms = BTreeMap::<String, usize>::new();
    let mut users = BTreeMap::<String, usize>::new();
    let mut shard_summaries = BTreeMap::<usize, ExportWalShardSummary>::new();
    let mut live_objects = BTreeMap::<String, ObjectMetadata>::new();
    let mut object_committed = 0usize;
    let mut object_deleted = 0usize;
    let mut checksum_missing = 0usize;
    let mut checksum_mismatch = 0usize;
    let mut samples = Vec::new();
    let include_samples = query.include_samples.unwrap_or(false);
    let sample_limit = query.sample_limit.map_or(20, |limit| limit.min(100));

    for record in &records {
        *payloads
            .entry(wal_payload_type(&record.payload).to_string())
            .or_default() += 1;
        match record.verify_checksum().map_err(ApiError::internal)? {
            WalChecksumStatus::Valid => {}
            WalChecksumStatus::Missing => checksum_missing += 1,
            WalChecksumStatus::Mismatch { .. } => checksum_mismatch += 1,
        }

        let shard = shard_summaries
            .entry(record.shard)
            .or_insert_with(|| ExportWalShardSummary {
                shard: record.shard,
                records: 0,
                lowest_lsn: None,
                highest_lsn: None,
            });
        shard.records += 1;
        shard.lowest_lsn = Some(
            shard
                .lowest_lsn
                .map_or(record.lsn, |lsn| lsn.min(record.lsn)),
        );
        shard.highest_lsn = Some(
            shard
                .highest_lsn
                .map_or(record.lsn, |lsn| lsn.max(record.lsn)),
        );

        if include_samples && samples.len() < sample_limit {
            samples.push(record.clone());
        }

        match &record.payload {
            WalPayload::MessageCreated { message } => {
                *rooms.entry(message.room_id.clone()).or_default() += 1;
            }
            WalPayload::UserEventPublished { event } => {
                *users.entry(event.user_id.clone()).or_default() += 1;
            }
            WalPayload::UserUpserted { user } => {
                users.entry(user.user_id.clone()).or_default();
            }
            WalPayload::ObjectCommitted { object, .. } => {
                object_committed += 1;
                live_objects.insert(object.id.clone(), object.clone());
            }
            WalPayload::ObjectDeleted { object_id, .. } => {
                object_deleted += 1;
                live_objects.remove(object_id);
            }
            WalPayload::RecordUpserted { record } => {
                *tables.entry(record.table.clone()).or_default() += 1;
            }
            WalPayload::RecordDeleted { record } => {
                *tables.entry(record.table.clone()).or_default() += 1;
            }
            WalPayload::RecordTransactionCommitted { operations, .. } => {
                for operation in operations {
                    match operation {
                        DbRecordMutationDraft::Upsert { record } => {
                            *tables.entry(record.table.clone()).or_default() += 1;
                        }
                        DbRecordMutationDraft::Delete { record } => {
                            *tables.entry(record.table.clone()).or_default() += 1;
                        }
                    }
                }
            }
            WalPayload::SchemaApplied { .. }
            | WalPayload::BehaviorPublished { .. }
            | WalPayload::ActorReminderScheduled { .. }
            | WalPayload::ActorReminderCancelled { .. }
            | WalPayload::ActorReminderFired { .. }
            | WalPayload::HostHttpRequested { .. }
            | WalPayload::HostHttpCompleted { .. }
            | WalPayload::ClientMutationRecorded { .. } => {}
        }
    }

    let cluster_control = build_export_cluster_control_summary(state)
        .await
        .map_err(ApiError::internal)?;

    Ok(ExportManifestResponse {
        format: "nextdb.logical-export-manifest.v1".to_string(),
        generated_at_ms: now_ms(),
        node_id: state.cluster.node_id().to_string(),
        base_lsn,
        incremental: base_lsn > 0,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
        last_snapshot_lsn: state.last_snapshot_lsn.load(Ordering::Acquire),
        last_compaction_lsn: state.last_compaction_lsn.load(Ordering::Acquire),
        schema_version: state.schema.version(),
        schema_history_versions: state
            .schema
            .history()
            .await
            .map_err(ApiError::internal)?
            .into_iter()
            .map(|entry| entry.version)
            .collect(),
        schema_proposals: state.schema_proposals.read().await.len(),
        cluster_control,
        wal: ExportWalSummary {
            records: records.len(),
            lowest_lsn: records.first().map(|record| record.lsn),
            highest_lsn: records.last().map(|record| record.lsn).unwrap_or(0),
            checksum_missing,
            checksum_mismatch,
            shards: shard_summaries.into_values().collect(),
        },
        payloads,
        tables,
        rooms,
        users,
        objects: ExportObjectSummary {
            committed: object_committed,
            deleted: object_deleted,
            live: live_objects.len(),
            live_bytes: live_objects.values().map(|object| object.byte_size).sum(),
        },
        encryption: ExportBundleEncryptionSummary::default(),
        samples,
    })
}

pub(crate) async fn create_export_bundle_internal(
    state: &AppState,
    request: ExportBundleCreateRequest,
) -> Result<ExportBundleResponse, ApiError> {
    let encryption_key = bundle_encryption_key(request.encryption_key);
    let base_lsn = request.base_lsn.unwrap_or(0);
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    if base_lsn > current_lsn {
        return Err(ApiError::bad_request(format!(
            "baseLsn {base_lsn} is ahead of current LSN {current_lsn}"
        )));
    }
    let mut manifest = build_export_manifest(
        state,
        ExportManifestQuery {
            include_samples: Some(false),
            sample_limit: None,
            base_lsn: Some(base_lsn),
        },
    )
    .await?;
    if manifest.wal.checksum_mismatch > 0 {
        return Err(ApiError::conflict(
            "refusing to create export bundle while WAL checksum mismatches exist",
        ));
    }
    let encrypted = encryption_key.is_some();
    if encrypted {
        manifest.encryption = ExportBundleEncryptionSummary {
            encrypted: true,
            algorithm: Some("AES-256-GCM".to_string()),
            key_derivation: Some("SHA-256(export key)".to_string()),
            encrypted_files: 0,
        };
    }

    let records: Vec<WalRecord> = wal::read_records_from_wal_paths(&state.wal_paths)
        .map_err(ApiError::internal)?
        .into_iter()
        .filter(|record| record.lsn > base_lsn)
        .collect();
    let id = if base_lsn > 0 {
        format!(
            "export-{}-lsn-{}-{}",
            now_ms(),
            base_lsn + 1,
            manifest.current_lsn
        )
    } else {
        format!("export-{}-lsn-{}", now_ms(), manifest.current_lsn)
    };
    let root = state.export_root.join(&id);
    let schema_root = root.join("schema");
    let schema_history_dir = schema_root.join("history");
    let schema_proposals_path = schema_root.join("proposals.json");
    let cluster_control_dir = root.join("cluster");
    let objects_root = root.join("objects");
    let object_metadata_dir = objects_root.join("metadata");
    let object_blob_dir = objects_root.join("blobs");
    fs::create_dir_all(&object_metadata_dir)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    fs::create_dir_all(&object_blob_dir)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    fs::create_dir_all(&schema_history_dir)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    fs::create_dir_all(&cluster_control_dir)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;

    let manifest_path = root.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;

    let schema_path = root.join("schema.json");
    fs::write(
        &schema_path,
        serde_json::to_vec_pretty(&state.schema.schema())
            .map_err(|err| ApiError::internal(err.into()))?,
    )
    .await
    .map_err(|err| ApiError::internal(err.into()))?;
    let schema_history_versions =
        write_export_bundle_schema_history(state, &schema_history_dir).await?;
    let schema_proposals =
        write_export_bundle_schema_proposals(state, &schema_proposals_path).await?;
    let cluster_control = write_export_bundle_cluster_control(state, &cluster_control_dir).await?;

    let wal_records_path = root.join("wal-records.jsonl");
    write_wal_records_jsonl(&wal_records_path, &records).await?;

    let objects = export_bundle_object_ids(state, &records, base_lsn).await?;
    let mut object_bytes = 0u64;
    let mut object_count = 0usize;
    for object_id in objects {
        let (metadata, body) = state
            .objects
            .body(&object_id)
            .await
            .map_err(ApiError::internal)?;
        object_bytes += metadata.byte_size;
        object_count += 1;
        fs::write(
            object_metadata_dir.join(format!("{}.json", metadata.id)),
            serde_json::to_vec_pretty(&metadata).map_err(|err| ApiError::internal(err.into()))?,
        )
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
        fs::write(object_blob_dir.join(format!("{}.bin", metadata.id)), body)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    if let Some(encryption_key) = encryption_key.as_deref() {
        let encrypted_files = encrypt_export_bundle_files(&root, &id, encryption_key).await?;
        manifest.encryption.encrypted_files = encrypted_files;
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).map_err(|err| ApiError::internal(err.into()))?,
        )
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    }

    Ok(ExportBundleResponse {
        id,
        path: root.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        schema_path: schema_path.display().to_string(),
        schema_history_dir: schema_history_dir.display().to_string(),
        schema_history_versions,
        schema_proposals_path: schema_proposals_path.display().to_string(),
        schema_proposals,
        cluster_control_dir: cluster_control_dir.display().to_string(),
        cluster_control,
        wal_records_path: wal_records_path.display().to_string(),
        object_metadata_dir: object_metadata_dir.display().to_string(),
        object_blob_dir: object_blob_dir.display().to_string(),
        wal_records: records.len(),
        objects: object_count,
        object_bytes,
        encrypted,
        manifest,
    })
}

pub(crate) async fn verify_export_bundle_internal(
    state: &AppState,
    bundle_id: String,
    encryption_key: Option<String>,
) -> Result<ExportBundleVerifyResponse, ApiError> {
    if !ensure_safe_export_bundle_id(&bundle_id) {
        return Err(ApiError::bad_request("invalid export bundle id"));
    }

    let root = state.export_root.join(&bundle_id);
    if !root.exists() {
        return Err(ApiError::not_found("export bundle not found"));
    }

    let mut problems = Vec::new();
    let manifest_path = root.join("manifest.json");
    let manifest = match fs::read(&manifest_path).await {
        Ok(bytes) => match serde_json::from_slice::<ExportManifestResponse>(&bytes) {
            Ok(manifest) => Some(manifest),
            Err(err) => {
                problems.push(format!("manifest.json is not valid: {err}"));
                None
            }
        },
        Err(err) => {
            problems.push(format!("manifest.json could not be read: {err}"));
            None
        }
    };

    let encrypted = manifest
        .as_ref()
        .map(|manifest| manifest.encryption.encrypted)
        .unwrap_or(false);
    if encrypted && encryption_key.is_none() {
        let mut problems = problems;
        problems.push("bundle is encrypted; encryptionKey is required".to_string());
        return Ok(ExportBundleVerifyResponse {
            id: bundle_id,
            path: root.display().to_string(),
            ok: false,
            checked_at_ms: now_ms(),
            wal_records: manifest
                .as_ref()
                .map(|manifest| manifest.wal.records)
                .unwrap_or(0),
            schema_version: manifest.as_ref().map(|manifest| manifest.schema_version),
            schema_history_versions: manifest
                .as_ref()
                .map(|manifest| manifest.schema_history_versions.clone())
                .unwrap_or_default(),
            schema_proposals: manifest
                .as_ref()
                .map(|manifest| manifest.schema_proposals)
                .unwrap_or_default(),
            cluster_control: manifest
                .as_ref()
                .map(|manifest| manifest.cluster_control.clone())
                .unwrap_or_default(),
            objects: manifest
                .as_ref()
                .map(|manifest| manifest.objects.live)
                .unwrap_or_default(),
            object_bytes: manifest
                .as_ref()
                .map(|manifest| manifest.objects.live_bytes)
                .unwrap_or_default(),
            encrypted,
            problems,
            manifest,
        });
    }
    let prepared = prepare_export_bundle_read_root(
        state,
        &root,
        manifest.as_ref(),
        encryption_key.as_deref(),
        &mut problems,
    )
    .await?;
    let read_root = prepared.root.clone();

    let wal_records_path = read_root.join("wal-records.jsonl");
    let records = match wal::read_records_file(&wal_records_path) {
        Ok(records) => records,
        Err(err) => {
            problems.push(format!("wal-records.jsonl is not valid: {err:#}"));
            Vec::new()
        }
    };
    if let Some(manifest) = &manifest {
        if manifest.wal.records != records.len() {
            problems.push(format!(
                "manifest WAL count {} does not match wal-records.jsonl count {}",
                manifest.wal.records,
                records.len()
            ));
        }
        if manifest.wal.highest_lsn != records.last().map(|record| record.lsn).unwrap_or(0) {
            problems.push("manifest highest LSN does not match wal-records.jsonl".to_string());
        }
        if manifest.incremental != (manifest.base_lsn > 0) {
            problems.push("manifest incremental flag does not match baseLsn".to_string());
        }
        if let Some(first) = records.first()
            && first.lsn <= manifest.base_lsn
        {
            problems.push(format!(
                "wal-records.jsonl contains LSN {} at or before manifest baseLsn {}",
                first.lsn, manifest.base_lsn
            ));
        }
        if records.iter().any(|record| record.lsn <= manifest.base_lsn) {
            problems
                .push("wal-records.jsonl contains records outside the manifest range".to_string());
        }
    }

    let schema_version = read_export_bundle_schema(&read_root, &mut problems)
        .await
        .map(|schema| schema.version);
    let schema_history = read_export_bundle_schema_history(&read_root, &mut problems).await;
    let schema_history_versions: Vec<u32> =
        schema_history.iter().map(|schema| schema.version).collect();
    let schema_proposals = read_export_bundle_schema_proposals(&read_root, &mut problems)
        .await
        .len();
    let cluster_control = read_export_bundle_cluster_control(&read_root, &mut problems)
        .await
        .summary();
    if let (Some(manifest), Some(schema_version)) = (&manifest, schema_version)
        && manifest.schema_version != schema_version
    {
        problems.push(format!(
            "manifest schema version {} does not match schema.json version {}",
            manifest.schema_version, schema_version
        ));
    }
    if let Some(manifest) = &manifest {
        if !manifest.schema_history_versions.is_empty()
            && manifest.schema_history_versions != schema_history_versions
        {
            problems.push(format!(
                "manifest schema history versions {:?} do not match schema/history versions {:?}",
                manifest.schema_history_versions, schema_history_versions
            ));
        }
        if manifest.schema_proposals != schema_proposals {
            problems.push(format!(
                "manifest schema proposal count {} does not match schema/proposals.json count {}",
                manifest.schema_proposals, schema_proposals
            ));
        }
        if manifest.cluster_control != cluster_control {
            problems.push(format!(
                "manifest cluster control summary {:?} does not match cluster bundle summary {:?}",
                manifest.cluster_control, cluster_control
            ));
        }
    }
    validate_wal_schema_versions(
        &records,
        schema_version,
        &schema_history_versions,
        &mut problems,
    );

    let object_metadata_dir = read_root.join("objects").join("metadata");
    let object_blob_dir = read_root.join("objects").join("blobs");
    let (objects, object_bytes) =
        verify_export_bundle_objects(&object_metadata_dir, &object_blob_dir, &mut problems).await;

    if let Some(manifest) = &manifest {
        if manifest.objects.live != objects {
            problems.push(format!(
                "manifest live object count {} does not match object files {}",
                manifest.objects.live, objects
            ));
        }
        if manifest.objects.live_bytes != object_bytes {
            problems.push(format!(
                "manifest live object bytes {} does not match object files {}",
                manifest.objects.live_bytes, object_bytes
            ));
        }
    }

    let response = ExportBundleVerifyResponse {
        id: bundle_id,
        path: root.display().to_string(),
        ok: problems.is_empty(),
        checked_at_ms: now_ms(),
        wal_records: records.len(),
        schema_version,
        schema_history_versions,
        schema_proposals,
        cluster_control,
        objects,
        object_bytes,
        encrypted,
        problems,
        manifest,
    };
    cleanup_prepared_export_bundle(prepared).await;
    Ok(response)
}

pub(crate) async fn verify_export_bundle_chain_internal(
    state: &AppState,
    bundle_ids: Vec<String>,
    encryption_key: Option<String>,
) -> Result<ExportBundleChainVerifyResponse, ApiError> {
    let mut problems = Vec::new();
    let mut entries = Vec::new();
    if bundle_ids.is_empty() {
        problems.push("bundleIds must not be empty".to_string());
    }

    let mut expected_base_lsn = 0u64;
    let mut highest_lsn = 0u64;
    for (index, bundle_id) in bundle_ids.into_iter().enumerate() {
        let verify =
            verify_export_bundle_internal(state, bundle_id.clone(), encryption_key.clone()).await?;
        if !verify.ok {
            problems.push(format!(
                "{} failed bundle verification: {}",
                bundle_id,
                verify.problems.join("; ")
            ));
        }

        let Some(manifest) = verify.manifest.as_ref() else {
            problems.push(format!("{bundle_id} is missing manifest"));
            entries.push(ExportBundleChainEntry {
                id: verify.id,
                ok: false,
                incremental: false,
                base_lsn: 0,
                current_lsn: 0,
                wal_records: verify.wal_records,
                objects: verify.objects,
                encrypted: verify.encrypted,
                problems: verify.problems,
            });
            continue;
        };

        if manifest.format != "nextdb.logical-export-manifest.v1" {
            problems.push(format!(
                "{} has unsupported export format {}",
                bundle_id, manifest.format
            ));
        }
        if index == 0 {
            if manifest.incremental || manifest.base_lsn != 0 {
                problems.push(format!(
                    "{bundle_id} must be a full base bundle with baseLsn 0"
                ));
            }
        } else if !manifest.incremental || manifest.base_lsn == 0 {
            problems.push(format!("{bundle_id} must be an incremental bundle"));
        }
        if manifest.base_lsn != expected_base_lsn {
            problems.push(format!(
                "{} baseLsn {} does not match expected {}",
                bundle_id, manifest.base_lsn, expected_base_lsn
            ));
        }
        if manifest.current_lsn < manifest.base_lsn {
            problems.push(format!(
                "{} currentLsn {} is before baseLsn {}",
                bundle_id, manifest.current_lsn, manifest.base_lsn
            ));
        }
        if manifest.wal.records > 0 && manifest.wal.highest_lsn > manifest.current_lsn {
            problems.push(format!(
                "{} WAL highest LSN {} is beyond currentLsn {}",
                bundle_id, manifest.wal.highest_lsn, manifest.current_lsn
            ));
        }
        expected_base_lsn = manifest.current_lsn;
        highest_lsn = manifest.current_lsn;
        entries.push(ExportBundleChainEntry {
            id: verify.id,
            ok: verify.ok,
            incremental: manifest.incremental,
            base_lsn: manifest.base_lsn,
            current_lsn: manifest.current_lsn,
            wal_records: verify.wal_records,
            objects: verify.objects,
            encrypted: verify.encrypted,
            problems: verify.problems,
        });
    }

    Ok(ExportBundleChainVerifyResponse {
        ok: problems.is_empty(),
        checked_at_ms: now_ms(),
        base_lsn: entries.first().map(|entry| entry.base_lsn).unwrap_or(0),
        highest_lsn,
        bundles: entries,
        problems,
    })
}

pub(crate) async fn import_bundle_preflight_internal(
    state: &AppState,
    bundle_id: String,
    encryption_key: Option<String>,
) -> Result<ImportBundlePreflightResponse, ApiError> {
    let verify = verify_export_bundle_internal(state, bundle_id, encryption_key.clone()).await?;
    let mut problems = verify.problems;
    let mut notes = Vec::new();
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    let original_root = PathBuf::from(&verify.path);
    let prepared = if verify.encrypted && problems.is_empty() {
        Some(
            prepare_export_bundle_read_root(
                state,
                &original_root,
                verify.manifest.as_ref(),
                encryption_key.as_deref(),
                &mut problems,
            )
            .await?,
        )
    } else {
        None
    };
    let root = prepared
        .as_ref()
        .map(|prepared| prepared.root.as_path())
        .unwrap_or(original_root.as_path());

    if current_lsn > 0 {
        problems.push(format!(
            "current database is not empty: current LSN is {current_lsn}"
        ));
    } else {
        notes.push("current database is empty".to_string());
    }

    let bundle_highest_lsn = verify
        .manifest
        .as_ref()
        .map(|manifest| manifest.wal.highest_lsn)
        .unwrap_or(0);
    if let Some(manifest) = &verify.manifest {
        if manifest.format != "nextdb.logical-export-manifest.v1" {
            problems.push(format!("unsupported export format {}", manifest.format));
        }
        if manifest.incremental || manifest.base_lsn > 0 {
            problems.push(format!(
                "bundle is incremental from base LSN {}; restore a base bundle first and apply delta import instead",
                manifest.base_lsn
            ));
        }
        if manifest.wal.checksum_mismatch > 0 {
            problems.push(format!(
                "bundle manifest reports {} WAL checksum mismatches",
                manifest.wal.checksum_mismatch
            ));
        }
        if manifest.wal.checksum_missing > 0 {
            notes.push(format!(
                "bundle contains {} legacy WAL records without checksums",
                manifest.wal.checksum_missing
            ));
        }
    } else {
        problems.push("bundle manifest is required for import preflight".to_string());
    }

    let bundle_schema = read_export_bundle_schema(root, &mut problems).await;
    let bundle_schema_version = bundle_schema.as_ref().map(|schema| schema.version);
    let bundle_schema_history = read_export_bundle_schema_history(root, &mut problems).await;
    let bundle_schema_history_versions: Vec<u32> = bundle_schema_history
        .iter()
        .map(|schema| schema.version)
        .collect();
    let bundle_schema_proposals = verify.schema_proposals;
    let bundle_cluster_control = verify.cluster_control.clone();
    if let (Some(manifest), Some(schema_version)) = (&verify.manifest, bundle_schema_version)
        && manifest.schema_version != schema_version
    {
        problems.push(format!(
            "manifest schema version {} does not match schema.json version {}",
            manifest.schema_version, schema_version
        ));
    }
    if let Some(manifest) = &verify.manifest
        && !manifest.schema_history_versions.is_empty()
        && manifest.schema_history_versions != bundle_schema_history_versions
    {
        problems.push(format!(
            "manifest schema history versions {:?} do not match schema/history versions {:?}",
            manifest.schema_history_versions, bundle_schema_history_versions
        ));
    }
    validate_import_bundle_wal_projection(
        state,
        root,
        bundle_schema.as_ref(),
        &mut problems,
        &mut notes,
    )
    .await;

    if problems.is_empty() {
        notes.push("bundle is ready for empty-database restore".to_string());
    }

    let response = ImportBundlePreflightResponse {
        id: verify.id,
        path: verify.path,
        ok: problems.is_empty(),
        checked_at_ms: now_ms(),
        current_lsn,
        requires_empty_database: true,
        bundle_wal_records: verify.wal_records,
        bundle_highest_lsn,
        bundle_schema_version,
        bundle_schema_history_versions,
        bundle_schema_proposals,
        bundle_cluster_control,
        bundle_objects: verify.objects,
        bundle_object_bytes: verify.object_bytes,
        bundle_encrypted: verify.encrypted,
        problems,
        notes,
        manifest: verify.manifest,
    };
    if let Some(prepared) = prepared {
        cleanup_prepared_export_bundle(prepared).await;
    }
    Ok(response)
}

pub(crate) async fn import_bundle_delta_preflight_internal(
    state: &AppState,
    bundle_id: String,
    encryption_key: Option<String>,
) -> Result<ImportBundleDeltaPreflightResponse, ApiError> {
    let verify = verify_export_bundle_internal(state, bundle_id, encryption_key.clone()).await?;
    let mut problems = verify.problems;
    let mut notes = Vec::new();
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    let original_root = PathBuf::from(&verify.path);
    let prepared = if verify.encrypted && problems.is_empty() {
        Some(
            prepare_export_bundle_read_root(
                state,
                &original_root,
                verify.manifest.as_ref(),
                encryption_key.as_deref(),
                &mut problems,
            )
            .await?,
        )
    } else {
        None
    };
    let root = prepared
        .as_ref()
        .map(|prepared| prepared.root.as_path())
        .unwrap_or(original_root.as_path());

    let mut base_lsn = 0u64;
    let mut bundle_highest_lsn = 0u64;
    if let Some(manifest) = &verify.manifest {
        base_lsn = manifest.base_lsn;
        bundle_highest_lsn = manifest.wal.highest_lsn;
        if manifest.format != "nextdb.logical-export-manifest.v1" {
            problems.push(format!("unsupported export format {}", manifest.format));
        }
        if !manifest.incremental || manifest.base_lsn == 0 {
            problems.push("bundle is not incremental".to_string());
        }
        if current_lsn != manifest.base_lsn {
            problems.push(format!(
                "current LSN {current_lsn} does not match bundle baseLsn {}",
                manifest.base_lsn
            ));
        } else {
            notes.push(format!("current database is at base LSN {current_lsn}"));
        }
        if manifest.wal.checksum_mismatch > 0 {
            problems.push(format!(
                "bundle manifest reports {} WAL checksum mismatches",
                manifest.wal.checksum_mismatch
            ));
        }
        if manifest.wal.checksum_missing > 0 {
            notes.push(format!(
                "bundle contains {} legacy WAL records without checksums",
                manifest.wal.checksum_missing
            ));
        }
    } else {
        problems.push("bundle manifest is required for delta import preflight".to_string());
    }

    let bundle_schema = read_export_bundle_schema(root, &mut problems).await;
    let bundle_schema_version = bundle_schema.as_ref().map(|schema| schema.version);
    if let (Some(manifest), Some(schema_version)) = (&verify.manifest, bundle_schema_version) {
        if manifest.schema_version != schema_version {
            problems.push(format!(
                "manifest schema version {} does not match schema.json version {}",
                manifest.schema_version, schema_version
            ));
        }
        if schema_version < state.schema.version() {
            problems.push(format!(
                "bundle schema version {schema_version} is older than current schema version {}",
                state.schema.version()
            ));
        }
    }
    validate_import_bundle_wal_projection(
        state,
        root,
        bundle_schema.as_ref(),
        &mut problems,
        &mut notes,
    )
    .await;

    if problems.is_empty() {
        notes.push("bundle delta is ready to apply".to_string());
    }

    let response = ImportBundleDeltaPreflightResponse {
        id: verify.id,
        path: verify.path,
        ok: problems.is_empty(),
        checked_at_ms: now_ms(),
        current_lsn,
        base_lsn,
        bundle_wal_records: verify.wal_records,
        bundle_highest_lsn,
        bundle_schema_version,
        bundle_objects: verify.objects,
        bundle_object_bytes: verify.object_bytes,
        bundle_encrypted: verify.encrypted,
        problems,
        notes,
        manifest: verify.manifest,
    };
    if let Some(prepared) = prepared {
        cleanup_prepared_export_bundle(prepared).await;
    }
    Ok(response)
}

pub(crate) fn validate_wal_schema_versions(
    records: &[WalRecord],
    schema_version: Option<u32>,
    schema_history_versions: &[u32],
    problems: &mut Vec<String>,
) {
    let mut known_versions: BTreeSet<u32> = schema_history_versions.iter().copied().collect();
    if let Some(version) = schema_version {
        known_versions.insert(version);
    }
    if known_versions.is_empty() {
        if !records.is_empty() {
            problems.push("bundle has WAL records but no readable schema versions".to_string());
        }
        return;
    }

    let missing_versions: BTreeSet<u32> = records
        .iter()
        .map(|record| record.schema_version)
        .filter(|version| !known_versions.contains(version))
        .collect();
    if !missing_versions.is_empty() {
        problems.push(format!(
            "bundle WAL references schema versions {:?} that are missing from schema/history",
            missing_versions.into_iter().collect::<Vec<_>>()
        ));
    }
}

pub(crate) async fn validate_import_bundle_wal_projection(
    state: &AppState,
    root: &Path,
    schema: Option<&DatabaseSchema>,
    problems: &mut Vec<String>,
    notes: &mut Vec<String>,
) {
    let records = match wal::read_records_file(&root.join("wal-records.jsonl")) {
        Ok(records) => records,
        Err(err) => {
            problems.push(format!(
                "wal-records.jsonl could not be read for projection validation: {err:#}"
            ));
            return;
        }
    };
    let Some(schema) = schema else {
        problems.push("schema.json is required for projection validation".to_string());
        return;
    };
    for record in &records {
        if record.shard >= state.wal_shards.len() {
            problems.push(format!(
                "bundle references shard {}, but local node only has {} shards",
                record.shard,
                state.wal_shards.len()
            ));
        }
    }
    if problems
        .iter()
        .any(|problem| problem.starts_with("bundle references shard "))
    {
        return;
    }

    let final_records = records_from_wal_records(records);
    for record in &final_records {
        if let Err(err) = validate_record_against_schema(schema, record) {
            problems.push(format!(
                "bundle record {} failed schema validation: {err:#}",
                record.path
            ));
        }
    }
    if problems
        .iter()
        .any(|problem| problem.contains("failed schema validation"))
    {
        return;
    }

    let schema_indexes = schema_indexes_by_table(schema);
    let schema_orders = match schema_orders_by_table(schema) {
        Ok(orders) => orders,
        Err(err) => {
            problems.push(format!("schema order validation failed: {err}"));
            return;
        }
    };
    match state
        .records
        .validate_rebuild_from_records_with_indexes(&final_records, &schema_indexes, &schema_orders)
        .await
    {
        Ok(_) => notes.push(format!(
            "bundle record projection validated for {} live records",
            final_records.len()
        )),
        Err(err) => problems.push(format!(
            "bundle record projection validation failed: {err:#}"
        )),
    }
}

pub(crate) async fn apply_schema_for_empty_restore(
    state: &AppState,
    schema: DatabaseSchema,
) -> Result<(), ApiError> {
    schema
        .validation_report()
        .into_result()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let records = Vec::<DbRecord>::new();
    let schema_indexes = schema_indexes_by_table(&schema);
    let schema_orders =
        schema_orders_by_table(&schema).map_err(|err| ApiError::bad_request(err.to_string()))?;
    state
        .records
        .validate_rebuild_from_records_with_indexes(&records, &schema_indexes, &schema_orders)
        .await
        .map_err(ApiError::internal)?;
    state
        .records
        .force_rebuild_from_records_with_indexes(&records, &schema_indexes, &schema_orders)
        .await
        .map_err(ApiError::internal)?;
    state
        .schema
        .persist_candidate(&schema)
        .await
        .map_err(ApiError::internal)?;
    state
        .record_hot
        .reconfigure(&schema, &records, state.record_hot_durable_idle_ttl_ms)
        .await;
    state.schema.apply(schema);
    Ok(())
}

pub(crate) async fn write_wal_records_jsonl(
    path: &Path,
    records: &[WalRecord],
) -> Result<(), ApiError> {
    let mut file = fs::File::create(path)
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    for record in records {
        let bytes = serde_json::to_vec(record).map_err(|err| ApiError::internal(err.into()))?;
        file.write_all(&bytes)
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
        file.write_all(b"\n")
            .await
            .map_err(|err| ApiError::internal(err.into()))?;
    }
    file.flush()
        .await
        .map_err(|err| ApiError::internal(err.into()))?;
    Ok(())
}

pub(crate) async fn restore_export_bundle_cluster_control(
    state: &AppState,
    bundle: ExportClusterControlBundle,
) -> Result<(), ApiError> {
    *state.topology_overrides.write().await = bundle.topology_overrides;
    persist_topology_overrides(state).await?;
    write_topology_log_entries(&state.topology_log_path, &bundle.topology_log).await?;
    *state.topology_proposals.write().await = bundle.topology_proposals;
    persist_topology_proposals(state).await?;
    *state.topology_lease.write().await = bundle.topology_lease;
    persist_topology_lease(state).await?;
    *state.handoff_workflows.write().await = bundle.handoff_workflows;
    persist_handoff_workflows(state).await?;
    for shard in 0..state.wal_shards.len() {
        refresh_wal_remote_replicas_for_shard(state, shard).await?;
    }
    Ok(())
}

fn validate_record_against_schema(schema: &DatabaseSchema, record: &DbRecord) -> Result<()> {
    if let Some((table_name, nested_name)) = record.table.split_once('.') {
        schema.validate_nested_table_record(table_name, nested_name, &record.value)?;
        if let Some((_, nested_key)) = record.key.split_once(':')
            && let Some(id) = record.value.get("id").and_then(serde_json::Value::as_str)
            && id != nested_key
        {
            anyhow::bail!("nested record value.id must match nested key");
        }
        return Ok(());
    }

    schema.validate_table_record(&record.table, &record.value)?;
    if let Some(id) = record.value.get("id").and_then(serde_json::Value::as_str)
        && id != record.key
    {
        anyhow::bail!("record value.id must match key");
    }
    Ok(())
}

pub(crate) fn bundle_encryption_key(request_key: Option<String>) -> Option<String> {
    request_key
        .or_else(|| std::env::var("NEXTDB_EXPORT_BUNDLE_KEY").ok())
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportManifestResponse {
    pub(crate) format: String,
    pub(crate) generated_at_ms: u64,
    pub(crate) node_id: String,
    #[serde(default)]
    pub(crate) base_lsn: u64,
    #[serde(default)]
    pub(crate) incremental: bool,
    pub(crate) current_lsn: u64,
    pub(crate) last_snapshot_lsn: u64,
    pub(crate) last_compaction_lsn: u64,
    pub(crate) schema_version: u32,
    #[serde(default)]
    pub(crate) schema_history_versions: Vec<u32>,
    #[serde(default)]
    pub(crate) schema_proposals: usize,
    #[serde(default)]
    pub(crate) cluster_control: ExportClusterControlSummary,
    pub(crate) wal: ExportWalSummary,
    pub(crate) payloads: BTreeMap<String, usize>,
    pub(crate) tables: BTreeMap<String, usize>,
    pub(crate) rooms: BTreeMap<String, usize>,
    pub(crate) users: BTreeMap<String, usize>,
    pub(crate) objects: ExportObjectSummary,
    #[serde(default)]
    pub(crate) encryption: ExportBundleEncryptionSummary,
    pub(crate) samples: Vec<WalRecord>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleResponse {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) manifest_path: String,
    pub(crate) schema_path: String,
    pub(crate) schema_history_dir: String,
    pub(crate) schema_history_versions: Vec<u32>,
    pub(crate) schema_proposals_path: String,
    pub(crate) schema_proposals: usize,
    pub(crate) cluster_control_dir: String,
    pub(crate) cluster_control: ExportClusterControlSummary,
    pub(crate) wal_records_path: String,
    pub(crate) object_metadata_dir: String,
    pub(crate) object_blob_dir: String,
    pub(crate) wal_records: usize,
    pub(crate) objects: usize,
    pub(crate) object_bytes: u64,
    pub(crate) encrypted: bool,
    pub(crate) manifest: ExportManifestResponse,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleListResponse {
    pub(crate) bundles: Vec<ExportBundleListEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleListEntry {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) ok: bool,
    pub(crate) schema_version: Option<u32>,
    pub(crate) schema_history_versions: Vec<u32>,
    pub(crate) schema_proposals: usize,
    pub(crate) cluster_control: ExportClusterControlSummary,
    pub(crate) wal_records: Option<usize>,
    pub(crate) highest_lsn: Option<u64>,
    pub(crate) objects: Option<usize>,
    pub(crate) object_bytes: Option<u64>,
    pub(crate) encrypted: bool,
    pub(crate) problems: Vec<String>,
    pub(crate) manifest: Option<ExportManifestResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleVerifyResponse {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) ok: bool,
    pub(crate) checked_at_ms: u64,
    pub(crate) wal_records: usize,
    pub(crate) schema_version: Option<u32>,
    pub(crate) schema_history_versions: Vec<u32>,
    pub(crate) schema_proposals: usize,
    pub(crate) cluster_control: ExportClusterControlSummary,
    pub(crate) objects: usize,
    pub(crate) object_bytes: u64,
    pub(crate) encrypted: bool,
    pub(crate) problems: Vec<String>,
    pub(crate) manifest: Option<ExportManifestResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleChainVerifyResponse {
    pub(crate) ok: bool,
    pub(crate) checked_at_ms: u64,
    pub(crate) base_lsn: u64,
    pub(crate) highest_lsn: u64,
    pub(crate) bundles: Vec<ExportBundleChainEntry>,
    pub(crate) problems: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleChainEntry {
    pub(crate) id: String,
    pub(crate) ok: bool,
    pub(crate) incremental: bool,
    pub(crate) base_lsn: u64,
    pub(crate) current_lsn: u64,
    pub(crate) wal_records: usize,
    pub(crate) objects: usize,
    pub(crate) encrypted: bool,
    pub(crate) problems: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupRunRecord {
    pub(crate) id: String,
    pub(crate) created_at_ms: u64,
    pub(crate) mode: String,
    pub(crate) base_lsn: u64,
    pub(crate) current_lsn: u64,
    pub(crate) no_op: bool,
    pub(crate) bundle_id: Option<String>,
    pub(crate) object_id: Option<String>,
    pub(crate) chain_bundle_ids: Vec<String>,
    pub(crate) chain_ok: Option<bool>,
    pub(crate) bundle_wal_records: Option<usize>,
    pub(crate) bundle_objects: Option<usize>,
    pub(crate) bundle_object_bytes: Option<u64>,
    pub(crate) archive_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupRunListResponse {
    pub(crate) runs: Vec<ExportBackupRunRecord>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupRetentionResponse {
    pub(crate) dry_run: bool,
    pub(crate) keep_last: Option<usize>,
    pub(crate) before_timestamp_ms: Option<u64>,
    pub(crate) candidates: usize,
    pub(crate) retained: usize,
    pub(crate) deleted_runs: Vec<String>,
    pub(crate) deleted_bundles: Vec<String>,
    pub(crate) deleted_archive_objects: Vec<String>,
    pub(crate) protected_bundles: Vec<String>,
    pub(crate) protected_archive_objects: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupPolicyResponse {
    pub(crate) policy: ExportBackupPolicy,
    pub(crate) controller: ExportBackupControllerState,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupPolicyRunResponse {
    pub(crate) policy: ExportBackupPolicy,
    pub(crate) backup: ExportBackupRunResponse,
    pub(crate) retention: Option<ExportBackupRetentionResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBackupRunResponse {
    pub(crate) run: ExportBackupRunRecord,
    pub(crate) mode: String,
    pub(crate) base_lsn: u64,
    pub(crate) current_lsn: u64,
    pub(crate) no_op: bool,
    pub(crate) bundle: Option<ExportBundleResponse>,
    pub(crate) archived: Option<ExportBundleArchiveObjectResponse>,
    pub(crate) chain: Option<ExportBundleChainVerifyResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundlePreflightResponse {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) ok: bool,
    pub(crate) checked_at_ms: u64,
    pub(crate) current_lsn: u64,
    pub(crate) requires_empty_database: bool,
    pub(crate) bundle_wal_records: usize,
    pub(crate) bundle_highest_lsn: u64,
    pub(crate) bundle_schema_version: Option<u32>,
    pub(crate) bundle_schema_history_versions: Vec<u32>,
    pub(crate) bundle_schema_proposals: usize,
    pub(crate) bundle_cluster_control: ExportClusterControlSummary,
    pub(crate) bundle_objects: usize,
    pub(crate) bundle_object_bytes: u64,
    pub(crate) bundle_encrypted: bool,
    pub(crate) problems: Vec<String>,
    pub(crate) notes: Vec<String>,
    pub(crate) manifest: Option<ExportManifestResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleRestoreResponse {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) restored: bool,
    pub(crate) restored_at_ms: u64,
    pub(crate) wal_records: usize,
    pub(crate) schema_version: Option<u32>,
    pub(crate) schema_history_versions: Vec<u32>,
    pub(crate) schema_proposals: usize,
    pub(crate) cluster_control: ExportClusterControlSummary,
    pub(crate) objects: usize,
    pub(crate) object_bytes: u64,
    pub(crate) encrypted: bool,
    pub(crate) current_lsn: u64,
    pub(crate) manifest: Option<ExportManifestResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleDeltaPreflightResponse {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) ok: bool,
    pub(crate) checked_at_ms: u64,
    pub(crate) current_lsn: u64,
    pub(crate) base_lsn: u64,
    pub(crate) bundle_wal_records: usize,
    pub(crate) bundle_highest_lsn: u64,
    pub(crate) bundle_schema_version: Option<u32>,
    pub(crate) bundle_objects: usize,
    pub(crate) bundle_object_bytes: u64,
    pub(crate) bundle_encrypted: bool,
    pub(crate) problems: Vec<String>,
    pub(crate) notes: Vec<String>,
    pub(crate) manifest: Option<ExportManifestResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleDeltaApplyResponse {
    pub(crate) id: String,
    pub(crate) path: String,
    pub(crate) applied: bool,
    pub(crate) applied_at_ms: u64,
    pub(crate) base_lsn: u64,
    pub(crate) wal_records: usize,
    pub(crate) objects: usize,
    pub(crate) object_bytes: u64,
    pub(crate) encrypted: bool,
    pub(crate) current_lsn: u64,
    pub(crate) manifest: Option<ExportManifestResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportBundleChainRestoreResponse {
    pub(crate) restored: bool,
    pub(crate) restored_at_ms: u64,
    pub(crate) chain: ExportBundleChainVerifyResponse,
    pub(crate) base: ImportBundleRestoreResponse,
    pub(crate) deltas: Vec<ImportBundleDeltaApplyResponse>,
    pub(crate) wal_records: usize,
    pub(crate) objects: usize,
    pub(crate) object_bytes: u64,
    pub(crate) current_lsn: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportWalSummary {
    pub(crate) records: usize,
    pub(crate) lowest_lsn: Option<u64>,
    pub(crate) highest_lsn: u64,
    pub(crate) checksum_missing: usize,
    pub(crate) checksum_mismatch: usize,
    pub(crate) shards: Vec<ExportWalShardSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportWalShardSummary {
    pub(crate) shard: usize,
    pub(crate) records: usize,
    pub(crate) lowest_lsn: Option<u64>,
    pub(crate) highest_lsn: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportObjectSummary {
    pub(crate) committed: usize,
    pub(crate) deleted: usize,
    pub(crate) live: usize,
    pub(crate) live_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportClusterControlSummary {
    pub(crate) topology_overrides: usize,
    pub(crate) topology_log_entries: usize,
    pub(crate) topology_proposals: usize,
    pub(crate) handoff_workflows: usize,
    pub(crate) topology_lease_term: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportBundleEncryptionSummary {
    pub(crate) encrypted: bool,
    pub(crate) algorithm: Option<String>,
    pub(crate) key_derivation: Option<String>,
    pub(crate) encrypted_files: usize,
}

pub(crate) struct LatestExportBundleChain {
    pub(crate) bundle_ids: Vec<String>,
    pub(crate) last_current_lsn: Option<u64>,
}

pub(crate) struct ExportClusterControlBundle {
    pub(crate) topology_overrides: BTreeMap<usize, ClusterShardOverride>,
    pub(crate) topology_log: Vec<TopologyLogEntry>,
    pub(crate) topology_proposals: BTreeMap<String, TopologyProposal>,
    pub(crate) topology_lease: TopologyLease,
    pub(crate) handoff_workflows: BTreeMap<String, HandoffWorkflow>,
}

impl ExportClusterControlBundle {
    pub(crate) fn summary(&self) -> ExportClusterControlSummary {
        ExportClusterControlSummary {
            topology_overrides: self.topology_overrides.len(),
            topology_log_entries: self.topology_log.len(),
            topology_proposals: self.topology_proposals.len(),
            handoff_workflows: self.handoff_workflows.len(),
            topology_lease_term: self.topology_lease.current_term,
        }
    }
}

pub(crate) struct PreparedExportBundleReadRoot {
    pub(crate) root: PathBuf,
    pub(crate) cleanup_root: Option<PathBuf>,
}
