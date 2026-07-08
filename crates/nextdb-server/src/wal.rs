use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
    sync::{Mutex, RwLock, mpsc, oneshot},
    time,
};
use tracing::error;

use crate::{
    actor::RoomLiveState,
    model::{
        ActorReminderDraft, BinaryJsonValue, ClientMutationRecord, DbRecordDeleteDraft,
        DbRecordDraft, DbRecordMutationDraft, Durability, MessageDraft, ObjectMetadata, ObjectRef,
        UserEventDraft, UserProfileDraft, WalChecksumStatus, WalPayload, WalRecord,
    },
    util::now_ms,
};

pub const DEFAULT_WAL_BATCH_MAX: usize = 1_024;
pub const DEFAULT_WAL_BATCH_WAIT_MS: u64 = 2;
const WAL_FRAME_MAGIC: [u8; 4] = *b"NDBW";
const WAL_FRAME_VERSION_V1: u16 = 1;
const WAL_FRAME_VERSION_V2: u16 = 2;
const WAL_FRAME_VERSION: u16 = WAL_FRAME_VERSION_V2;
const WAL_FRAME_ENCODING_JSON: u16 = 1;
const WAL_FRAME_ENCODING_POSTCARD: u16 = 2;
const WAL_FRAME_ENCODING: u16 = WAL_FRAME_ENCODING_POSTCARD;
const WAL_FRAME_HEADER_LEN_V1: usize = 12;
const WAL_FRAME_HEADER_LEN_V2: usize = 16;
const WAL_FRAME_MIN_HEADER_LEN: usize = WAL_FRAME_HEADER_LEN_V1;
const WAL_FRAME_MAX_BYTES: u32 = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct WalWriter {
    tx: mpsc::Sender<WalCommand>,
    status: Arc<RwLock<WalWriterStatus>>,
}

#[derive(Debug, Clone, Copy)]
pub struct WalWriterConfig {
    pub batch_max: usize,
    pub batch_wait: Duration,
}

impl WalWriterConfig {
    pub fn new(batch_max: usize, batch_wait_ms: u64) -> Self {
        Self {
            batch_max: batch_max.max(1),
            batch_wait: Duration::from_millis(batch_wait_ms),
        }
    }

    pub fn batch_wait_ms(&self) -> u64 {
        duration_ms(self.batch_wait)
    }
}

pub struct WalAppendRequest {
    pub lsn: u64,
    pub shard_epoch: u64,
    pub owner_node_id: String,
    pub durability: Durability,
    pub schema_version: u32,
    pub payload: WalPayload,
}

pub(crate) fn read_records_from_wal_paths(paths: &[PathBuf]) -> Result<Vec<WalRecord>> {
    let mut records = BTreeMap::new();
    for path in paths {
        for record in read_records_including_archives(path)? {
            records.insert(record.lsn, record);
        }
    }
    Ok(records.into_values().collect())
}

pub(crate) fn read_records_from_wal_paths_after_lsn(
    paths: &[PathBuf],
    after_lsn: u64,
) -> Result<Vec<WalRecord>> {
    let mut records = BTreeMap::new();
    for path in paths {
        for record in read_records_after_lsn_including_archives(path, after_lsn)? {
            records.insert(record.lsn, record);
        }
    }
    Ok(records.into_values().collect())
}

#[derive(Clone)]
pub(crate) struct WalShard {
    pub(crate) index: usize,
    pub(crate) path: PathBuf,
    pub(crate) replica_paths: Vec<PathBuf>,
    pub(crate) remote_ack_policy: WalRemoteAckPolicy,
    pub(crate) append_send_lock: Arc<Mutex<()>>,
    pub(crate) writer: WalWriter,
}

pub struct PendingWalAppends {
    waits: Vec<oneshot::Receiver<Result<WalRecord, String>>>,
}

impl PendingWalAppends {
    pub async fn wait(self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::with_capacity(self.waits.len());
        for wait in self.waits {
            records.push(
                wait.await
                    .context("WAL worker dropped append acknowledgement")?
                    .map_err(anyhow::Error::msg)?,
            );
        }
        Ok(records)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalReplica {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalRemoteReplica {
    pub url: String,
    #[serde(skip_serializing)]
    pub token: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WalRemoteAckPolicy {
    All,
    Quorum,
    None,
    Count(usize),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalWriterStatus {
    pub shard: usize,
    pub batch_max: usize,
    pub batch_wait_ms: u64,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub local_batches: u64,
    pub local_failed_batches: u64,
    pub local_records: u64,
    pub local_bytes: u64,
    pub local_syncs: u64,
    pub local_total_write_ms: u64,
    pub local_total_sync_ms: u64,
    pub local_last_batch_records: usize,
    pub local_last_batch_bytes: usize,
    pub local_last_batch_sync: bool,
    pub local_last_batch_started_at_ms: Option<u64>,
    pub local_last_batch_finished_at_ms: Option<u64>,
    pub local_last_batch_write_ms: u64,
    pub local_last_batch_sync_ms: u64,
    pub remote_ack_policy: WalRemoteAckPolicy,
    pub remote_required_acks: usize,
    pub remote_replica_count: usize,
    pub remote_replicas: Vec<WalRemoteReplicaStatus>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalRemoteReplicaStatus {
    pub url: String,
    pub ok: bool,
    pub highest_acked_lsn: u64,
    pub last_attempt_ms: Option<u64>,
    pub last_success_ms: Option<u64>,
    pub last_error_ms: Option<u64>,
    pub last_error: Option<String>,
    pub acked_batches: u64,
    pub failed_batches: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalRemoteRepairReport {
    pub shard: usize,
    pub remote_ack_policy: WalRemoteAckPolicy,
    pub remote_required_acks: usize,
    pub remote_replica_count: usize,
    pub repaired_replicas: usize,
    pub records_sent: usize,
    pub highest_lsn: u64,
    pub satisfied: bool,
    pub replicas: Vec<WalRemoteRepairReplicaReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalRemoteRepairReplicaReport {
    pub url: String,
    pub ok: bool,
    pub before_acked_lsn: u64,
    pub after_acked_lsn: u64,
    pub sent: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalCompactReport {
    pub upto_lsn: u64,
    pub archived: usize,
    pub retained: usize,
    pub archive_path: Option<String>,
    pub replicas: Vec<WalReplicaCompactReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalReplicaCompactReport {
    pub path: String,
    pub archived: usize,
    pub retained: usize,
    pub archive_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalChecksumSealReport {
    pub path: String,
    pub records: usize,
    pub sealed: usize,
    pub already_sealed: usize,
    pub rewritten: bool,
    pub replicas: Vec<WalChecksumSealFileReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalChecksumSealFileReport {
    pub path: String,
    pub records: usize,
    pub sealed: usize,
    pub already_sealed: usize,
    pub rewritten: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalIntegrityReport {
    pub ok: bool,
    pub shard_count: usize,
    pub file_count: usize,
    pub record_count: usize,
    pub unique_lsn_count: usize,
    pub duplicate_lsn_count: usize,
    pub checksum_missing_count: usize,
    pub checksum_mismatch_count: usize,
    pub lowest_lsn: Option<u64>,
    pub highest_lsn: u64,
    pub gaps: Vec<WalIntegrityGap>,
    pub shards: Vec<WalIntegrityShardReport>,
    pub issue_count: usize,
    pub issues_truncated: bool,
    pub issues: Vec<WalIntegrityIssue>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalIntegrityShardReport {
    pub shard: usize,
    pub active_path: String,
    pub archive_dir: String,
    pub file_count: usize,
    pub record_count: usize,
    pub first_lsn: Option<u64>,
    pub last_lsn: Option<u64>,
    pub files: Vec<WalIntegrityFileReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalIntegrityFileReport {
    pub path: String,
    pub kind: WalIntegrityFileKind,
    pub exists: bool,
    pub line_count: usize,
    pub record_count: usize,
    pub first_lsn: Option<u64>,
    pub last_lsn: Option<u64>,
    pub min_timestamp_ms: Option<u64>,
    pub max_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WalIntegrityFileKind {
    Active,
    Archive,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalIntegrityGap {
    pub after_lsn: u64,
    pub before_lsn: u64,
    pub missing_count: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalIntegrityIssue {
    pub severity: WalIntegrityIssueSeverity,
    pub code: String,
    pub path: Option<String>,
    pub line: Option<usize>,
    pub lsn: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WalIntegrityIssueSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone)]
struct WalIntegrityLocation {
    path: String,
    line: usize,
    shard: usize,
}

impl fmt::Display for WalIntegrityLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{} shard {}",
            self.path, self.line, self.shard
        )
    }
}

struct WalAppendCommand {
    lsn: u64,
    shard_epoch: u64,
    owner_node_id: String,
    durability: Durability,
    schema_version: u32,
    payload: WalPayload,
    ack: oneshot::Sender<Result<WalRecord, String>>,
}

struct WalCompactCommand {
    upto_lsn: u64,
    archive_dir: PathBuf,
    ack: oneshot::Sender<Result<WalCompactReport, String>>,
}

struct WalReplicateCommand {
    records: Vec<WalRecord>,
    sync: bool,
    ack: oneshot::Sender<Result<usize, String>>,
}

struct WalRepairRemoteCommand {
    after_lsn: Option<u64>,
    sync: bool,
    ack: oneshot::Sender<Result<WalRemoteRepairReport, String>>,
}

struct WalConfigureRemoteCommand {
    replicas: Vec<WalRemoteReplica>,
    remote_ack_policy: WalRemoteAckPolicy,
    ack: oneshot::Sender<Result<(), String>>,
}

struct WalSealChecksumsCommand {
    ack: oneshot::Sender<Result<WalChecksumSealReport, String>>,
}

enum WalCommand {
    AppendBatch(Vec<WalAppendCommand>),
    Compact(WalCompactCommand),
    Replicate(WalReplicateCommand),
    RepairRemote(WalRepairRemoteCommand),
    ConfigureRemote(WalConfigureRemoteCommand),
    SealChecksums(WalSealChecksumsCommand),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WalRemoteReplicateRequest<'a> {
    shard: usize,
    records: &'a [WalRecord],
    sync: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WalRemoteReplicateResponse {
    accepted: usize,
}

impl WalWriter {
    pub fn spawn(
        path: PathBuf,
        shard: usize,
        replicas: Vec<WalReplica>,
        remote_replicas: Vec<WalRemoteReplica>,
        remote_ack_policy: WalRemoteAckPolicy,
        config: WalWriterConfig,
    ) -> Self {
        let (tx, rx) = mpsc::channel(16_384);
        let status = Arc::new(RwLock::new(WalWriterStatus {
            shard,
            batch_max: config.batch_max,
            batch_wait_ms: config.batch_wait_ms(),
            queue_capacity: 0,
            queue_depth: 0,
            local_batches: 0,
            local_failed_batches: 0,
            local_records: 0,
            local_bytes: 0,
            local_syncs: 0,
            local_total_write_ms: 0,
            local_total_sync_ms: 0,
            local_last_batch_records: 0,
            local_last_batch_bytes: 0,
            local_last_batch_sync: false,
            local_last_batch_started_at_ms: None,
            local_last_batch_finished_at_ms: None,
            local_last_batch_write_ms: 0,
            local_last_batch_sync_ms: 0,
            remote_ack_policy,
            remote_required_acks: required_remote_acks(remote_ack_policy, remote_replicas.len()),
            remote_replica_count: remote_replicas.len(),
            remote_replicas: remote_replicas
                .iter()
                .map(|replica| WalRemoteReplicaStatus {
                    url: remote_replication_endpoint(&replica.url),
                    ok: true,
                    highest_acked_lsn: 0,
                    last_attempt_ms: None,
                    last_success_ms: None,
                    last_error_ms: None,
                    last_error: None,
                    acked_batches: 0,
                    failed_batches: 0,
                })
                .collect(),
        }));
        let worker_status = status.clone();
        tokio::spawn(async move {
            if let Err(err) = wal_worker(
                path,
                shard,
                replicas,
                remote_replicas,
                remote_ack_policy,
                config,
                worker_status,
                rx,
            )
            .await
            {
                error!(?err, "WAL worker stopped");
            }
        });
        Self { tx, status }
    }

    pub async fn enqueue_many(&self, requests: Vec<WalAppendRequest>) -> Result<PendingWalAppends> {
        if requests.is_empty() {
            return Ok(PendingWalAppends { waits: Vec::new() });
        }

        let mut commands = Vec::with_capacity(requests.len());
        let mut waits = Vec::with_capacity(requests.len());
        for request in requests {
            if request.durability == Durability::Volatile {
                anyhow::bail!("volatile events must not be appended to durable WAL");
            }
            let (ack, wait) = oneshot::channel();
            commands.push(WalAppendCommand {
                lsn: request.lsn,
                shard_epoch: request.shard_epoch,
                owner_node_id: request.owner_node_id,
                durability: request.durability,
                schema_version: request.schema_version,
                payload: request.payload,
                ack,
            });
            waits.push(wait);
        }

        self.tx
            .send(WalCommand::AppendBatch(commands))
            .await
            .context("WAL worker is not accepting writes")?;

        Ok(PendingWalAppends { waits })
    }

    pub async fn compact(&self, upto_lsn: u64, archive_dir: PathBuf) -> Result<WalCompactReport> {
        let (ack, wait) = oneshot::channel();
        let command = WalCompactCommand {
            upto_lsn,
            archive_dir,
            ack,
        };

        self.tx
            .send(WalCommand::Compact(command))
            .await
            .context("WAL worker is not accepting compaction requests")?;

        wait.await
            .context("WAL worker dropped compaction acknowledgement")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn seal_checksums(&self) -> Result<WalChecksumSealReport> {
        let (ack, wait) = oneshot::channel();
        let command = WalSealChecksumsCommand { ack };

        self.tx
            .send(WalCommand::SealChecksums(command))
            .await
            .context("WAL worker is not accepting checksum seal requests")?;

        wait.await
            .context("WAL worker dropped checksum seal acknowledgement")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn replicate(&self, records: Vec<WalRecord>, sync: bool) -> Result<usize> {
        let (ack, wait) = oneshot::channel();
        let command = WalReplicateCommand { records, sync, ack };

        self.tx
            .send(WalCommand::Replicate(command))
            .await
            .context("WAL worker is not accepting replication requests")?;

        wait.await
            .context("WAL worker dropped replication acknowledgement")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn repair_remote_replicas(
        &self,
        after_lsn: Option<u64>,
        sync: bool,
    ) -> Result<WalRemoteRepairReport> {
        let (ack, wait) = oneshot::channel();
        let command = WalRepairRemoteCommand {
            after_lsn,
            sync,
            ack,
        };

        self.tx
            .send(WalCommand::RepairRemote(command))
            .await
            .context("WAL worker is not accepting remote repair requests")?;

        wait.await
            .context("WAL worker dropped remote repair acknowledgement")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn configure_remote_replicas(
        &self,
        replicas: Vec<WalRemoteReplica>,
        remote_ack_policy: WalRemoteAckPolicy,
    ) -> Result<()> {
        let (ack, wait) = oneshot::channel();
        let command = WalConfigureRemoteCommand {
            replicas,
            remote_ack_policy,
            ack,
        };

        self.tx
            .send(WalCommand::ConfigureRemote(command))
            .await
            .context("WAL worker is not accepting remote replica configuration")?;

        wait.await
            .context("WAL worker dropped remote replica configuration acknowledgement")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn status(&self) -> WalWriterStatus {
        self.status_with_queue(self.status.read().await.clone())
    }

    pub fn try_status(&self) -> Option<WalWriterStatus> {
        self.status
            .try_read()
            .ok()
            .map(|status| self.status_with_queue(status.clone()))
    }

    fn status_with_queue(&self, mut status: WalWriterStatus) -> WalWriterStatus {
        status.queue_capacity = self.tx.max_capacity();
        status.queue_depth = status.queue_capacity.saturating_sub(self.tx.capacity());
        status
    }
}

struct WalOpenFile {
    path: PathBuf,
    file: Option<File>,
}

struct WalLocalWriteReport {
    bytes: usize,
    write_ms: u64,
    sync_ms: u64,
}

#[allow(clippy::too_many_arguments)]
async fn wal_worker(
    path: PathBuf,
    shard: usize,
    replicas: Vec<WalReplica>,
    mut remote_replicas: Vec<WalRemoteReplica>,
    mut remote_ack_policy: WalRemoteAckPolicy,
    config: WalWriterConfig,
    status: Arc<RwLock<WalWriterStatus>>,
    mut rx: mpsc::Receiver<WalCommand>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open WAL at {}", path.display()))?;
    let mut file = Some(file);
    let mut replica_files = Vec::with_capacity(replicas.len());
    for replica in replicas {
        if let Some(parent) = replica.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&replica.path)
            .await
            .with_context(|| format!("open replica WAL at {}", replica.path.display()))?;
        replica_files.push(WalOpenFile {
            path: replica.path,
            file: Some(file),
        });
    }
    let http = reqwest::Client::new();

    while let Some(first) = rx.recv().await {
        match first {
            WalCommand::Compact(command) => {
                handle_compact(command, &path, &mut file, &mut replica_files).await;
            }
            WalCommand::Replicate(command) => {
                handle_replicate(command, &mut file, &mut replica_files).await;
            }
            WalCommand::RepairRemote(command) => {
                handle_repair_remote(
                    command,
                    shard,
                    &path,
                    remote_replicas.as_slice(),
                    remote_ack_policy,
                    &status,
                    &http,
                    &mut file,
                )
                .await;
            }
            WalCommand::ConfigureRemote(command) => {
                handle_configure_remote(
                    command,
                    &mut remote_replicas,
                    &mut remote_ack_policy,
                    &status,
                )
                .await;
            }
            WalCommand::SealChecksums(command) => {
                handle_seal_checksums(command, &path, &mut file, &mut replica_files).await;
            }
            WalCommand::AppendBatch(mut batch) => {
                let mut commands = Vec::with_capacity(batch.len().max(config.batch_max));
                commands.append(&mut batch);
                let mut deferred_compact = None;
                let mut deferred_replicate = None;
                let mut deferred_repair_remote = None;
                let mut deferred_configure_remote = None;
                let mut deferred_seal_checksums = None;

                if !config.batch_wait.is_zero() {
                    let batch_deadline = time::sleep(config.batch_wait);
                    tokio::pin!(batch_deadline);

                    loop {
                        tokio::select! {
                            _ = &mut batch_deadline => break,
                            maybe_command = rx.recv(), if commands.len() < config.batch_max => {
                                match maybe_command {
                                    Some(WalCommand::AppendBatch(mut batch)) => commands.append(&mut batch),
                                    Some(WalCommand::Compact(command)) => {
                                        deferred_compact = Some(command);
                                        break;
                                    }
                                    Some(WalCommand::Replicate(command)) => {
                                        deferred_replicate = Some(command);
                                        break;
                                    }
                                    Some(WalCommand::RepairRemote(command)) => {
                                        deferred_repair_remote = Some(command);
                                        break;
                                    }
                                    Some(WalCommand::ConfigureRemote(command)) => {
                                        deferred_configure_remote = Some(command);
                                        break;
                                    }
                                    Some(WalCommand::SealChecksums(command)) => {
                                        deferred_seal_checksums = Some(command);
                                        break;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                }

                handle_appends(
                    commands,
                    shard,
                    remote_replicas.as_slice(),
                    remote_ack_policy,
                    &status,
                    &http,
                    &mut file,
                    &mut replica_files,
                )
                .await;
                if let Some(command) = deferred_replicate {
                    handle_replicate(command, &mut file, &mut replica_files).await;
                }
                if let Some(command) = deferred_repair_remote {
                    handle_repair_remote(
                        command,
                        shard,
                        &path,
                        remote_replicas.as_slice(),
                        remote_ack_policy,
                        &status,
                        &http,
                        &mut file,
                    )
                    .await;
                }
                if let Some(command) = deferred_configure_remote {
                    handle_configure_remote(
                        command,
                        &mut remote_replicas,
                        &mut remote_ack_policy,
                        &status,
                    )
                    .await;
                }
                if let Some(command) = deferred_compact {
                    handle_compact(command, &path, &mut file, &mut replica_files).await;
                }
                if let Some(command) = deferred_seal_checksums {
                    handle_seal_checksums(command, &path, &mut file, &mut replica_files).await;
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_appends(
    mut commands: Vec<WalAppendCommand>,
    shard: usize,
    remote_replicas: &[WalRemoteReplica],
    remote_ack_policy: WalRemoteAckPolicy,
    status: &Arc<RwLock<WalWriterStatus>>,
    http: &reqwest::Client,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) {
    if !wal_append_commands_are_sorted_by_lsn(&commands) {
        commands.sort_by_key(|command| command.lsn);
    }
    let record_count = commands.len();
    let mut records = Vec::with_capacity(record_count);
    let mut acks: Vec<oneshot::Sender<Result<WalRecord, String>>> =
        Vec::with_capacity(record_count);
    let mut strict = false;

    let mut commands = commands.into_iter();
    while let Some(command) = commands.next() {
        strict |= command.durability == Durability::Strict;
        let ack = command.ack;
        let record = WalRecord {
            lsn: command.lsn,
            shard,
            shard_epoch: command.shard_epoch,
            owner_node_id: command.owner_node_id,
            timestamp_ms: now_ms(),
            schema_version: command.schema_version,
            durability: command.durability,
            payload: command.payload,
            checksum: None,
        };
        let mut record = record;
        if let Err(err) = record.refresh_checksum() {
            let message = err.to_string();
            let _ = ack.send(Err(message.clone()));
            for ack in acks {
                let _ = ack.send(Err(message.clone()));
            }
            for command in commands {
                let _ = command.ack.send(Err(message.clone()));
            }
            return;
        }
        records.push(record);
        acks.push(ack);
    }

    let strict_prefix_end = strict_prefix_end(&records);
    if let Some(end) = strict_prefix_end {
        if let Err(err) = append_wal_segment(
            &records[..end],
            true,
            shard,
            remote_replicas,
            remote_ack_policy,
            status,
            http,
            file,
            replicas,
        )
        .await
        {
            fail_wal_acks(acks, err);
            return;
        }

        let acknowledged_records: Vec<_> = records.drain(..end).collect();
        let acknowledged_acks: Vec<_> = acks.drain(..end).collect();
        succeed_wal_acks(acknowledged_acks, acknowledged_records);
    }

    if records.is_empty() {
        return;
    }

    let sync = strict_prefix_end.is_none() && strict;
    match append_wal_segment(
        &records,
        sync,
        shard,
        remote_replicas,
        remote_ack_policy,
        status,
        http,
        file,
        replicas,
    )
    .await
    {
        Ok(()) => succeed_wal_acks(acks, records),
        Err(err) => fail_wal_acks(acks, err),
    }
}

fn wal_append_commands_are_sorted_by_lsn(commands: &[WalAppendCommand]) -> bool {
    commands
        .windows(2)
        .all(|window| window[0].lsn <= window[1].lsn)
}

fn strict_prefix_end(records: &[WalRecord]) -> Option<usize> {
    records
        .iter()
        .rposition(|record| record.durability == Durability::Strict)
        .map(|index| index + 1)
}

#[allow(clippy::too_many_arguments)]
async fn append_wal_segment(
    records: &[WalRecord],
    sync: bool,
    shard: usize,
    remote_replicas: &[WalRemoteReplica],
    remote_ack_policy: WalRemoteAckPolicy,
    status: &Arc<RwLock<WalWriterStatus>>,
    http: &reqwest::Client,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let started_at_ms = now_ms();
    match write_records_to_open_files(file, replicas, records, sync).await {
        Ok(report) => {
            note_local_append_success(
                status,
                records.len(),
                report.bytes,
                sync,
                started_at_ms,
                report.write_ms,
                report.sync_ms,
            )
            .await;
        }
        Err(err) => {
            note_local_append_failure(status).await;
            return Err(err);
        }
    }
    replicate_remote_records(
        http,
        remote_replicas,
        remote_ack_policy,
        status,
        shard,
        records,
        sync,
    )
    .await
}

fn succeed_wal_acks(
    acks: Vec<oneshot::Sender<Result<WalRecord, String>>>,
    records: Vec<WalRecord>,
) {
    for (ack, record) in acks.into_iter().zip(records) {
        let _ = ack.send(Ok(record));
    }
}

fn fail_wal_acks(acks: Vec<oneshot::Sender<Result<WalRecord, String>>>, err: anyhow::Error) {
    let message = err.to_string();
    for ack in acks {
        let _ = ack.send(Err(message.clone()));
    }
}

async fn handle_configure_remote(
    command: WalConfigureRemoteCommand,
    remote_replicas: &mut Vec<WalRemoteReplica>,
    remote_ack_policy: &mut WalRemoteAckPolicy,
    status: &Arc<RwLock<WalWriterStatus>>,
) {
    *remote_replicas = command.replicas;
    *remote_ack_policy = command.remote_ack_policy;
    reset_remote_status(status, remote_replicas, *remote_ack_policy).await;
    let _ = command.ack.send(Ok(()));
}

async fn handle_replicate(
    command: WalReplicateCommand,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) {
    let count = command.records.len();
    let result = write_records_to_open_files(file, replicas, &command.records, command.sync)
        .await
        .map(|_| count)
        .map_err(|err| err.to_string());
    let _ = command.ack.send(result);
}

#[allow(clippy::too_many_arguments)]
async fn handle_repair_remote(
    command: WalRepairRemoteCommand,
    shard: usize,
    path: &Path,
    remote_replicas: &[WalRemoteReplica],
    remote_ack_policy: WalRemoteAckPolicy,
    status: &Arc<RwLock<WalWriterStatus>>,
    http: &reqwest::Client,
    file: &mut Option<File>,
) {
    let result = repair_remote_records(
        command.after_lsn,
        command.sync,
        shard,
        path,
        remote_replicas,
        remote_ack_policy,
        status,
        http,
        file,
    )
    .await
    .map_err(|err| err.to_string());
    let _ = command.ack.send(result);
}

async fn write_records_to_open_files(
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
    records: &[WalRecord],
    sync: bool,
) -> Result<WalLocalWriteReport> {
    let Some(file) = file.as_mut() else {
        anyhow::bail!("WAL file is not open");
    };

    let encoded = encode_wal_records(records)?;
    let bytes = encoded.len();

    let write_started = Instant::now();
    file.write_all(&encoded).await?;
    for replica in replicas.iter_mut() {
        let Some(replica_file) = replica.file.as_mut() else {
            anyhow::bail!("replica WAL file is not open: {}", replica.path.display());
        };
        replica_file
            .write_all(&encoded)
            .await
            .with_context(|| format!("write replica WAL at {}", replica.path.display()))?;
    }
    let write_ms = duration_ms(write_started.elapsed());
    let mut sync_ms = 0;
    if sync {
        let sync_started = Instant::now();
        file.sync_data().await?;
        for replica in replicas.iter_mut() {
            let Some(replica_file) = replica.file.as_mut() else {
                anyhow::bail!("replica WAL file is not open: {}", replica.path.display());
            };
            replica_file
                .sync_data()
                .await
                .with_context(|| format!("sync replica WAL at {}", replica.path.display()))?;
        }
        sync_ms = duration_ms(sync_started.elapsed());
    }

    Ok(WalLocalWriteReport {
        bytes,
        write_ms,
        sync_ms,
    })
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[allow(clippy::too_many_arguments)]
async fn repair_remote_records(
    after_lsn: Option<u64>,
    sync: bool,
    shard: usize,
    path: &Path,
    remote_replicas: &[WalRemoteReplica],
    remote_ack_policy: WalRemoteAckPolicy,
    status: &Arc<RwLock<WalWriterStatus>>,
    http: &reqwest::Client,
    file: &mut Option<File>,
) -> Result<WalRemoteRepairReport> {
    if let Some(file) = file.as_mut() {
        file.sync_data().await?;
    }

    let records = read_records_including_archives(path)?;
    let highest_lsn = records.iter().map(|record| record.lsn).max().unwrap_or(0);
    let required_acks = required_remote_acks(remote_ack_policy, remote_replicas.len());
    let starting_status = status.read().await.remote_replicas.clone();
    let mut repaired_replicas = 0_usize;
    let mut records_sent = 0_usize;
    let mut replicas = Vec::with_capacity(remote_replicas.len());

    for (index, replica) in remote_replicas.iter().enumerate() {
        let endpoint = remote_replication_endpoint(&replica.url);
        let before_acked_lsn = starting_status
            .get(index)
            .filter(|status| status.url == endpoint)
            .map(|status| status.highest_acked_lsn)
            .unwrap_or(0);
        let start_lsn = after_lsn.unwrap_or(before_acked_lsn);
        let missing_records: Vec<_> = records
            .iter()
            .filter(|record| record.shard == shard && record.lsn > start_lsn)
            .cloned()
            .collect();

        if missing_records.is_empty() {
            repaired_replicas += 1;
            replicas.push(WalRemoteRepairReplicaReport {
                url: endpoint,
                ok: true,
                before_acked_lsn,
                after_acked_lsn: before_acked_lsn,
                sent: 0,
                error: None,
            });
            continue;
        }

        note_remote_attempt(status, index, &endpoint).await;
        let request = WalRemoteReplicateRequest {
            shard,
            records: &missing_records,
            sync,
        };
        let mut builder = http.post(&endpoint).json(&request);
        if let Some(token) = &replica.token {
            builder = builder
                .bearer_auth(token)
                .header("x-nextdb-replication-token", token);
        }

        match replicate_one_remote(builder, &endpoint, missing_records.len()).await {
            Ok(()) => {
                repaired_replicas += 1;
                records_sent += missing_records.len();
                let after_acked_lsn = missing_records
                    .last()
                    .map(|record| record.lsn)
                    .unwrap_or(before_acked_lsn);
                note_remote_success(status, index, &endpoint, after_acked_lsn).await;
                replicas.push(WalRemoteRepairReplicaReport {
                    url: endpoint,
                    ok: true,
                    before_acked_lsn,
                    after_acked_lsn,
                    sent: missing_records.len(),
                    error: None,
                });
            }
            Err(err) => {
                let message = err.to_string();
                note_remote_failure(status, index, &endpoint, &message).await;
                replicas.push(WalRemoteRepairReplicaReport {
                    url: endpoint,
                    ok: false,
                    before_acked_lsn,
                    after_acked_lsn: before_acked_lsn,
                    sent: 0,
                    error: Some(message),
                });
            }
        }
    }

    Ok(WalRemoteRepairReport {
        shard,
        remote_ack_policy,
        remote_required_acks: required_acks,
        remote_replica_count: remote_replicas.len(),
        repaired_replicas,
        records_sent,
        highest_lsn,
        satisfied: repaired_replicas >= required_acks,
        replicas,
    })
}

async fn reset_remote_status(
    status: &Arc<RwLock<WalWriterStatus>>,
    replicas: &[WalRemoteReplica],
    remote_ack_policy: WalRemoteAckPolicy,
) {
    let mut status = status.write().await;
    let previous: BTreeMap<_, _> = status
        .remote_replicas
        .iter()
        .map(|replica| (replica.url.clone(), replica.clone()))
        .collect();
    let remote_replicas: Vec<_> = replicas
        .iter()
        .map(|replica| {
            let endpoint = remote_replication_endpoint(&replica.url);
            previous
                .get(&endpoint)
                .cloned()
                .unwrap_or(WalRemoteReplicaStatus {
                    url: endpoint,
                    ok: true,
                    highest_acked_lsn: 0,
                    last_attempt_ms: None,
                    last_success_ms: None,
                    last_error_ms: None,
                    last_error: None,
                    acked_batches: 0,
                    failed_batches: 0,
                })
        })
        .collect();
    status.remote_ack_policy = remote_ack_policy;
    status.remote_required_acks = required_remote_acks(remote_ack_policy, remote_replicas.len());
    status.remote_replica_count = remote_replicas.len();
    status.remote_replicas = remote_replicas;
}

async fn note_local_append_success(
    status: &Arc<RwLock<WalWriterStatus>>,
    records: usize,
    bytes: usize,
    sync: bool,
    started_at_ms: u64,
    write_ms: u64,
    sync_ms: u64,
) {
    let mut status = status.write().await;
    status.local_batches += 1;
    status.local_records += records as u64;
    status.local_bytes += bytes as u64;
    status.local_syncs += u64::from(sync);
    status.local_total_write_ms = status.local_total_write_ms.saturating_add(write_ms);
    status.local_total_sync_ms = status.local_total_sync_ms.saturating_add(sync_ms);
    status.local_last_batch_records = records;
    status.local_last_batch_bytes = bytes;
    status.local_last_batch_sync = sync;
    status.local_last_batch_started_at_ms = Some(started_at_ms);
    status.local_last_batch_finished_at_ms = Some(now_ms());
    status.local_last_batch_write_ms = write_ms;
    status.local_last_batch_sync_ms = sync_ms;
}

async fn note_local_append_failure(status: &Arc<RwLock<WalWriterStatus>>) {
    status.write().await.local_failed_batches += 1;
}

async fn replicate_remote_records(
    http: &reqwest::Client,
    replicas: &[WalRemoteReplica],
    remote_ack_policy: WalRemoteAckPolicy,
    status: &Arc<RwLock<WalWriterStatus>>,
    shard: usize,
    records: &[WalRecord],
    sync: bool,
) -> Result<()> {
    if replicas.is_empty() || records.is_empty() {
        return Ok(());
    }

    let required_acks = required_remote_acks(remote_ack_policy, replicas.len());
    let highest_lsn = records.last().map(|record| record.lsn).unwrap_or(0);
    let record_count = records.len();
    let (result_tx, mut result_rx) = mpsc::channel(replicas.len().max(1));
    for (index, replica) in replicas.iter().enumerate() {
        let endpoint = remote_replication_endpoint(&replica.url);
        note_remote_attempt(status, index, &endpoint).await;
        let request = WalRemoteReplicateRequest {
            shard,
            records,
            sync,
        };
        let mut builder = http.post(&endpoint).json(&request);
        if let Some(token) = &replica.token {
            builder = builder
                .bearer_auth(token)
                .header("x-nextdb-replication-token", token);
        }
        let status = status.clone();
        let result_tx = result_tx.clone();
        tokio::spawn(async move {
            let result = replicate_one_remote(builder, &endpoint, record_count)
                .await
                .map_err(|err| err.to_string());
            match &result {
                Ok(()) => note_remote_success(&status, index, &endpoint, highest_lsn).await,
                Err(message) => note_remote_failure(&status, index, &endpoint, message).await,
            }
            let _ = result_tx.send((endpoint, result)).await;
        });
    }
    drop(result_tx);

    if required_acks == 0 {
        return Ok(());
    }

    let mut acked = 0_usize;
    let mut completed = 0_usize;
    let mut errors = Vec::new();
    while let Some((endpoint, result)) = result_rx.recv().await {
        completed += 1;
        match result {
            Ok(()) => {
                acked += 1;
                if acked >= required_acks {
                    return Ok(());
                }
            }
            Err(message) => {
                errors.push(format!("{endpoint}: {message}"));
            }
        }
        let remaining = replicas.len().saturating_sub(completed);
        if acked + remaining < required_acks {
            break;
        }
    }

    anyhow::bail!(
        "remote WAL ack policy {:?} requires {required_acks} acks, got {acked}: {}",
        remote_ack_policy,
        errors.join("; ")
    );
}

async fn replicate_one_remote(
    builder: reqwest::RequestBuilder,
    endpoint: &str,
    record_count: usize,
) -> Result<()> {
    let response = builder
        .send()
        .await
        .with_context(|| format!("replicate WAL to remote replica {endpoint}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("remote WAL replica rejected batch: {status} {body}");
    }
    let body: WalRemoteReplicateResponse = response
        .json()
        .await
        .with_context(|| format!("decode remote WAL replica response from {endpoint}"))?;
    if body.accepted > record_count {
        anyhow::bail!("remote WAL replica returned an invalid accepted count");
    }
    Ok(())
}

async fn note_remote_attempt(status: &Arc<RwLock<WalWriterStatus>>, index: usize, endpoint: &str) {
    let now = now_ms();
    let mut status = status.write().await;
    if let Some(replica) = status.remote_replicas.get_mut(index) {
        replica.url = endpoint.to_string();
        replica.last_attempt_ms = Some(now);
    }
}

async fn note_remote_success(
    status: &Arc<RwLock<WalWriterStatus>>,
    index: usize,
    endpoint: &str,
    highest_lsn: u64,
) {
    let now = now_ms();
    let mut status = status.write().await;
    if let Some(replica) = status.remote_replicas.get_mut(index) {
        replica.url = endpoint.to_string();
        replica.ok = true;
        replica.highest_acked_lsn = replica.highest_acked_lsn.max(highest_lsn);
        replica.last_success_ms = Some(now);
        replica.last_error = None;
        replica.acked_batches += 1;
    }
}

async fn note_remote_failure(
    status: &Arc<RwLock<WalWriterStatus>>,
    index: usize,
    endpoint: &str,
    error: &str,
) {
    let now = now_ms();
    let mut status = status.write().await;
    if let Some(replica) = status.remote_replicas.get_mut(index) {
        replica.url = endpoint.to_string();
        replica.ok = false;
        replica.last_error_ms = Some(now);
        replica.last_error = Some(error.to_string());
        replica.failed_batches += 1;
    }
}

pub fn required_remote_acks(policy: WalRemoteAckPolicy, replica_count: usize) -> usize {
    match policy {
        WalRemoteAckPolicy::All => replica_count,
        WalRemoteAckPolicy::None => 0,
        WalRemoteAckPolicy::Count(count) => count.min(replica_count),
        WalRemoteAckPolicy::Quorum => {
            let total_voters = replica_count + 1;
            let quorum = (total_voters / 2) + 1;
            quorum.saturating_sub(1).min(replica_count)
        }
    }
}

fn remote_replication_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/wal/replicate") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/wal/replicate")
    }
}

async fn handle_compact(
    command: WalCompactCommand,
    path: &Path,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) {
    let result =
        compact_with_replicas(path, command.upto_lsn, &command.archive_dir, file, replicas)
            .await
            .map_err(|err| err.to_string());
    let _ = command.ack.send(result);
}

async fn handle_seal_checksums(
    command: WalSealChecksumsCommand,
    path: &Path,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) {
    let result = seal_checksums_with_replicas(path, file, replicas)
        .await
        .map_err(|err| err.to_string());
    let _ = command.ack.send(result);
}

async fn seal_checksums_with_replicas(
    path: &Path,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) -> Result<WalChecksumSealReport> {
    let active = seal_checksum_file_with_open(path, file).await?;
    let mut report = WalChecksumSealReport {
        path: active.path,
        records: active.records,
        sealed: active.sealed,
        already_sealed: active.already_sealed,
        rewritten: active.rewritten,
        replicas: Vec::new(),
    };
    for replica in replicas.iter_mut() {
        report
            .replicas
            .push(seal_checksum_file_with_open(&replica.path, &mut replica.file).await?);
    }
    Ok(report)
}

async fn compact_with_replicas(
    path: &Path,
    upto_lsn: u64,
    archive_dir: &Path,
    file: &mut Option<File>,
    replicas: &mut [WalOpenFile],
) -> Result<WalCompactReport> {
    let mut report = compact_wal_file(path, upto_lsn, archive_dir, file).await?;
    for replica in replicas.iter_mut() {
        let archive_dir = replica
            .path
            .parent()
            .map(|parent| parent.join("archive"))
            .unwrap_or_else(|| PathBuf::from("archive"));
        let replica_report =
            compact_wal_file(&replica.path, upto_lsn, &archive_dir, &mut replica.file).await?;
        report.replicas.push(WalReplicaCompactReport {
            path: replica.path.display().to_string(),
            archived: replica_report.archived,
            retained: replica_report.retained,
            archive_path: replica_report.archive_path,
        });
    }
    Ok(report)
}

pub async fn seal_checksum_file(path: &Path) -> Result<WalChecksumSealFileReport> {
    let mut file = None;
    seal_checksum_file_with_open(path, &mut file).await
}

async fn seal_checksum_file_with_open(
    path: &Path,
    file: &mut Option<File>,
) -> Result<WalChecksumSealFileReport> {
    if let Some(open_file) = file.as_mut() {
        open_file.sync_data().await?;
    }

    let records = read_records(path)?;
    let sealed = records
        .iter()
        .filter(|record| record.checksum.is_none())
        .count();
    let already_sealed = records.len().saturating_sub(sealed);
    if sealed == 0 {
        return Ok(WalChecksumSealFileReport {
            path: path.display().to_string(),
            records: records.len(),
            sealed,
            already_sealed,
            rewritten: false,
        });
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("wal.jsonl");
    let temp_path = path.with_file_name(format!("{file_name}.seal.tmp"));
    write_records_file(&temp_path, &records).await?;

    let had_open_file = file.is_some();
    drop(file.take());
    fs::rename(&temp_path, path).await?;
    if had_open_file {
        let reopened = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("reopen sealed WAL at {}", path.display()))?;
        *file = Some(reopened);
    }

    Ok(WalChecksumSealFileReport {
        path: path.display().to_string(),
        records: records.len(),
        sealed,
        already_sealed,
        rewritten: true,
    })
}

async fn compact_wal_file(
    path: &Path,
    upto_lsn: u64,
    archive_dir: &Path,
    file: &mut Option<File>,
) -> Result<WalCompactReport> {
    if let Some(open_file) = file.as_mut() {
        open_file.sync_data().await?;
    }

    let records = read_records(path)?;
    let archived_records: Vec<_> = records
        .iter()
        .filter(|record| record.lsn <= upto_lsn)
        .cloned()
        .collect();
    let retained_records: Vec<_> = records
        .into_iter()
        .filter(|record| record.lsn > upto_lsn)
        .collect();

    if archived_records.is_empty() {
        return Ok(WalCompactReport {
            upto_lsn,
            archived: 0,
            retained: retained_records.len(),
            archive_path: None,
            replicas: Vec::new(),
        });
    }

    fs::create_dir_all(archive_dir).await?;
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("wal");
    let archive_path = archive_dir.join(format!("{stem}-through-{upto_lsn}-{}.jsonl", now_ms()));
    write_records_file(&archive_path, &archived_records).await?;

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("wal.jsonl");
    let temp_path = path.with_file_name(format!("{file_name}.compact.tmp"));
    write_records_file(&temp_path, &retained_records).await?;

    drop(file.take());
    fs::rename(&temp_path, path).await?;
    let reopened = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("reopen compacted WAL at {}", path.display()))?;
    *file = Some(reopened);

    Ok(WalCompactReport {
        upto_lsn,
        archived: archived_records.len(),
        retained: retained_records.len(),
        archive_path: Some(archive_path.display().to_string()),
        replicas: Vec::new(),
    })
}

async fn write_records_file(path: &Path, records: &[WalRecord]) -> Result<()> {
    let mut file = File::create(path)
        .await
        .with_context(|| format!("create WAL records file {}", path.display()))?;
    let encoded = encode_wal_records(records)?;
    file.write_all(&encoded).await?;
    file.sync_all().await?;
    Ok(())
}

fn encode_wal_records(records: &[WalRecord]) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for record in records {
        if record.checksum.is_some() {
            encode_wal_record_frame(record, &mut encoded)?;
        } else {
            let mut signed = record.clone();
            signed.ensure_checksum()?;
            encode_wal_record_frame(&signed, &mut encoded)?;
        }
    }
    Ok(encoded)
}

fn encode_wal_record_frame(record: &WalRecord, encoded: &mut Vec<u8>) -> Result<()> {
    let payload = postcard::to_allocvec(&PostcardWalRecordFrame::from_record(record)?)?;
    if payload.len() > WAL_FRAME_MAX_BYTES as usize {
        anyhow::bail!(
            "WAL frame for LSN {} exceeds max size: {} bytes > {} bytes",
            record.lsn,
            payload.len(),
            WAL_FRAME_MAX_BYTES
        );
    }
    encoded.extend_from_slice(&WAL_FRAME_MAGIC);
    encoded.extend_from_slice(&WAL_FRAME_VERSION.to_be_bytes());
    encoded.extend_from_slice(&WAL_FRAME_ENCODING.to_be_bytes());
    encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&crc32c::crc32c(&payload).to_be_bytes());
    encoded.extend_from_slice(&payload);
    Ok(())
}

fn wal_frame_header_len(version: u16) -> Result<usize> {
    match version {
        WAL_FRAME_VERSION_V1 => Ok(WAL_FRAME_HEADER_LEN_V1),
        WAL_FRAME_VERSION_V2 => Ok(WAL_FRAME_HEADER_LEN_V2),
        _ => anyhow::bail!("unsupported WAL frame version {version}"),
    }
}

fn wal_frame_len(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset + 8],
        bytes[offset + 9],
        bytes[offset + 10],
        bytes[offset + 11],
    ])
}

fn verify_wal_frame_crc32c(
    bytes: &[u8],
    offset: usize,
    version: u16,
    payload: &[u8],
) -> Result<()> {
    if version != WAL_FRAME_VERSION_V2 {
        return Ok(());
    }
    let expected = u32::from_be_bytes([
        bytes[offset + 12],
        bytes[offset + 13],
        bytes[offset + 14],
        bytes[offset + 15],
    ]);
    let actual = crc32c::crc32c(payload);
    if actual != expected {
        anyhow::bail!("WAL frame CRC32C mismatch: expected {expected:08x}, found {actual:08x}");
    }
    Ok(())
}

fn decode_wal_frame_payload(encoding: u16, payload: &[u8], frame: usize) -> Result<WalRecord> {
    match encoding {
        WAL_FRAME_ENCODING_JSON => {
            serde_json::from_slice(payload).with_context(|| format!("parse JSON WAL frame {frame}"))
        }
        WAL_FRAME_ENCODING_POSTCARD => postcard::from_bytes::<PostcardWalRecordFrame>(payload)
            .with_context(|| format!("parse postcard WAL frame {frame}"))?
            .into_record()
            .with_context(|| format!("decode postcard WAL frame {frame}")),
        _ => anyhow::bail!("unsupported WAL frame encoding {encoding} at frame {frame}"),
    }
}

pub struct ReplayState {
    pub rooms: HashMap<String, RoomLiveState>,
    pub highest_lsn: u64,
    pub scanned_records: usize,
    pub records_after_snapshot: usize,
    pub quarantined_wal: Option<WalQuarantineReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalQuarantineReport {
    pub path: String,
    pub frame: usize,
    pub offset: usize,
    pub reason: String,
}

struct RecoverableWalRecords {
    records: Vec<WalRecord>,
    quarantined: Option<WalQuarantineReport>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PostcardWalRecordFrame {
    lsn: u64,
    shard: u64,
    shard_epoch: u64,
    owner_node_id: String,
    timestamp_ms: u64,
    schema_version: u32,
    durability: Durability,
    payload: PostcardWalPayload,
    checksum: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
enum PostcardWalPayload {
    MessageCreated {
        message: BinaryMessageDraft,
    },
    UserEventPublished {
        event: BinaryUserEventDraft,
    },
    UserUpserted {
        user: BinaryUserProfileDraft,
    },
    ObjectCommitted {
        object: ObjectMetadata,
        client_mutation_id: Option<String>,
    },
    ObjectDeleted {
        object_id: String,
        deleted_at_ms: u64,
        path: String,
        force: bool,
        client_mutation_id: Option<String>,
    },
    RecordUpserted {
        record: BinaryDbRecordDraft,
    },
    RecordDeleted {
        record: BinaryDbRecordDeleteDraft,
    },
    RecordTransactionCommitted {
        operations: Vec<BinaryDbRecordMutationDraft>,
        client_mutation_id: Option<String>,
    },
    SchemaApplied {
        schema_json: Vec<u8>,
        migration_json: Vec<u8>,
    },
    BehaviorPublished {
        publish: crate::model::BehaviorPublishedDraft,
    },
    ActorReminderScheduled {
        reminder: BinaryActorReminderDraft,
    },
    ActorReminderCancelled {
        actor_kind: String,
        actor_key: String,
        reminder_id: String,
        cancelled_at_ms: u64,
    },
    ActorReminderFired {
        actor_kind: String,
        actor_key: String,
        reminder_id: String,
        due_at_ms: u64,
        fired_at_ms: u64,
    },
    HostHttpRequested {
        request: BinaryHostHttpRequestDraft,
    },
    HostHttpCompleted {
        request_id: String,
        completed_at_ms: u64,
    },
    ClientMutationRecorded {
        client_mutation_id: String,
        record: BinaryClientMutationRecord,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryMessageDraft {
    id: String,
    client_mutation_id: Option<String>,
    room_id: String,
    sender_id: String,
    body: String,
    attachments: Vec<ObjectRef>,
    created_at_ms: u64,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryUserEventDraft {
    id: String,
    client_mutation_id: Option<String>,
    user_id: String,
    name: String,
    payload: BinaryJsonValue,
    created_at_ms: u64,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryUserProfileDraft {
    user_id: String,
    client_mutation_id: Option<String>,
    display_name: Option<String>,
    metadata: BinaryJsonValue,
    created_at_ms: u64,
    updated_at_ms: u64,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryActorReminderDraft {
    actor_kind: String,
    actor_key: String,
    reminder_id: String,
    due_at_ms: u64,
    payload: Option<BinaryJsonValue>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryHostHttpRequestDraft {
    request_id: String,
    method: String,
    url: String,
    headers: BTreeMap<String, String>,
    body: Option<BinaryJsonValue>,
    body_base64: Option<String>,
    timeout_ms: u64,
    actor_kind: String,
    actor_key: String,
    reminder_id: String,
    continuation: BinaryJsonValue,
    requested_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryDbRecordDraft {
    table: String,
    key: String,
    value: BinaryJsonValue,
    updated_at_ms: u64,
    path: String,
    client_mutation_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryDbRecordDeleteDraft {
    table: String,
    key: String,
    deleted_at_ms: u64,
    path: String,
    client_mutation_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
enum BinaryDbRecordMutationDraft {
    Upsert { record: BinaryDbRecordDraft },
    Delete { record: BinaryDbRecordDeleteDraft },
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize)]
enum BinaryClientMutationRecord {
    RecordDeleteNoop {
        table: String,
        key: String,
        path: String,
    },
    RecordTransactionNoop,
    ObjectDeleteNoop {
        object_id: String,
        path: String,
    },
}

impl PostcardWalRecordFrame {
    fn from_record(record: &WalRecord) -> Result<Self> {
        Ok(Self {
            lsn: record.lsn,
            shard: record.shard as u64,
            shard_epoch: record.shard_epoch,
            owner_node_id: record.owner_node_id.clone(),
            timestamp_ms: record.timestamp_ms,
            schema_version: record.schema_version,
            durability: record.durability,
            payload: PostcardWalPayload::from_payload(&record.payload)?,
            checksum: record.checksum.clone(),
        })
    }

    fn into_record(self) -> Result<WalRecord> {
        Ok(WalRecord {
            lsn: self.lsn,
            shard: usize::try_from(self.shard)
                .context("WAL postcard shard index overflows usize")?,
            shard_epoch: self.shard_epoch,
            owner_node_id: self.owner_node_id,
            timestamp_ms: self.timestamp_ms,
            schema_version: self.schema_version,
            durability: self.durability,
            payload: self.payload.into_payload().context("decode WAL payload")?,
            checksum: self.checksum,
        })
    }
}

impl PostcardWalPayload {
    fn from_payload(payload: &WalPayload) -> Result<Self> {
        Ok(match payload {
            WalPayload::MessageCreated { message } => Self::MessageCreated {
                message: BinaryMessageDraft::from_draft(message),
            },
            WalPayload::UserEventPublished { event } => Self::UserEventPublished {
                event: BinaryUserEventDraft::from_draft(event)?,
            },
            WalPayload::UserUpserted { user } => Self::UserUpserted {
                user: BinaryUserProfileDraft::from_draft(user)?,
            },
            WalPayload::ObjectCommitted {
                object,
                client_mutation_id,
            } => Self::ObjectCommitted {
                object: object.clone(),
                client_mutation_id: client_mutation_id.clone(),
            },
            WalPayload::ObjectDeleted {
                object_id,
                deleted_at_ms,
                path,
                force,
                client_mutation_id,
            } => Self::ObjectDeleted {
                object_id: object_id.clone(),
                deleted_at_ms: *deleted_at_ms,
                path: path.clone(),
                force: *force,
                client_mutation_id: client_mutation_id.clone(),
            },
            WalPayload::RecordUpserted { record } => Self::RecordUpserted {
                record: BinaryDbRecordDraft::from_draft(record)?,
            },
            WalPayload::RecordDeleted { record } => Self::RecordDeleted {
                record: BinaryDbRecordDeleteDraft::from_draft(record),
            },
            WalPayload::RecordTransactionCommitted {
                operations,
                client_mutation_id,
            } => Self::RecordTransactionCommitted {
                operations: operations
                    .iter()
                    .map(BinaryDbRecordMutationDraft::from_draft)
                    .collect::<Result<Vec<_>>>()?,
                client_mutation_id: client_mutation_id.clone(),
            },
            WalPayload::SchemaApplied { schema, migration } => Self::SchemaApplied {
                schema_json: serde_json::to_vec(schema)
                    .context("encode schema WAL postcard JSON")?,
                migration_json: serde_json::to_vec(migration)
                    .context("encode schema migration WAL postcard JSON")?,
            },
            WalPayload::BehaviorPublished { publish } => Self::BehaviorPublished {
                publish: publish.clone(),
            },
            WalPayload::ActorReminderScheduled { reminder } => Self::ActorReminderScheduled {
                reminder: BinaryActorReminderDraft::from_draft(reminder)?,
            },
            WalPayload::ActorReminderCancelled {
                actor_kind,
                actor_key,
                reminder_id,
                cancelled_at_ms,
            } => Self::ActorReminderCancelled {
                actor_kind: actor_kind.clone(),
                actor_key: actor_key.clone(),
                reminder_id: reminder_id.clone(),
                cancelled_at_ms: *cancelled_at_ms,
            },
            WalPayload::ActorReminderFired {
                actor_kind,
                actor_key,
                reminder_id,
                due_at_ms,
                fired_at_ms,
            } => Self::ActorReminderFired {
                actor_kind: actor_kind.clone(),
                actor_key: actor_key.clone(),
                reminder_id: reminder_id.clone(),
                due_at_ms: *due_at_ms,
                fired_at_ms: *fired_at_ms,
            },
            WalPayload::HostHttpRequested { request } => Self::HostHttpRequested {
                request: BinaryHostHttpRequestDraft::from_draft(request)?,
            },
            WalPayload::HostHttpCompleted {
                request_id,
                completed_at_ms,
            } => Self::HostHttpCompleted {
                request_id: request_id.clone(),
                completed_at_ms: *completed_at_ms,
            },
            WalPayload::ClientMutationRecorded {
                client_mutation_id,
                record,
            } => Self::ClientMutationRecorded {
                client_mutation_id: client_mutation_id.clone(),
                record: BinaryClientMutationRecord::from_record(record),
            },
        })
    }

    fn into_payload(self) -> Result<WalPayload> {
        Ok(match self {
            Self::MessageCreated { message } => WalPayload::MessageCreated {
                message: message.into_draft(),
            },
            Self::UserEventPublished { event } => WalPayload::UserEventPublished {
                event: event.into_draft()?,
            },
            Self::UserUpserted { user } => WalPayload::UserUpserted {
                user: user.into_draft()?,
            },
            Self::ObjectCommitted {
                object,
                client_mutation_id,
            } => WalPayload::ObjectCommitted {
                object,
                client_mutation_id,
            },
            Self::ObjectDeleted {
                object_id,
                deleted_at_ms,
                path,
                force,
                client_mutation_id,
            } => WalPayload::ObjectDeleted {
                object_id,
                deleted_at_ms,
                path,
                force,
                client_mutation_id,
            },
            Self::RecordUpserted { record } => WalPayload::RecordUpserted {
                record: record.into_draft()?,
            },
            Self::RecordDeleted { record } => WalPayload::RecordDeleted {
                record: record.into_draft(),
            },
            Self::RecordTransactionCommitted {
                operations,
                client_mutation_id,
            } => WalPayload::RecordTransactionCommitted {
                operations: operations
                    .into_iter()
                    .map(BinaryDbRecordMutationDraft::into_draft)
                    .collect::<Result<Vec<_>>>()?,
                client_mutation_id,
            },
            Self::SchemaApplied {
                schema_json,
                migration_json,
            } => WalPayload::SchemaApplied {
                schema: serde_json::from_slice(&schema_json)
                    .context("decode schema WAL postcard JSON")?,
                migration: serde_json::from_slice(&migration_json)
                    .context("decode schema migration WAL postcard JSON")?,
            },
            Self::BehaviorPublished { publish } => WalPayload::BehaviorPublished { publish },
            Self::ActorReminderScheduled { reminder } => WalPayload::ActorReminderScheduled {
                reminder: reminder.into_draft()?,
            },
            Self::ActorReminderCancelled {
                actor_kind,
                actor_key,
                reminder_id,
                cancelled_at_ms,
            } => WalPayload::ActorReminderCancelled {
                actor_kind,
                actor_key,
                reminder_id,
                cancelled_at_ms,
            },
            Self::ActorReminderFired {
                actor_kind,
                actor_key,
                reminder_id,
                due_at_ms,
                fired_at_ms,
            } => WalPayload::ActorReminderFired {
                actor_kind,
                actor_key,
                reminder_id,
                due_at_ms,
                fired_at_ms,
            },
            Self::HostHttpRequested { request } => WalPayload::HostHttpRequested {
                request: request.into_draft()?,
            },
            Self::HostHttpCompleted {
                request_id,
                completed_at_ms,
            } => WalPayload::HostHttpCompleted {
                request_id,
                completed_at_ms,
            },
            Self::ClientMutationRecorded {
                client_mutation_id,
                record,
            } => WalPayload::ClientMutationRecorded {
                client_mutation_id,
                record: record.into_record(),
            },
        })
    }
}

impl BinaryMessageDraft {
    fn from_draft(draft: &MessageDraft) -> Self {
        Self {
            id: draft.id.clone(),
            client_mutation_id: draft.client_mutation_id.clone(),
            room_id: draft.room_id.clone(),
            sender_id: draft.sender_id.clone(),
            body: draft.body.clone(),
            attachments: draft.attachments.clone(),
            created_at_ms: draft.created_at_ms,
            path: draft.path.clone(),
        }
    }

    fn into_draft(self) -> MessageDraft {
        MessageDraft {
            id: self.id,
            client_mutation_id: self.client_mutation_id,
            room_id: self.room_id,
            sender_id: self.sender_id,
            body: self.body,
            attachments: self.attachments,
            created_at_ms: self.created_at_ms,
            path: self.path,
        }
    }
}

impl BinaryUserEventDraft {
    fn from_draft(draft: &UserEventDraft) -> Result<Self> {
        Ok(Self {
            id: draft.id.clone(),
            client_mutation_id: draft.client_mutation_id.clone(),
            user_id: draft.user_id.clone(),
            name: draft.name.clone(),
            payload: BinaryJsonValue::from_json(&draft.payload)?,
            created_at_ms: draft.created_at_ms,
            path: draft.path.clone(),
        })
    }

    fn into_draft(self) -> Result<UserEventDraft> {
        Ok(UserEventDraft {
            id: self.id,
            client_mutation_id: self.client_mutation_id,
            user_id: self.user_id,
            name: self.name,
            payload: self.payload.into_json()?,
            created_at_ms: self.created_at_ms,
            path: self.path,
        })
    }
}

impl BinaryUserProfileDraft {
    fn from_draft(draft: &UserProfileDraft) -> Result<Self> {
        Ok(Self {
            user_id: draft.user_id.clone(),
            client_mutation_id: draft.client_mutation_id.clone(),
            display_name: draft.display_name.clone(),
            metadata: BinaryJsonValue::from_json(&draft.metadata)?,
            created_at_ms: draft.created_at_ms,
            updated_at_ms: draft.updated_at_ms,
            path: draft.path.clone(),
        })
    }

    fn into_draft(self) -> Result<UserProfileDraft> {
        Ok(UserProfileDraft {
            user_id: self.user_id,
            client_mutation_id: self.client_mutation_id,
            display_name: self.display_name,
            metadata: self.metadata.into_json()?,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            path: self.path,
        })
    }
}

impl BinaryActorReminderDraft {
    fn from_draft(draft: &ActorReminderDraft) -> Result<Self> {
        Ok(Self {
            actor_kind: draft.actor_kind.clone(),
            actor_key: draft.actor_key.clone(),
            reminder_id: draft.reminder_id.clone(),
            due_at_ms: draft.due_at_ms,
            payload: draft
                .payload
                .as_ref()
                .map(BinaryJsonValue::from_json)
                .transpose()?,
        })
    }

    fn into_draft(self) -> Result<ActorReminderDraft> {
        Ok(ActorReminderDraft {
            actor_kind: self.actor_kind,
            actor_key: self.actor_key,
            reminder_id: self.reminder_id,
            due_at_ms: self.due_at_ms,
            payload: self.payload.map(BinaryJsonValue::into_json).transpose()?,
        })
    }
}

impl BinaryHostHttpRequestDraft {
    fn from_draft(draft: &crate::model::HostHttpRequestDraft) -> Result<Self> {
        Ok(Self {
            request_id: draft.request_id.clone(),
            method: draft.method.clone(),
            url: draft.url.clone(),
            headers: draft.headers.clone(),
            body: draft
                .body
                .as_ref()
                .map(BinaryJsonValue::from_json)
                .transpose()?,
            body_base64: draft.body_base64.clone(),
            timeout_ms: draft.timeout_ms,
            actor_kind: draft.actor_kind.clone(),
            actor_key: draft.actor_key.clone(),
            reminder_id: draft.reminder_id.clone(),
            continuation: BinaryJsonValue::from_json(&draft.continuation)?,
            requested_at_ms: draft.requested_at_ms,
        })
    }

    fn into_draft(self) -> Result<crate::model::HostHttpRequestDraft> {
        Ok(crate::model::HostHttpRequestDraft {
            request_id: self.request_id,
            method: self.method,
            url: self.url,
            headers: self.headers,
            body: self.body.map(BinaryJsonValue::into_json).transpose()?,
            body_base64: self.body_base64,
            timeout_ms: self.timeout_ms,
            actor_kind: self.actor_kind,
            actor_key: self.actor_key,
            reminder_id: self.reminder_id,
            continuation: self.continuation.into_json()?,
            requested_at_ms: self.requested_at_ms,
        })
    }
}

impl BinaryDbRecordDraft {
    fn from_draft(draft: &DbRecordDraft) -> Result<Self> {
        Ok(Self {
            table: draft.table.clone(),
            key: draft.key.clone(),
            value: BinaryJsonValue::from_json(&draft.value)?,
            updated_at_ms: draft.updated_at_ms,
            path: draft.path.clone(),
            client_mutation_id: draft.client_mutation_id.clone(),
        })
    }

    fn into_draft(self) -> Result<DbRecordDraft> {
        Ok(DbRecordDraft {
            table: self.table,
            key: self.key,
            value: self.value.into_json()?,
            updated_at_ms: self.updated_at_ms,
            path: self.path,
            client_mutation_id: self.client_mutation_id,
        })
    }
}

impl BinaryDbRecordDeleteDraft {
    fn from_draft(draft: &DbRecordDeleteDraft) -> Self {
        Self {
            table: draft.table.clone(),
            key: draft.key.clone(),
            deleted_at_ms: draft.deleted_at_ms,
            path: draft.path.clone(),
            client_mutation_id: draft.client_mutation_id.clone(),
        }
    }

    fn into_draft(self) -> DbRecordDeleteDraft {
        DbRecordDeleteDraft {
            table: self.table,
            key: self.key,
            deleted_at_ms: self.deleted_at_ms,
            path: self.path,
            client_mutation_id: self.client_mutation_id,
        }
    }
}

impl BinaryDbRecordMutationDraft {
    fn from_draft(draft: &DbRecordMutationDraft) -> Result<Self> {
        Ok(match draft {
            DbRecordMutationDraft::Upsert { record } => Self::Upsert {
                record: BinaryDbRecordDraft::from_draft(record)?,
            },
            DbRecordMutationDraft::Delete { record } => Self::Delete {
                record: BinaryDbRecordDeleteDraft::from_draft(record),
            },
        })
    }

    fn into_draft(self) -> Result<DbRecordMutationDraft> {
        Ok(match self {
            Self::Upsert { record } => DbRecordMutationDraft::Upsert {
                record: record.into_draft()?,
            },
            Self::Delete { record } => DbRecordMutationDraft::Delete {
                record: record.into_draft(),
            },
        })
    }
}

impl BinaryClientMutationRecord {
    fn from_record(record: &ClientMutationRecord) -> Self {
        match record {
            ClientMutationRecord::RecordDeleteNoop { table, key, path } => Self::RecordDeleteNoop {
                table: table.clone(),
                key: key.clone(),
                path: path.clone(),
            },
            ClientMutationRecord::RecordTransactionNoop => Self::RecordTransactionNoop,
            ClientMutationRecord::ObjectDeleteNoop { object_id, path } => Self::ObjectDeleteNoop {
                object_id: object_id.clone(),
                path: path.clone(),
            },
        }
    }

    fn into_record(self) -> ClientMutationRecord {
        match self {
            Self::RecordDeleteNoop { table, key, path } => {
                ClientMutationRecord::RecordDeleteNoop { table, key, path }
            }
            Self::RecordTransactionNoop => ClientMutationRecord::RecordTransactionNoop,
            Self::ObjectDeleteNoop { object_id, path } => {
                ClientMutationRecord::ObjectDeleteNoop { object_id, path }
            }
        }
    }
}

pub fn replay_from(
    path: &Path,
    hot_window: usize,
    since_lsn: u64,
    mut rooms: HashMap<String, RoomLiveState>,
) -> Result<ReplayState> {
    if !path.exists() {
        return Ok(ReplayState {
            rooms,
            highest_lsn: since_lsn,
            scanned_records: 0,
            records_after_snapshot: 0,
            quarantined_wal: None,
        });
    }

    let recovered = read_recoverable_records_file(path)?;
    let quarantined_wal = recovered.quarantined;
    let mut highest_lsn = since_lsn;
    let mut scanned_records = 0;
    let mut records_after_snapshot = 0;

    for record in recovered.records {
        scanned_records += 1;
        highest_lsn = highest_lsn.max(record.lsn);
        if record.lsn <= since_lsn {
            continue;
        }
        records_after_snapshot += 1;
        match record.payload {
            WalPayload::MessageCreated { message } => {
                let message = message.into_message(record.lsn);
                rooms
                    .entry(message.room_id.clone())
                    .or_insert_with(RoomLiveState::new)
                    .apply_message(message.clone(), hot_window);
            }
            WalPayload::ObjectCommitted { .. }
            | WalPayload::ObjectDeleted { .. }
            | WalPayload::UserEventPublished { .. }
            | WalPayload::UserUpserted { .. }
            | WalPayload::RecordUpserted { .. }
            | WalPayload::RecordDeleted { .. }
            | WalPayload::RecordTransactionCommitted { .. }
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

    Ok(ReplayState {
        rooms,
        highest_lsn,
        scanned_records,
        records_after_snapshot,
        quarantined_wal,
    })
}

pub fn read_records(path: &Path) -> Result<Vec<WalRecord>> {
    read_records_file(path)
}

pub fn read_records_including_archives(path: &Path) -> Result<Vec<WalRecord>> {
    let mut records = BTreeMap::new();
    for record in read_archive_records(path)? {
        records.insert(record.lsn, record);
    }
    for record in read_records(path)? {
        records.insert(record.lsn, record);
    }
    Ok(records.into_values().collect())
}

pub fn read_records_after_lsn_including_archives(
    path: &Path,
    after_lsn: u64,
) -> Result<Vec<WalRecord>> {
    let mut records = BTreeMap::new();
    for record in read_archive_records_after_lsn(path, after_lsn)? {
        if record.lsn > after_lsn {
            records.insert(record.lsn, record);
        }
    }
    for record in read_records(path)? {
        if record.lsn > after_lsn {
            records.insert(record.lsn, record);
        }
    }
    Ok(records.into_values().collect())
}

fn read_archive_records(path: &Path) -> Result<Vec<WalRecord>> {
    let archive_dir = path
        .parent()
        .map(|parent| parent.join("archive"))
        .unwrap_or_else(|| PathBuf::from("archive"));
    if !archive_dir.exists() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in std::fs::read_dir(&archive_dir)
        .with_context(|| format!("read WAL archive dir {}", archive_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            paths.push(path);
        }
    }
    paths.sort();

    let mut records = Vec::new();
    for archive_path in paths {
        records.extend(read_records_file(&archive_path)?);
    }
    Ok(records)
}

fn read_archive_records_after_lsn(path: &Path, after_lsn: u64) -> Result<Vec<WalRecord>> {
    let archive_dir = path
        .parent()
        .map(|parent| parent.join("archive"))
        .unwrap_or_else(|| PathBuf::from("archive"));
    if !archive_dir.exists() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in std::fs::read_dir(&archive_dir)
        .with_context(|| format!("read WAL archive dir {}", archive_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        if archive_path_max_lsn(&path).is_some_and(|max_lsn| max_lsn <= after_lsn) {
            continue;
        }
        paths.push(path);
    }
    paths.sort();

    let mut records = Vec::new();
    for archive_path in paths {
        records.extend(read_records_file(&archive_path)?);
    }
    Ok(records)
}

fn archive_path_max_lsn(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let (_, after_through) = file_name.split_once("-through-")?;
    let max_lsn = after_through.split(['-', '.']).next()?;
    max_lsn.parse().ok()
}

pub fn read_records_file(path: &Path) -> Result<Vec<WalRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let bytes = std::fs::read(path).with_context(|| format!("read WAL at {}", path.display()))?;
    let mut records = Vec::new();

    for (index, record) in
        decode_wal_records(&bytes).with_context(|| format!("parse WAL file {}", path.display()))?
    {
        if let WalChecksumStatus::Mismatch { expected } = record.verify_checksum()? {
            anyhow::bail!(
                "WAL checksum mismatch at frame {}: expected {expected}, found {}",
                index,
                record.checksum.as_deref().unwrap_or_default()
            );
        }
        records.push(record);
    }

    Ok(records)
}

fn read_recoverable_records_file(path: &Path) -> Result<RecoverableWalRecords> {
    if !path.exists() {
        return Ok(RecoverableWalRecords {
            records: Vec::new(),
            quarantined: None,
        });
    }

    let bytes = std::fs::read(path).with_context(|| format!("read WAL at {}", path.display()))?;
    if bytes.is_empty() {
        return Ok(RecoverableWalRecords {
            records: Vec::new(),
            quarantined: None,
        });
    }
    if bytes.starts_with(&WAL_FRAME_MAGIC) {
        return Ok(decode_framed_wal_records_prefix(path, &bytes));
    }

    decode_jsonl_wal_records(&bytes)
        .with_context(|| format!("parse legacy WAL file {}", path.display()))
        .and_then(|decoded| {
            let mut records = Vec::with_capacity(decoded.len());
            for (index, record) in decoded {
                if let WalChecksumStatus::Mismatch { expected } = record.verify_checksum()? {
                    return Ok(RecoverableWalRecords {
                        records,
                        quarantined: Some(WalQuarantineReport {
                            path: path.display().to_string(),
                            frame: index,
                            offset: 0,
                            reason: format!(
                                "WAL checksum mismatch at legacy line {index}: expected {expected}, found {}",
                                record.checksum.as_deref().unwrap_or_default()
                            ),
                        }),
                    });
                }
                records.push(record);
            }
            Ok(RecoverableWalRecords {
                records,
                quarantined: None,
            })
        })
}

fn decode_framed_wal_records_prefix(path: &Path, bytes: &[u8]) -> RecoverableWalRecords {
    let mut records = Vec::new();
    let mut offset = 0_usize;
    let mut frame = 1_usize;
    while offset < bytes.len() {
        if bytes.len() - offset < WAL_FRAME_MIN_HEADER_LEN {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!("truncated WAL frame header at frame {frame}"),
            );
        }
        if bytes[offset..offset + 4] != WAL_FRAME_MAGIC {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!("invalid WAL frame magic at frame {frame}"),
            );
        }
        let version = u16::from_be_bytes([bytes[offset + 4], bytes[offset + 5]]);
        let header_len = match wal_frame_header_len(version) {
            Ok(header_len) => header_len,
            Err(_) => {
                return quarantine_wal_prefix(
                    path,
                    records,
                    frame,
                    offset,
                    format!("unsupported WAL frame version {version} at frame {frame}"),
                );
            }
        };
        if bytes.len() - offset < header_len {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!("truncated WAL frame header at frame {frame}"),
            );
        }
        let encoding = u16::from_be_bytes([bytes[offset + 6], bytes[offset + 7]]);
        let len = wal_frame_len(bytes, offset);
        if len == 0 {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!("empty WAL frame payload at frame {frame}"),
            );
        }
        if len > WAL_FRAME_MAX_BYTES {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!(
                    "WAL frame {frame} exceeds max size: {len} bytes > {WAL_FRAME_MAX_BYTES} bytes"
                ),
            );
        }
        let start = offset + header_len;
        let end = start + len as usize;
        if end > bytes.len() {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!("truncated WAL frame payload at frame {frame}"),
            );
        }
        if let Err(err) = verify_wal_frame_crc32c(bytes, offset, version, &bytes[start..end]) {
            return quarantine_wal_prefix(
                path,
                records,
                frame,
                offset,
                format!("failed WAL frame CRC32C at frame {frame}: {err}"),
            );
        }
        let record: WalRecord = match decode_wal_frame_payload(encoding, &bytes[start..end], frame)
        {
            Ok(record) => record,
            Err(err) => {
                return quarantine_wal_prefix(
                    path,
                    records,
                    frame,
                    offset,
                    format!("parse WAL frame {frame}: {err}"),
                );
            }
        };
        match record.verify_checksum() {
            Ok(WalChecksumStatus::Valid) | Ok(WalChecksumStatus::Missing) => {}
            Ok(WalChecksumStatus::Mismatch { expected }) => {
                return quarantine_wal_prefix(
                    path,
                    records,
                    frame,
                    offset,
                    format!(
                        "WAL checksum mismatch at frame {frame}: expected {expected}, found {}",
                        record.checksum.as_deref().unwrap_or_default()
                    ),
                );
            }
            Err(err) => {
                return quarantine_wal_prefix(
                    path,
                    records,
                    frame,
                    offset,
                    format!("failed to compute WAL checksum at frame {frame}: {err}"),
                );
            }
        }
        records.push(record);
        offset = end;
        frame += 1;
    }
    RecoverableWalRecords {
        records,
        quarantined: None,
    }
}

fn quarantine_wal_prefix(
    path: &Path,
    records: Vec<WalRecord>,
    frame: usize,
    offset: usize,
    reason: String,
) -> RecoverableWalRecords {
    RecoverableWalRecords {
        records,
        quarantined: Some(WalQuarantineReport {
            path: path.display().to_string(),
            frame,
            offset,
            reason,
        }),
    }
}

fn decode_wal_records(bytes: &[u8]) -> Result<Vec<(usize, WalRecord)>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.starts_with(&WAL_FRAME_MAGIC) {
        return decode_framed_wal_records(bytes);
    }
    decode_jsonl_wal_records(bytes)
}

fn decode_framed_wal_records(bytes: &[u8]) -> Result<Vec<(usize, WalRecord)>> {
    let mut records = Vec::new();
    let mut offset = 0_usize;
    let mut frame = 1_usize;
    while offset < bytes.len() {
        if bytes.len() - offset < WAL_FRAME_MIN_HEADER_LEN {
            anyhow::bail!("truncated WAL frame header at frame {frame}");
        }
        if bytes[offset..offset + 4] != WAL_FRAME_MAGIC {
            anyhow::bail!("invalid WAL frame magic at frame {frame}");
        }
        let version = u16::from_be_bytes([bytes[offset + 4], bytes[offset + 5]]);
        let header_len = wal_frame_header_len(version)
            .with_context(|| format!("parse WAL frame {frame} header"))?;
        if bytes.len() - offset < header_len {
            anyhow::bail!("truncated WAL frame header at frame {frame}");
        }
        let encoding = u16::from_be_bytes([bytes[offset + 6], bytes[offset + 7]]);
        let len = wal_frame_len(bytes, offset);
        if len == 0 {
            anyhow::bail!("empty WAL frame payload at frame {frame}");
        }
        if len > WAL_FRAME_MAX_BYTES {
            anyhow::bail!(
                "WAL frame {frame} exceeds max size: {len} bytes > {WAL_FRAME_MAX_BYTES} bytes"
            );
        }
        let start = offset + header_len;
        let end = start + len as usize;
        if end > bytes.len() {
            anyhow::bail!("truncated WAL frame payload at frame {frame}");
        }
        verify_wal_frame_crc32c(bytes, offset, version, &bytes[start..end])
            .with_context(|| format!("verify WAL frame {frame} CRC32C"))?;
        let record = decode_wal_frame_payload(encoding, &bytes[start..end], frame)?;
        records.push((frame, record));
        offset = end;
        frame += 1;
    }
    Ok(records)
}

fn decode_jsonl_wal_records(bytes: &[u8]) -> Result<Vec<(usize, WalRecord)>> {
    let contents = std::str::from_utf8(bytes).context("legacy WAL JSONL is not valid UTF-8")?;
    let mut records = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let line_number = index + 1;
        let record: WalRecord = serde_json::from_str(line)
            .with_context(|| format!("parse legacy WAL JSON line {line_number}"))?;
        records.push((line_number, record));
    }
    Ok(records)
}

pub fn inspect_integrity(paths: &[PathBuf]) -> WalIntegrityReport {
    let mut issues = Vec::new();
    let mut shards = Vec::with_capacity(paths.len());
    let mut lsn_locations = BTreeMap::<u64, Vec<WalIntegrityLocation>>::new();
    let mut record_count = 0_usize;
    let mut checksum_missing_count = 0_usize;
    let mut checksum_mismatch_count = 0_usize;

    for (shard, path) in paths.iter().enumerate() {
        let mut files = Vec::new();
        let archive_dir = wal_archive_dir(path);
        for archive_path in inspect_archive_paths(path, &mut issues) {
            let file = inspect_integrity_file(
                &archive_path,
                WalIntegrityFileKind::Archive,
                shard,
                &mut lsn_locations,
                &mut checksum_missing_count,
                &mut checksum_mismatch_count,
                &mut issues,
            );
            record_count += file.record_count;
            files.push(file);
        }
        let active = inspect_integrity_file(
            path,
            WalIntegrityFileKind::Active,
            shard,
            &mut lsn_locations,
            &mut checksum_missing_count,
            &mut checksum_mismatch_count,
            &mut issues,
        );
        record_count += active.record_count;
        files.push(active);

        let first_lsn = files.iter().filter_map(|file| file.first_lsn).min();
        let last_lsn = files.iter().filter_map(|file| file.last_lsn).max();
        let shard_record_count = files.iter().map(|file| file.record_count).sum();
        shards.push(WalIntegrityShardReport {
            shard,
            active_path: path.display().to_string(),
            archive_dir: archive_dir.display().to_string(),
            file_count: files.len(),
            record_count: shard_record_count,
            first_lsn,
            last_lsn,
            files,
        });
    }

    let mut duplicate_lsn_count = 0_usize;
    for (lsn, locations) in &lsn_locations {
        if locations.len() <= 1 {
            continue;
        }
        duplicate_lsn_count += locations.len() - 1;
        issues.push(WalIntegrityIssue {
            severity: WalIntegrityIssueSeverity::Error,
            code: "duplicateLsn".to_string(),
            path: locations.first().map(|location| location.path.clone()),
            line: locations.first().map(|location| location.line),
            lsn: Some(*lsn),
            message: format!(
                "LSN {lsn} appears in multiple WAL positions: {}",
                locations
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
    }

    let mut gaps = Vec::new();
    let mut previous = None;
    for lsn in lsn_locations.keys().copied() {
        if let Some(previous_lsn) = previous
            && lsn > previous_lsn + 1
        {
            let gap = WalIntegrityGap {
                after_lsn: previous_lsn,
                before_lsn: lsn,
                missing_count: lsn - previous_lsn - 1,
            };
            if gaps.len() < 100 {
                gaps.push(gap);
            }
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Warning,
                code: "lsnGap".to_string(),
                path: None,
                line: None,
                lsn: Some(previous_lsn),
                message: format!(
                    "WAL LSN gap after {previous_lsn} before {lsn} ({} missing)",
                    lsn - previous_lsn - 1
                ),
            });
        }
        previous = Some(lsn);
    }

    let highest_lsn = lsn_locations.keys().next_back().copied().unwrap_or(0);
    let lowest_lsn = lsn_locations.keys().next().copied();
    let ok = !issues
        .iter()
        .any(|issue| issue.severity == WalIntegrityIssueSeverity::Error);
    let issue_count = issues.len();
    let issues_truncated = issue_count > 200;
    if issues_truncated {
        issues.truncate(200);
    }

    WalIntegrityReport {
        ok,
        shard_count: paths.len(),
        file_count: shards.iter().map(|shard| shard.file_count).sum(),
        record_count,
        unique_lsn_count: lsn_locations.len(),
        duplicate_lsn_count,
        checksum_missing_count,
        checksum_mismatch_count,
        lowest_lsn,
        highest_lsn,
        gaps,
        shards,
        issue_count,
        issues_truncated,
        issues,
    }
}

fn inspect_archive_paths(path: &Path, issues: &mut Vec<WalIntegrityIssue>) -> Vec<PathBuf> {
    let archive_dir = wal_archive_dir(path);
    if !archive_dir.exists() {
        return Vec::new();
    }

    let mut paths = Vec::new();
    let entries = match std::fs::read_dir(&archive_dir) {
        Ok(entries) => entries,
        Err(err) => {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "archiveReadFailed".to_string(),
                path: Some(archive_dir.display().to_string()),
                line: None,
                lsn: None,
                message: format!("failed to read WAL archive directory: {err}"),
            });
            return Vec::new();
        }
    };
    for entry in entries {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
                    paths.push(path);
                }
            }
            Err(err) => issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "archiveEntryReadFailed".to_string(),
                path: Some(archive_dir.display().to_string()),
                line: None,
                lsn: None,
                message: format!("failed to read WAL archive entry: {err}"),
            }),
        }
    }
    paths.sort();
    paths
}

fn inspect_integrity_file(
    path: &Path,
    kind: WalIntegrityFileKind,
    expected_shard: usize,
    lsn_locations: &mut BTreeMap<u64, Vec<WalIntegrityLocation>>,
    checksum_missing_count: &mut usize,
    checksum_mismatch_count: &mut usize,
    issues: &mut Vec<WalIntegrityIssue>,
) -> WalIntegrityFileReport {
    let path_string = path.display().to_string();
    if !path.exists() {
        issues.push(WalIntegrityIssue {
            severity: WalIntegrityIssueSeverity::Warning,
            code: "missingWalFile".to_string(),
            path: Some(path_string.clone()),
            line: None,
            lsn: None,
            message: "WAL file is missing; this is valid only before the shard has accepted writes"
                .to_string(),
        });
        return WalIntegrityFileReport {
            path: path_string,
            kind,
            exists: false,
            line_count: 0,
            record_count: 0,
            first_lsn: None,
            last_lsn: None,
            min_timestamp_ms: None,
            max_timestamp_ms: None,
        };
    }

    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "walReadFailed".to_string(),
                path: Some(path_string.clone()),
                line: None,
                lsn: None,
                message: format!("failed to read WAL file: {err}"),
            });
            return WalIntegrityFileReport {
                path: path_string,
                kind,
                exists: true,
                line_count: 0,
                record_count: 0,
                first_lsn: None,
                last_lsn: None,
                min_timestamp_ms: None,
                max_timestamp_ms: None,
            };
        }
    };
    let mut line_count = 0_usize;
    let mut record_count = 0_usize;
    let mut first_lsn = None;
    let mut last_lsn = None;
    let mut min_timestamp_ms = None::<u64>;
    let mut max_timestamp_ms = None::<u64>;
    let mut previous_file_lsn = None::<u64>;

    let decoded = if bytes.starts_with(&WAL_FRAME_MAGIC) {
        match decode_framed_wal_records(&bytes) {
            Ok(records) => records,
            Err(err) => {
                issues.push(WalIntegrityIssue {
                    severity: WalIntegrityIssueSeverity::Error,
                    code: "walParseFailed".to_string(),
                    path: Some(path_string.clone()),
                    line: None,
                    lsn: None,
                    message: format!("failed to parse WAL frames: {err:#}"),
                });
                Vec::new()
            }
        }
    } else {
        inspect_decode_jsonl_wal_records(&bytes, &path_string, issues)
    };

    for (line_number, record) in decoded {
        line_count += 1;
        record_count += 1;
        first_lsn.get_or_insert(record.lsn);
        last_lsn = Some(record.lsn);
        min_timestamp_ms = Some(
            min_timestamp_ms
                .map(|existing| existing.min(record.timestamp_ms))
                .unwrap_or(record.timestamp_ms),
        );
        max_timestamp_ms = Some(
            max_timestamp_ms
                .map(|existing| existing.max(record.timestamp_ms))
                .unwrap_or(record.timestamp_ms),
        );

        if let Some(previous_lsn) = previous_file_lsn
            && record.lsn < previous_lsn
        {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "fileLsnDecreased".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: format!(
                    "WAL file LSN decreased from {previous_lsn} to {}",
                    record.lsn
                ),
            });
        }
        previous_file_lsn = Some(record.lsn);

        if record.lsn == 0 {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "zeroLsn".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: "WAL records must have non-zero LSNs".to_string(),
            });
        }
        if record.shard != expected_shard {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "shardMismatch".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: format!(
                    "WAL record shard {} does not match path shard {expected_shard}",
                    record.shard
                ),
            });
        }
        if record.shard_epoch == 0 {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "zeroShardEpoch".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: "WAL records must have a non-zero shard epoch".to_string(),
            });
        }
        if record.schema_version == 0 {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "zeroSchemaVersion".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: "WAL records must have a non-zero schema version".to_string(),
            });
        }
        if record.timestamp_ms == 0 {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Warning,
                code: "zeroTimestamp".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: "WAL record timestamp is zero".to_string(),
            });
        }
        if record.owner_node_id.trim().is_empty() {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Warning,
                code: "emptyOwnerNodeId".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: "WAL record ownerNodeId is empty; this is valid only for legacy records"
                    .to_string(),
            });
        }
        if record.durability == Durability::Volatile {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "volatileWalRecord".to_string(),
                path: Some(path_string.clone()),
                line: Some(line_number),
                lsn: Some(record.lsn),
                message: "volatile events must not be persisted in WAL".to_string(),
            });
        }
        match record.verify_checksum() {
            Ok(WalChecksumStatus::Valid) => {}
            Ok(WalChecksumStatus::Missing) => {
                *checksum_missing_count += 1;
                issues.push(WalIntegrityIssue {
                    severity: WalIntegrityIssueSeverity::Warning,
                    code: "missingChecksum".to_string(),
                    path: Some(path_string.clone()),
                    line: Some(line_number),
                    lsn: Some(record.lsn),
                    message: "WAL record has no checksum; this is valid only for legacy records"
                        .to_string(),
                });
            }
            Ok(WalChecksumStatus::Mismatch { expected }) => {
                *checksum_mismatch_count += 1;
                issues.push(WalIntegrityIssue {
                    severity: WalIntegrityIssueSeverity::Error,
                    code: "checksumMismatch".to_string(),
                    path: Some(path_string.clone()),
                    line: Some(line_number),
                    lsn: Some(record.lsn),
                    message: format!(
                        "WAL checksum mismatch: expected {expected}, found {}",
                        record.checksum.clone().unwrap_or_default()
                    ),
                });
            }
            Err(err) => {
                *checksum_mismatch_count += 1;
                issues.push(WalIntegrityIssue {
                    severity: WalIntegrityIssueSeverity::Error,
                    code: "checksumFailed".to_string(),
                    path: Some(path_string.clone()),
                    line: Some(line_number),
                    lsn: Some(record.lsn),
                    message: format!("failed to compute WAL checksum: {err}"),
                });
            }
        }

        lsn_locations
            .entry(record.lsn)
            .or_default()
            .push(WalIntegrityLocation {
                path: path_string.clone(),
                line: line_number,
                shard: expected_shard,
            });
    }

    WalIntegrityFileReport {
        path: path_string,
        kind,
        exists: true,
        line_count,
        record_count,
        first_lsn,
        last_lsn,
        min_timestamp_ms,
        max_timestamp_ms,
    }
}

fn inspect_decode_jsonl_wal_records(
    bytes: &[u8],
    path_string: &str,
    issues: &mut Vec<WalIntegrityIssue>,
) -> Vec<(usize, WalRecord)> {
    let contents = match std::str::from_utf8(bytes) {
        Ok(contents) => contents,
        Err(err) => {
            issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "walParseFailed".to_string(),
                path: Some(path_string.to_string()),
                line: None,
                lsn: None,
                message: format!("legacy WAL JSONL is not valid UTF-8: {err}"),
            });
            return Vec::new();
        }
    };

    let mut records = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<WalRecord>(line) {
            Ok(record) => records.push((line_number, record)),
            Err(err) => issues.push(WalIntegrityIssue {
                severity: WalIntegrityIssueSeverity::Error,
                code: "walParseFailed".to_string(),
                path: Some(path_string.to_string()),
                line: Some(line_number),
                lsn: None,
                message: format!("failed to parse WAL JSON line: {err}"),
            }),
        }
    }
    records
}

fn wal_archive_dir(path: &Path) -> PathBuf {
    path.parent()
        .map(|parent| parent.join("archive"))
        .unwrap_or_else(|| PathBuf::from("archive"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::MessageDraft,
        schema::{DatabaseSchema, SchemaMigrationPlan},
    };
    use axum::{Json, Router, extract::State as AxumState, routing::post};
    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };
    use tokio::{net::TcpListener, task::JoinHandle};

    fn test_wal_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "nextdb-wal-{name}-{}-{}.jsonl",
            std::process::id(),
            now_ms()
        ))
    }

    fn test_wal_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "nextdb-wal-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ))
    }

    #[derive(Clone)]
    struct DelayedRemoteState {
        delay: Duration,
        active: StdArc<AtomicUsize>,
        max_active: StdArc<AtomicUsize>,
        accepted: StdArc<AtomicUsize>,
    }

    impl DelayedRemoteState {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                active: StdArc::new(AtomicUsize::new(0)),
                max_active: StdArc::new(AtomicUsize::new(0)),
                accepted: StdArc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Default for DelayedRemoteState {
        fn default() -> Self {
            Self::new(Duration::from_millis(150))
        }
    }

    async fn delayed_remote_replicate(
        AxumState(state): AxumState<DelayedRemoteState>,
        Json(value): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let active = state.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        state.max_active.fetch_max(active, AtomicOrdering::SeqCst);
        time::sleep(state.delay).await;
        state.active.fetch_sub(1, AtomicOrdering::SeqCst);
        let accepted = value
            .get("records")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        state.accepted.fetch_add(accepted, AtomicOrdering::SeqCst);
        Json(serde_json::json!({ "accepted": accepted }))
    }

    async fn spawn_delayed_remote(state: DelayedRemoteState) -> Result<(String, JoinHandle<()>)> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/v1/admin/wal/replicate", post(delayed_remote_replicate))
            .with_state(state);
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok((format!("http://{address}"), server))
    }

    fn message_record(lsn: u64, room_id: &str, body: &str) -> WalRecord {
        WalRecord {
            lsn,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: lsn,
            schema_version: 1,
            durability: Durability::Strict,
            payload: WalPayload::MessageCreated {
                message: MessageDraft {
                    id: format!("message-{lsn}"),
                    client_mutation_id: None,
                    room_id: room_id.to_string(),
                    sender_id: "tester".to_string(),
                    body: body.to_string(),
                    attachments: Vec::new(),
                    created_at_ms: lsn,
                    path: format!("rooms/{room_id}/messages/message-{lsn}"),
                },
            },
            checksum: None,
        }
    }

    fn append_command(lsn: u64) -> WalAppendCommand {
        let (ack, _wait) = oneshot::channel();
        WalAppendCommand {
            lsn,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            durability: Durability::Strict,
            schema_version: 1,
            payload: message_record(lsn, "general", "append").payload,
            ack,
        }
    }

    fn append_request(lsn: u64, durability: Durability) -> WalAppendRequest {
        WalAppendRequest {
            lsn,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            durability,
            schema_version: 1,
            payload: message_record(lsn, "general", "append").payload,
        }
    }

    #[test]
    fn append_command_order_check_accepts_only_lsn_ordered_batches() {
        let ordered = vec![append_command(1), append_command(2), append_command(3)];
        assert!(wal_append_commands_are_sorted_by_lsn(&ordered));

        let unordered = vec![append_command(1), append_command(3), append_command(2)];
        assert!(!wal_append_commands_are_sorted_by_lsn(&unordered));
    }

    #[test]
    fn strict_prefix_end_splits_after_last_strict_record() {
        let mut first_relaxed = message_record(1, "general", "relaxed");
        first_relaxed.durability = Durability::Relaxed;
        let mut strict = message_record(2, "general", "strict");
        strict.durability = Durability::Strict;
        let mut trailing_relaxed = message_record(3, "general", "relaxed");
        trailing_relaxed.durability = Durability::Relaxed;

        assert_eq!(
            strict_prefix_end(&[
                first_relaxed.clone(),
                strict.clone(),
                trailing_relaxed.clone()
            ]),
            Some(2)
        );
        assert_eq!(
            strict_prefix_end(&[first_relaxed.clone(), trailing_relaxed]),
            None
        );
        assert_eq!(strict_prefix_end(&[first_relaxed, strict]), Some(2));
    }

    #[tokio::test]
    async fn mixed_strict_relaxed_batch_splits_trailing_relaxed_without_second_sync() {
        let path = test_wal_path("mixed-durability-split");
        let writer = WalWriter::spawn(
            path.clone(),
            0,
            Vec::new(),
            Vec::new(),
            WalRemoteAckPolicy::All,
            WalWriterConfig::new(1024, 0),
        );

        let pending = writer
            .enqueue_many(vec![
                append_request(1, Durability::Relaxed),
                append_request(2, Durability::Strict),
                append_request(3, Durability::Relaxed),
                append_request(4, Durability::Relaxed),
            ])
            .await
            .expect("enqueue WAL appends");
        let records = pending.wait().await.expect("wait for WAL appends");

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );

        let status = writer.status().await;
        assert_eq!(status.local_batches, 2);
        assert_eq!(status.local_syncs, 1);
        assert_eq!(status.local_records, 4);
        assert_eq!(status.local_last_batch_records, 2);
        assert!(!status.local_last_batch_sync);

        let persisted = read_records_file(&path).expect("read WAL records");
        assert_eq!(
            persisted
                .iter()
                .map(|record| (record.lsn, record.durability))
                .collect::<Vec<_>>(),
            vec![
                (1, Durability::Relaxed),
                (2, Durability::Strict),
                (3, Durability::Relaxed),
                (4, Durability::Relaxed),
            ]
        );

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn remote_replication_sends_replicas_concurrently() {
        let remote_state = DelayedRemoteState::default();
        let (first_url, first_server) = spawn_delayed_remote(remote_state.clone())
            .await
            .expect("spawn first remote replica");
        let (second_url, second_server) = spawn_delayed_remote(remote_state.clone())
            .await
            .expect("spawn second remote replica");
        let path = test_wal_path("remote-replication-concurrent");
        let writer = WalWriter::spawn(
            path.clone(),
            0,
            Vec::new(),
            vec![
                WalRemoteReplica {
                    url: first_url,
                    token: None,
                },
                WalRemoteReplica {
                    url: second_url,
                    token: None,
                },
            ],
            WalRemoteAckPolicy::All,
            WalWriterConfig::new(1024, 0),
        );

        let pending = writer
            .enqueue_many(vec![append_request(1, Durability::Strict)])
            .await
            .expect("enqueue WAL append");
        let records = pending.wait().await.expect("wait for WAL append");

        assert_eq!(records.len(), 1);
        assert_eq!(remote_state.accepted.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(remote_state.max_active.load(AtomicOrdering::SeqCst), 2);

        let status = writer.status().await;
        assert_eq!(status.remote_replicas.len(), 2);
        assert!(status.remote_replicas.iter().all(|replica| replica.ok));
        assert!(
            status
                .remote_replicas
                .iter()
                .all(|replica| replica.highest_acked_lsn == 1)
        );

        drop(writer);
        first_server.abort();
        second_server.abort();
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn remote_replication_returns_after_required_acks() {
        let fast_state = DelayedRemoteState::new(Duration::from_millis(80));
        let slow_state = DelayedRemoteState::new(Duration::from_millis(600));
        let (fast_url, fast_server) = spawn_delayed_remote(fast_state.clone())
            .await
            .expect("spawn fast remote replica");
        let (slow_url, slow_server) = spawn_delayed_remote(slow_state.clone())
            .await
            .expect("spawn slow remote replica");
        let path = test_wal_path("remote-replication-required-acks");
        let writer = WalWriter::spawn(
            path.clone(),
            0,
            Vec::new(),
            vec![
                WalRemoteReplica {
                    url: fast_url,
                    token: None,
                },
                WalRemoteReplica {
                    url: slow_url,
                    token: None,
                },
            ],
            WalRemoteAckPolicy::Count(1),
            WalWriterConfig::new(1024, 0),
        );

        let started = Instant::now();
        let pending = writer
            .enqueue_many(vec![append_request(1, Durability::Strict)])
            .await
            .expect("enqueue WAL append");
        let records = pending.wait().await.expect("wait for WAL append");
        let elapsed = started.elapsed();

        assert_eq!(records.len(), 1);
        assert!(
            elapsed < Duration::from_millis(400),
            "append waited {elapsed:?} despite satisfying required remote acks"
        );
        assert_eq!(fast_state.accepted.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(slow_state.accepted.load(AtomicOrdering::SeqCst), 0);

        let status = time::timeout(Duration::from_secs(2), async {
            loop {
                let status = writer.status().await;
                if status
                    .remote_replicas
                    .iter()
                    .all(|replica| replica.highest_acked_lsn == 1)
                {
                    break status;
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("slow remote replica ack should be observed in status");
        assert_eq!(slow_state.accepted.load(AtomicOrdering::SeqCst), 1);
        assert!(
            status
                .remote_replicas
                .iter()
                .all(|replica| replica.highest_acked_lsn == 1)
        );

        drop(writer);
        fast_server.abort();
        slow_server.abort();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn encode_wal_records_preserves_existing_checksum_without_recomputing() {
        let mut record = message_record(1, "general", "signed");
        record.refresh_checksum().expect("sign WAL record");
        let checksum = record.checksum.clone();

        let encoded = encode_wal_records(&[record]).expect("encode WAL record");
        let decoded = decode_wal_records(&encoded).expect("decode WAL frames");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1.checksum, checksum);
        assert!(matches!(
            decoded[0].1.verify_checksum().expect("verify checksum"),
            WalChecksumStatus::Valid
        ));
    }

    #[test]
    fn encode_wal_records_backfills_missing_checksum_for_legacy_records() {
        let record = message_record(1, "general", "legacy");
        assert!(record.checksum.is_none());

        let encoded = encode_wal_records(&[record]).expect("encode WAL record");
        let decoded = decode_wal_records(&encoded).expect("decode WAL frames");

        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].1.checksum.is_some());
        assert!(matches!(
            decoded[0].1.verify_checksum().expect("verify checksum"),
            WalChecksumStatus::Valid
        ));
    }

    #[test]
    fn encode_wal_records_writes_v2_postcard_frame_crc32c() {
        let encoded =
            encode_wal_records(&[message_record(1, "general", "crc")]).expect("encode WAL record");

        assert_eq!(&encoded[..4], &WAL_FRAME_MAGIC);
        assert_eq!(
            u16::from_be_bytes([encoded[4], encoded[5]]),
            WAL_FRAME_VERSION_V2
        );
        assert_eq!(
            u16::from_be_bytes([encoded[6], encoded[7]]),
            WAL_FRAME_ENCODING_POSTCARD
        );
        let len = wal_frame_len(&encoded, 0) as usize;
        let start = WAL_FRAME_HEADER_LEN_V2;
        let end = start + len;
        let expected = u32::from_be_bytes([encoded[12], encoded[13], encoded[14], encoded[15]]);

        assert_eq!(crc32c::crc32c(&encoded[start..end]), expected);
        assert!(serde_json::from_slice::<WalRecord>(&encoded[start..end]).is_err());
        assert_eq!(
            decode_wal_records(&encoded)
                .expect("decode WAL record")
                .len(),
            1
        );
    }

    #[test]
    fn encode_wal_records_roundtrips_schema_applied_payload() {
        let schema = DatabaseSchema::default_nextdb();
        let migration = SchemaMigrationPlan {
            from_version: 0,
            to_version: schema.version,
            compatible: true,
            errors: Vec::new(),
            warnings: vec!["bootstrap".to_string()],
            requires_replay_rebuild: false,
            replay_safe_breaking_changes: Vec::new(),
            unsafe_breaking_changes: Vec::new(),
            projection_rebuild_required: false,
            projection_rebuild_reasons: Vec::new(),
        };
        let record = WalRecord {
            lsn: 6,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: 6,
            schema_version: schema.version,
            durability: Durability::Strict,
            payload: WalPayload::SchemaApplied {
                schema: schema.clone(),
                migration: migration.clone(),
            },
            checksum: None,
        };

        let encoded = encode_wal_records(&[record]).expect("encode WAL record");
        let decoded = decode_wal_records(&encoded).expect("decode WAL record");

        let WalPayload::SchemaApplied {
            schema: decoded_schema,
            migration: decoded_migration,
        } = &decoded[0].1.payload
        else {
            panic!("expected schemaApplied payload");
        };
        assert_eq!(decoded_schema.version, schema.version);
        assert_eq!(decoded_schema.behaviors, schema.behaviors);
        assert_eq!(decoded_migration, &migration);
    }

    #[test]
    fn encode_wal_records_roundtrips_behavior_published_payload() {
        let record = WalRecord {
            lsn: 7,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: 7,
            schema_version: 1,
            durability: Durability::Strict,
            payload: WalPayload::BehaviorPublished {
                publish: crate::model::BehaviorPublishedDraft {
                    epoch: 2,
                    loaded: 1,
                    manifests: vec![crate::model::BehaviorPublishedManifest {
                        name: "echo".to_string(),
                        version: "0.1.0".to_string(),
                        module_path: "/tmp/echo/module.wasm".to_string(),
                        mutations: vec!["echo".to_string()],
                    }],
                    published_at_ms: 42,
                },
            },
            checksum: None,
        };

        let encoded = encode_wal_records(&[record]).expect("encode WAL record");
        let decoded = decode_wal_records(&encoded).expect("decode WAL record");

        let WalPayload::BehaviorPublished { publish } = &decoded[0].1.payload else {
            panic!("expected behaviorPublished payload");
        };
        assert_eq!(publish.epoch, 2);
        assert_eq!(publish.loaded, 1);
        assert_eq!(publish.manifests[0].name, "echo");
    }

    #[test]
    fn encode_wal_records_roundtrips_host_http_payloads() {
        let requested = WalRecord {
            lsn: 8,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: 8,
            schema_version: 1,
            durability: Durability::Strict,
            payload: WalPayload::HostHttpRequested {
                request: crate::model::HostHttpRequestDraft {
                    request_id: "http-1".to_string(),
                    method: "GET".to_string(),
                    url: "https://api.example.test/v1/jobs".to_string(),
                    headers: Default::default(),
                    body: None,
                    body_base64: None,
                    timeout_ms: 1000,
                    actor_kind: "scope".to_string(),
                    actor_key: "table:jobs/bucket:00".to_string(),
                    reminder_id: "host-http-http-1".to_string(),
                    continuation: serde_json::json!({
                        "type": "behaviorContinuation",
                        "behavior": "jobs",
                        "mutation": "onHttpResult"
                    }),
                    requested_at_ms: 8,
                },
            },
            checksum: None,
        };
        let completed = WalRecord {
            lsn: 9,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: 9,
            schema_version: 1,
            durability: Durability::Strict,
            payload: WalPayload::HostHttpCompleted {
                request_id: "http-1".to_string(),
                completed_at_ms: 9,
            },
            checksum: None,
        };

        let encoded = encode_wal_records(&[requested, completed]).expect("encode WAL records");
        let decoded = decode_wal_records(&encoded).expect("decode WAL records");

        let WalPayload::HostHttpRequested { request } = &decoded[0].1.payload else {
            panic!("expected hostHttpRequested payload");
        };
        assert_eq!(request.request_id, "http-1");
        assert_eq!(request.actor_kind, "scope");
        let WalPayload::HostHttpCompleted {
            request_id,
            completed_at_ms,
        } = &decoded[1].1.payload
        else {
            panic!("expected hostHttpCompleted payload");
        };
        assert_eq!(request_id, "http-1");
        assert_eq!(*completed_at_ms, 9);
    }

    #[test]
    fn decode_wal_records_rejects_v2_frame_crc32c_mismatch() {
        let mut encoded =
            encode_wal_records(&[message_record(1, "general", "crc")]).expect("encode WAL record");
        let (start, _) = frame_bounds(&encoded, 1);
        encoded[start] ^= 0x01;

        let err = decode_wal_records(&encoded).expect_err("CRC mismatch should fail");

        assert!(format!("{err:#}").contains("WAL frame CRC32C mismatch"));
    }

    #[test]
    fn decode_wal_records_accepts_legacy_v2_json_frames() {
        let mut record = message_record(1, "general", "v2-json");
        record.ensure_checksum().expect("sign WAL record");
        let payload = serde_json::to_vec(&record).expect("serialize WAL record");
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&WAL_FRAME_MAGIC);
        encoded.extend_from_slice(&WAL_FRAME_VERSION_V2.to_be_bytes());
        encoded.extend_from_slice(&WAL_FRAME_ENCODING_JSON.to_be_bytes());
        encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&crc32c::crc32c(&payload).to_be_bytes());
        encoded.extend_from_slice(&payload);

        let decoded = decode_wal_records(&encoded).expect("decode legacy v2 JSON WAL frame");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1.lsn, 1);
    }

    #[test]
    fn decode_wal_records_accepts_legacy_v1_frames() {
        let mut record = message_record(1, "general", "v1");
        record.ensure_checksum().expect("sign WAL record");
        let payload = serde_json::to_vec(&record).expect("serialize WAL record");
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&WAL_FRAME_MAGIC);
        encoded.extend_from_slice(&WAL_FRAME_VERSION_V1.to_be_bytes());
        encoded.extend_from_slice(&WAL_FRAME_ENCODING_JSON.to_be_bytes());
        encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&payload);

        let decoded = decode_wal_records(&encoded).expect("decode legacy v1 WAL frame");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1.lsn, 1);
    }

    #[test]
    fn replay_reports_scanned_and_after_snapshot_records() {
        let path = test_wal_path("replay-report");
        let records = [
            message_record(3, "general", "before"),
            message_record(7, "general", "after"),
        ];
        let mut encoded = Vec::new();
        for record in records {
            serde_json::to_writer(&mut encoded, &record).expect("serialize WAL record");
            encoded.push(b'\n');
        }
        std::fs::write(&path, encoded).expect("write WAL file");

        let replay = replay_from(&path, 100, 5, HashMap::new()).expect("replay WAL");

        assert_eq!(replay.highest_lsn, 7);
        assert_eq!(replay.scanned_records, 2);
        assert_eq!(replay.records_after_snapshot, 1);
        assert_eq!(replay.rooms.len(), 1);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_quarantines_truncated_frame_and_recovers_intact_prefix() {
        let path = test_wal_path("replay-truncated-frame");
        let mut encoded = encode_wal_records(&[
            message_record(1, "general", "one"),
            message_record(2, "general", "two"),
            message_record(3, "general", "three"),
        ])
        .expect("encode WAL records");
        let (_, third_end) = frame_bounds(&encoded, 3);
        encoded.truncate(third_end - 3);
        std::fs::write(&path, encoded).expect("write truncated WAL");

        let replay = replay_from(&path, 100, 0, HashMap::new()).expect("replay WAL prefix");

        assert_eq!(replay.highest_lsn, 2);
        assert_eq!(replay.scanned_records, 2);
        let quarantine = replay.quarantined_wal.expect("quarantined tail");
        assert_eq!(quarantine.frame, 3);
        assert!(quarantine.reason.contains("truncated WAL frame payload"));
        assert!(read_records_file(&path).is_err());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_quarantines_bit_flip_and_recovers_intact_prefix() {
        let path = test_wal_path("replay-bit-flip");
        let mut flipped = message_record(3, "general", "three");
        flipped.refresh_checksum().expect("sign WAL record");
        if let WalPayload::MessageCreated { message } = &mut flipped.payload {
            message.body = "tampered".to_string();
        }
        let encoded = encode_wal_records(&[
            message_record(1, "general", "one"),
            message_record(2, "general", "two"),
            flipped,
        ])
        .expect("encode WAL records");
        std::fs::write(&path, encoded).expect("write tampered WAL");

        let replay = replay_from(&path, 100, 0, HashMap::new()).expect("replay WAL prefix");

        assert_eq!(replay.highest_lsn, 2);
        assert_eq!(replay.scanned_records, 2);
        let quarantine = replay.quarantined_wal.expect("quarantined bit flip");
        assert_eq!(quarantine.frame, 3);
        assert!(quarantine.reason.contains("WAL checksum mismatch"));
        assert!(read_records_file(&path).is_err());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_quarantines_torn_write_header_and_recovers_intact_prefix() {
        let path = test_wal_path("replay-torn-header");
        let mut encoded = encode_wal_records(&[
            message_record(1, "general", "one"),
            message_record(2, "general", "two"),
        ])
        .expect("encode WAL records");
        encoded.extend_from_slice(&WAL_FRAME_MAGIC[..2]);
        std::fs::write(&path, encoded).expect("write torn WAL");

        let replay = replay_from(&path, 100, 0, HashMap::new()).expect("replay WAL prefix");

        assert_eq!(replay.highest_lsn, 2);
        assert_eq!(replay.scanned_records, 2);
        let quarantine = replay.quarantined_wal.expect("quarantined torn header");
        assert_eq!(quarantine.frame, 3);
        assert!(quarantine.reason.contains("truncated WAL frame header"));
        assert!(read_records_file(&path).is_err());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn integrity_scans_active_and_archive_records() {
        let dir = test_wal_dir("integrity-ok");
        let archive_dir = dir.join("archive");
        std::fs::create_dir_all(&archive_dir).expect("create archive dir");
        let active_path = dir.join("shard-0000.jsonl");
        let archive_path = archive_dir.join("shard-0000-through-1.jsonl");

        let archive = [message_record(1, "general", "archived")];
        let active = [message_record(2, "general", "active")];
        write_test_wal(&archive_path, &archive);
        write_test_wal(&active_path, &active);

        let report = inspect_integrity(&[active_path]);

        assert!(report.ok);
        assert_eq!(report.shard_count, 1);
        assert_eq!(report.file_count, 2);
        assert_eq!(report.record_count, 2);
        assert_eq!(report.unique_lsn_count, 2);
        assert_eq!(report.checksum_missing_count, 0);
        assert_eq!(report.checksum_mismatch_count, 0);
        assert_eq!(report.highest_lsn, 2);
        assert!(report.issues.is_empty());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_records_after_lsn_skips_archives_covered_by_cursor() {
        let dir = test_wal_dir("read-after-lsn");
        let archive_dir = dir.join("archive");
        std::fs::create_dir_all(&archive_dir).expect("create archive dir");
        let active_path = dir.join("shard-0000.jsonl");
        let old_archive_path = archive_dir.join("shard-0000-through-2-100.jsonl");
        let newer_archive_path = archive_dir.join("shard-0000-through-4-200.jsonl");

        std::fs::write(&old_archive_path, b"not json\n").expect("write skipped archive");
        write_test_wal(
            &newer_archive_path,
            &[
                message_record(3, "general", "archived-three"),
                message_record(4, "general", "archived-four"),
            ],
        );
        write_test_wal(&active_path, &[message_record(5, "general", "active-five")]);

        let records = read_records_after_lsn_including_archives(&active_path, 2)
            .expect("read records after cursor");

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn integrity_reports_parse_duplicate_and_shard_errors() {
        let dir = test_wal_dir("integrity-bad");
        std::fs::create_dir_all(&dir).expect("create wal dir");
        let active_path = dir.join("shard-0000.jsonl");
        let mut wrong_shard = message_record(1, "general", "duplicate wrong shard");
        wrong_shard.shard = 1;

        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &message_record(1, "general", "first"))
            .expect("serialize WAL record");
        encoded.push(b'\n');
        encoded.extend_from_slice(b"not json\n");
        serde_json::to_writer(&mut encoded, &wrong_shard).expect("serialize WAL record");
        encoded.push(b'\n');
        std::fs::write(&active_path, encoded).expect("write WAL file");

        let report = inspect_integrity(&[active_path]);
        let codes = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<Vec<_>>();

        assert!(!report.ok);
        assert!(codes.contains(&"walParseFailed"));
        assert!(codes.contains(&"duplicateLsn"));
        assert!(codes.contains(&"shardMismatch"));
        assert_eq!(report.duplicate_lsn_count, 1);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn integrity_reports_checksum_mismatch_and_reader_rejects_it() {
        let dir = test_wal_dir("integrity-checksum");
        std::fs::create_dir_all(&dir).expect("create wal dir");
        let active_path = dir.join("shard-0000.jsonl");
        let mut record = message_record(1, "general", "first");
        record.refresh_checksum().expect("sign WAL record");
        if let WalPayload::MessageCreated { message } = &mut record.payload {
            message.body = "tampered".to_string();
        }
        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &record).expect("serialize WAL record");
        encoded.push(b'\n');
        std::fs::write(&active_path, encoded).expect("write WAL file");

        let report = inspect_integrity(std::slice::from_ref(&active_path));
        let codes = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<Vec<_>>();

        assert!(!report.ok);
        assert!(codes.contains(&"checksumMismatch"));
        assert_eq!(report.checksum_mismatch_count, 1);
        assert!(read_records_file(&active_path).is_err());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn seal_checksum_file_backfills_missing_checksums() {
        let dir = test_wal_dir("seal-checksum");
        std::fs::create_dir_all(&dir).expect("create wal dir");
        let active_path = dir.join("shard-0000.jsonl");
        let records = [
            message_record(1, "general", "legacy-one"),
            message_record(2, "general", "legacy-two"),
        ];
        let mut encoded = Vec::new();
        for record in &records {
            serde_json::to_writer(&mut encoded, record).expect("serialize legacy WAL record");
            encoded.push(b'\n');
        }
        std::fs::write(&active_path, encoded).expect("write legacy WAL file");

        let report = seal_checksum_file(&active_path).await.expect("seal WAL");
        assert_eq!(report.records, 2);
        assert_eq!(report.sealed, 2);
        assert_eq!(report.already_sealed, 0);
        assert!(report.rewritten);

        let integrity = inspect_integrity(std::slice::from_ref(&active_path));
        assert!(integrity.ok);
        assert_eq!(integrity.checksum_missing_count, 0);
        assert_eq!(integrity.checksum_mismatch_count, 0);

        let second = seal_checksum_file(&active_path)
            .await
            .expect("seal WAL again");
        assert_eq!(second.sealed, 0);
        assert_eq!(second.already_sealed, 2);
        assert!(!second.rewritten);

        let _ = std::fs::remove_dir_all(dir);
    }

    fn write_test_wal(path: &Path, records: &[WalRecord]) {
        let mut encoded = Vec::new();
        for record in records {
            let mut record = record.clone();
            record.ensure_checksum().expect("sign WAL record");
            serde_json::to_writer(&mut encoded, &record).expect("serialize WAL record");
            encoded.push(b'\n');
        }
        std::fs::write(path, encoded).expect("write WAL file");
    }

    fn frame_bounds(bytes: &[u8], target_frame: usize) -> (usize, usize) {
        let mut offset = 0_usize;
        for frame in 1..=target_frame {
            let version = u16::from_be_bytes([bytes[offset + 4], bytes[offset + 5]]);
            let header_len = wal_frame_header_len(version).expect("known WAL frame version");
            let len = wal_frame_len(bytes, offset) as usize;
            let start = offset + header_len;
            let end = start + len;
            if frame == target_frame {
                return (start, end);
            }
            offset = end;
        }
        unreachable!("target frame should exist")
    }
}
