import React, { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { createRoot, type Root } from "react-dom/client"
import {
  Activity,
  Archive,
  Boxes,
  Cable,
  CloudOff,
  Database,
  FileClock,
  Gauge,
  HardDrive,
  Network,
  Play,
  RefreshCw,
  RotateCw,
  ServerCog,
  ShieldCheck,
  Trash2,
  Waypoints,
} from "lucide-react"
import {
  AuditReplayResponse,
  AuditTraceKind,
  AuditTraceResponse,
  AuditTraceTarget,
  AuditWalResponse,
  BehaviorInvokeResponse,
  BehaviorManifest,
  BehaviorReadCapability,
  ClientCacheInvalidateResponse,
  ClientCacheProfileUpdateOptions,
  ConnectionEvent,
  ConnectionListResponse,
  DeleteObjectResponse,
  DeleteRecordResponse,
  ExportBackupPolicyResponse,
  ExportBackupRetentionResponse,
  ExportBackupRunListResponse,
  ExportBackupRunResponse,
  ExportBundleArchiveObjectResponse,
  ExportBundleChainVerifyResponse,
  ExportBundleListResponse,
  ExportBundleResponse,
  ExportBundleVerifyResponse,
  ExportManifestResponse,
  FieldSchema,
  FieldType,
  FailoverPlanResponse,
  FailoverProposalResponse,
  HandoffAutoResponse,
  HandoffPlanResponse,
  HandoffApplyResponse,
  HandoffWorkflow,
  HandoffWorkflowResponse,
  ImportBundleFromObjectResponse,
  ImportBundleChainRestoreResponse,
  ImportBundleDeltaApplyResponse,
  ImportBundleDeltaPreflightResponse,
  ImportBundlePreflightResponse,
  ImportBundleRestoreResponse,
  ListRecordsResponse,
  ListUsersResponse,
  ListObjectsResponse,
  NextDbClient,
  NextDbHealth,
  NextDbLocalDataStatus,
  NextDbPendingWrite,
  NextDbReadiness,
  NextDbRealtimeTransportKind,
  NextDbRecord,
  NextDbSchema,
  NextDbWalRecord,
  PendingWriteQueueStatus,
  RecordResponse,
  RealtimeChannelListResponse,
  RealtimeMember,
  RealtimeChannelStateResponse,
  RealtimeChannelStateUpdateResponse,
  RecordProjectionStatus,
  RuntimePrepareRestartResponse,
  RuntimeActivationStatusResponse,
  SchemaMigrationPlan,
  SchemaApplyResponse,
  SchemaHistoryResponse,
  SchemaStoragePolicyResponse,
  SchemaValidationReport,
  TopologyLogResponse,
  TopologyProposalListResponse,
  TopologyProposalResponse,
  WalArchiveRetentionResponse,
  WalChecksumSealResponse,
  WalCompactResponse,
} from "@nextdb/client"
import "./styles.css"

type ObjectGcResponse = {
  dryRun: boolean
  force: boolean
  graceMs: number
  deleted: string[]
  retained: string[]
  protected: string[]
}

type SchemaReloadResponse = {
  name: string
  version: number
  report: SchemaValidationReport
  migration: SchemaMigrationPlan
}

type SnapshotResponse = {
  lsn: number
  roomCount: number
  recordHotTableCount: number
  recordHotRecordCount: number
}

type ProjectionRebuildResponse = {
  messages: number
  records: number
  objectRefs: number
}

type OperationLog = {
  id: string
  at: string
  label: string
  detail: string
  ok: boolean
}

type BehaviorInvokeState = {
  behavior: string
  mutation: string
  userId: string
  input: Record<string, string>
}

type CacheInvalidationState = {
  scope: "object" | "room" | "user" | "table" | "nestedTable"
  key: string
  minValidLsn: string
}

type CacheProfileDraftState = {
  leaseTtlMs: string
  maxObjects: string
  maxObjectBytes: string
  maxRoomMessages: string
  maxUserEvents: string
  maxRecordsPerTable: string
  maxNestedPartitions: string
  maxPendingWrites: string
  maxPendingWriteBytes: string
  offlineWrites: "unchanged" | "true" | "false"
}

type RuntimeRecordActivationDraft = {
  table: string
  parentKey: string
  nested: string
  key: string
  indexName: string
  value: string
  lower: string
  upper: string
  afterKey: string
  afterCursor: string
  order: "key" | "schema"
  limit: string
}

type RuntimeRoomActivationDraft = {
  roomId: string
  limit: string
}

type RealtimeStateDraft = {
  channelId: string
  fromUserId: string
  expectedVersion: string
  stateJson: string
}

type RealtimeEventDraft = {
  kind: string
  payloadJson: string
  includeSelf: boolean
}

type RealtimeSignalDraft = {
  toUserId: string
  kind: string
  payloadJson: string
}

type DataExplorerTarget = {
  id: string
  table: string
  nested?: string
  label: string
}

type WalIntegrityReport = {
  ok: boolean
  shardCount: number
  fileCount: number
  recordCount: number
  uniqueLsnCount: number
  duplicateLsnCount: number
  checksumMissingCount: number
  checksumMismatchCount: number
  lowestLsn?: number | null
  highestLsn: number
  gaps: Array<{ afterLsn: number; beforeLsn: number; missingCount: number }>
  issueCount: number
  issuesTruncated: boolean
  issues: WalIntegrityIssue[]
}

type WalIntegrityIssue = {
  severity: "warning" | "error"
  code: string
  path?: string | null
  line?: number | null
  lsn?: number | null
  message: string
}

type AuditMode = "wal" | "trace" | "replay"
type AdminRealtimeTransportKind = Extract<NextDbRealtimeTransportKind, "websocket" | "jsonl">

const DEFAULT_ENDPOINT = "http://127.0.0.1:3188"
const DEFAULT_ADMIN_TRANSPORT: AdminRealtimeTransportKind = "websocket"

function App() {
  const [endpoint, setEndpoint] = useState(() => localStorage.getItem("nextdb-admin:endpoint") ?? DEFAULT_ENDPOINT)
  const [adminToken, setAdminToken] = useState(() => localStorage.getItem("nextdb-admin:token") ?? "")
  const [adminRealtimeTransport, setAdminRealtimeTransport] = useState<AdminRealtimeTransportKind>(() =>
    normalizeAdminRealtimeTransport(localStorage.getItem("nextdb-admin:transport")),
  )
  const [health, setHealth] = useState<NextDbHealth | undefined>()
  const [readiness, setReadiness] = useState<NextDbReadiness | undefined>()
  const [audit, setAudit] = useState<AuditWalResponse | undefined>()
  const [schema, setSchema] = useState<NextDbSchema | undefined>()
  const [schemaDraft, setSchemaDraft] = useState("")
  const [schemaApplyResult, setSchemaApplyResult] = useState<SchemaApplyResponse | undefined>()
  const [schemaHistory, setSchemaHistory] = useState<SchemaHistoryResponse | undefined>()
  const [schemaReport, setSchemaReport] = useState<SchemaValidationReport | undefined>()
  const [migrationPlan, setMigrationPlan] = useState<SchemaMigrationPlan | undefined>()
  const [storagePolicy, setStoragePolicy] = useState<SchemaStoragePolicyResponse | undefined>()
  const [projectionStatus, setProjectionStatus] = useState<RecordProjectionStatus | undefined>()
  const [runtimeActivation, setRuntimeActivation] = useState<RuntimeActivationStatusResponse | undefined>()
  const [runtimeRecordActivation, setRuntimeRecordActivation] = useState<RuntimeRecordActivationDraft>({
    table: "rooms",
    parentKey: "",
    nested: "",
    key: "",
    indexName: "",
    value: "",
    lower: "",
    upper: "",
    afterKey: "",
    afterCursor: "",
    order: "key",
    limit: "20",
  })
  const [runtimeRoomActivation, setRuntimeRoomActivation] = useState<RuntimeRoomActivationDraft>({
    roomId: "general",
    limit: "20",
  })
  const [behaviors, setBehaviors] = useState<BehaviorManifest[]>([])
  const [behaviorInvoke, setBehaviorInvoke] = useState<BehaviorInvokeState>({
    behavior: "",
    mutation: "",
    userId: "admin",
    input: {
      body: "hello from admin behavior",
      roomId: "admin-behavior",
    },
  })
  const [cacheInvalidation, setCacheInvalidation] = useState<CacheInvalidationState>({
    scope: "user",
    key: "",
    minValidLsn: "",
  })
  const [cacheProfileDraft, setCacheProfileDraft] = useState<CacheProfileDraftState>({
    leaseTtlMs: "",
    maxObjects: "",
    maxObjectBytes: "",
    maxRoomMessages: "",
    maxUserEvents: "",
    maxRecordsPerTable: "",
    maxNestedPartitions: "",
    maxPendingWrites: "",
    maxPendingWriteBytes: "",
    offlineWrites: "unchanged",
  })
  const [behaviorResult, setBehaviorResult] = useState<BehaviorInvokeResponse | undefined>()
  const [objectList, setObjectList] = useState<ListObjectsResponse | undefined>()
  const [objectGc, setObjectGc] = useState<ObjectGcResponse | undefined>()
  const [walCompact, setWalCompact] = useState<WalCompactResponse | undefined>()
  const [walRetention, setWalRetention] = useState<WalArchiveRetentionResponse | undefined>()
  const [walIntegrity, setWalIntegrity] = useState<WalIntegrityReport | undefined>()
  const [walChecksumSeal, setWalChecksumSeal] = useState<WalChecksumSealResponse | undefined>()
  const [metricsText, setMetricsText] = useState<string | undefined>()
  const [exportManifest, setExportManifest] = useState<ExportManifestResponse | undefined>()
  const [exportBundle, setExportBundle] = useState<ExportBundleResponse | undefined>()
  const [exportBackupRun, setExportBackupRun] = useState<ExportBackupRunResponse | undefined>()
  const [exportBackupRuns, setExportBackupRuns] = useState<ExportBackupRunListResponse | undefined>()
  const [exportBackupPolicy, setExportBackupPolicy] = useState<ExportBackupPolicyResponse | undefined>()
  const [exportBackupRetention, setExportBackupRetention] = useState<ExportBackupRetentionResponse | undefined>()
  const [exportBundleList, setExportBundleList] = useState<ExportBundleListResponse | undefined>()
  const [selectedExportBundleId, setSelectedExportBundleId] = useState("")
  const [exportBundleEncryptionKey, setExportBundleEncryptionKey] = useState("")
  const [exportBundleBaseLsn, setExportBundleBaseLsn] = useState("")
  const [exportBundleObjectId, setExportBundleObjectId] = useState("")
  const [exportBundleChainIds, setExportBundleChainIds] = useState("")
  const [exportBundleVerify, setExportBundleVerify] = useState<ExportBundleVerifyResponse | undefined>()
  const [exportBundleChainVerify, setExportBundleChainVerify] = useState<ExportBundleChainVerifyResponse | undefined>()
  const [exportBundleArchiveObject, setExportBundleArchiveObject] = useState<ExportBundleArchiveObjectResponse | undefined>()
  const [importBundleObject, setImportBundleObject] = useState<ImportBundleFromObjectResponse | undefined>()
  const [importBundlePreflight, setImportBundlePreflight] = useState<ImportBundlePreflightResponse | undefined>()
  const [importBundleRestore, setImportBundleRestore] = useState<ImportBundleRestoreResponse | undefined>()
  const [importBundleDeltaPreflight, setImportBundleDeltaPreflight] = useState<ImportBundleDeltaPreflightResponse | undefined>()
  const [importBundleDeltaApply, setImportBundleDeltaApply] = useState<ImportBundleDeltaApplyResponse | undefined>()
  const [importBundleChainRestore, setImportBundleChainRestore] = useState<ImportBundleChainRestoreResponse | undefined>()
  const [retentionBeforeLsn, setRetentionBeforeLsn] = useState("")
  const [retentionBeforeTimestampMs, setRetentionBeforeTimestampMs] = useState("")
  const [auditMode, setAuditMode] = useState<AuditMode>("wal")
  const [auditRecordKey, setAuditRecordKey] = useState("")
  const [auditPath, setAuditPath] = useState("")
  const [auditClientMutationId, setAuditClientMutationId] = useState("")
  const [auditTraceKind, setAuditTraceKind] = useState<AuditTraceKind>("room")
  const [auditTraceId, setAuditTraceId] = useState("general")
  const [auditTraceTable, setAuditTraceTable] = useState("rooms")
  const [auditTraceParentKey, setAuditTraceParentKey] = useState("general")
  const [auditTraceNested, setAuditTraceNested] = useState("messages")
  const [auditTraceTarget, setAuditTraceTarget] = useState<AuditTraceTarget | undefined>()
  const [auditReplayAtLsn, setAuditReplayAtLsn] = useState("")
  const [auditReplay, setAuditReplay] = useState<AuditReplayResponse | undefined>()
  const [handoffPlan, setHandoffPlan] = useState<HandoffPlanResponse | undefined>()
  const [failoverPlan, setFailoverPlan] = useState<FailoverPlanResponse | undefined>()
  const [failoverProposal, setFailoverProposal] = useState<FailoverProposalResponse | undefined>()
  const [handoffWorkflow, setHandoffWorkflow] = useState<HandoffWorkflow | undefined>()
  const [topologyLog, setTopologyLog] = useState<TopologyLogResponse | undefined>()
  const [topologyProposals, setTopologyProposals] = useState<TopologyProposalListResponse | undefined>()
  const [connections, setConnections] = useState<ConnectionListResponse | undefined>()
  const [users, setUsers] = useState<ListUsersResponse | undefined>()
  const [realtimeChannels, setRealtimeChannels] = useState<RealtimeChannelListResponse | undefined>()
  const [selectedRealtimeChannelId, setSelectedRealtimeChannelId] = useState("")
  const [realtimeStateDraft, setRealtimeStateDraft] = useState<RealtimeStateDraft>({
    channelId: "",
    fromUserId: "",
    expectedVersion: "",
    stateJson: "{}",
  })
  const [realtimeEventDraft, setRealtimeEventDraft] = useState<RealtimeEventDraft>({
    kind: "admin.event",
    payloadJson: "{\n  \"source\": \"admin-ui\"\n}",
    includeSelf: true,
  })
  const [realtimeSignalDraft, setRealtimeSignalDraft] = useState<RealtimeSignalDraft>({
    toUserId: "nextdb-admin",
    kind: "admin.signal",
    payloadJson: "{\n  \"source\": \"admin-ui\"\n}",
  })
  const [realtimeState, setRealtimeState] = useState<RealtimeChannelStateResponse | undefined>()
  const [dataTargetId, setDataTargetId] = useState("")
  const [dataParentKey, setDataParentKey] = useState("general")
  const [dataKey, setDataKey] = useState("")
  const [dataValue, setDataValue] = useState("{}")
  const [dataPage, setDataPage] = useState<ListRecordsResponse | undefined>()
  const [dataPageCursor, setDataPageCursor] = useState<string | undefined>()
  const [dataPageHistory, setDataPageHistory] = useState<string[]>([])
  const [selectedDataRecord, setSelectedDataRecord] = useState<NextDbRecord | undefined>()
  const [selectedRecord, setSelectedRecord] = useState<NextDbWalRecord | undefined>()
  const [localDataStatus, setLocalDataStatus] = useState<NextDbLocalDataStatus | undefined>()
  const [localPendingQueue, setLocalPendingQueue] = useState<PendingWriteQueueStatus | undefined>()
  const [logs, setLogs] = useState<OperationLog[]>([])
  const [loading, setLoading] = useState<string | undefined>()
  const logIdCounter = useRef(0)
  const base = endpoint.replace(/\/$/, "")
  const localDataClient = useMemo(
    () =>
      new NextDbClient({
        endpoint: base,
        adminToken: adminToken.trim() || undefined,
        clientId: "nextdb-admin-local",
        userId: "nextdb-admin",
        sessionId: "nextdb-admin-session",
        realtimeTransportKind: adminRealtimeTransport,
      }),
    [adminRealtimeTransport, adminToken, base],
  )

  const request = useCallback(
    async <T,>(path: string, options?: RequestInit): Promise<T> => {
      const headers = new Headers(options?.headers)
      if (adminToken.trim()) {
        headers.set("authorization", `Bearer ${adminToken.trim()}`)
        headers.set("x-nextdb-admin-token", adminToken.trim())
      }
      const response = await fetch(`${base}${path}`, { ...options, headers })
      const payload = await response.json().catch(() => undefined)
      if (!response.ok) {
        throw new Error(payload?.error ?? `Request failed with ${response.status}`)
      }
      return payload as T
    },
    [adminToken, base],
  )

  const pushLog = useCallback((label: string, detail: string, ok = true) => {
    setLogs((current) =>
      [
        {
          id: `${Date.now()}-${++logIdCounter.current}`,
          at: new Date().toLocaleTimeString(),
          label,
          detail,
          ok,
        },
        ...current,
      ].slice(0, 8),
    )
  }, [])

  const run = useCallback(
    async <T,>(label: string, work: () => Promise<T>, onSuccess?: (value: T) => void): Promise<T | undefined> => {
      setLoading(label)
      try {
        const value = await work()
        onSuccess?.(value)
        pushLog(label, "completed")
        return value
      } catch (error) {
        pushLog(label, error instanceof Error ? error.message : String(error), false)
        return undefined
      } finally {
        setLoading(undefined)
      }
    },
    [pushLog],
  )

  const refreshHealth = useCallback(
    () =>
      run("Refresh health", async () => {
        const [nextHealth, nextReadiness] = await Promise.all([
          request<NextDbHealth>("/v1/health"),
          request<NextDbReadiness>("/v1/ready"),
        ])
        return { health: nextHealth, readiness: nextReadiness }
      }, ({ health, readiness }) => {
        setHealth(health)
        setReadiness(readiness)
      }),
    [request, run],
  )

  const refreshRuntimeActivation = useCallback(
    () =>
      run("Refresh runtime activation", () => localDataClient.runtimeActivationStatus(), (status) => {
        setRuntimeActivation(status)
      }),
    [localDataClient, run],
  )

  const syncLocalDataStatus = useCallback(async () => {
    const [status, queue] = await Promise.all([
      localDataClient.localDataStatus(),
      localDataClient.pendingWriteQueueStatus(20),
    ])
    setLocalDataStatus(status)
    setLocalPendingQueue(queue)
    return { status, queue }
  }, [localDataClient])

  const refreshLocalDataStatus = useCallback(
    () =>
      run(
        "Refresh local data",
        syncLocalDataStatus,
      ),
    [run, syncLocalDataStatus],
  )

  const refreshMetrics = useCallback(
    () =>
      run("Refresh metrics", () => localDataClient.metrics(), (text) => {
        setMetricsText(text)
        pushLog("Refresh metrics", metricsSummary(text))
      }),
    [localDataClient, pushLog, run],
  )

  const restoreLocalSubscriptions = useCallback(
    () =>
      run("Restore local subscriptions", () => localDataClient.restoreSubscriptions(), (subscriptions) => {
        pushLog("Restore local subscriptions", `${subscriptions.length} restored`)
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const clearLocalSubscriptions = useCallback(
    () =>
      run("Clear local subscriptions", () => localDataClient.clearStoredSubscriptions(), (removed) => {
        pushLog("Clear local subscriptions", `${removed} removed`)
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const clearLocalCache = useCallback(
    () =>
      run("Clear local cache", () => localDataClient.clearCache(), (removed) => {
        pushLog("Clear local cache", `${removed} entries removed`)
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const enforceLocalCacheProfile = useCallback(
    () =>
      run("Enforce local cache profile", () => localDataClient.enforceLocalCacheProfile(), (result) => {
        pushLog("Enforce local cache profile", `${result.removed.total} entries removed`)
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const refreshExportManifest = useCallback(
    () =>
      run(
        "Export manifest",
        () => localDataClient.exportManifest({
          includeSamples: true,
          sampleLimit: 5,
          baseLsn: numericInput(exportBundleBaseLsn),
        }),
        (manifest) => {
          setExportManifest(manifest)
          pushLog("Export manifest", `${manifest.wal.records} WAL records, ${manifest.objects.live} live objects`)
        },
      ),
    [exportBundleBaseLsn, localDataClient, pushLog, run],
  )

  const createExportBundle = useCallback(
    () =>
      run("Create export bundle", () => localDataClient.createExportBundle(bundleCreateOptions(exportBundleEncryptionKey, exportBundleBaseLsn)), (bundle) => {
        setExportBundle(bundle)
        setSelectedExportBundleId(bundle.id)
        setExportBundleChainIds((current) => appendBundleChainId(current, bundle.id))
        setExportManifest(bundle.manifest)
        setExportBundleVerify(undefined)
        setExportBundleChainVerify(undefined)
        setExportBundleArchiveObject(undefined)
        setImportBundleObject(undefined)
        setImportBundlePreflight(undefined)
        setImportBundleRestore(undefined)
        setImportBundleDeltaPreflight(undefined)
        setImportBundleDeltaApply(undefined)
        setImportBundleChainRestore(undefined)
        pushLog(
          "Create export bundle",
          `${bundle.id}: ${bundle.walRecords} WAL records, ${bundle.objects} objects, ${bundle.schemaProposals} schema proposals, ${bundleEncryptionSummary(bundle.encrypted)}, ${clusterControlSummary(bundle.clusterControl)}`,
        )
      }),
    [exportBundleBaseLsn, exportBundleEncryptionKey, localDataClient, pushLog, run],
  )

  const runExportBackup = useCallback(
    () =>
      run("Run export backup", () => localDataClient.runExportBackup({
        encryptionKey: bundleAccessOptions(exportBundleEncryptionKey).encryptionKey,
        forceFull: numericInput(exportBundleBaseLsn) === 0,
        archiveObject: true,
        objectId: exportBundleObjectId.trim() || undefined,
      }), (result) => {
        setExportBackupRun(result)
        setExportBackupRuns((current) => ({
          runs: [result.run, ...(current?.runs.filter((run) => run.id !== result.run.id) ?? [])],
        }))
        if (result.bundle) {
          setExportBundle(result.bundle)
          setSelectedExportBundleId(result.bundle.id)
          setExportManifest(result.bundle.manifest)
          setExportBundleChainIds((current) => appendBundleChainId(current, result.bundle!.id))
        }
        if (result.archived) {
          setExportBundleArchiveObject(result.archived)
          setExportBundleObjectId(result.archived.object.id)
        }
        if (result.chain) {
          setExportBundleChainVerify(result.chain)
        }
        pushLog(
          "Run export backup",
          result.noOp ? `no-op at LSN ${result.currentLsn}` : `${result.mode} ${result.bundle?.id ?? "-"} to LSN ${result.currentLsn}`,
          !result.chain || result.chain.ok,
        )
      }),
    [exportBundleBaseLsn, exportBundleEncryptionKey, exportBundleObjectId, localDataClient, pushLog, run],
  )

  const refreshExportBackupRuns = useCallback(
    () =>
      run("List backup runs", () => localDataClient.listExportBackupRuns(), (result) => {
        setExportBackupRuns(result)
        pushLog("List backup runs", `${result.runs.length} runs`)
      }),
    [localDataClient, pushLog, run],
  )

  const refreshExportBackupPolicy = useCallback(
    () =>
      run("Get backup policy", () => localDataClient.getExportBackupPolicy(), (result) => {
        setExportBackupPolicy(result)
        pushLog("Get backup policy", exportBackupPolicySummary(result))
      }),
    [localDataClient, pushLog, run],
  )

  const saveDefaultExportBackupPolicy = useCallback(
    () =>
      run(
        "Save backup policy",
        () => localDataClient.setExportBackupPolicy({
          enabled: false,
          intervalMs: 0,
          archiveObject: true,
          retentionKeepLast: 8,
          retentionDeleteBundles: true,
          retentionDeleteArchiveObjects: false,
        }),
        (result) => {
          setExportBackupPolicy(result)
          pushLog("Save backup policy", exportBackupPolicySummary(result))
        },
      ),
    [localDataClient, pushLog, run],
  )

  const runExportBackupPolicy = useCallback(
    () =>
      run("Run backup policy", () => localDataClient.runExportBackupPolicy(), (result) => {
        setExportBackupPolicy({ policy: result.policy, controller: {
          enabled: result.policy.enabled,
          intervalMs: result.policy.intervalMs,
          lastRunId: result.backup.run.id,
        } })
        setExportBackupRun(result.backup)
        setExportBackupRuns((current) => ({
          runs: [result.backup.run, ...(current?.runs.filter((run) => run.id !== result.backup.run.id) ?? [])],
        }))
        if (result.retention) {
          setExportBackupRetention(result.retention)
        }
        if (result.backup.bundle) {
          setExportBundle(result.backup.bundle)
          setSelectedExportBundleId(result.backup.bundle.id)
          setExportManifest(result.backup.bundle.manifest)
        }
        if (result.backup.chain) {
          setExportBundleChainVerify(result.backup.chain)
        }
        pushLog(
          "Run backup policy",
          result.backup.noOp ? `no-op at LSN ${result.backup.currentLsn}` : `${result.backup.mode} to LSN ${result.backup.currentLsn}`,
          !result.backup.chain || result.backup.chain.ok,
        )
      }),
    [localDataClient, pushLog, run],
  )

  const planExportBackupRetention = useCallback(
    () =>
      run(
        "Plan backup retention",
        () => localDataClient.retainExportBackups({
          dryRun: true,
          keepLast: 8,
          deleteBundles: true,
          deleteArchiveObjects: false,
        }),
        (result) => {
          setExportBackupRetention(result)
          pushLog("Plan backup retention", `${result.candidates} candidate runs`)
        },
      ),
    [localDataClient, pushLog, run],
  )

  const refreshExportBundles = useCallback(
    () =>
      run("List export bundles", () => localDataClient.listExportBundles(), (result) => {
        setExportBundleList(result)
        setSelectedExportBundleId((current) => current || result.bundles[0]?.id || "")
        pushLog("List export bundles", `${result.bundles.length} bundles`)
      }),
    [localDataClient, pushLog, run],
  )

  const verifyExportBundle = useCallback(() => {
    const bundleId = selectedExportBundleId.trim()
    if (!bundleId) {
      pushLog("Verify export bundle", "select a bundle first", false)
      return Promise.resolve(undefined)
    }
    return run("Verify export bundle", () => localDataClient.verifyExportBundle(bundleId, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setExportBundleVerify(result)
      if (result.manifest) {
        setExportManifest(result.manifest)
      }
      pushLog(
        "Verify export bundle",
        result.ok ? "ok" : `${result.problems.length} problems`,
        result.ok,
      )
    })
  }, [exportBundleEncryptionKey, localDataClient, pushLog, run, selectedExportBundleId])

  const verifyExportBundleChain = useCallback(() => {
    const bundleIds = parseBundleChainIds(exportBundleChainIds)
    if (bundleIds.length === 0) {
      pushLog("Verify bundle chain", "enter bundle ids first", false)
      return Promise.resolve(undefined)
    }
    return run("Verify bundle chain", () => localDataClient.verifyExportBundleChain(bundleIds, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setExportBundleChainVerify(result)
      pushLog(
        "Verify bundle chain",
        result.ok ? `${result.bundles.length} bundles to LSN ${result.highestLsn}` : `${result.problems.length} problems`,
        result.ok,
      )
    })
  }, [exportBundleChainIds, exportBundleEncryptionKey, localDataClient, pushLog, run])

  const archiveExportBundleToObject = useCallback(() => {
    const bundleId = selectedExportBundleId.trim()
    if (!bundleId) {
      pushLog("Archive export bundle", "select a bundle first", false)
      return Promise.resolve(undefined)
    }
    const objectId = exportBundleObjectId.trim() || undefined
    return run("Archive export bundle", () => localDataClient.archiveExportBundleToObject(bundleId, { objectId }), (result) => {
      setExportBundleArchiveObject(result)
      setExportBundleObjectId(result.object.id)
      pushLog(
        "Archive export bundle",
        `${result.object.id}: ${result.files} files, ${formatBytes(result.bytes)}`,
      )
    })
  }, [exportBundleObjectId, localDataClient, pushLog, run, selectedExportBundleId])

  const importBundleFromObject = useCallback(() => {
    const objectId = exportBundleObjectId.trim()
    if (!objectId) {
      pushLog("Import bundle object", "enter an object id first", false)
      return Promise.resolve(undefined)
    }
    const bundleId = selectedExportBundleId.trim() || undefined
    return run("Import bundle object", () => localDataClient.importBundleFromObject(objectId, { bundleId }), (result) => {
      setImportBundleObject(result)
      setSelectedExportBundleId(result.bundle.id)
      setExportBundleList((current) => current ? {
        bundles: [result.bundle, ...current.bundles.filter((bundle) => bundle.id !== result.bundle.id)],
      } : { bundles: [result.bundle] })
      pushLog(
        "Import bundle object",
        `${result.bundle.id}: ${result.files} files, ${result.overwritten ? "overwritten" : "created"}`,
      )
    })
  }, [exportBundleObjectId, localDataClient, pushLog, run, selectedExportBundleId])

  const runImportBundlePreflight = useCallback(() => {
    const bundleId = selectedExportBundleId.trim()
    if (!bundleId) {
      pushLog("Import preflight", "select a bundle first", false)
      return Promise.resolve(undefined)
    }
    return run("Import preflight", () => localDataClient.importBundlePreflight(bundleId, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setImportBundlePreflight(result)
      if (result.manifest) {
        setExportManifest(result.manifest)
      }
      pushLog(
        "Import preflight",
        result.ok ? "ready for empty restore" : `${result.problems.length} problems`,
        result.ok,
      )
    })
  }, [exportBundleEncryptionKey, localDataClient, pushLog, run, selectedExportBundleId])

  const runImportBundleDeltaPreflight = useCallback(() => {
    const bundleId = selectedExportBundleId.trim()
    if (!bundleId) {
      pushLog("Delta preflight", "select a bundle first", false)
      return Promise.resolve(undefined)
    }
    return run("Delta preflight", () => localDataClient.importBundleDeltaPreflight(bundleId, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setImportBundleDeltaPreflight(result)
      if (result.manifest) {
        setExportManifest(result.manifest)
      }
      pushLog(
        "Delta preflight",
        result.ok ? `ready from LSN ${result.baseLsn}` : `${result.problems.length} problems`,
        result.ok,
      )
    })
  }, [exportBundleEncryptionKey, localDataClient, pushLog, run, selectedExportBundleId])

  const restoreImportBundle = useCallback(() => {
    const bundleId = selectedExportBundleId.trim()
    if (!bundleId) {
      pushLog("Restore import bundle", "select a bundle first", false)
      return Promise.resolve(undefined)
    }
    return run("Restore import bundle", () => localDataClient.restoreImportBundle(bundleId, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setImportBundleRestore(result)
      if (result.manifest) {
        setExportManifest(result.manifest)
      }
      pushLog(
        "Restore import bundle",
        `${result.walRecords} WAL, ${result.objects} objects, ${result.schemaProposals} schema proposals, ${bundleEncryptionSummary(result.encrypted)}, ${clusterControlSummary(result.clusterControl)}`,
        result.restored,
      )
      void refreshHealth()
    })
  }, [exportBundleEncryptionKey, localDataClient, pushLog, refreshHealth, run, selectedExportBundleId])

  const applyImportBundleDelta = useCallback(() => {
    const bundleId = selectedExportBundleId.trim()
    if (!bundleId) {
      pushLog("Apply delta bundle", "select a bundle first", false)
      return Promise.resolve(undefined)
    }
    return run("Apply delta bundle", () => localDataClient.applyImportBundleDelta(bundleId, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setImportBundleDeltaApply(result)
      if (result.manifest) {
        setExportManifest(result.manifest)
      }
      pushLog(
        "Apply delta bundle",
        `${result.walRecords} WAL, ${result.objects} objects, current LSN ${result.currentLsn}`,
        result.applied,
      )
      void refreshHealth()
    })
  }, [exportBundleEncryptionKey, localDataClient, pushLog, refreshHealth, run, selectedExportBundleId])

  const restoreImportBundleChain = useCallback(() => {
    const bundleIds = parseBundleChainIds(exportBundleChainIds)
    if (bundleIds.length === 0) {
      pushLog("Restore bundle chain", "enter bundle ids first", false)
      return Promise.resolve(undefined)
    }
    return run("Restore bundle chain", () => localDataClient.restoreImportBundleChain(bundleIds, bundleAccessOptions(exportBundleEncryptionKey)), (result) => {
      setImportBundleChainRestore(result)
      setExportBundleChainVerify(result.chain)
      setImportBundleRestore(result.base)
      setImportBundleDeltaApply(result.deltas.at(-1))
      if (result.base.manifest) {
        setExportManifest(result.base.manifest)
      }
      pushLog(
        "Restore bundle chain",
        `${result.chain.bundles.length} bundles, ${result.walRecords} WAL, ${result.objects} objects, current LSN ${result.currentLsn}`,
        result.restored,
      )
      void refreshHealth()
    })
  }, [exportBundleChainIds, exportBundleEncryptionKey, localDataClient, pushLog, refreshHealth, run])

  const flushLocalPendingWrites = useCallback(
    () =>
      run("Flush local pending writes", () => localDataClient.flushPendingWrites(), (result) => {
        pushLog("Flush local pending writes", `${result.committed}/${result.attempted} committed, ${result.remaining} remaining`)
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const clearLocalPendingWrites = useCallback(
    () =>
      run("Clear local pending writes", () => localDataClient.clearPendingWrites(), (removed) => {
        pushLog("Clear local pending writes", `${removed} removed`)
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const resetLocalPendingWrite = useCallback(
    (id: string) =>
      run("Reset local pending write", () => localDataClient.resetPendingWrite(id), (result) => {
        pushLog("Reset local pending write", result.reset ? id : "not found")
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const discardLocalPendingWrite = useCallback(
    (id: string) =>
      run("Discard local pending write", () => localDataClient.discardPendingWrite(id, { removeOptimistic: true }), (result) => {
        pushLog("Discard local pending write", result.discarded ? `${id}, optimistic ${result.removedOptimistic}` : "not found")
        void refreshLocalDataStatus()
      }),
    [localDataClient, pushLog, refreshLocalDataStatus, run],
  )

  const auditQueryParams = useCallback(() => {
    const params = new URLSearchParams({ limit: "25" })
    if (auditRecordKey.trim()) {
      params.set("recordKey", auditRecordKey.trim())
    }
    if (auditPath.trim()) {
      params.set("path", auditPath.trim())
    }
    if (auditClientMutationId.trim()) {
      params.set("clientMutationId", auditClientMutationId.trim())
    }
    return params
  }, [auditClientMutationId, auditPath, auditRecordKey])

  const auditTraceQueryParams = useCallback(() => {
    const params = new URLSearchParams({
      kind: auditTraceKind,
      limit: "25",
    })
    if (auditTraceId.trim()) {
      params.set("id", auditTraceId.trim())
    }
    if (auditTraceKind === "record" || auditTraceKind === "nestedRecord") {
      params.set("table", auditTraceTable.trim() || "rooms")
    }
    if (auditTraceKind === "record") {
      params.set("recordKey", auditTraceId.trim())
    }
    if (auditTraceKind === "nestedRecord") {
      params.set("parentKey", auditTraceParentKey.trim() || "general")
      params.set("nested", auditTraceNested.trim() || "messages")
      params.set("nestedKey", auditTraceId.trim())
    }
    if (auditTraceKind === "path") {
      params.set("path", auditTraceId.trim())
    }
    if (auditTraceKind === "clientMutation") {
      params.set("clientMutationId", auditTraceId.trim())
    }
    return params
  }, [auditTraceId, auditTraceKind, auditTraceNested, auditTraceParentKey, auditTraceTable])

  const auditReplayQueryParams = useCallback(() => {
    const params = new URLSearchParams({ kind: replayAuditKind(auditTraceKind) })
    if (auditTraceId.trim()) {
      params.set("id", auditTraceId.trim())
    }
    if (auditReplayAtLsn.trim()) {
      params.set("atLsn", auditReplayAtLsn.trim())
    }
    if (auditTraceKind === "record" || auditTraceKind === "nestedRecord") {
      params.set("table", auditTraceTable.trim() || "rooms")
    }
    if (auditTraceKind === "record") {
      params.set("recordKey", auditTraceId.trim())
    }
    if (auditTraceKind === "nestedRecord") {
      params.set("parentKey", auditTraceParentKey.trim() || "general")
      params.set("nested", auditTraceNested.trim() || "messages")
      params.set("nestedKey", auditTraceId.trim())
    }
    return params
  }, [auditReplayAtLsn, auditTraceId, auditTraceKind, auditTraceNested, auditTraceParentKey, auditTraceTable])

  const refreshAudit = useCallback(
    () =>
      run(
        auditMode === "replay" ? "Replay entity" : auditMode === "trace" ? "Trace entity" : "Refresh WAL",
        async () => {
          if (auditMode === "replay") {
            const value = await request<AuditReplayResponse>(`/v1/audit/replay?${auditReplayQueryParams()}`)
            return {
              records: [],
              nextAfterLsn: value.sourceLsn ?? 0,
              hasMore: false,
              target: value.target,
              replay: value,
            }
          }
          if (auditMode === "trace") {
            const value = await request<AuditTraceResponse>(`/v1/audit/trace?${auditTraceQueryParams()}`)
            return {
              records: value.records,
              nextAfterLsn: value.nextAfterLsn,
              hasMore: value.hasMore,
              target: value.target,
              replay: undefined,
            }
          }
          const value = await request<AuditWalResponse>(`/v1/audit/wal?${auditQueryParams()}`)
          return {
            records: value.records,
            nextAfterLsn: value.nextAfterLsn,
            hasMore: value.hasMore,
            target: undefined,
            replay: undefined,
          }
        },
        (value) => {
          setAudit({
            records: value.records,
            nextAfterLsn: value.nextAfterLsn,
            hasMore: value.hasMore,
          })
          setAuditTraceTarget(value.target)
          setAuditReplay(value.replay)
          setSelectedRecord(value.records[0])
        },
      ),
    [auditMode, auditQueryParams, auditReplayQueryParams, auditTraceQueryParams, request, run],
  )

  const refreshWalIntegrity = useCallback(
    () => run("Verify WAL", () => request<WalIntegrityReport>("/v1/admin/wal/integrity"), setWalIntegrity),
    [request, run],
  )

  const refreshSchema = useCallback(
    () =>
      run("Refresh schema", async () => {
        const [schemaValue, historyValue, reportValue, planValue, storagePolicyValue, projectionStatusValue] = await Promise.all([
          request<NextDbSchema>("/v1/schema"),
          request<SchemaHistoryResponse>("/v1/schema/history"),
          request<SchemaValidationReport>("/v1/schema/validate"),
          request<SchemaMigrationPlan>("/v1/schema/migration-plan"),
          request<SchemaStoragePolicyResponse>("/v1/schema/storage-policy"),
          request<RecordProjectionStatus>("/v1/admin/projections/status"),
        ])
        return { schemaValue, historyValue, reportValue, planValue, storagePolicyValue, projectionStatusValue }
      }, ({ schemaValue, historyValue, reportValue, planValue, storagePolicyValue, projectionStatusValue }) => {
        setSchema(schemaValue)
        setSchemaDraft(JSON.stringify(schemaValue, null, 2))
        setSchemaHistory(historyValue)
        setSchemaReport(reportValue)
        setMigrationPlan(planValue)
        setStoragePolicy(storagePolicyValue)
        setProjectionStatus(projectionStatusValue)
      }),
    [request, run],
  )

  const refreshBehaviors = useCallback(
    () => run("Refresh behaviors", () => request<BehaviorManifest[]>("/v1/behaviors"), setBehaviors),
    [request, run],
  )

  const refreshTopologyLog = useCallback(
    () => run("Refresh topology log", () => request<TopologyLogResponse>("/v1/admin/cluster/topology/log"), setTopologyLog),
    [request, run],
  )

  const refreshTopologyProposals = useCallback(
    () =>
      run(
        "Refresh topology proposals",
        () => request<TopologyProposalListResponse>("/v1/admin/cluster/topology/proposals"),
        setTopologyProposals,
      ),
    [request, run],
  )

  const refreshConnections = useCallback(
    () =>
      run("Refresh connections", async () => {
        const [connectionValue, userValue] = await Promise.all([
          request<ConnectionListResponse>("/v1/admin/connections"),
          request<ListUsersResponse>("/v1/admin/users?limit=5"),
        ])
        return { connectionValue, userValue }
      }, ({ connectionValue, userValue }) => {
        setConnections(connectionValue)
        setUsers(userValue)
      }),
    [request, run],
  )

  const disconnectUserConnections = useCallback(
    (userId: string) =>
      run("Disconnect user", () => localDataClient.disconnectConnections({
        userId,
        reason: "admin requested disconnect",
      }), (result) => {
        pushLog("Disconnect user", `${result.targeted} sessions for ${result.userId ?? userId}`)
        void refreshConnections()
      }),
    [localDataClient, pushLog, refreshConnections, run],
  )

  const refreshRealtimeChannels = useCallback(
    () => run("Refresh realtime", () => request<RealtimeChannelListResponse>("/v1/realtime/channels"), setRealtimeChannels),
    [request, run],
  )

  const refreshRealtimeState = useCallback(
    (channelId = realtimeStateDraft.channelId.trim()) => {
      if (!channelId) {
        pushLog("Realtime state", "channel id is required", false)
        return
      }
      run(
        "Refresh realtime state",
        () => request<RealtimeChannelStateResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`),
        (response) => {
          setRealtimeState(response)
          setRealtimeStateDraft((current) => ({
            ...current,
            channelId: response.channelId,
            expectedVersion: String(response.state.version),
            stateJson: JSON.stringify(response.state.state, null, 2),
          }))
        },
      )
    },
    [pushLog, realtimeStateDraft.channelId, request, run],
  )

  const refreshObjects = useCallback(
    () => run("Refresh objects", () => request<ListObjectsResponse>("/v1/objects?limit=5"), setObjectList),
    [request, run],
  )

  const refreshAll = useCallback(async () => {
    localStorage.setItem("nextdb-admin:endpoint", endpoint)
    localStorage.setItem("nextdb-admin:token", adminToken)
    localStorage.setItem("nextdb-admin:transport", adminRealtimeTransport)
    await Promise.all([
      refreshHealth(),
      refreshRuntimeActivation(),
      refreshAudit(),
      refreshWalIntegrity(),
      refreshSchema(),
      refreshBehaviors(),
      refreshTopologyLog(),
      refreshTopologyProposals(),
      refreshConnections(),
      refreshRealtimeChannels(),
      refreshObjects(),
      refreshLocalDataStatus(),
    ])
  }, [adminRealtimeTransport, adminToken, endpoint, refreshAudit, refreshBehaviors, refreshConnections, refreshHealth, refreshLocalDataStatus, refreshObjects, refreshRealtimeChannels, refreshRuntimeActivation, refreshSchema, refreshTopologyLog, refreshTopologyProposals, refreshWalIntegrity])

  useEffect(() => {
    void refreshAll()
    const timer = window.setInterval(() => {
      void refreshHealth()
      void refreshRuntimeActivation()
      void refreshConnections()
      void refreshRealtimeChannels()
      void refreshLocalDataStatus()
    }, 3000)
    return () => window.clearInterval(timer)
  }, [refreshAll, refreshConnections, refreshHealth, refreshLocalDataStatus, refreshRealtimeChannels, refreshRuntimeActivation])

  useEffect(() => {
    return () => localDataClient.close()
  }, [localDataClient])

  useEffect(() => {
    return localDataClient.watchLocalDataStatus(({ status, pendingQueue }) => {
      setLocalDataStatus(status)
      setLocalPendingQueue(pendingQueue)
    }, {
      limit: 20,
      immediate: false,
    })
  }, [localDataClient])

  useEffect(() => {
    let stop: (() => void) | undefined
    let cancelled = false
    const timer = window.setTimeout(() => {
      if (cancelled) {
        return
      }
      stop = localDataClient.onConnectionEvent((event) => {
        pushLog("Connection event", connectionEventSummary(event))
      })
    }, 250)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
      stop?.()
    }
  }, [localDataClient, pushLog])

  useEffect(() => {
    let stop: (() => void) | undefined
    let cancelled = false
    const timer = window.setTimeout(() => {
      if (cancelled) {
        return
      }
      stop = localDataClient.watchConnections(({ connections }) => {
        if (connections) {
          setConnections(connections)
        }
      }, {
        immediate: false,
      })
    }, 250)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
      stop?.()
    }
  }, [localDataClient])

  useEffect(() => {
    if (!selectedRealtimeChannelId) {
      return
    }
    let cancelled = false
    const channel = localDataClient.realtimeChannel(selectedRealtimeChannelId)
    channel.join({ role: "admin-observer", source: "admin-ui" })
      .then(() => {
        if (!cancelled) {
          void refreshRealtimeChannels()
        }
      })
      .catch((error) => {
        if (!cancelled) {
          pushLog("Realtime observe", error instanceof Error ? error.message : String(error), false)
        }
      })
    const stop = channel.watchRecentEvents(() => undefined, {
      limit: 20,
      immediate: false,
    })
    return () => {
      cancelled = true
      stop()
    }
  }, [localDataClient, pushLog, refreshRealtimeChannels, selectedRealtimeChannelId])

  const stats = useMemo(() => {
    const records = audit?.records ?? []
    return {
      messages: records.filter((record) => record.payload.type === "messageCreated").length,
      objects: records.filter((record) => record.payload.type === "objectCommitted").length,
      objectDeletes: records.filter((record) => record.payload.type === "objectDeleted").length,
      schemaVersion: schema?.version ?? 0,
      behaviors: behaviors.length,
    }
  }, [audit?.records, behaviors.length, schema?.version])

  const selectedBehaviorField = useMemo(
    () => behaviorMutationField(schema, behaviorInvoke.behavior, behaviorInvoke.mutation),
    [behaviorInvoke.behavior, behaviorInvoke.mutation, schema],
  )

  const selectedBehaviorFields = useMemo(() => behaviorInputFields(selectedBehaviorField), [selectedBehaviorField])
  const dataTargets = useMemo(() => schemaDataTargets(schema), [schema])
  const selectedDataTarget = useMemo(
    () => dataTargets.find((target) => target.id === dataTargetId) ?? dataTargets[0],
    [dataTargetId, dataTargets],
  )

  useEffect(() => {
    if (!selectedDataTarget && dataTargets[0]) {
      setDataTargetId(dataTargets[0].id)
    }
  }, [dataTargets, selectedDataTarget])

  const selectDataRecord = (record: NextDbRecord) => {
    setSelectedDataRecord(record)
    setDataKey(nestedKeyForEditor(selectedDataTarget, dataParentKey, record.key))
    setDataValue(JSON.stringify(record.value, null, 2))
  }

  const refreshDataExplorer = (after?: string) => {
    const target = selectedDataTarget
    if (!target) {
      pushLog("Data explorer", "schema target unavailable", false)
      return
    }
    if (target.nested && !dataParentKey.trim()) {
      pushLog("Data explorer", "parent key is required for nested tables", false)
      return
    }
    const params = new URLSearchParams({ limit: "25" })
    if (target.nested) {
      params.set("order", "schema")
      if (after) {
        params.set("afterCursor", after)
      }
    } else if (after) {
      params.set("afterKey", after)
    }
    const path = target.nested
      ? `/v1/records/${encodeURIComponent(target.table)}/${encodeURIComponent(dataParentKey.trim())}/${encodeURIComponent(target.nested)}?${params}`
      : `/v1/records/${encodeURIComponent(target.table)}?${params}`
    run("Refresh data", () => request<ListRecordsResponse>(path), (value) => {
      setDataPage(value)
      setDataPageCursor(after)
      const first = value.records[0]
      if (first) {
        selectDataRecord(first)
      } else {
        setSelectedDataRecord(undefined)
      }
    })
  }

  const nextDataPage = () => {
    const next = selectedDataTarget?.nested ? dataPage?.nextCursor : dataPage?.nextAfterKey
    if (!next) {
      return
    }
    setDataPageHistory((current) => [...current, dataPageCursor ?? ""])
    refreshDataExplorer(next)
  }

  const previousDataPage = () => {
    const previous = dataPageHistory.at(-1)
    if (previous === undefined) {
      return
    }
    setDataPageHistory((current) => current.slice(0, -1))
    refreshDataExplorer(previous || undefined)
  }

  const upsertDataRecord = () => {
    const target = selectedDataTarget
    if (!target) {
      pushLog("Data upsert", "schema target unavailable", false)
      return
    }
    const key = dataKey.trim()
    if (!key) {
      pushLog("Data upsert", "record key is required", false)
      return
    }
    if (target.nested && !dataParentKey.trim()) {
      pushLog("Data upsert", "parent key is required for nested tables", false)
      return
    }
    let value: unknown
    try {
      value = JSON.parse(dataValue)
    } catch {
      pushLog("Data upsert", "value must be valid JSON", false)
      return
    }
    const mutationId = `admin-data-${Date.now()}`
    const path = target.nested
      ? `/v1/records/${encodeURIComponent(target.table)}/${encodeURIComponent(dataParentKey.trim())}/${encodeURIComponent(target.nested)}/${encodeURIComponent(key)}`
      : `/v1/records/${encodeURIComponent(target.table)}/${encodeURIComponent(key)}`
    run(
      "Data upsert",
      () => request<RecordResponse>(path, postJson({ value, durability: "strict", clientMutationId: mutationId })),
      (response) => {
        setSelectedDataRecord(response.record)
        setDataValue(JSON.stringify(response.record.value, null, 2))
        void refreshAudit()
        void refreshHealth()
        setDataPageHistory([])
        refreshDataExplorer()
      },
    )
  }

  const deleteDataRecord = () => {
    const target = selectedDataTarget
    const key = dataKey.trim()
    if (!target || !key) {
      pushLog("Data delete", "select a record or enter a key first", false)
      return
    }
    if (target.nested && !dataParentKey.trim()) {
      pushLog("Data delete", "parent key is required for nested tables", false)
      return
    }
    const mutationId = `admin-data-delete-${Date.now()}`
    const path = target.nested
      ? `/v1/records/${encodeURIComponent(target.table)}/${encodeURIComponent(dataParentKey.trim())}/${encodeURIComponent(target.nested)}/${encodeURIComponent(key)}?durability=strict&clientMutationId=${encodeURIComponent(mutationId)}`
      : `/v1/records/${encodeURIComponent(target.table)}/${encodeURIComponent(key)}?durability=strict&clientMutationId=${encodeURIComponent(mutationId)}`
    run("Data delete", () => request<DeleteRecordResponse>(path, { method: "DELETE" }), () => {
      setSelectedDataRecord(undefined)
      setDataValue("{}")
      void refreshAudit()
      void refreshHealth()
      setDataPageHistory([])
      refreshDataExplorer()
    })
  }

  const reloadSchema = () =>
    run("Reload schema", () => request<SchemaReloadResponse>("/v1/admin/schema/reload", postJson({})), (value) => {
      setSchemaReport(value.report)
      setMigrationPlan(value.migration)
      void refreshSchema()
    })

  const applySchemaDraft = (dryRun: boolean) => {
    let candidate: NextDbSchema
    try {
      candidate = JSON.parse(schemaDraft) as NextDbSchema
    } catch (error) {
      pushLog("Schema apply", error instanceof Error ? error.message : "invalid schema JSON", false)
      return
    }
    run(
      dryRun ? "Dry-run schema" : "Apply schema",
      () =>
        request<SchemaApplyResponse>(
          "/v1/admin/schema/apply",
          postJson({
            schema: candidate,
            dryRun,
          }),
        ),
      (value) => {
        setSchemaApplyResult(value)
        setSchemaReport(value.report)
        setMigrationPlan(value.migration)
        pushLog("Schema apply", `${value.applied ? "applied" : "validated"} v${value.version}`)
        if (value.applied) {
          void refreshHealth()
        }
        void refreshSchema()
      },
    )
  }

  const createSnapshot = () =>
    run("Create snapshot", () => request<SnapshotResponse>("/v1/admin/snapshot", postJson({})), (value) => {
      pushLog("Snapshot", `LSN ${value.lsn}, ${value.roomCount} rooms, ${value.recordHotRecordCount} hot records`)
      void refreshHealth()
    })

  const setRuntimeDrain = (draining: boolean) =>
    run(
      draining ? "Drain runtime" : "Resume runtime",
      () =>
        request<NextDbHealth["runtimeDrain"]>(
          "/v1/admin/runtime/drain",
          postJson({
            draining,
            reason: draining ? "operator rolling restart" : "operator resume",
          }),
        ),
      () => void refreshHealth(),
    )

  const prepareRestart = () =>
    run(
      "Prepare restart",
      () =>
        request<RuntimePrepareRestartResponse>(
          "/v1/admin/runtime/prepare-restart",
          postJson({
            reason: "operator prepare restart",
            snapshot: true,
            compactWal: false,
            waitForWritesMs: 10_000,
          }),
        ),
      (value) => {
        if (value.compactWal) {
          setWalCompact(value.compactWal)
        }
        pushLog(
          "Prepare restart",
          `${value.readyForRestart ? "ready" : "not ready"} at LSN ${value.currentLsn}, writes=${value.runtimeWrites.inFlight}, waited=${value.waitedForWritesMs}ms${value.snapshot ? `, snapshot ${value.snapshot.lsn}, ${value.snapshot.recordHotRecordCount} hot records` : ""}`,
        )
        void refreshHealth()
      },
    )

  const activateRuntimeRecords = () =>
    run(
      "Activate runtime records",
      () =>
        localDataClient.activateRuntimeRecords({
          table: runtimeRecordActivation.table.trim(),
          parentKey: runtimeRecordActivation.parentKey.trim() || undefined,
          nested: runtimeRecordActivation.nested.trim() || undefined,
          key: runtimeRecordActivation.key.trim() || undefined,
          indexName: runtimeRecordActivation.indexName.trim() || undefined,
          value: runtimeRecordActivation.value.trim() || undefined,
          lower: runtimeRecordActivation.lower.trim() || undefined,
          upper: runtimeRecordActivation.upper.trim() || undefined,
          afterKey: runtimeRecordActivation.afterKey.trim() || undefined,
          afterCursor: runtimeRecordActivation.afterCursor.trim() || undefined,
          order: runtimeRecordActivation.order === "schema" ? "schema" : undefined,
          limit: numericInput(runtimeRecordActivation.limit),
        }),
      (value) => {
        pushLog("Activate runtime records", `${value.activated}/${value.found} records in ${value.table}`)
        void refreshHealth()
        void refreshRuntimeActivation()
      },
    )

  const evictRuntimeRecords = () =>
    run(
      "Evict runtime records",
      () =>
        localDataClient.evictRuntimeRecords({
          table: runtimeRecordActivation.table.trim(),
          parentKey: runtimeRecordActivation.parentKey.trim() || undefined,
          nested: runtimeRecordActivation.nested.trim() || undefined,
          key: runtimeRecordActivation.key.trim() || undefined,
          afterKey: runtimeRecordActivation.afterKey.trim() || undefined,
          limit: numericInput(runtimeRecordActivation.limit),
        }),
      (value) => {
        pushLog("Evict runtime records", `${value.evicted}/${value.found} records in ${value.table}`)
        void refreshHealth()
        void refreshRuntimeActivation()
      },
    )

  const activateRuntimeRoom = () =>
    run(
      "Activate runtime room",
      () =>
        localDataClient.activateRuntimeRoom({
          roomId: runtimeRoomActivation.roomId.trim(),
          limit: numericInput(runtimeRoomActivation.limit),
        }),
      (value) => {
        pushLog("Activate runtime room", `${value.roomId}: ${value.source}, ${value.found} messages`)
        void refreshHealth()
        void refreshRuntimeActivation()
      },
    )

  const evictRuntimeRoom = () =>
    run(
      "Evict runtime room",
      () =>
        localDataClient.evictRuntimeRoom({
          roomId: runtimeRoomActivation.roomId.trim(),
          limit: numericInput(runtimeRoomActivation.limit),
        }),
      (value) => {
        pushLog("Evict runtime room", `${value.roomId}: ${value.evicted ? "evicted" : "not resident"}`)
        void refreshHealth()
        void refreshRuntimeActivation()
      },
    )

  const compactWal = () =>
    run("Compact WAL", () => request<WalCompactResponse>("/v1/admin/wal/compact", postJson({})), (value) => {
      setWalCompact(value)
      pushLog("WAL compact", `${value.archived} archived, ${value.retained} retained`)
      void refreshHealth()
      void refreshAudit()
    })

  const sealWalChecksums = () =>
    run(
      "Seal WAL checksums",
      () => request<WalChecksumSealResponse>("/v1/admin/wal/seal-checksums", postJson({})),
      (value) => {
        setWalChecksumSeal(value)
        pushLog("WAL seal", `${value.sealed} sealed, ${value.rewrittenFiles} files rewritten`)
        void refreshWalIntegrity()
        void refreshAudit()
        void refreshHealth()
      },
    )

  const walRetentionParams = (dryRun: boolean) => {
    const params = new URLSearchParams({ dryRun: String(dryRun) })
    const beforeLsn = Number.parseInt(retentionBeforeLsn, 10)
    const beforeTimestampMs = Number.parseInt(retentionBeforeTimestampMs, 10)
    if (Number.isFinite(beforeLsn) && beforeLsn > 0) {
      params.set("beforeLsn", String(beforeLsn))
    } else if (health?.lastCompactionLsn) {
      params.set("beforeLsn", String(health.lastCompactionLsn + 1))
    }
    if (Number.isFinite(beforeTimestampMs) && beforeTimestampMs > 0) {
      params.set("beforeTimestampMs", String(beforeTimestampMs))
    }
    return params
  }

  const retainWalArchives = (dryRun: boolean) =>
    run(
      dryRun ? "WAL retention dry-run" : "Apply WAL retention",
      () =>
        request<WalArchiveRetentionResponse>(
          `/v1/admin/wal/archive/retention?${walRetentionParams(dryRun)}`,
          postJson({}),
        ),
      (value) => {
        setWalRetention(value)
        pushLog("WAL retention", `${value.candidates} candidates, ${value.deleted} deleted, ${value.retained} retained`)
        void refreshHealth()
        void refreshAudit()
      },
    )

  const rebuildProjections = () =>
    run("Rebuild projections", () => request<ProjectionRebuildResponse>("/v1/admin/projections/rebuild", postJson({})), (value) => {
      pushLog("Projection rebuild", `${value.messages} messages, ${value.records} records, ${value.objectRefs} refs`)
      void request<RecordProjectionStatus>("/v1/admin/projections/status").then(setProjectionStatus)
    })

  const freezeFirstShard = () => {
    const shard = health?.clusterTopology.shards[0]
    if (!shard) return
    run(
      "Freeze shard",
      () => request(`/v1/admin/cluster/shards/${shard.shard}/freeze`, postJson({ reason: "handoff preparation" })),
      () => void refreshHealth(),
    )
  }

  const planFirstHandoff = () => {
    const shard = health?.clusterTopology.shards[0]
    const targetOwner = shard?.replicas[0]
    if (!shard || !targetOwner) return
    run(
      "Plan handoff",
      () => request<HandoffPlanResponse>("/v1/admin/cluster/handoff/plan", postJson({ shard: shard.shard, targetOwner })),
      setHandoffPlan,
    )
  }

  const planLocalFailover = () => {
    const shard = health?.clusterTopology.shards[0]
    if (!shard) return
    run(
      "Plan failover",
      () => request<FailoverPlanResponse>("/v1/admin/cluster/failover/plan", postJson({ shard: shard.shard })),
      setFailoverPlan,
    )
  }

  const startLocalFailoverProposal = () => {
    const shard = health?.clusterTopology.shards[0]
    if (!shard) return
    run(
      "Start failover proposal",
      () => request<FailoverProposalResponse>("/v1/admin/cluster/failover/proposals", postJson({ shard: shard.shard })),
      (value) => {
        setFailoverProposal(value)
        setFailoverPlan(value.plan)
        void refreshTopologyProposals()
        void refreshHealth()
      },
    )
  }

  const startFirstWorkflow = () => {
    const shard = health?.clusterTopology.shards[0]
    const targetOwner = shard?.replicas[0]
    if (!shard || !targetOwner) return
    run(
      "Start handoff",
      () => request<HandoffWorkflowResponse>("/v1/admin/cluster/handoff/workflows", postJson({ shard: shard.shard, targetOwner })),
      (value) => {
        setHandoffWorkflow(value.workflow)
        setHandoffPlan(value.plan)
        void refreshHealth()
      },
    )
  }

  const stepWorkflow = () => {
    const workflow = handoffWorkflow ?? health?.handoffWorkflows[0]
    if (!workflow) return
    run(
      "Step handoff",
      () => request<HandoffWorkflowResponse>(`/v1/admin/cluster/handoff/workflows/${workflow.id}/step`, postJson({})),
      (value) => {
        setHandoffWorkflow(value.workflow)
        setHandoffPlan(value.plan)
        void refreshHealth()
      },
    )
  }

  const applyWorkflow = () => {
    const workflow = handoffWorkflow ?? health?.handoffWorkflows[0]
    if (!workflow) return
    run(
      "Apply handoff",
      () => request<HandoffApplyResponse>(`/v1/admin/cluster/handoff/workflows/${workflow.id}/apply`, postJson({})),
      (value) => {
        setHandoffWorkflow(value.workflow)
        void refreshHealth()
        void refreshTopologyLog()
        void refreshTopologyProposals()
      },
    )
  }

  const autoWorkflow = () => {
    const workflow = handoffWorkflow ?? health?.handoffWorkflows[0]
    if (!workflow) return
    run(
      "Auto handoff",
      () => request<HandoffAutoResponse>(`/v1/admin/cluster/handoff/workflows/${workflow.id}/auto`, postJson({})),
      (value) => {
        setHandoffWorkflow(value.workflow)
        setHandoffPlan(value.plan)
        void refreshHealth()
        if (value.applied) {
          void refreshTopologyLog()
          void refreshTopologyProposals()
        }
      },
    )
  }

  const retryLatestProposal = () => {
    const proposal = topologyProposals?.proposals.at(-1)
    if (!proposal || proposal.phase === "committed") return
    run(
      "Retry topology proposal",
      () => request<TopologyProposalResponse>(`/v1/admin/cluster/topology/proposals/${proposal.id}/retry`, postJson({})),
      () => {
        void refreshHealth()
        void refreshTopologyProposals()
      },
    )
  }

  const abortLatestProposal = () => {
    const proposal = topologyProposals?.proposals.at(-1)
    if (!proposal || proposal.phase === "committed") return
    run(
      "Abort topology proposal",
      () => request<TopologyProposalResponse>(`/v1/admin/cluster/topology/proposals/${proposal.id}/abort`, postJson({})),
      () => {
        void refreshHealth()
        void refreshTopologyProposals()
      },
    )
  }

  const dryRunGc = () =>
    run("Object GC dry-run", () => request<ObjectGcResponse>("/v1/admin/objects/gc?dryRun=true", postJson({})), (value) => {
      setObjectGc(value)
      void refreshObjects()
    })

  const forceDryRunGc = () =>
    run(
      "Object GC force dry-run",
      () => request<ObjectGcResponse>("/v1/admin/objects/gc?dryRun=true&force=true", postJson({})),
      (value) => {
        setObjectGc(value)
        void refreshObjects()
      },
    )

  const reloadBehaviors = () =>
    run("Reload behaviors", () => request<{ loaded: number }>("/v1/admin/behaviors/reload", postJson({})), (value) => {
      pushLog("Behaviors", `${value.loaded} modules loaded`)
      void refreshBehaviors()
    })

  const invokeBehavior = () => {
    const behavior = behaviorInvoke.behavior.trim()
    const mutation = behaviorInvoke.mutation.trim()
    const userId = behaviorInvoke.userId.trim()
    if (!behavior || !mutation) {
      pushLog("Invoke behavior", "behavior and mutation are required", false)
      return
    }
    let input: unknown
    try {
      input = parseBehaviorInput(selectedBehaviorField, behaviorInvoke.input)
    } catch (error) {
      pushLog("Invoke behavior", error instanceof Error ? error.message : String(error), false)
      return
    }
    const roomId = isRecord(input) && typeof input.roomId === "string" ? input.roomId : undefined
    const selectedManifest = behaviors.find((entry) => entry.name === behavior)
    const read = roomId && behaviorAllowsRead(selectedManifest, "latestMessages")
      ? { latestMessages: [{ roomId, limit: 5 }] }
      : undefined
    run(
      "Invoke behavior",
      () =>
        request<BehaviorInvokeResponse>(
          "/v1/behaviors/invoke",
          postJson({
            behavior,
            mutation,
            userId: userId || undefined,
            input,
            read,
          }),
        ),
      (value) => {
        setBehaviorResult(value)
        pushLog("Behavior invoke", `${value.committed.length} commits`)
        void refreshAudit()
        void refreshHealth()
      },
    )
  }

  const invalidateAllClientCaches = () =>
    run(
      "Invalidate client caches",
      () =>
        request<ClientCacheInvalidateResponse>(
          "/v1/admin/cache/invalidate",
          postJson({ scope: "all", reason: "admin requested full cache refresh" }),
        ),
      (value) => {
        pushLog("Client cache", `generation ${value.entry.generation}`)
        void refreshHealth()
      },
    )

  const invalidateObjectCache = (objectId: string) =>
    run(
      "Invalidate object cache",
      () =>
        request<ClientCacheInvalidateResponse>(
          "/v1/admin/cache/invalidate",
          postJson({ scope: "object", key: objectId, reason: "admin requested object cache refresh" }),
        ),
      (value) => {
        pushLog("Object cache", `${objectId} generation ${value.entry.generation}`)
        void refreshHealth()
        void refreshObjects()
      },
    )

  const loadCacheProfileDraft = () => {
    const profile = health?.clientCache?.profile
    if (!profile) {
      pushLog("Client cache", "refresh health first", false)
      return
    }
    setCacheProfileDraft({
      leaseTtlMs: String(profile.leaseTtlMs),
      maxObjects: String(profile.maxObjects),
      maxObjectBytes: String(profile.maxObjectBytes),
      maxRoomMessages: String(profile.maxRoomMessages),
      maxUserEvents: String(profile.maxUserEvents),
      maxRecordsPerTable: String(profile.maxRecordsPerTable),
      maxNestedPartitions: String(profile.maxNestedPartitions ?? 0),
      maxPendingWrites: String(profile.maxPendingWrites ?? 0),
      maxPendingWriteBytes: String(profile.maxPendingWriteBytes ?? 0),
      offlineWrites: profile.offlineWrites ? "true" : "false",
    })
  }

  const updateClientCacheProfile = () => {
    const currentVersion = health?.clientCache?.profile.version
    if (currentVersion === undefined) {
      pushLog("Client cache profile", "refresh health first", false)
      return
    }
    const numeric = [
      ["leaseTtlMs", cacheProfileDraft.leaseTtlMs, 1],
      ["maxObjects", cacheProfileDraft.maxObjects, 0],
      ["maxObjectBytes", cacheProfileDraft.maxObjectBytes, 0],
      ["maxRoomMessages", cacheProfileDraft.maxRoomMessages, 0],
      ["maxUserEvents", cacheProfileDraft.maxUserEvents, 0],
      ["maxRecordsPerTable", cacheProfileDraft.maxRecordsPerTable, 0],
      ["maxNestedPartitions", cacheProfileDraft.maxNestedPartitions, 0],
      ["maxPendingWrites", cacheProfileDraft.maxPendingWrites, 0],
      ["maxPendingWriteBytes", cacheProfileDraft.maxPendingWriteBytes, 0],
    ] as const
    const patch: ClientCacheProfileUpdateOptions = {
      expectedVersion: currentVersion,
      reason: "admin updated client cache profile",
    }
    for (const [field, raw, min] of numeric) {
      const value = raw.trim()
      if (!value) {
        continue
      }
      const parsed = Number(value)
      if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < min) {
        pushLog("Client cache profile", `${field} must be an integer >= ${min}`, false)
        return
      }
      patch[field] = parsed
    }
    if (cacheProfileDraft.offlineWrites !== "unchanged") {
      patch.offlineWrites = cacheProfileDraft.offlineWrites === "true"
    }
    if (Object.keys(patch).length <= 2) {
      pushLog("Client cache profile", "enter at least one value to change", false)
      return
    }
    run(
      "Update cache profile",
      () => localDataClient.updateClientCacheProfile(patch),
      (value) => {
        pushLog("Client cache profile", `v${value.profile.version}, invalidation ${value.invalidation.generation}`)
        setCacheProfileDraft((current) => ({
          ...current,
          offlineWrites: "unchanged",
        }))
        void refreshHealth()
      },
    )
  }

  const selectRealtimeChannelState = (channelId: string) => {
    const channel = realtimeChannels?.channels.find((candidate) => candidate.channelId === channelId)
    setSelectedRealtimeChannelId(channelId)
    setRealtimeStateDraft((current) => ({
      ...current,
      channelId,
      fromUserId: current.fromUserId || channel?.members[0]?.userId || "",
      expectedVersion: String(channel?.stateVersion ?? 0),
    }))
    refreshRealtimeState(channelId)
  }

  const updateRealtimeChannelState = () => {
    const channelId = realtimeStateDraft.channelId.trim()
    const fromUserId = realtimeStateDraft.fromUserId.trim()
    if (!channelId) {
      pushLog("Realtime state", "channel id is required", false)
      return
    }
    if (!fromUserId) {
      pushLog("Realtime state", "from user id is required", false)
      return
    }
    let stateValue: unknown
    try {
      stateValue = JSON.parse(realtimeStateDraft.stateJson || "null")
    } catch {
      pushLog("Realtime state", "state must be valid JSON", false)
      return
    }
    const expectedVersionRaw = realtimeStateDraft.expectedVersion.trim()
    const expectedVersion = expectedVersionRaw ? Number(expectedVersionRaw) : undefined
    if (expectedVersion !== undefined && (!Number.isInteger(expectedVersion) || expectedVersion < 0)) {
      pushLog("Realtime state", "expected version must be an integer >= 0", false)
      return
    }
    run(
      "Update realtime state",
      () =>
        request<RealtimeChannelStateUpdateResponse>(
          `/v1/realtime/channels/${encodeURIComponent(channelId)}/state`,
          postJson({
            fromUserId,
            expectedVersion,
            state: stateValue,
          }),
        ),
      (response) => {
        setRealtimeState({ channelId: response.channelId, state: response.state })
        setRealtimeStateDraft((current) => ({
          ...current,
          expectedVersion: String(response.state.version),
          stateJson: JSON.stringify(response.state.state, null, 2),
        }))
        pushLog("Realtime state", `v${response.state.version}, delivered ${response.delivered}`)
        void refreshRealtimeChannels()
      },
    )
  }

  const broadcastRealtimeChannelEvent = () => {
    const channelId = realtimeStateDraft.channelId.trim()
    const kind = realtimeEventDraft.kind.trim()
    if (!channelId) {
      pushLog("Realtime broadcast", "channel id is required", false)
      return
    }
    if (!kind) {
      pushLog("Realtime broadcast", "event kind is required", false)
      return
    }
    let payload: unknown
    try {
      payload = JSON.parse(realtimeEventDraft.payloadJson || "null")
    } catch {
      pushLog("Realtime broadcast", "payload must be valid JSON", false)
      return
    }
    run(
      "Broadcast realtime event",
      async () => {
        const channel = localDataClient.realtimeChannel(channelId)
        await channel.join({ role: "admin-observer", source: "admin-ui" })
        return channel.broadcast(kind, payload, {
          includeSelf: realtimeEventDraft.includeSelf,
        })
      },
      (response) => {
        pushLog("Realtime broadcast", `#${response.sequence}, delivered ${response.delivered}`)
        void refreshRealtimeChannels()
        void refreshLocalDataStatus()
      },
    )
  }

  const sendRealtimeChannelSignal = () => {
    const channelId = realtimeStateDraft.channelId.trim()
    const toUserId = realtimeSignalDraft.toUserId.trim()
    const kind = realtimeSignalDraft.kind.trim()
    if (!channelId) {
      pushLog("Realtime signal", "channel id is required", false)
      return
    }
    if (!toUserId) {
      pushLog("Realtime signal", "target user is required", false)
      return
    }
    if (!kind) {
      pushLog("Realtime signal", "signal kind is required", false)
      return
    }
    let payload: unknown
    try {
      payload = JSON.parse(realtimeSignalDraft.payloadJson || "null")
    } catch {
      pushLog("Realtime signal", "payload must be valid JSON", false)
      return
    }
    run(
      "Send realtime signal",
      async () => {
        const channel = localDataClient.realtimeChannel(channelId)
        await channel.join({ role: "admin-observer", source: "admin-ui" })
        return channel.signal(toUserId, kind, payload)
      },
      (response) => {
        pushLog("Realtime signal", `#${response.sequence}, delivered ${String(response.delivered)}, sessions ${response.deliveredSessions}`)
        void refreshRealtimeChannels()
      },
    )
  }

  const invalidateScopedClientCache = () => {
    const key = cacheInvalidation.key.trim()
    if (!key) {
      pushLog("Client cache", `${cacheInvalidation.scope} key is required`, false)
      return
    }
    let nestedParts: string[] = []
    if (cacheInvalidation.scope === "nestedTable") {
      const firstSeparator = key.indexOf(":")
      const lastSeparator = key.lastIndexOf(":")
      if (firstSeparator <= 0 || lastSeparator <= firstSeparator) {
        pushLog("Client cache", "nestedTable key must be table:parentKey:nested", false)
        return
      }
      nestedParts = [
        key.slice(0, firstSeparator).trim(),
        key.slice(firstSeparator + 1, lastSeparator).trim(),
        key.slice(lastSeparator + 1).trim(),
      ]
      if (nestedParts.some((part) => !part)) {
        pushLog("Client cache", "nestedTable key must be table:parentKey:nested", false)
        return
      }
    }
    const rawMinValidLsn = cacheInvalidation.minValidLsn.trim()
    const minValidLsn = rawMinValidLsn ? Number(rawMinValidLsn) : undefined
    if (minValidLsn !== undefined && (!Number.isFinite(minValidLsn) || minValidLsn < 0)) {
      pushLog("Client cache", "min valid LSN must be a non-negative number", false)
      return
    }
    run(
      "Invalidate scoped cache",
      () =>
        request<ClientCacheInvalidateResponse>(
          "/v1/admin/cache/invalidate",
          postJson({
            scope: cacheInvalidation.scope,
            key: cacheInvalidation.scope === "nestedTable" ? undefined : key,
            table: cacheInvalidation.scope === "nestedTable" ? nestedParts[0] : undefined,
            parentKey: cacheInvalidation.scope === "nestedTable" ? nestedParts[1] : undefined,
            nested: cacheInvalidation.scope === "nestedTable" ? nestedParts[2] : undefined,
            minValidLsn,
            reason: `admin requested ${cacheInvalidation.scope} cache refresh`,
          }),
        ),
      (value) => {
        pushLog("Client cache", `${cacheInvalidation.scope}:${key} generation ${value.entry.generation}`)
        setCacheInvalidation((current) => ({ ...current, key: "", minValidLsn: "" }))
        void refreshHealth()
      },
    )
  }

  const deleteObject = (objectId: string, force = false) =>
    run(
      force ? "Force delete object" : "Delete object",
      () =>
        request<DeleteObjectResponse>(
          `/v1/objects/${encodeURIComponent(objectId)}?${new URLSearchParams({
            clientMutationId: `admin-object-delete-${Date.now()}-${objectId}`,
            ...(force ? { force: "true" } : {}),
          })}`,
          { method: "DELETE" },
        ),
      (value) => {
        pushLog("Object delete", `${value.objectId} ${value.deleted ? `LSN ${value.lsn}` : "not found"}`)
        void refreshObjects()
        void refreshAudit()
        void refreshHealth()
      },
    )

  useEffect(() => {
    setBehaviorInvoke((current) => {
      const behavior = behaviors.find((entry) => entry.name === current.behavior) ?? behaviors[0]
      if (!behavior) return current
      const mutation = behavior.mutations.includes(current.mutation) ? current.mutation : behavior.mutations[0] ?? ""
      const input = mergeBehaviorInput(behaviorMutationField(schema, behavior.name, mutation), current.input)
      if (current.behavior === behavior.name && current.mutation === mutation && shallowEqual(current.input, input)) {
        return current
      }
      return { ...current, behavior: behavior.name, mutation, input }
    })
  }, [behaviors, schema])

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <Database size={22} />
          <div>
            <strong>NextDB</strong>
            <span>Admin</span>
          </div>
        </div>
        <nav className="nav">
          <NavItem icon={<Gauge />} label="Runtime" active />
          <NavItem icon={<FileClock />} label="WAL" />
          <NavItem icon={<Database />} label="Data" />
          <NavItem icon={<Waypoints />} label="Actors" />
          <NavItem icon={<Archive />} label="Objects" />
          <NavItem icon={<ShieldCheck />} label="Schema" />
          <NavItem icon={<Boxes />} label="Behaviors" />
          <NavItem icon={<Cable />} label="Realtime" />
        </nav>
        <form
          className="sidebar-footer"
          onSubmit={(event) => {
            event.preventDefault()
            void refreshAll()
          }}
        >
          <span>Endpoint</span>
          <input value={endpoint} onChange={(event) => setEndpoint(event.target.value)} />
          <span>Admin token</span>
          <input
            value={adminToken}
            type="password"
            autoComplete="off"
            placeholder="optional"
            onChange={(event) => setAdminToken(event.target.value)}
          />
          <span>Realtime transport</span>
          <select
            data-testid="admin-realtime-transport"
            value={adminRealtimeTransport}
            onChange={(event) => setAdminRealtimeTransport(normalizeAdminRealtimeTransport(event.target.value))}
          >
            <option value="websocket">WebSocket</option>
            <option value="jsonl">HTTP JSONL</option>
          </select>
          <button type="submit">
            <RefreshCw size={15} />
            Connect
          </button>
        </form>
      </aside>

      <main className="content">
        <header className="topbar">
          <div>
            <h1>NextDB Admin</h1>
            <p>Runtime, event sourcing, virtual actors, schema, objects, and behavior modules.</p>
          </div>
          <div className="status-strip">
            <StatusDot ok={Boolean(readiness?.ok ?? health?.ok)} />
            <span>{readiness ? readinessStatusLabel(readiness) : health?.draining ? "Draining" : health?.ok ? "Online" : "Disconnected"}</span>
            <button className="icon-button" onClick={() => void refreshAll()} aria-label="Refresh all">
              <RefreshCw size={17} />
            </button>
          </div>
        </header>

        <section className="metrics">
          <Metric title="Current LSN" value={health?.currentLsn ?? 0} icon={<Activity />} />
          <Metric title="Hot Rooms" value={`${health?.hotRoomCount ?? 0} / ${health?.maxHotRooms ?? 0}`} icon={<Waypoints />} />
          <Metric title="Schema Version" value={stats.schemaVersion} icon={<ShieldCheck />} />
          <Metric title="Readiness" value={readinessSummary(readiness)} icon={<Network />} />
        </section>

        <section className="grid">
          <Panel className="span-8" title="WAL Audit Stream" action={<button onClick={() => void refreshAudit()}>Refresh</button>}>
            <form
              className="audit-filters"
              onSubmit={(event) => {
                event.preventDefault()
                void refreshAudit()
              }}
            >
              <label>
                <span>Mode</span>
                <select
                  data-testid="audit-mode"
                  onChange={(event) => {
                    const nextMode = event.target.value as AuditMode
                    setAuditMode(nextMode)
                    if (nextMode === "replay" && !replaySupportedAuditKind(auditTraceKind)) {
                      setAuditTraceKind("record")
                    }
                  }}
                  value={auditMode}
                >
                  <option value="wal">WAL filter</option>
                  <option value="trace">Entity trace</option>
                  <option value="replay">Entity replay</option>
                </select>
              </label>
              {auditMode !== "wal" && (
                <>
                  <label>
                    <span>Entity</span>
                    <select
                      data-testid="audit-trace-kind"
                      onChange={(event) => setAuditTraceKind(event.target.value as AuditTraceKind)}
                      value={auditTraceKind}
                    >
                      {auditMode === "trace" && <option value="room">Room</option>}
                      <option value="user">User</option>
                      <option value="object">Object</option>
                      <option value="record">Record</option>
                      <option value="nestedRecord">Nested record</option>
                      {auditMode === "trace" && <option value="path">Path</option>}
                      {auditMode === "trace" && <option value="clientMutation">Client mutation</option>}
                    </select>
                  </label>
                  <label>
                    <span>{auditTraceKind === "path" ? "Path" : auditTraceKind === "clientMutation" ? "Mutation id" : "Id"}</span>
                    <input
                      data-testid="audit-trace-id"
                      onChange={(event) => setAuditTraceId(event.target.value)}
                      placeholder={auditTracePlaceholder(auditTraceKind)}
                      value={auditTraceId}
                    />
                  </label>
                  {auditMode === "replay" && (
                    <label>
                      <span>At LSN</span>
                      <input
                        data-testid="audit-replay-lsn"
                        onChange={(event) => setAuditReplayAtLsn(event.target.value)}
                        placeholder="latest"
                        value={auditReplayAtLsn}
                      />
                    </label>
                  )}
                  {(auditTraceKind === "record" || auditTraceKind === "nestedRecord") && (
                    <label>
                      <span>Table</span>
                      <input
                        data-testid="audit-trace-table"
                        onChange={(event) => setAuditTraceTable(event.target.value)}
                        placeholder="rooms"
                        value={auditTraceTable}
                      />
                    </label>
                  )}
                  {auditTraceKind === "nestedRecord" && (
                    <>
                      <label>
                        <span>Parent key</span>
                        <input
                          data-testid="audit-trace-parent"
                          onChange={(event) => setAuditTraceParentKey(event.target.value)}
                          placeholder="general"
                          value={auditTraceParentKey}
                        />
                      </label>
                      <label>
                        <span>Nested</span>
                        <input
                          data-testid="audit-trace-nested"
                          onChange={(event) => setAuditTraceNested(event.target.value)}
                          placeholder="messages"
                          value={auditTraceNested}
                        />
                      </label>
                    </>
                  )}
                </>
              )}
              {auditMode === "wal" && (
                <>
              <label>
                <span>Record key</span>
                <input
                  onChange={(event) => setAuditRecordKey(event.target.value)}
                  placeholder="room/message/object id"
                  value={auditRecordKey}
                />
              </label>
              <label>
                <span>Path</span>
                <input
                  onChange={(event) => setAuditPath(event.target.value)}
                  placeholder="tables/rooms/general"
                  value={auditPath}
                />
              </label>
              <label>
                <span>Mutation id</span>
                <input
                  onChange={(event) => setAuditClientMutationId(event.target.value)}
                  placeholder="clientMutationId"
                  value={auditClientMutationId}
                />
              </label>
                </>
              )}
              <button type="submit">{auditMode === "replay" ? "Replay Entity" : auditMode === "trace" ? "Trace Entity" : "Trace"}</button>
            </form>
            {auditTraceTarget && (
              <p className="audit-target">
                Trace target: <strong>{traceTargetSummary(auditTraceTarget)}</strong>
              </p>
            )}
            {auditReplay && (
              <div className="audit-replay" data-testid="audit-replay-result">
                <div>
                  <strong>Replay {auditReplay.status}</strong>
                  <span>at LSN {auditReplayAtLsn.trim() ? auditReplay.atLsn : "latest"}, source {auditReplay.sourceLsn ?? "-"}</span>
                </div>
                <pre>{JSON.stringify(auditReplay.record ?? auditReplay.user ?? auditReplay.object ?? auditReplay.delete ?? null, null, 2)}</pre>
              </div>
            )}
            {audit && (
              <p className="audit-target">
                {audit.records.length} WAL records, next after LSN {audit.nextAfterLsn}{audit.hasMore ? ", more available" : ""}
              </p>
            )}
            <div className="table">
              <div className="table-row table-head">
                <span>LSN</span>
                <span>Type</span>
                <span>Durability</span>
                <span>Schema</span>
                <span>Subject</span>
              </div>
              {(audit?.records ?? []).map((record, index) => (
                <button
                  className={`table-row row-button ${selectedRecord?.lsn === record.lsn ? "selected" : ""}`}
                  key={walRecordRowKey(record, index)}
                  onClick={() => setSelectedRecord(record)}
                >
                  <span>{record.lsn}</span>
                  <span>{record.payload.type}</span>
                  <span>{record.durability}</span>
                  <span>v{record.schemaVersion}</span>
                  <span>{recordSubject(record)}</span>
                </button>
              ))}
            </div>
          </Panel>

          <Panel className="span-4" title="WAL Integrity" action={<button onClick={() => void refreshWalIntegrity()}>Verify</button>}>
            <div className={`integrity-summary ${walIntegrity ? (walIntegrity.ok && walIntegrity.issueCount === 0 ? "ok" : "warn") : ""}`}>
              <ShieldCheck size={20} />
              <div>
                <strong>{walIntegrity ? (walIntegrity.ok ? (walIntegrity.issueCount ? "Warnings" : "Verified") : "Issues found") : "Not checked"}</strong>
                <span>{walIntegrity ? `${walIntegrity.recordCount} records across ${walIntegrity.fileCount} files` : "Run verification against active WAL and archives"}</span>
              </div>
            </div>
            <Row label="Shards" value={String(walIntegrity?.shardCount ?? "-")} />
            <Row label="Highest LSN" value={String(walIntegrity?.highestLsn ?? "-")} />
            <Row label="Unique LSNs" value={String(walIntegrity?.uniqueLsnCount ?? "-")} />
            <Row label="Duplicate LSNs" value={String(walIntegrity?.duplicateLsnCount ?? "-")} tone={walIntegrity?.duplicateLsnCount ? "warn" : "good"} />
            <Row label="Checksum missing" value={String(walIntegrity?.checksumMissingCount ?? "-")} tone={walIntegrity?.checksumMissingCount ? "warn" : "good"} />
            <Row label="Checksum mismatch" value={String(walIntegrity?.checksumMismatchCount ?? "-")} tone={walIntegrity?.checksumMismatchCount ? "warn" : "good"} />
            <Row label="Gaps" value={String(walIntegrity?.gaps.length ?? "-")} tone={walIntegrity?.gaps.length ? "warn" : "good"} />
            <Row label="Issues" value={walIntegrity ? `${walIntegrity.issueCount}${walIntegrity.issuesTruncated ? "+" : ""}` : "-"} tone={walIntegrity?.issueCount ? "warn" : "good"} />
            <div className="button-row compact-buttons">
              <button onClick={() => void sealWalChecksums()}>
                <ShieldCheck size={15} />
                Seal checksums
              </button>
              <button onClick={() => void refreshWalIntegrity()}>
                <RefreshCw size={15} />
                Verify
              </button>
            </div>
            {walChecksumSeal && (
              <div className="seal-result">
                <Row label="Sealed" value={String(walChecksumSeal.sealed)} />
                <Row label="Already sealed" value={String(walChecksumSeal.alreadySealed)} />
                <Row label="Rewritten files" value={String(walChecksumSeal.rewrittenFiles)} />
              </div>
            )}
            <div className="issue-list">
              {(walIntegrity?.issues ?? []).slice(0, 4).map((issue, index) => (
                <div className={`issue-row ${issue.severity}`} key={`${issue.code}-${index}`}>
                  <strong>{issue.code}</strong>
                  <span>{issue.message}</span>
                </div>
              ))}
              {walIntegrity && walIntegrity.issues.length === 0 && <EmptyLine text="No WAL issues detected" />}
            </div>
          </Panel>

          <Panel className="span-8" title="Data Explorer" action={<button onClick={() => refreshDataExplorer()}>Refresh</button>}>
            <div className="data-explorer">
              <form
                className="data-toolbar"
                onSubmit={(event) => {
                  event.preventDefault()
                  setDataPageHistory([])
                  refreshDataExplorer()
                }}
              >
                <label>
                  <span>Target</span>
                  <select
                    value={selectedDataTarget?.id ?? ""}
                    onChange={(event) => {
                      setDataTargetId(event.target.value)
                      setDataPage(undefined)
                      setDataPageCursor(undefined)
                      setDataPageHistory([])
                      setSelectedDataRecord(undefined)
                    }}
                  >
                    {dataTargets.map((target) => (
                      <option key={target.id} value={target.id}>
                        {target.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>Parent key</span>
                  <input
                    disabled={!selectedDataTarget?.nested}
                    onChange={(event) => setDataParentKey(event.target.value)}
                    placeholder={selectedDataTarget?.nested ? "parent partition key" : "top-level table"}
                    value={dataParentKey}
                  />
                </label>
                <label>
                  <span>Record key</span>
                  <input onChange={(event) => setDataKey(event.target.value)} placeholder="key" value={dataKey} />
                </label>
                <button type="submit">Load</button>
              </form>

              <div className="data-body">
                <div className="data-records">
                  <div className="table data-table">
                    <div className="table-row table-head">
                      <span>Key</span>
                      <span>LSN</span>
                      <span>Updated</span>
                      <span>Preview</span>
                    </div>
                    {(dataPage?.records ?? []).map((record) => (
                      <button
                        className={`table-row row-button ${selectedDataRecord?.path === record.path ? "selected" : ""}`}
                        key={record.path}
                        onClick={() => selectDataRecord(record)}
                      >
                        <span>{nestedKeyForEditor(selectedDataTarget, dataParentKey, record.key)}</span>
                        <span>{record.lsn}</span>
                        <span>{new Date(record.updatedAtMs).toLocaleTimeString()}</span>
                        <span>{recordValuePreview(record.value)}</span>
                      </button>
                    ))}
                    {(dataPage?.records.length ?? 0) === 0 ? <EmptyLine text="No records loaded" /> : null}
                  </div>
                  <div className="data-page-meta">
                    <span>{dataPage ? `${dataPage.records.length} records` : "Load a schema target"}</span>
                    <span>{dataPage?.hasMore ? "more records available" : "end of page"}</span>
                  </div>
                  <div className="data-pager">
                    <button disabled={dataPageHistory.length === 0} onClick={() => previousDataPage()} type="button">
                      Previous
                    </button>
                    <span>{dataPageCursor ? "paged" : "first page"}</span>
                    <button
                      disabled={!dataPage?.hasMore || !(selectedDataTarget?.nested ? dataPage?.nextCursor : dataPage?.nextAfterKey)}
                      onClick={() => nextDataPage()}
                      type="button"
                    >
                      Next
                    </button>
                  </div>
                </div>

                <form
                  className="data-editor"
                  onSubmit={(event) => {
                    event.preventDefault()
                    upsertDataRecord()
                  }}
                >
                  <div className="data-editor-head">
                    <strong>{selectedDataRecord ? selectedDataRecord.path : "New or selected record"}</strong>
                    <span>{selectedDataTarget?.nested ? "nested partition row" : "top-level row"}</span>
                  </div>
                  <textarea
                    aria-label="Record value JSON"
                    onChange={(event) => setDataValue(event.target.value)}
                    spellCheck={false}
                    value={dataValue}
                  />
                  <div className="button-row compact-buttons">
                    <button type="submit">
                      <Database size={15} />
                      Upsert
                    </button>
                    <button type="button" onClick={() => deleteDataRecord()}>
                      <Trash2 size={15} />
                      Delete
                    </button>
                  </div>
                </form>
              </div>
            </div>
          </Panel>

          <Panel className="span-8" title="Cluster Topology">
            <div className="table">
              <div className="table-row table-head">
                <span>Shard</span>
                <span>Epoch</span>
                <span>Owner</span>
                <span>Role</span>
                <span>Remote Ack</span>
              </div>
              {(health?.clusterTopology.shards ?? []).map((shard) => (
                <div className="table-row" key={shard.shard}>
                  <span>{shard.shard}</span>
                  <span>{shard.epoch}</span>
                  <span>{shard.owner}</span>
                  <span>{shard.role}</span>
                  <span>{remoteAckSummary(health, shard.shard)}</span>
                </div>
              ))}
            </div>
            <div className="kv compact-kv">
              <Row label="Local node" value={health?.clusterTopology.nodeId ?? "-"} />
              <Row label="Nodes" value={String(health?.clusterTopology.nodes.length ?? 0)} />
              <Row label="Shard count" value={String(health?.clusterTopology.shardCount ?? 0)} />
              <Row label="Epochs" value={clusterEpochSummary(health)} />
              <Row label="Ownership gate" value={String(health?.clusterTopology.enforceOwnership ?? false)} />
              <Row label="Runtime overrides" value={String(Object.keys(health?.topologyOverrides ?? {}).length)} />
              <Row label="Topology term" value={String(health?.topologyLease?.currentTerm ?? 0)} />
              <Row label="Lease holder" value={health?.topologyLease?.holderNodeId ?? "-"} />
              <Row label="Remote ack policy" value={remoteAckPolicySummary(health)} />
              <Row label="Frozen shards" value={frozenShardSummary(health)} />
              <Row label="Read ready" value={String(readiness?.readReady ?? false)} />
              <Row label="Write ready" value={String(readiness?.writeReady ?? health?.acceptingWrites ?? false)} />
              <Row label="Realtime ready" value={String(readiness?.realtimeReady ?? !health?.draining)} />
              <Row label="Readiness checks" value={readinessChecksSummary(readiness)} />
                <Row label="Runtime drain" value={runtimeDrainSummary(health)} />
                <Row label="In-flight writes" value={runtimeWritesSummary(health)} />
                <Row label="Live queries" value={liveQuerySummary(health)} />
                <Row label="Handoff controller" value={handoffControllerSummary(health)} />
              <Row label="Controller workflow" value={health?.handoffController?.lastWorkflowId ?? "-"} />
              <Row label="Failover controller" value={failoverControllerSummary(health)} />
              <Row label="Controller proposal" value={health?.failoverController?.lastProposalId ?? "-"} />
              <Row label="WAL repair" value={walRepairControllerSummary(health)} />
              <Row label="WAL repair shards" value={repairShardSummary(health?.walRepairController?.lastShards)} />
              <Row label="Backup controller" value={backupControllerSummary(health)} />
              <Row label="Peer monitor" value={peerHealthSummary(health)} />
            </div>
            <div className="button-row">
              <button onClick={() => void setRuntimeDrain(!health?.draining)}>
                <ServerCog size={15} />
                {health?.draining ? "Resume runtime" : "Drain runtime"}
              </button>
              <button onClick={() => void freezeFirstShard()}>Freeze first shard</button>
              <button onClick={() => void planFirstHandoff()}>Plan first handoff</button>
              <button onClick={() => void planLocalFailover()}>Plan local failover</button>
              <button onClick={() => void startLocalFailoverProposal()}>Start failover proposal</button>
              <button onClick={() => void startFirstWorkflow()}>Start workflow</button>
              <button onClick={() => void stepWorkflow()}>Step workflow</button>
              <button onClick={() => void autoWorkflow()}>Auto workflow</button>
              <button onClick={() => void applyWorkflow()}>Apply workflow</button>
            </div>
            {handoffWorkflow ?? health?.handoffWorkflows[0] ? (
              <div className="handoff-plan">
                <strong>workflow {(handoffWorkflow ?? health?.handoffWorkflows[0])?.id}</strong>
                <span>{(handoffWorkflow ?? health?.handoffWorkflows[0])?.phase}</span>
                <em>
                  target {(handoffWorkflow ?? health?.handoffWorkflows[0])?.targetAckedLsn}/
                  {(handoffWorkflow ?? health?.handoffWorkflows[0])?.currentShardLsn} LSN
                </em>
              </div>
            ) : null}
            {handoffPlan ? (
              <div className="handoff-plan">
                <strong>
                  shard {handoffPlan.shard} to {handoffPlan.targetOwner} epoch {handoffPlan.nextEpoch}
                </strong>
                <span>{handoffPlan.ready ? "ready" : "not ready"}</span>
                <em>
                  target {handoffPlan.targetAckedLsn}/{handoffPlan.currentShardLsn} LSN, frozen {String(handoffPlan.frozen)}
                </em>
              </div>
            ) : null}
            {failoverPlan ? (
              <div className="handoff-plan">
                <strong>
                  failover shard {failoverPlan.shard} to {failoverPlan.targetOwner} epoch {failoverPlan.nextEpoch}
                </strong>
                <span>{failoverPlan.ready ? "ready" : "not ready"}</span>
                <em>
                  local {failoverPlan.localLsn}/{failoverPlan.ownerLastSeenOkLsn ?? 0} LSN, owner healthy{" "}
                  {String(failoverPlan.ownerHealthy)}
                  {failoverPlan.reason ? `, ${failoverPlan.reason}` : ""}
                </em>
              </div>
            ) : null}
            {failoverProposal ? (
              <div className="handoff-plan">
                <strong>failover proposal {failoverProposal.proposal.id}</strong>
                <span>{failoverProposal.proposal.phase}</span>
                <em>
                  acks {failoverProposal.proposal.prepareAcks.filter((ack) => ack.applied).length}/
                  {failoverProposal.proposal.requiredAcks}
                  {failoverProposal.proposal.lastError ? `, ${failoverProposal.proposal.lastError}` : ""}
                </em>
              </div>
            ) : null}
            {topologyProposals?.proposals.at(-1) ? (
              <div className="handoff-plan">
                <strong>proposal {topologyProposals.proposals.at(-1)?.id}</strong>
                <span>{topologyProposals.proposals.at(-1)?.phase}</span>
                <em>
                  term {topologyProposals.proposals.at(-1)?.term}, acks{" "}
                  {topologyProposals.proposals.at(-1)?.prepareAcks.filter((ack) => ack.applied).length}/
                  {topologyProposals.proposals.at(-1)?.requiredAcks}
                </em>
                <div className="button-row compact-buttons">
                  <button onClick={() => void retryLatestProposal()}>Retry proposal</button>
                  <button onClick={() => void abortLatestProposal()}>Abort proposal</button>
                </div>
              </div>
            ) : null}
            <div className="topology-log">
              <div className="subhead">
                <strong>Topology log</strong>
                <button onClick={() => void refreshTopologyLog()}>Refresh</button>
              </div>
              {(topologyLog?.entries ?? []).slice(-4).reverse().map((entry) => (
                <div className="topology-log-row" key={entry.id}>
                  <span>{new Date(entry.timestampMs).toLocaleTimeString()}</span>
                  <strong>{entry.reason}</strong>
                  <em>
                    shard {entry.request.shard}, owner {entry.request.owner ?? "-"}, epoch {entry.request.epoch ?? "-"}
                  </em>
                </div>
              ))}
              {(topologyLog?.entries.length ?? 0) === 0 ? <span className="muted">No topology events</span> : null}
            </div>
          </Panel>

          <Panel className="span-4" title="Connection Layer" action={<button onClick={() => void refreshConnections()}>Refresh</button>}>
            <div className="object-summary">
              <ServerCog size={34} />
              <div>
                <strong>{connections?.total ?? health?.connectionCount ?? 0}</strong>
                <span>active sessions</span>
              </div>
            </div>
            <div className="kv">
              <Row label="Logical users" value={String(connections?.users ?? health?.connectedUsers ?? 0)} />
              <Row label="Durable users" value={String(users?.users.length ?? 0)} />
              <Row label="Protocol" value={connectionProtocolSummary(health)} />
              <Row label="Supported" value={connectionCapabilitySummary(health)} />
              <Row label="Default transport" value={connectionDefaultTransportSummary(health)} />
              <Row label="WebSocket path" value={connectionWebSocketPathSummary(health)} />
              <Row label="JSONL gateway" value={connectionJsonLineGatewaySummary(health)} />
              <Row label="WebTransport path" value={connectionWebTransportPathSummary(health)} />
              <Row label="Admin configured" value={localConfiguredTransportSummary(localDataStatus)} />
              <Row label="Admin active" value={localActiveTransportSummary(localDataStatus)} />
              <Row label="Transport" value={connectionTransportSummary(connections)} />
              <Row label="Subscribed rooms" value={String(connectionRoomCount(connections))} />
              <Row label="Subscribed tables" value={String(connectionTableCount(connections))} />
              <Row label="Live queries" value={String(connectionQueryCount(connections))} />
              <Row label="Query tables" value={String(connectionQueryTableCount(connections))} />
              <Row label="User inboxes" value={String(connectionUserEventSubscriptionCount(connections))} />
              <Row label="Object feeds" value={String(connectionObjectSubscriptionCount(connections))} />
              <Row label="Session metadata" value={String(connectionMetadataSessionCount(connections))} />
            </div>
            <div className="connection-list">
              {(connections?.userSummaries ?? []).slice(0, 5).map((user) => (
                <div className="connection-row" key={user.userId}>
                  <strong>{user.userId}</strong>
                  <span>{user.sessionCount} sessions</span>
                  <em>{connectionUserSummary(user)}</em>
                  <button onClick={() => void disconnectUserConnections(user.userId)}>Disconnect</button>
                </div>
              ))}
              {(connections?.userSummaries.length ?? 0) === 0 && (connections?.sessions.length ?? 0) > 0 ? (
                <div className="connection-row">
                  <strong>anonymous</strong>
                  <span>{connections?.sessions.length ?? 0} sessions</span>
                  <em>{connectionTransportSummary(connections)}</em>
                </div>
              ) : null}
              {(connections?.sessions.length ?? 0) === 0 ? <EmptyLine text="No active sessions" /> : null}
            </div>
            <div className="connection-list">
              {(connections?.sessions ?? []).slice(0, 5).map((session) => (
                <div className="connection-row" key={session.sessionId}>
                  <strong>{session.userId ?? "anonymous"}</strong>
                  <span>{session.transport}</span>
                  <em>{connectionMetadataSummary(session.metadata)}</em>
                </div>
              ))}
            </div>
            <div className="connection-list">
              {(users?.users ?? []).slice(0, 5).map((user) => (
                <div className="connection-row" key={user.userId}>
                  <strong>{user.displayName ?? user.userId}</strong>
                  <span>user</span>
                  <em>LSN {user.lsn}</em>
                </div>
              ))}
              {(users?.users.length ?? 0) === 0 ? <EmptyLine text="No durable users" /> : null}
            </div>
          </Panel>

          <Panel className="span-4" title="Actor Residency">
            <div className="residency">
              <div className="ring" style={{ "--value": residencyRatio(health) } as React.CSSProperties}>
                <strong>{Math.round(residencyRatio(health) * 100)}%</strong>
                <span>resident</span>
              </div>
              <div className="kv">
                <Row label="Hot rooms" value={String(health?.hotRoomCount ?? 0)} />
                <Row label="Max hot rooms" value={String(health?.maxHotRooms ?? 0)} />
                <Row label="Hot window" value={`${health?.hotWindow ?? 0} messages`} />
                <Row label="Idle TTL" value={formatDurationMs(health?.hotRoomIdleTtlMs ?? 0)} />
                <Row label="Room sweep" value={hotRoomMaintenanceSummary(health)} />
                <Row label="Checkpoint every" value={`${health?.checkpointEveryLsn ?? 0} LSN`} />
                <Row label="Last snapshot" value={String(health?.lastSnapshotLsn ?? 0)} />
                <Row label="Last compact" value={String(health?.lastCompactionLsn ?? 0)} />
                <Row label="Startup snapshot" value={startupSnapshotSummary(health)} />
                <Row label="Startup replay" value={startupReplaySummary(health)} />
                <Row label="Schema WAL" value={startupSchemaWalSummary(health)} />
                <Row label="Projection rebuild" value={startupProjectionSummary(health)} />
                <Row label="WAL restore" value={startupRestoreSummary(health)} />
                <Row label="Record hot tables" value={recordHotCacheSummary(health)} />
                <Row label="Record idle TTL" value={formatDurationMs(health?.recordHotCache.durableIdleTtlMs ?? 0)} />
                <Row label="Record sweep" value={recordHotMaintenanceSummary(health)} />
                <Row label="Record prewarm" value={recordHotPrewarmSummary(health)} />
                <Row label="Metrics" value={metricsSummary(metricsText)} />
                <Row label="Runtime limits" value={runtimeLimitsSummary(health)} />
                <Row label="Auto compact" value={String(health?.autoCompactWal ?? false)} />
                <Row label="Checkpoint" value={checkpointSummary(health)} />
                <Row label="WAL shards" value={String(health?.walShardCount ?? 0)} />
                <Row label="WAL local writes" value={walLocalWriteSummary(health)} />
                <Row label="WAL queue" value={walQueueSummary(health)} />
                <Row label="Local node" value={health?.nodeId ?? "-"} />
                <Row label="Ownership gate" value={String(health?.clusterEnforceOwnership ?? false)} />
                <Row label="Admin auth" value={String(health?.adminAuthEnabled ?? false)} />
                <Row label="Client auth" value={String(health?.clientAuthEnabled ?? false)} />
                <Row label="User token auth" value={String(health?.clientUserAuthEnabled ?? false)} />
                <Row label="Remote replicas" value={String(health?.walRemoteReplicaCount ?? 0)} />
                <Row label="Object replicas" value={String(health?.objectRemoteReplicaCount ?? 0)} />
                <Row label="WAL archived" value={String(walCompact?.archived ?? 0)} />
                <Row label="Export manifest" value={exportManifestSummary(exportManifest)} />
                <Row label="Export bundle" value={exportBundleSummary(exportBundle)} />
                <Row label="Backup run" value={exportBackupRunSummary(exportBackupRun)} />
                <Row label="Backup catalog" value={exportBackupRunsSummary(exportBackupRuns)} />
                <Row label="Backup policy" value={exportBackupPolicySummary(exportBackupPolicy)} />
                <Row label="Backup retention" value={exportBackupRetentionSummary(exportBackupRetention)} />
                <Row label="Bundle verify" value={exportBundleVerifySummary(exportBundleVerify)} />
                <Row label="Chain verify" value={exportBundleChainVerifySummary(exportBundleChainVerify)} />
                <Row label="Bundle object" value={exportBundleArchiveObjectSummary(exportBundleArchiveObject)} />
                <Row label="Object import" value={importBundleFromObjectSummary(importBundleObject)} />
                <Row label="Import preflight" value={importBundlePreflightSummary(importBundlePreflight)} />
                <Row label="Import restore" value={importBundleRestoreSummary(importBundleRestore)} />
                <Row label="Delta preflight" value={importBundleDeltaPreflightSummary(importBundleDeltaPreflight)} />
                <Row label="Delta apply" value={importBundleDeltaApplySummary(importBundleDeltaApply)} />
                <Row label="Chain restore" value={importBundleChainRestoreSummary(importBundleChainRestore)} />
              </div>
            </div>
            <div className="runtime-snapshot">
              <div className="subhead">
                <strong>Runtime Snapshot</strong>
                <span>{health?.runtimeId ? shortRuntimeId(health.runtimeId) : "-"}</span>
              </div>
              <div className="snapshot-stats">
                <SnapshotStat label="Snapshot LSN" value={String(health?.startupRecovery.snapshotLsn ?? 0)} />
                <SnapshotStat label="Rooms" value={String(health?.startupRecovery.snapshotRoomCount ?? 0)} />
                <SnapshotStat label="Hot tables" value={String(health?.startupRecovery.snapshotRecordHotTableCount ?? 0)} />
                <SnapshotStat label="Hot records" value={String(health?.startupRecovery.snapshotRecordHotRecordCount ?? 0)} />
              </div>
              <div className="hot-table-list">
                {(health?.recordHotCache.tables ?? []).map((table) => (
                  <div className="hot-table-row" key={table.table}>
                    <strong>{table.table}</strong>
                    <span>{storageClassSummary(table.storage)} · hit {table.getHitTotal}/{table.getTotal} · miss {table.getMissTotal}</span>
                    <em>{table.records} records{table.maxItems ? ` / ${table.maxItems}` : ""} · volatile {table.volatileRecords} · hydrate {table.hydrateDurableTotal} · lru {table.lruEvictedTotal}</em>
                  </div>
                ))}
                {(health?.recordHotCache.tables.length ?? 0) === 0 ? <EmptyLine text="No record hot tables" /> : null}
              </div>
            </div>
            <div className="runtime-snapshot">
              <div className="subhead">
                <strong>Runtime Activation</strong>
                <button onClick={() => void refreshRuntimeActivation()}>
                  <RefreshCw size={14} />
                  Refresh
                </button>
              </div>
              <div className="snapshot-stats">
                <SnapshotStat label="Active rooms" value={String(runtimeActivation?.roomCount ?? 0)} />
                <SnapshotStat label="Max rooms" value={String(runtimeActivation?.maxHotRooms ?? health?.maxHotRooms ?? 0)} />
                <SnapshotStat label="Hot window" value={`${runtimeActivation?.hotWindow ?? health?.hotWindow ?? 0}`} />
                <SnapshotStat label="Idle TTL" value={formatDurationMs(runtimeActivation?.hotRoomIdleTtlMs ?? health?.hotRoomIdleTtlMs ?? 0)} />
                <SnapshotStat label="Room sweep" value={formatDurationMs(runtimeActivation?.hotRoomMaintenanceIntervalMs ?? health?.hotRoomMaintenanceIntervalMs ?? 0)} />
                <SnapshotStat label="Record sweep" value={formatDurationMs(runtimeActivation?.recordHotMaintenanceIntervalMs ?? health?.recordHotMaintenanceIntervalMs ?? 0)} />
                <SnapshotStat label="Prewarm" value={recordHotPrewarmSummary(runtimeActivation ?? health)} />
                <SnapshotStat label="Hot records" value={String(runtimeActivation?.recordHotCache.recordCount ?? 0)} />
              </div>
              <div className="connection-list compact-list">
                {(runtimeActivation?.rooms ?? []).slice(0, 6).map((room) => (
                  <button
                    className="connection-row button-row-item"
                    key={room.roomId}
                    onClick={() =>
                      setRuntimeRoomActivation({
                        roomId: room.roomId,
                        limit: String(runtimeActivation?.hotWindow ?? runtimeRoomActivation.limit),
                      })
                    }
                  >
                    <strong>{room.roomId}</strong>
                    <span>{room.messages} messages</span>
                    <em>
                      LSN {room.oldestLsn ?? "-"}..{room.newestLsn ?? "-"} / {new Date(room.lastAccessedMs).toLocaleTimeString()}
                    </em>
                  </button>
                ))}
                {(runtimeActivation?.rooms.length ?? 0) === 0 ? <EmptyLine text="No active room actors" /> : null}
              </div>
              <div className="hot-table-list">
                {(runtimeActivation?.recordHotCache.tables ?? []).map((table) => (
                  <button
                    className="hot-table-row hot-table-button"
                    key={table.table}
                    onClick={() => setRuntimeRecordActivation((current) => ({ ...current, table: table.table }))}
                  >
                    <strong>{table.table}</strong>
                    <span>{storageClassSummary(table.storage)} · hit {table.getHitTotal}/{table.getTotal} · miss {table.getMissTotal}</span>
                    <em>{table.records} records{table.maxItems ? ` / ${table.maxItems}` : ""} · volatile {table.volatileRecords} · hydrate {table.hydrateDurableTotal} · lru {table.lruEvictedTotal}</em>
                  </button>
                ))}
                {(runtimeActivation?.recordHotCache.tables.length ?? 0) === 0 ? <EmptyLine text="No active record tables" /> : null}
              </div>
              <div className="runtime-control-grid">
                <label>
                  <span>Room</span>
                  <input
                    onChange={(event) => setRuntimeRoomActivation((current) => ({ ...current, roomId: event.target.value }))}
                    placeholder="general"
                    value={runtimeRoomActivation.roomId}
                  />
                </label>
                <label>
                  <span>Messages</span>
                  <input
                    inputMode="numeric"
                    min="1"
                    onChange={(event) => setRuntimeRoomActivation((current) => ({ ...current, limit: event.target.value }))}
                    type="number"
                    value={runtimeRoomActivation.limit}
                  />
                </label>
                <div className="runtime-control-actions">
                  <button disabled={!runtimeRoomActivation.roomId.trim()} onClick={() => void activateRuntimeRoom()}>
                    <Play size={14} />
                    Activate
                  </button>
                  <button disabled={!runtimeRoomActivation.roomId.trim()} onClick={() => void evictRuntimeRoom()}>
                    <Trash2 size={14} />
                    Evict
                  </button>
                </div>
              </div>
              <div className="runtime-control-grid records">
                <label>
                  <span>Table</span>
                  <input
                    data-testid="runtime-record-table"
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, table: event.target.value }))}
                    placeholder="rooms"
                    value={runtimeRecordActivation.table}
                  />
                </label>
                <label>
                  <span>Parent key</span>
                  <input
                    data-testid="runtime-record-parent-key"
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, parentKey: event.target.value }))}
                    placeholder="nested partition"
                    value={runtimeRecordActivation.parentKey}
                  />
                </label>
                <label>
                  <span>Nested</span>
                  <input
                    data-testid="runtime-record-nested"
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, nested: event.target.value }))}
                    placeholder="messages"
                    value={runtimeRecordActivation.nested}
                  />
                </label>
                <label>
                  <span>Key</span>
                  <input
                    data-testid="runtime-record-key"
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, key: event.target.value }))}
                    placeholder="optional exact key"
                    value={runtimeRecordActivation.key}
                  />
                </label>
                <label>
                  <span>Order</span>
                  <select
                    data-testid="runtime-record-order"
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, order: event.target.value === "schema" ? "schema" : "key" }))}
                    value={runtimeRecordActivation.order}
                  >
                    <option value="key">key</option>
                    <option value="schema">schema</option>
                  </select>
                </label>
                <label>
                  <span>Index</span>
                  <input
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, indexName: event.target.value }))}
                    placeholder="byTitle"
                    value={runtimeRecordActivation.indexName}
                  />
                </label>
                <label>
                  <span>Value</span>
                  <input
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, value: event.target.value }))}
                    placeholder="exact index value"
                    value={runtimeRecordActivation.value}
                  />
                </label>
                <label>
                  <span>Lower</span>
                  <input
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, lower: event.target.value }))}
                    placeholder="range lower"
                    value={runtimeRecordActivation.lower}
                  />
                </label>
                <label>
                  <span>Upper</span>
                  <input
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, upper: event.target.value }))}
                    placeholder="range upper"
                    value={runtimeRecordActivation.upper}
                  />
                </label>
                <label>
                  <span>After key</span>
                  <input
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, afterKey: event.target.value }))}
                    placeholder="optional cursor"
                    value={runtimeRecordActivation.afterKey}
                  />
                </label>
                <label>
                  <span>After cursor</span>
                  <input
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, afterCursor: event.target.value }))}
                    placeholder="range cursor"
                    value={runtimeRecordActivation.afterCursor}
                  />
                </label>
                <label>
                  <span>Limit</span>
                  <input
                    inputMode="numeric"
                    min="1"
                    onChange={(event) => setRuntimeRecordActivation((current) => ({ ...current, limit: event.target.value }))}
                    type="number"
                    value={runtimeRecordActivation.limit}
                  />
                </label>
                <div className="runtime-control-actions">
                  <button data-testid="runtime-record-activate" disabled={!runtimeRecordActivation.table.trim()} onClick={() => void activateRuntimeRecords()}>
                    <Play size={14} />
                    Activate
                  </button>
                  <button data-testid="runtime-record-evict" disabled={!runtimeRecordActivation.table.trim()} onClick={() => void evictRuntimeRecords()}>
                    <Trash2 size={14} />
                    Evict
                  </button>
                </div>
              </div>
            </div>
            <div className="button-row">
              <button onClick={() => void prepareRestart()}>
                <ServerCog size={15} />
                Prepare restart
              </button>
              <button onClick={() => void refreshMetrics()}>
                <Gauge size={15} />
                Metrics
              </button>
              <button onClick={() => void createSnapshot()}>
                <HardDrive size={15} />
                Snapshot
              </button>
              <button onClick={() => void compactWal()}>
                <Archive size={15} />
                Compact WAL
              </button>
              <button onClick={() => void rebuildProjections()}>
                <RotateCw size={15} />
                Rebuild
              </button>
              <button onClick={() => void refreshExportManifest()}>
                <FileClock size={15} />
                Export Manifest
              </button>
              <button onClick={() => void createExportBundle()}>
                <Archive size={15} />
                Export Bundle
              </button>
              <button onClick={() => void runExportBackup()}>
                <Archive size={15} />
                Run Backup
              </button>
              <button onClick={() => void refreshExportBackupRuns()}>
                <RefreshCw size={15} />
                List Backups
              </button>
              <button onClick={() => void refreshExportBackupPolicy()}>
                <RefreshCw size={15} />
                Backup Policy
              </button>
              <button onClick={() => void saveDefaultExportBackupPolicy()}>
                <Archive size={15} />
                Save Policy
              </button>
              <button onClick={() => void runExportBackupPolicy()}>
                <Play size={15} />
                Run Policy
              </button>
              <button onClick={() => void planExportBackupRetention()}>
                <Trash2 size={15} />
                Plan Retention
              </button>
              <button onClick={() => void refreshExportBundles()}>
                <RefreshCw size={15} />
                List Bundles
              </button>
              <button disabled={!selectedExportBundleId.trim()} onClick={() => void verifyExportBundle()}>
                <ShieldCheck size={15} />
                Verify Bundle
              </button>
              <button disabled={parseBundleChainIds(exportBundleChainIds).length === 0} onClick={() => void verifyExportBundleChain()}>
                <ShieldCheck size={15} />
                Verify Chain
              </button>
              <button disabled={parseBundleChainIds(exportBundleChainIds).length === 0} onClick={() => void restoreImportBundleChain()}>
                <Archive size={15} />
                Restore Chain
              </button>
              <button disabled={!selectedExportBundleId.trim()} onClick={() => void archiveExportBundleToObject()}>
                <Boxes size={15} />
                Archive Object
              </button>
              <button disabled={!exportBundleObjectId.trim()} onClick={() => void importBundleFromObject()}>
                <Database size={15} />
                Import Object
              </button>
              <button disabled={!selectedExportBundleId.trim()} onClick={() => void runImportBundlePreflight()}>
                <Database size={15} />
                Import Preflight
              </button>
              <button disabled={!selectedExportBundleId.trim()} onClick={() => void restoreImportBundle()}>
                <Archive size={15} />
                Restore Bundle
              </button>
              <button disabled={!selectedExportBundleId.trim()} onClick={() => void runImportBundleDeltaPreflight()}>
                <Database size={15} />
                Delta Preflight
              </button>
              <button disabled={!selectedExportBundleId.trim()} onClick={() => void applyImportBundleDelta()}>
                <Archive size={15} />
                Apply Delta
              </button>
            </div>
            <div className="retention-box">
              <div className="retention-fields">
                <label>
                  <span>Bundle ID</span>
                  <input
                    onChange={(event) => setSelectedExportBundleId(event.target.value)}
                    placeholder="export-..."
                    value={selectedExportBundleId}
                  />
                </label>
                <label>
                  <span>Bundle key</span>
                  <input
                    onChange={(event) => setExportBundleEncryptionKey(event.target.value)}
                    type="password"
                    value={exportBundleEncryptionKey}
                  />
                </label>
                <label>
                  <span>Base LSN</span>
                  <input
                    inputMode="numeric"
                    min="0"
                    onChange={(event) => setExportBundleBaseLsn(event.target.value)}
                    placeholder="0 for full"
                    type="number"
                    value={exportBundleBaseLsn}
                  />
                </label>
                <label>
                  <span>Bundle object</span>
                  <input
                    onChange={(event) => setExportBundleObjectId(event.target.value)}
                    placeholder="export-bundle-..."
                    value={exportBundleObjectId}
                  />
                </label>
                <label>
                  <span>Chain IDs</span>
                  <input
                    onChange={(event) => setExportBundleChainIds(event.target.value)}
                    placeholder="base, delta..."
                    value={exportBundleChainIds}
                  />
                </label>
              </div>
              <div className="connection-list compact-list">
                {(exportBundleList?.bundles ?? []).slice(0, 4).map((bundle) => (
                  <button
                    className="connection-row button-row-item"
                    key={bundle.id}
                    onClick={() => setSelectedExportBundleId(bundle.id)}
                  >
                    <strong>{bundle.id}</strong>
                    <span>
                      {bundle.ok
                        ? `schema v${bundle.schemaVersion ?? "-"}, history ${schemaHistoryVersionsSummary(bundle.schemaHistoryVersions)}, proposals ${bundle.schemaProposals}, ${bundleEncryptionSummary(bundle.encrypted)}, ${clusterControlSummary(bundle.clusterControl)}`
                        : `${bundle.problems.length} problems`}
                    </span>
                    <em>{bundle.walRecords ?? 0} WAL, {formatBytes(bundle.objectBytes ?? 0)}</em>
                  </button>
                ))}
                {(exportBundleList?.bundles.length ?? 0) === 0 ? <EmptyLine text="No listed bundles" /> : null}
              </div>
            </div>
            <div className="retention-box">
              <div className="retention-fields">
                <label>
                  <span>Before LSN</span>
                  <input
                    inputMode="numeric"
                    min="1"
                    onChange={(event) => setRetentionBeforeLsn(event.target.value)}
                    placeholder={health?.lastCompactionLsn ? String(health.lastCompactionLsn + 1) : "LSN"}
                    type="number"
                    value={retentionBeforeLsn}
                  />
                </label>
                <label>
                  <span>Before time ms</span>
                  <input
                    inputMode="numeric"
                    min="1"
                    onChange={(event) => setRetentionBeforeTimestampMs(event.target.value)}
                    placeholder="timestamp"
                    type="number"
                    value={retentionBeforeTimestampMs}
                  />
                </label>
              </div>
              <div className="button-row compact-buttons">
                <button onClick={() => void retainWalArchives(true)}>
                  <Archive size={15} />
                  Dry-run
                </button>
                <button onClick={() => void retainWalArchives(false)}>
                  <Trash2 size={15} />
                  Apply
                </button>
              </div>
              {walRetention ? (
                <div className="retention-result">
                  <Row label="Mode" value={walRetention.dryRun ? "dry-run" : "apply"} />
                  <Row label="Candidates" value={String(walRetention.candidates)} />
                  <Row label="Deleted" value={String(walRetention.deleted)} />
                  <Row label="Retained" value={String(walRetention.retained)} />
                  {walRetention.reports.slice(0, 3).map((report) => (
                    <div className="retention-file" key={report.path}>
                      <strong>{report.action}</strong>
                      <span>
                        shard {report.shard} LSN {report.minLsn ?? "-"}-{report.maxLsn ?? "-"}
                      </span>
                      {report.reason ? <em>{report.reason}</em> : null}
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          </Panel>

          <Panel className="span-4" title="Client Cache Control" action={<button onClick={() => void invalidateAllClientCaches()}>Invalidate</button>}>
            <div className="object-summary">
              <CloudOff size={34} />
              <div>
                <strong>v{health?.clientCache?.profile.version ?? 0}</strong>
                <span>profile</span>
              </div>
            </div>
            <div className="kv">
              <Row label="Lease TTL" value={`${health?.clientCache?.profile.leaseTtlMs ?? 0} ms`} />
              <Row label="Object limit" value={String(health?.clientCache?.profile.maxObjects ?? 0)} />
              <Row label="Object bytes" value={formatBytes(health?.clientCache?.profile.maxObjectBytes ?? 0)} />
              <Row label="Room limit" value={String(health?.clientCache?.profile.maxRoomMessages ?? 0)} />
              <Row label="User event limit" value={String(health?.clientCache?.profile.maxUserEvents ?? 0)} />
              <Row label="Table limit" value={String(health?.clientCache?.profile.maxRecordsPerTable ?? 0)} />
              <Row label="Nested partitions" value={String(health?.clientCache?.profile.maxNestedPartitions ?? 0)} />
              <Row label="Pending write limit" value={String(health?.clientCache?.profile.maxPendingWrites ?? 0)} />
              <Row label="Pending write bytes" value={formatBytes(health?.clientCache?.profile.maxPendingWriteBytes ?? 0)} />
              <Row label="Offline writes" value={String(health?.clientCache?.profile.offlineWrites ?? false)} />
              <Row label="Invalidations" value={String(health?.clientCache?.invalidations.length ?? 0)} />
              <Row label="Latest generation" value={String(latestCacheInvalidation(health)?.generation ?? 0)} />
            </div>
            <div className="cache-invalidation-form">
              <label>
                <span>Lease TTL</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.leaseTtlMs}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, leaseTtlMs: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.leaseTtlMs ?? 0)}
                />
              </label>
              <label>
                <span>Objects</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxObjects}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxObjects: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxObjects ?? 0)}
                />
              </label>
              <label>
                <span>Object bytes</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxObjectBytes}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxObjectBytes: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxObjectBytes ?? 0)}
                />
              </label>
              <label>
                <span>Room messages</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxRoomMessages}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxRoomMessages: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxRoomMessages ?? 0)}
                />
              </label>
              <label>
                <span>User events</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxUserEvents}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxUserEvents: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxUserEvents ?? 0)}
                />
              </label>
              <label>
                <span>Table records</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxRecordsPerTable}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxRecordsPerTable: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxRecordsPerTable ?? 0)}
                />
              </label>
              <label>
                <span>Nested partitions</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxNestedPartitions}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxNestedPartitions: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxNestedPartitions ?? 0)}
                />
              </label>
              <label>
                <span>Pending writes</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxPendingWrites}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxPendingWrites: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxPendingWrites ?? 0)}
                />
              </label>
              <label>
                <span>Pending bytes</span>
                <input
                  inputMode="numeric"
                  value={cacheProfileDraft.maxPendingWriteBytes}
                  onChange={(event) => setCacheProfileDraft((current) => ({ ...current, maxPendingWriteBytes: event.target.value }))}
                  placeholder={String(health?.clientCache?.profile.maxPendingWriteBytes ?? 0)}
                />
              </label>
              <label>
                <span>Offline writes</span>
                <select
                  value={cacheProfileDraft.offlineWrites}
                  onChange={(event) =>
                    setCacheProfileDraft((current) => ({
                      ...current,
                      offlineWrites: event.target.value as CacheProfileDraftState["offlineWrites"],
                    }))
                  }
                >
                  <option value="unchanged">unchanged</option>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              </label>
              <button onClick={loadCacheProfileDraft}>Load Current</button>
              <button onClick={updateClientCacheProfile}>Apply Profile</button>
            </div>
            <div className="cache-invalidation-form">
              <label>
                <span>Scope</span>
                <select
                  value={cacheInvalidation.scope}
                  onChange={(event) =>
                    setCacheInvalidation((current) => ({
                      ...current,
                      scope: event.target.value as CacheInvalidationState["scope"],
                    }))
                  }
                >
                  <option value="user">user</option>
                  <option value="room">room</option>
                  <option value="table">table</option>
                  <option value="nestedTable">nestedTable</option>
                  <option value="object">object</option>
                </select>
              </label>
              <label>
                <span>Key</span>
                <input
                  value={cacheInvalidation.key}
                  onChange={(event) => setCacheInvalidation((current) => ({ ...current, key: event.target.value }))}
                  placeholder={cacheInvalidation.scope === "nestedTable" ? "table:parentKey:nested" : `${cacheInvalidation.scope} key`}
                />
              </label>
              <label>
                <span>Min LSN</span>
                <input
                  inputMode="numeric"
                  value={cacheInvalidation.minValidLsn}
                  onChange={(event) => setCacheInvalidation((current) => ({ ...current, minValidLsn: event.target.value }))}
                  placeholder="0"
                />
              </label>
              <button onClick={() => void invalidateScopedClientCache()}>Invalidate Scope</button>
            </div>
          </Panel>

          <Panel
            className="span-4"
            title="Admin Local Data"
            action={
              <div className="panel-actions">
                <button onClick={() => void refreshLocalDataStatus()}>Refresh</button>
                <button onClick={() => void flushLocalPendingWrites()}>Flush</button>
                <button onClick={() => void clearLocalPendingWrites()}>Clear Queue</button>
                <button onClick={() => void enforceLocalCacheProfile()}>Enforce Profile</button>
                <button onClick={() => void restoreLocalSubscriptions()}>Restore</button>
                <button onClick={() => void clearLocalSubscriptions()}>Clear Subs</button>
                <button onClick={() => void clearLocalCache()}>Clear Cache</button>
              </div>
            }
          >
            <div className="object-summary">
              <HardDrive size={34} />
              <div>
                <strong>{localDataStatus?.cache.totalRecords ?? 0}</strong>
                <span>cached records</span>
              </div>
            </div>
            <div className="kv">
              <Row label="Endpoint" value={localDataStatus?.endpoint ?? base} />
              <Row label="Cache scope" value={localCacheScopeSummary(localDataStatus)} />
              <Row label="Transport state" value={localDataStatus?.transportState ?? "idle"} />
              <Row label="Configured transport" value={localConfiguredTransportSummary(localDataStatus)} />
              <Row label="Active transport" value={localActiveTransportSummary(localDataStatus)} />
              <Row label="Last seen LSN" value={String(localDataStatus?.lastSeenLsn ?? 0)} />
              <Row label="Objects" value={String(localDataStatus?.cache.totalObjects ?? 0)} />
              <Row label="Object cache" value={`${formatBytes(localDataStatus?.cache.totalObjectCachedBytes ?? 0)} / ${localDataStatus?.cache.totalObjectRangeChunks ?? 0} ranges`} />
              <Row label="Messages" value={String(localDataStatus?.cache.totalMessages ?? 0)} />
              <Row label="User events" value={String(localDataStatus?.cache.totalUserEvents ?? 0)} />
              <Row label="User profiles" value={String(localDataStatus?.cache.totalUserProfiles ?? 0)} />
              <Row label="Tables" value={String(Object.keys(localDataStatus?.cache.tables ?? {}).length)} />
              <Row label="Cursors" value={localCursorSummary(localDataStatus)} />
              <Row label="Pending writes" value={localPendingWriteStatsSummary(localDataStatus?.pendingWrites)} />
              <Row label="Auto flush" value={localPendingQueue ? localPendingAutoFlushSummary(localPendingQueue) : "-"} />
              <Row label="Coverage" value={localCoverageSummary(localDataStatus)} />
              <Row label="Stored subscriptions" value={String(localDataStatus?.storedSubscriptions.length ?? 0)} />
              <Row label="Active subscriptions" value={localSubscriptionSummary(localDataStatus?.activeSubscriptions)} />
              <Row label="Persistent subscriptions" value={localSubscriptionSummary(localDataStatus?.persistentSubscriptions)} />
              <Row label="Realtime state" value={String(Object.keys(localDataStatus?.realtimeChannelStates ?? {}).length)} />
              <Row label="Realtime members" value={localRealtimeMembersSummary(localDataStatus)} />
              <Row label="Realtime events" value={localRealtimeEventsSummary(localDataStatus)} />
              <Row label="Realtime signals" value={localRealtimeSignalsSummary(localDataStatus)} />
              <Row label="Connection sessions" value={localConnectionSessionsSummary(localDataStatus)} />
            </div>
            <div className="connection-list compact-list">
              {localCoverageRows(localDataStatus).map((entry) => (
                <div className="connection-row" key={entry.id}>
                  <strong>{entry.label}</strong>
                  <span>{entry.value}</span>
                  <em>{entry.detail}</em>
                </div>
              ))}
              {localCoverageRows(localDataStatus).length === 0 ? <EmptyLine text="No cached coverage" /> : null}
            </div>
            <div className="pending-write-list">
              {(localPendingQueue?.writes ?? []).length === 0 ? (
                <div className="pending-write-empty">No pending writes</div>
              ) : (
                localPendingQueue?.writes.map((write) => (
                  <div className="pending-write-row" key={write.id}>
                    <div>
                      <strong>{write.type}</strong>
                      <span>{pendingWriteTarget(write)}</span>
                      <em>{write.lastError ? `attempts ${write.attempts}: ${write.lastError}` : `attempts ${write.attempts}`}</em>
                    </div>
                    <div className="pending-write-actions">
                      <button onClick={() => void resetLocalPendingWrite(write.id)}>Reset</button>
                      <button onClick={() => void discardLocalPendingWrite(write.id)}>Discard</button>
                    </div>
                  </div>
                ))
              )}
            </div>
          </Panel>

          <Panel
            className="span-4"
            title="Schema Registry"
            action={
              <div className="panel-actions">
                <button onClick={() => void reloadSchema()}>Reload</button>
                <button data-testid="schema-dry-run" onClick={() => void applySchemaDraft(true)}>Dry-run</button>
                <button data-testid="schema-apply" onClick={() => void applySchemaDraft(false)}>Apply</button>
              </div>
            }
          >
            <div className="kv">
              <Row label="Name" value={schema?.name ?? "-"} />
              <Row label="Version" value={`v${schema?.version ?? 0}`} />
              <Row label="History" value={schemaHistoryVersionsSummary(schemaHistory?.entries.map((entry) => entry.version))} />
              <Row label="Validation" value={schemaReport?.ok ? "ok" : "needs attention"} tone={schemaReport?.ok ? "good" : "warn"} />
              <Row label="Migration" value={schemaMigrationSummary(migrationPlan)} tone={migrationPlan?.compatible ? "good" : "warn"} />
              <Row label="Records" value={String(projectionStatus?.records ?? 0)} />
              <Row label="Key order" value={String(projectionStatus?.keyOrderEntries ?? 0)} />
              <Row label="Recent" value={String(projectionStatus?.recentEntries ?? 0)} />
              <Row label="Indexes" value={String(projectionStatus?.indexEntries ?? 0)} />
              <Row label="Partitions" value={String(projectionStatus?.partitionEntries ?? 0)} />
              <Row label="Schema orders" value={String(projectionStatus?.orderEntries ?? 0)} />
            </div>
            {schemaReport && !schemaReport.ok ? (
              <div className="schema-errors">
                {schemaReport.errors.map((error, index) => (
                  <span key={`${error}-${index}`}>{error}</span>
                ))}
              </div>
            ) : null}
            <div className="schema-list">
              {(schemaHistory?.entries ?? []).map((entry) => (
                <span data-testid="schema-history-entry" key={`history.${entry.version}`}>
                  history v{entry.version}{entry.current ? " current" : ""}: {entry.tableCount} tables, {entry.objectCount} objects
                </span>
              ))}
              {(storagePolicy?.schema.entries ?? []).map((entry) => (
                <span key={entry.path}>
                  {entry.path.replace("tables.", "")}: {entry.storage.kind} / {entry.physicalRole}
                </span>
              ))}
              {schemaIndexEntries(schema).map((entry) => (
                <span key={`${entry.table}.${entry.index}`}>
                  {entry.table}.{entry.index}: {entry.fields.join(", ")}
                </span>
              ))}
              {schemaEventEntries(schema).map((entry) => (
                <span key={`event.${entry.name}`}>event {entry.name}: {entry.summary}</span>
              ))}
            </div>
            {chatLogStorageEntries(storagePolicy).length > 0 ? (
              <div className="schema-storage-grid">
                {chatLogStorageEntries(storagePolicy).map((entry) => (
                  <div className="schema-storage-row" key={entry.path}>
                    <strong>{entry.path.replace("tables.", "")}</strong>
                    <span>bucket {entry.bucket}</span>
                    <span>order {entry.order.join(", ")}</span>
                    <span>window {entry.liveWindow}</span>
                  </div>
                ))}
              </div>
            ) : null}
            <div className="schema-editor">
              <div className="schema-editor-head">
                <strong>Candidate JSON</strong>
                <span>
                  {schemaApplyResult
                    ? `${schemaApplyResult.applied ? "applied" : "validated"} v${schemaApplyResult.version}, ${schemaApplyResult.projectionRebuilt ? "projection rebuilt" : "projection kept"}, ${schemaApplyResult.projectionStatus.records} records`
                    : "current schema"}
                </span>
              </div>
              <textarea value={schemaDraft} onChange={(event) => setSchemaDraft(event.target.value)} spellCheck={false} />
            </div>
          </Panel>

          <Panel className="span-4" title="Object Storage" action={<button onClick={() => void dryRunGc()}>Dry-run GC</button>}>
            <div className="object-summary">
              <Archive size={34} />
              <div>
                <strong>{objectGc ? objectGc.deleted.length : objectList?.objects.length ?? stats.objects}</strong>
                <span>{objectGc ? "unreferenced candidates" : "objects in current page"}</span>
              </div>
            </div>
            <div className="kv">
              <Row label="Store" value={health?.objectStore ?? "-"} />
              <Row label="Listed" value={String(objectList?.objects.length ?? 0)} />
              <Row label="Has more" value={String(objectList?.hasMore ?? false)} />
              <Row label="Retained" value={String(objectGc?.retained.length ?? 0)} />
              <Row label="Protected" value={String(objectGc?.protected.length ?? 0)} />
              <Row label="Grace" value={`${objectGc?.graceMs ?? health?.objectGcGraceMs ?? 0} ms`} />
              <Row label="Object repair" value={objectRepairControllerSummary(health)} />
              <Row label="Object repair shards" value={repairShardSummary(health?.objectRepairController?.lastShards)} />
              <Row label="Dry-run" value={objectGc ? String(objectGc.dryRun) : "not run"} />
            </div>
            <div className="object-list">
              {(objectList?.objects ?? []).map((object) => (
                <div key={object.id}>
                  <strong>{object.id}</strong>
                  <span>{object.contentType}</span>
                  <span>{formatBytes(object.byteSize)}</span>
                  <span className="object-actions">
                    <button aria-label={`Invalidate ${object.id}`} onClick={() => void invalidateObjectCache(object.id)}>
                      <CloudOff size={13} />
                    </button>
                    <button aria-label={`Delete ${object.id}`} onClick={() => void deleteObject(object.id)}>
                      <Trash2 size={13} />
                    </button>
                    <button aria-label={`Force delete ${object.id}`} onClick={() => void deleteObject(object.id, true)}>
                      <ShieldCheck size={13} />
                    </button>
                  </span>
                </div>
              ))}
              {(objectList?.objects.length ?? 0) === 0 ? <span>No objects</span> : null}
            </div>
            <div className="button-row">
              <button onClick={() => void forceDryRunGc()}>
                <Trash2 size={15} />
                Force preview
              </button>
            </div>
          </Panel>

          <Panel className="span-4" title="Behavior Runtime" action={<button onClick={() => void reloadBehaviors()}>Reload</button>}>
            <div className="behavior-list">
              {behaviors.length === 0 ? (
                <EmptyLine text="No behavior modules loaded" />
              ) : (
                behaviors.map((behavior) => (
                  <div className="behavior" key={behavior.name}>
                    <Play size={15} />
                    <div>
                      <div className="behavior-title">
                        <strong>{behavior.name}</strong>
                        <em>{behavior.version}</em>
                      </div>
                      <span>{behavior.mutations.join(", ") || "no mutations"}</span>
                      <div className="behavior-capabilities">
                        {behaviorCapabilityRows(behavior).map((row) => (
                          <span key={row.label}>
                            <strong>{row.label}</strong>
                            {row.value}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
            <form
              className="behavior-invoke"
              onSubmit={(event) => {
                event.preventDefault()
                void invokeBehavior()
              }}
            >
              <label>
                <span>Behavior</span>
                <select
                  data-testid="behavior-select"
                  onChange={(event) =>
                    setBehaviorInvoke((current) => {
                      const behavior = behaviors.find((entry) => entry.name === event.target.value)
                      const mutation = behavior?.mutations[0] ?? ""
                      return {
                        ...current,
                        behavior: event.target.value,
                        mutation,
                        input: mergeBehaviorInput(behaviorMutationField(schema, event.target.value, mutation), current.input),
                      }
                    })
                  }
                  value={behaviorInvoke.behavior}
                >
                  {behaviors.map((behavior) => (
                    <option key={behavior.name} value={behavior.name}>
                      {behavior.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Mutation</span>
                <select
                  data-testid="behavior-mutation-select"
                  onChange={(event) =>
                    setBehaviorInvoke((current) => ({
                      ...current,
                      mutation: event.target.value,
                      input: mergeBehaviorInput(behaviorMutationField(schema, current.behavior, event.target.value), current.input),
                    }))
                  }
                  value={behaviorInvoke.mutation}
                >
                  {(behaviors.find((behavior) => behavior.name === behaviorInvoke.behavior)?.mutations ?? []).map((mutation) => (
                    <option key={mutation} value={mutation}>
                      {mutation}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>User</span>
                <input data-testid="behavior-user" value={behaviorInvoke.userId} onChange={(event) => setBehaviorInvoke((current) => ({ ...current, userId: event.target.value }))} />
              </label>
              <div className="behavior-input-header">
                <span>Input schema</span>
                <strong>{behaviorInputSummary(selectedBehaviorField)}</strong>
              </div>
              {selectedBehaviorFields.length > 0 ? (
                selectedBehaviorFields.map(({ name, field }) => (
                  <BehaviorFieldControl
                    field={field}
                    key={name}
                    name={name}
                    onChange={(value) =>
                      setBehaviorInvoke((current) => ({
                        ...current,
                        input: { ...current.input, [name]: value },
                      }))
                    }
                    value={behaviorInvoke.input[name] ?? ""}
                  />
                ))
              ) : (
                <BehaviorFieldControl
                  field={selectedBehaviorField}
                  name="input"
                  onChange={(value) =>
                    setBehaviorInvoke((current) => ({
                      ...current,
                      input: { ...current.input, $: value },
                    }))
                  }
                  value={behaviorInvoke.input.$ ?? ""}
                />
              )}
              <button data-testid="behavior-invoke-submit" type="submit">
                <Play size={15} />
                Invoke
              </button>
            </form>
            {behaviorResult ? (
              <div className="behavior-result">
                <div className="behavior-result-row">
                  <span>Committed</span>
                  <strong>{behaviorResult.committed.length}</strong>
                </div>
                <div className="behavior-result-row stacked">
                  <span>Types</span>
                  <div className="behavior-result-types">
                    {behaviorResult.committed.length === 0 ? (
                      <em>none</em>
                    ) : (
                      behaviorResult.committed.map((entry, index) => <em key={`${entry.type}-${index}`}>{entry.type}</em>)
                    )}
                  </div>
                </div>
              </div>
            ) : null}
          </Panel>

          <Panel className="span-4" title="Realtime Channels" action={<button onClick={() => void refreshRealtimeChannels()}>Refresh</button>}>
            <div className="kv">
              <Row label="Channels" value={String(realtimeChannels?.total ?? 0)} />
              <Row label="Members" value={String((realtimeChannels?.channels ?? []).reduce((sum, channel) => sum + channel.memberCount, 0))} />
              <Row label="Runtime state" value={realtimeRuntimeStateSummary(health)} />
              <Row label="Cleanup" value={realtimeMaintenanceSummary(health)} />
            </div>
            <div className="realtime-list">
              {(realtimeChannels?.channels ?? []).map((channel) => (
                <button
                  className="realtime-channel-row"
                  data-testid={`realtime-channel-${channel.channelId}`}
                  key={channel.channelId}
                  onClick={() => selectRealtimeChannelState(channel.channelId)}
                  type="button"
                >
                  <Cable size={15} />
                  <div>
                    <strong>{channel.channelId}</strong>
                    <span>{channel.members.map(realtimeMemberSummary).join(", ") || "no members"}</span>
                  </div>
                  <em>
                    #{channel.sequence}
                    {" / "}
                    state v{channel.stateVersion}
                    {" / "}
                    {localRealtimeChannelEventSummary(localDataStatus, channel.channelId)}
                  </em>
                </button>
              ))}
              {(realtimeChannels?.channels.length ?? 0) === 0 ? <EmptyLine text="No realtime channels" /> : null}
            </div>
            <div className="realtime-state-editor">
              <label>
                <span>Channel</span>
                <input
                  onChange={(event) => setRealtimeStateDraft((current) => ({ ...current, channelId: event.target.value }))}
                  placeholder="call-general"
                  value={realtimeStateDraft.channelId}
                />
              </label>
              <label>
                <span>From user</span>
                <input
                  onChange={(event) => setRealtimeStateDraft((current) => ({ ...current, fromUserId: event.target.value }))}
                  placeholder="joined user id"
                  value={realtimeStateDraft.fromUserId}
                />
              </label>
              <label>
                <span>Expected version</span>
                <input
                  min="0"
                  onChange={(event) => setRealtimeStateDraft((current) => ({ ...current, expectedVersion: event.target.value }))}
                  type="number"
                  value={realtimeStateDraft.expectedVersion}
                />
              </label>
              <label className="wide">
                <span>State JSON</span>
                <textarea
                  data-testid="realtime-state-json"
                  onChange={(event) => setRealtimeStateDraft((current) => ({ ...current, stateJson: event.target.value }))}
                  value={realtimeStateDraft.stateJson}
                />
              </label>
              <div className="button-row compact-buttons">
                <button onClick={() => refreshRealtimeState()} type="button">Load State</button>
                <button onClick={updateRealtimeChannelState} type="button">Update State</button>
              </div>
              <label>
                <span>Event kind</span>
                <input
                  data-testid="realtime-event-kind"
                  onChange={(event) => setRealtimeEventDraft((current) => ({ ...current, kind: event.target.value }))}
                  placeholder="admin.event"
                  value={realtimeEventDraft.kind}
                />
              </label>
              <label className="wide">
                <span>Event payload JSON</span>
                <textarea
                  data-testid="realtime-event-payload"
                  onChange={(event) => setRealtimeEventDraft((current) => ({ ...current, payloadJson: event.target.value }))}
                  value={realtimeEventDraft.payloadJson}
                />
              </label>
              <label>
                <span>Include self</span>
                <input
                  checked={realtimeEventDraft.includeSelf}
                  data-testid="realtime-event-include-self"
                  onChange={(event) => setRealtimeEventDraft((current) => ({ ...current, includeSelf: event.target.checked }))}
                  type="checkbox"
                />
              </label>
              <div className="button-row compact-buttons">
                <button onClick={broadcastRealtimeChannelEvent} type="button">Broadcast Event</button>
              </div>
              <label>
                <span>Signal to user</span>
                <input
                  data-testid="realtime-signal-user"
                  onChange={(event) => setRealtimeSignalDraft((current) => ({ ...current, toUserId: event.target.value }))}
                  placeholder="nextdb-admin"
                  value={realtimeSignalDraft.toUserId}
                />
              </label>
              <label>
                <span>Signal kind</span>
                <input
                  data-testid="realtime-signal-kind"
                  onChange={(event) => setRealtimeSignalDraft((current) => ({ ...current, kind: event.target.value }))}
                  placeholder="admin.signal"
                  value={realtimeSignalDraft.kind}
                />
              </label>
              <label className="wide">
                <span>Signal payload JSON</span>
                <textarea
                  data-testid="realtime-signal-payload"
                  onChange={(event) => setRealtimeSignalDraft((current) => ({ ...current, payloadJson: event.target.value }))}
                  value={realtimeSignalDraft.payloadJson}
                />
              </label>
              <div className="button-row compact-buttons">
                <button onClick={sendRealtimeChannelSignal} type="button">Signal User</button>
              </div>
              {realtimeState ? (
                <div className="realtime-state-summary">
                  <span>{realtimeState.channelId}</span>
                  <strong>v{realtimeState.state.version}</strong>
                  <em>{realtimeState.state.updatedAtMs ? new Date(realtimeState.state.updatedAtMs).toLocaleTimeString() : "not set"}</em>
                </div>
              ) : null}
            </div>
          </Panel>
        </section>
      </main>

      <aside className="inspector">
        <Panel title="Inspector">
          {selectedRecord ? (
            <>
              <div className="inspector-title">
                <FileClock size={18} />
                <div>
                  <strong>WAL #{selectedRecord.lsn}</strong>
                  <span>{new Date(selectedRecord.timestampMs).toLocaleString()}</span>
                </div>
              </div>
              <pre>{JSON.stringify(selectedRecord, null, 2)}</pre>
            </>
          ) : (
            <EmptyLine text="Select a WAL row" />
          )}
        </Panel>

        <Panel title="Operations">
          <div className="ops">
            {logs.length === 0 ? (
              <EmptyLine text="No operations yet" />
            ) : (
              logs.map((log) => (
                <div className={`op ${log.ok ? "ok" : "fail"}`} key={log.id}>
                  <span>{log.at}</span>
                  <strong>{log.label}</strong>
                  <em>{log.detail}</em>
                </div>
              ))
            )}
          </div>
          {loading ? <div className="loading">Running {loading}</div> : null}
        </Panel>

        <Panel title="Storage Paths">
          <div className="paths">
            <Row label="WAL" value={health?.wal ?? "-"} />
            <Row label="WAL shards" value={health?.walPaths?.join(", ") ?? "-"} />
            <Row label="Schema" value={health?.schema ?? "-"} />
            <Row label="Cache control" value={health?.clientCacheControl ?? "-"} />
            <Row label="Chat log" value={health?.chatLog ?? "-"} />
          </div>
        </Panel>
      </aside>
    </div>
  )
}

function NavItem({ icon, label, active = false }: { icon: React.ReactNode; label: string; active?: boolean }) {
  return (
    <button className={`nav-item ${active ? "active" : ""}`}>
      {icon}
      <span>{label}</span>
    </button>
  )
}

function Metric({ title, value, icon }: { title: string; value: string | number; icon: React.ReactNode }) {
  return (
    <div className="metric">
      <div className="metric-icon">{icon}</div>
      <span>{title}</span>
      <strong>{value}</strong>
    </div>
  )
}

function SnapshotStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="snapshot-stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function Panel({
  title,
  action,
  className = "",
  children,
}: {
  title: string
  action?: React.ReactNode
  className?: string
  children: React.ReactNode
}) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-head">
        <h2>{title}</h2>
        {action}
      </div>
      {children}
    </section>
  )
}

function Row({ label, value, tone }: { label: string; value: string; tone?: "good" | "warn" }) {
  return (
    <div className="kv-row">
      <span>{label}</span>
      <strong className={tone ? `tone-${tone}` : ""}>{value}</strong>
    </div>
  )
}

function EmptyLine({ text }: { text: string }) {
  return <div className="empty">{text}</div>
}

function StatusDot({ ok }: { ok: boolean }) {
  return <span className={`status-dot ${ok ? "ok" : ""}`} />
}

function BehaviorFieldControl({
  field,
  name,
  value,
  onChange,
}: {
  field?: FieldSchema
  name: string
  value: string
  onChange: (value: string) => void
}) {
  const kind = field?.type.kind ?? "json"
  const label = `${name}${field?.optional ? " optional" : ""}`
  if (kind === "boolean") {
    return (
      <label className="behavior-field">
        <span>
          {label}
          <em>{fieldTypeLabel(field?.type)}</em>
        </span>
        <select data-testid={`behavior-input-${name}`} onChange={(event) => onChange(event.target.value)} value={value || "false"}>
          <option value="false">false</option>
          <option value="true">true</option>
        </select>
      </label>
    )
  }
  if (kind === "json" || kind === "list" || kind === "object" || kind === "objectRef") {
    return (
      <label className="behavior-field">
        <span>
          {label}
          <em>{fieldTypeLabel(field?.type)}</em>
        </span>
        <textarea data-testid={`behavior-input-${name}`} onChange={(event) => onChange(event.target.value)} rows={4} value={value} />
      </label>
    )
  }
  return (
    <label className="behavior-field">
      <span>
        {label}
        <em>{fieldTypeLabel(field?.type)}</em>
      </span>
      <input data-testid={`behavior-input-${name}`} onChange={(event) => onChange(event.target.value)} type={kind === "int64" || kind === "timeMs" ? "number" : "text"} value={value} />
    </label>
  )
}

function postJson(body: unknown): RequestInit {
  return {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  }
}

function recordSubject(record: NextDbWalRecord): string {
  if (record.payload.type === "messageCreated") {
    return `${record.payload.message.roomId} / ${record.payload.message.senderId}`
  }
  if (record.payload.type === "userEventPublished") {
    return `${record.payload.event.userId} / ${record.payload.event.name}`
  }
  if (record.payload.type === "recordUpserted") {
    return `${record.payload.record.table} / ${record.payload.record.key}`
  }
  if (record.payload.type === "recordDeleted") {
    return `${record.payload.record.table} / ${record.payload.record.key}`
  }
  if (record.payload.type === "recordTransactionCommitted") {
    return `${record.payload.operations.length} record op${record.payload.operations.length === 1 ? "" : "s"}`
  }
  if (record.payload.type === "schemaApplied") {
    return `${record.payload.schema.name} v${record.payload.schema.version}`
  }
  if (record.payload.type === "objectCommitted") {
    return record.payload.object.id
  }
  if (record.payload.type === "objectDeleted") {
    return record.payload.objectId
  }
  return "-"
}

function remoteAckSummary(health: NextDbHealth | undefined, shard: number): string {
  const replica = health?.walReplicas[shard]
  if (!replica) {
    return "-"
  }
  const status = replica.remoteStatus
  const ok = status.remoteReplicas.filter((remote) => remote.ok).length
  return `${formatAckPolicy(status.remoteAckPolicy)} ${ok}/${status.remoteReplicaCount}, need ${status.remoteRequiredAcks}`
}

function remoteAckPolicySummary(health: NextDbHealth | undefined): string {
  const first = health?.walReplicas[0]?.remoteStatus
  return first ? formatAckPolicy(first.remoteAckPolicy) : "-"
}

function checkpointSummary(health: NextDbHealth | undefined): string {
  if (!health) {
    return "-"
  }
  return `${health.checkpointInFlight ? "running" : "idle"}, every ${health.checkpointEveryLsn} LSN`
}

function walLocalWriteSummary(health: NextDbHealth | undefined): string {
  const statuses = (health?.walReplicas ?? []).map((replica) => replica.remoteStatus)
  if (statuses.length === 0) {
    return "-"
  }
  const batches = statuses.reduce((sum, status) => sum + status.localBatches, 0)
  const records = statuses.reduce((sum, status) => sum + status.localRecords, 0)
  const bytes = statuses.reduce((sum, status) => sum + status.localBytes, 0)
  const syncs = statuses.reduce((sum, status) => sum + status.localSyncs, 0)
  const failed = statuses.reduce((sum, status) => sum + status.localFailedBatches, 0)
  return `${batches} batches, ${records} records, ${formatBytes(bytes)}, sync ${syncs}, failed ${failed}`
}

function walQueueSummary(health: NextDbHealth | undefined): string {
  const statuses = (health?.walReplicas ?? []).map((replica) => replica.remoteStatus)
  if (statuses.length === 0) {
    return "-"
  }
  const depth = statuses.reduce((sum, status) => sum + status.queueDepth, 0)
  const capacity = statuses.reduce((sum, status) => sum + status.queueCapacity, 0)
  const batchMax = statuses[0]?.batchMax ?? 0
  const batchWaitMs = statuses[0]?.batchWaitMs ?? 0
  const lastWriteMs = Math.max(...statuses.map((status) => status.localLastBatchWriteMs))
  const lastSyncMs = Math.max(...statuses.map((status) => status.localLastBatchSyncMs))
  return `${depth}/${capacity}, batch ${batchMax}/${batchWaitMs}ms, last write ${lastWriteMs}ms, sync ${lastSyncMs}ms`
}

function clusterEpochSummary(health: NextDbHealth | undefined): string {
  const epochs = new Set((health?.clusterTopology.shards ?? []).map((shard) => shard.epoch))
  if (epochs.size === 0) {
    return "-"
  }
  return [...epochs].sort((left, right) => left - right).join(", ")
}

function frozenShardSummary(health: NextDbHealth | undefined): string {
  const frozen = (health?.shardControls ?? []).filter((control) => control.frozen)
  if (frozen.length === 0) {
    return "none"
  }
  return frozen.map((control) => String(control.shard)).join(", ")
}

function runtimeDrainSummary(health: NextDbHealth | undefined): string {
  if (!health?.draining) {
    return "accepting writes"
  }
  return health.runtimeDrain.reason ? `draining: ${health.runtimeDrain.reason}` : "draining"
}

function readinessStatusLabel(readiness: NextDbReadiness): string {
  if (readiness.ok) {
    return "Ready"
  }
  if (readiness.draining) {
    return "Draining"
  }
  return "Not ready"
}

function readinessSummary(readiness: NextDbReadiness | undefined): string {
  if (!readiness) {
    return "-"
  }
  return `R:${readyFlag(readiness.readReady)} W:${readyFlag(readiness.writeReady)} RT:${readyFlag(readiness.realtimeReady)}`
}

function readinessChecksSummary(readiness: NextDbReadiness | undefined): string {
  if (!readiness) {
    return "-"
  }
  const failed = readiness.checks.filter((check) => !check.ok)
  if (failed.length === 0) {
    return `${readiness.checks.length} ok`
  }
  return failed.map((check) => check.name).join(", ")
}

function readyFlag(value: boolean): string {
  return value ? "ok" : "no"
}

function runtimeWritesSummary(health: NextDbHealth | undefined): string {
  if (!health?.runtimeWrites) {
    return "-"
  }
  const lastFinished = health.runtimeWrites.lastFinishedAtMs
    ? `, last finished ${formatDateTime(health.runtimeWrites.lastFinishedAtMs)}`
    : ""
  return `${health.runtimeWrites.inFlight} active${lastFinished}`
}

function liveQuerySummary(health: NextDbHealth | undefined): string {
  const live = health?.liveQueries
  if (!live) {
    return "-"
  }
  return `${live.current} active, batch max ${live.eventBatchMax}, batches ${live.eventBatchesTotal}/${live.batchedEventsTotal}, refresh ${live.refreshTotal}/${live.refreshCandidatesTotal}, executions ${live.queryExecutionsTotal}, cache hits ${live.evaluationCacheHitsTotal}, diff ${live.diffFramesTotal}, unchanged ${live.unchangedTotal}, errors ${live.errorsTotal}`
}

function runtimeLimitsSummary(health: NextDbHealth | undefined): string {
  const limits = health?.limits
  if (!limits) {
    return "-"
  }
  return `object ${formatBytes(limits.maxObjectBytes)}, message ${formatBytes(limits.maxMessageBytes)}, event ${formatBytes(limits.maxUserEventBytes)}, record ${formatBytes(limits.maxRecordValueBytes)}, queries ${limits.maxLiveQueriesPerConnection || "unlimited"} / table ${limits.maxLiveQueriesPerTablePerConnection || "unlimited"} / user ${limits.maxLiveQueriesPerUser || "unlimited"}`
}

function exportManifestSummary(manifest: ExportManifestResponse | undefined): string {
  if (!manifest) {
    return "-"
  }
  return `${exportRangeSummary(manifest)}, ${manifest.wal.records} WAL, LSN ${manifest.wal.lowestLsn ?? 0}-${manifest.wal.highestLsn}, ${manifest.objects.live} objects, history ${schemaHistoryVersionsSummary(manifest.schemaHistoryVersions)}, proposals ${manifest.schemaProposals}, ${bundleEncryptionSummary(manifest.encryption.encrypted)}, ${clusterControlSummary(manifest.clusterControl)}`
}

function metricsSummary(text: string | undefined): string {
  if (!text) {
    return "-"
  }
  const lines = text.split("\n").filter((line) => line.trim() && !line.startsWith("#")).length
  return `${lines} metrics, ${formatBytes(new TextEncoder().encode(text).length)}`
}

function exportBundleSummary(bundle: ExportBundleResponse | undefined): string {
  if (!bundle) {
    return "-"
  }
  return `${bundle.id}, ${exportRangeSummary(bundle.manifest)}, ${bundle.walRecords} WAL, ${formatBytes(bundle.objectBytes)}, history ${schemaHistoryVersionsSummary(bundle.schemaHistoryVersions)}, proposals ${bundle.schemaProposals}, ${bundleEncryptionSummary(bundle.encrypted)}, ${clusterControlSummary(bundle.clusterControl)}`
}

function exportBackupRunSummary(result: ExportBackupRunResponse | undefined): string {
  if (!result) {
    return "-"
  }
  if (result.noOp) {
    return `no-op, ${result.mode}, LSN ${result.currentLsn}`
  }
  const archive = result.archived ? `, object ${result.archived.object.id}` : ""
  const chain = result.chain ? `, chain ${result.chain.ok ? "ok" : `${result.chain.problems.length} problems`}` : ""
  return `${result.mode}, base ${result.baseLsn}, ${result.bundle?.walRecords ?? 0} WAL${archive}${chain}`
}

function exportBackupRunsSummary(result: ExportBackupRunListResponse | undefined): string {
  if (!result) {
    return "-"
  }
  const latest = result.runs[0]
  if (!latest) {
    return "0 runs"
  }
  const outcome = latest.noOp ? "no-op" : `${latest.bundleId ?? "-"} to LSN ${latest.currentLsn}`
  return `${result.runs.length} runs, latest ${latest.mode} ${outcome}`
}

function exportBackupPolicySummary(result: ExportBackupPolicyResponse | undefined): string {
  if (!result) {
    return "-"
  }
  const policy = result.policy
  const schedule = policy.enabled ? `${policy.intervalMs} ms` : "manual"
  const retention = policy.retentionKeepLast === undefined ? "no retention" : `keep ${policy.retentionKeepLast}`
  return `${schedule}, archive ${String(policy.archiveObject)}, ${retention}`
}

function exportBackupRetentionSummary(result: ExportBackupRetentionResponse | undefined): string {
  if (!result) {
    return "-"
  }
  const mode = result.dryRun ? "dry-run" : "applied"
  return `${mode}, ${result.candidates} candidates, ${result.deletedBundles.length} bundles, ${result.deletedArchiveObjects.length} objects`
}

function exportBundleVerifySummary(result: ExportBundleVerifyResponse | undefined): string {
  if (!result) {
    return "-"
  }
  if (result.ok) {
    return `ok, ${exportRangeSummary(result.manifest)}, schema v${result.schemaVersion ?? "-"}, history ${schemaHistoryVersionsSummary(result.schemaHistoryVersions)}, proposals ${result.schemaProposals}, ${bundleEncryptionSummary(result.encrypted)}, ${clusterControlSummary(result.clusterControl)}, ${result.walRecords} WAL, ${result.objects} objects`
  }
  return `${result.problems.length} problems`
}

function exportBundleChainVerifySummary(result: ExportBundleChainVerifyResponse | undefined): string {
  if (!result) {
    return "-"
  }
  if (result.ok) {
    return `ok, ${result.bundles.length} bundles, LSN ${result.baseLsn}-${result.highestLsn}`
  }
  return `${result.problems.length} problems, ${result.bundles.length} bundles`
}

function exportBundleArchiveObjectSummary(result: ExportBundleArchiveObjectResponse | undefined): string {
  if (!result) {
    return "-"
  }
  return `${result.object.id}, ${result.files} files, ${formatBytes(result.bytes)}`
}

function importBundleFromObjectSummary(result: ImportBundleFromObjectResponse | undefined): string {
  if (!result) {
    return "-"
  }
  return `${result.bundle.id}, ${result.files} files, ${formatBytes(result.bytes)}, ${result.overwritten ? "overwritten" : "created"}`
}

function importBundlePreflightSummary(result: ImportBundlePreflightResponse | undefined): string {
  if (!result) {
    return "-"
  }
  if (result.ok) {
    return `ready, ${exportRangeSummary(result.manifest)}, schema v${result.bundleSchemaVersion ?? "-"}, history ${schemaHistoryVersionsSummary(result.bundleSchemaHistoryVersions)}, proposals ${result.bundleSchemaProposals}, ${bundleEncryptionSummary(result.bundleEncrypted)}, ${clusterControlSummary(result.bundleClusterControl)}, ${result.bundleWalRecords} WAL, LSN ${result.bundleHighestLsn}`
  }
  return `${result.problems.length} problems, current LSN ${result.currentLsn}`
}

function importBundleRestoreSummary(result: ImportBundleRestoreResponse | undefined): string {
  if (!result) {
    return "-"
  }
  return `schema v${result.schemaVersion ?? "-"}, history ${schemaHistoryVersionsSummary(result.schemaHistoryVersions)}, proposals ${result.schemaProposals}, ${bundleEncryptionSummary(result.encrypted)}, ${clusterControlSummary(result.clusterControl)}, ${result.walRecords} WAL, ${result.objects} objects, current LSN ${result.currentLsn}`
}

function importBundleDeltaPreflightSummary(result: ImportBundleDeltaPreflightResponse | undefined): string {
  if (!result) {
    return "-"
  }
  if (result.ok) {
    return `ready, base LSN ${result.baseLsn}, ${result.bundleWalRecords} WAL, ${result.bundleObjects} objects, target LSN ${result.bundleHighestLsn}`
  }
  return `${result.problems.length} problems, current LSN ${result.currentLsn}, base LSN ${result.baseLsn}`
}

function importBundleDeltaApplySummary(result: ImportBundleDeltaApplyResponse | undefined): string {
  if (!result) {
    return "-"
  }
  return `applied from ${result.baseLsn}, ${result.walRecords} WAL, ${result.objects} objects, current LSN ${result.currentLsn}`
}

function importBundleChainRestoreSummary(result: ImportBundleChainRestoreResponse | undefined): string {
  if (!result) {
    return "-"
  }
  return `${result.chain.bundles.length} bundles, ${result.walRecords} WAL, ${result.objects} objects, current LSN ${result.currentLsn}`
}

function exportRangeSummary(manifest: ExportManifestResponse | undefined): string {
  if (!manifest) {
    return "range -"
  }
  return manifest.incremental || manifest.baseLsn > 0 ? `delta >${manifest.baseLsn}` : "full"
}

function bundleAccessOptions(encryptionKey: string): { encryptionKey?: string } {
  const key = encryptionKey.trim()
  return key ? { encryptionKey: key } : {}
}

function bundleCreateOptions(encryptionKey: string, baseLsn: string): { encryptionKey?: string; baseLsn?: number } {
  return {
    ...bundleAccessOptions(encryptionKey),
    baseLsn: numericInput(baseLsn),
  }
}

function parseBundleChainIds(value: string): string[] {
  return value
    .split(/[\s,]+/)
    .map((entry) => entry.trim())
    .filter(Boolean)
}

function appendBundleChainId(value: string, id: string): string {
  const ids = parseBundleChainIds(value)
  if (!ids.includes(id)) {
    ids.push(id)
  }
  return ids.join(", ")
}

function numericInput(value: string): number | undefined {
  const trimmed = value.trim()
  if (!trimmed) {
    return undefined
  }
  const parsed = Number(trimmed)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined
}

function bundleEncryptionSummary(encrypted: boolean | undefined): string {
  return encrypted ? "encrypted" : "plain"
}

function clusterControlSummary(control: { topologyOverrides: number; topologyLogEntries: number; topologyProposals: number; handoffWorkflows: number; topologyLeaseTerm: number } | undefined): string {
  if (!control) {
    return "cluster -"
  }
  return `cluster ${control.topologyOverrides} overrides, ${control.topologyLogEntries} log, ${control.topologyProposals} proposals, ${control.handoffWorkflows} handoffs, term ${control.topologyLeaseTerm}`
}

function schemaHistoryVersionsSummary(versions: number[] | undefined): string {
  if (!versions?.length) {
    return "-"
  }
  return versions.map((version) => `v${version}`).join(", ")
}

function schemaMigrationSummary(plan: SchemaMigrationPlan | undefined): string {
  if (!plan) {
    return "-"
  }
  if (plan.compatible) {
    if (plan.projectionRebuildRequired) {
      return `compatible, projection rebuild`
    }
    return "compatible"
  }
  if (plan.requiresReplayRebuild && plan.unsafeBreakingChanges.length === 0) {
    return `replay rebuild, ${plan.replaySafeBreakingChanges.length} change${plan.replaySafeBreakingChanges.length === 1 ? "" : "s"}`
  }
  const issueCount = plan.unsafeBreakingChanges.length || plan.errors.length
  return `blocked, ${issueCount} issue${issueCount === 1 ? "" : "s"}`
}

function localSubscriptionSummary(
  subscriptions: NextDbLocalDataStatus["activeSubscriptions"] | NextDbLocalDataStatus["persistentSubscriptions"] | undefined,
): string {
  if (!subscriptions) {
    return "-"
  }
  const total = subscriptions.rooms.length
    + subscriptions.tables.length
    + subscriptions.nestedTables.length
    + subscriptions.queries.length
    + subscriptions.realtimeChannels.length
    + Number(subscriptions.userEvents)
    + Number(subscriptions.objects)
  if (total === 0) {
    return "none"
  }
  return `${total} total, ${subscriptions.rooms.length} rooms, ${subscriptions.tables.length} tables, ${subscriptions.nestedTables.length} nested, ${subscriptions.queries.length} queries, ${subscriptions.realtimeChannels.length} channels`
}

function localCursorSummary(status: NextDbLocalDataStatus | undefined): string {
  if (!status) {
    return "-"
  }
  return `${Object.keys(status.roomSeenLsn).length} rooms, ${Object.keys(status.userSeenLsn).length} users, ${Object.keys(status.tableSeenLsn).length} tables`
}

function localPendingWriteStatsSummary(stats: NextDbLocalDataStatus["pendingWrites"] | undefined): string {
  if (!stats) {
    return "-"
  }
  const failed = stats.failed > 0 ? `, ${stats.failed} failed` : ""
  const attempts = stats.totalAttempts > 0 ? `, ${stats.totalAttempts} attempts` : ""
  const limits: string[] = []
  if (stats.maxWrites > 0) {
    limits.push(`${stats.total}/${stats.maxWrites} writes`)
  }
  if (stats.maxBytes > 0) {
    limits.push(`${formatBytes(stats.estimatedBytes)}/${formatBytes(stats.maxBytes)}`)
  }
  const overLimit = stats.overMaxWrites || stats.overMaxBytes ? ", over limit" : ""
  const limit = limits.length > 0 ? `, ${limits.join(", ")}` : ""
  return `${stats.total} total, ${formatBytes(stats.estimatedBytes)} queued, ${formatBytes(stats.objectPutBytes)} objects${limit}${failed}${attempts}${overLimit}`
}

function localRealtimeMembersSummary(status: NextDbLocalDataStatus | undefined): string {
  const channels = Object.values(status?.realtimeChannelMembers ?? {})
  const members = channels.reduce((sum, channel) => sum + channel.memberCount, 0)
  return `${channels.length} channels, ${members} members`
}

function localRealtimeEventsSummary(status: NextDbLocalDataStatus | undefined): string {
  const channels = Object.values(status?.realtimeChannelEvents ?? {})
  const events = channels.reduce((sum, channel) => sum + channel.eventCount, 0)
  const latest = channels.reduce<number | undefined>((current, channel) => {
    if (channel.latestSequence === undefined) {
      return current
    }
    return current === undefined ? channel.latestSequence : Math.max(current, channel.latestSequence)
  }, undefined)
  return latest === undefined ? `${channels.length} channels, ${events} events` : `${channels.length} channels, ${events} events, latest #${latest}`
}

function localRealtimeSignalsSummary(status: NextDbLocalDataStatus | undefined): string {
  const channels = Object.values(status?.realtimeChannelSignals ?? {})
  const signals = channels.reduce((sum, channel) => sum + channel.signalCount, 0)
  const latest = channels.reduce<number | undefined>((current, channel) => {
    if (channel.latestSequence === undefined) {
      return current
    }
    return current === undefined ? channel.latestSequence : Math.max(current, channel.latestSequence)
  }, undefined)
  return latest === undefined ? `${channels.length} channels, ${signals} signals` : `${channels.length} channels, ${signals} signals, latest #${latest}`
}

function localRealtimeChannelEventSummary(status: NextDbLocalDataStatus | undefined, channelId: string): string {
  const summary = status?.realtimeChannelEvents[channelId]
  if (!summary || summary.eventCount === 0) {
    return "0 local events"
  }
  const latest = summary.latestSequence === undefined ? "" : ` latest #${summary.latestSequence}`
  return `${summary.eventCount} local events${latest}`
}

function localConnectionSessionsSummary(status: NextDbLocalDataStatus | undefined): string {
  if (!status) {
    return "-"
  }
  return `${status.connectionSessions.sessionCount} sessions, ${status.connectionSessions.userCount} users`
}

function localCoverageSummary(status: NextDbLocalDataStatus | undefined): string {
  if (!status) {
    return "-"
  }
  const coverage = status.coverage
  const rooms = Object.keys(coverage.rooms).length
  const users = Object.keys(coverage.users).length
  const tables = Object.keys(coverage.tables).length
  const nested = Object.values(coverage.nestedTables).reduce((sum, partitions) => sum + Object.keys(partitions).length, 0)
  return `${rooms} rooms, ${users} users, ${tables} tables, ${nested} nested, ${coverage.objects.objects} objects`
}

function localCoverageRows(status: NextDbLocalDataStatus | undefined): Array<{ id: string; label: string; value: string; detail: string }> {
  if (!status) {
    return []
  }
  const rows: Array<{ id: string; label: string; value: string; detail: string; sort: number }> = []
  const flags = (entry: { activeSubscription: boolean; persistentSubscription: boolean; storedSubscription: boolean }) => [
    entry.activeSubscription ? "active" : undefined,
    entry.persistentSubscription ? "persistent" : undefined,
    entry.storedSubscription ? "stored" : undefined,
  ].filter(Boolean).join(", ") || "not subscribed"

  if (status.coverage.objects.objects > 0 || status.coverage.objects.pendingWrites > 0 || status.coverage.objects.storedSubscription) {
    rows.push({
      id: "objects",
      label: "objects",
      value: `${status.coverage.objects.objects} objects, ${formatBytes(status.coverage.objects.cachedByteSize)} cached`,
      detail: `metadata ${formatBytes(status.coverage.objects.byteSize)}, ${status.coverage.objects.rangeChunks} ranges, cursor ${status.coverage.objects.cursor}, pending ${status.coverage.objects.pendingWrites}, ${flags(status.coverage.objects)}`,
      sort: status.coverage.objects.objects + status.coverage.objects.pendingWrites,
    })
  }
  for (const [roomId, entry] of Object.entries(status.coverage.rooms)) {
    rows.push({
      id: `room:${roomId}`,
      label: `room ${roomId}`,
      value: `${entry.messages} messages`,
      detail: `cursor ${entry.cursor}, pending ${entry.pendingWrites}, ${flags(entry)}`,
      sort: entry.messages + entry.pendingWrites,
    })
  }
  for (const [userId, entry] of Object.entries(status.coverage.users)) {
    rows.push({
      id: `user:${userId}`,
      label: `user ${userId}`,
      value: `${entry.events} events${entry.profile ? ", profile" : ""}`,
      detail: `cursor ${entry.cursor}, pending ${entry.pendingWrites}, ${flags(entry)}`,
      sort: entry.events + entry.pendingWrites + Number(entry.profile),
    })
  }
  for (const [table, entry] of Object.entries(status.coverage.tables)) {
    rows.push({
      id: `table:${table}`,
      label: `table ${table}`,
      value: `${entry.records} records`,
      detail: `cursor ${entry.cursor}, pending ${entry.pendingWrites}, ${flags(entry)}`,
      sort: entry.records + entry.pendingWrites,
    })
  }
  for (const [logicalTable, partitions] of Object.entries(status.coverage.nestedTables)) {
    for (const [parentKey, entry] of Object.entries(partitions)) {
      rows.push({
        id: `nested:${logicalTable}:${parentKey}`,
        label: `${logicalTable}/${parentKey}`,
        value: `${entry.records} records`,
        detail: `cursor ${entry.cursor}, pending ${entry.pendingWrites}, ${flags(entry)}`,
        sort: entry.records + entry.pendingWrites,
      })
    }
  }
  return rows
    .sort((left, right) => right.sort - left.sort || left.id.localeCompare(right.id))
    .slice(0, 6)
    .map(({ sort: _sort, ...row }) => row)
}

function localCacheScopeSummary(status: NextDbLocalDataStatus | undefined): string {
  if (!status) {
    return "-"
  }
  const name = status.cacheScope.name ? `:${status.cacheScope.name}` : ""
  const user = status.cacheScope.userId ? ` / ${status.cacheScope.userId}` : ""
  return `${status.cacheScope.kind}${name} / ${status.cacheScope.namespace}${user}`
}

function localPendingAutoFlushSummary(queue: PendingWriteQueueStatus): string {
  if (!queue.autoFlush.enabled) {
    return "disabled"
  }
  const state = queue.autoFlush.inFlight ? "flushing" : queue.autoFlush.scheduled ? "scheduled" : "idle"
  return `${state}, ${queue.autoFlush.limit} per ${queue.autoFlush.intervalMs} ms`
}

function pendingWriteTarget(write: NextDbPendingWrite): string {
  if (write.type === "sendMessage") {
    return `${write.roomId} / ${write.userId}`
  }
  if (write.type === "userEvent") {
    return `${write.userId}/${write.name}`
  }
  if (write.type === "userProfileUpsert") {
    return write.userId
  }
  if (write.type === "recordUpsert" || write.type === "recordDelete") {
    return `${write.table}/${write.key}`
  }
  if (write.type === "nestedRecordUpsert" || write.type === "nestedRecordDelete") {
    return `${write.table}/${write.parentKey}/${write.nested}/${write.nestedKey}`
  }
  if (write.type === "recordTransaction") {
    return `${write.operations.length} operations`
  }
  return write.objectId
}

function auditTracePlaceholder(kind: AuditTraceKind): string {
  if (kind === "path") {
    return "tables/rooms/general"
  }
  if (kind === "clientMutation") {
    return "clientMutationId"
  }
  if (kind === "object") {
    return "object id"
  }
  if (kind === "user") {
    return "user id"
  }
  if (kind === "nestedRecord") {
    return "nested record key"
  }
  return "general"
}

function replaySupportedAuditKind(kind: AuditTraceKind): kind is "record" | "nestedRecord" | "user" | "object" {
  return kind === "record" || kind === "nestedRecord" || kind === "user" || kind === "object"
}

function replayAuditKind(kind: AuditTraceKind): "record" | "nestedRecord" | "user" | "object" {
  return replaySupportedAuditKind(kind) ? kind : "record"
}

function traceTargetSummary(target: AuditTraceTarget): string {
  if (target.kind === "record") {
    return `${target.table}/${target.recordKey}`
  }
  if (target.kind === "nestedRecord") {
    return `${target.table}/${target.parentKey}/${target.nested}/${target.nestedKey}`
  }
  if (target.kind === "path") {
    return target.path ?? target.id
  }
  if (target.kind === "clientMutation") {
    return target.clientMutationId ?? target.id
  }
  return `${target.kind}/${target.id}`
}

function formatDateTime(timestampMs: number): string {
  return new Date(timestampMs).toLocaleTimeString()
}

function handoffControllerSummary(health: NextDbHealth | undefined): string {
  const controller = health?.handoffController
  if (!controller?.enabled) {
    return "disabled"
  }
  const status = controller.lastError ? `error: ${controller.lastError}` : "running"
  return `${status}, ${controller.intervalMs} ms`
}

function failoverControllerSummary(health: NextDbHealth | undefined): string {
  const controller = health?.failoverController
  if (!controller?.enabled) {
    return "disabled"
  }
  const status = controller.lastError ? `error: ${controller.lastError}` : "running"
  const shard = controller.lastShard === undefined ? "" : `, shard ${controller.lastShard}`
  return `${status}${shard}, ${controller.intervalMs} ms`
}

function walRepairControllerSummary(health: NextDbHealth | undefined): string {
  const controller = health?.walRepairController
  if (!controller?.enabled) {
    return "disabled"
  }
  const status = controller.lastError
    ? `error: ${controller.lastError}`
    : controller.lastSatisfied
      ? "satisfied"
      : "repairing"
  const lastRun = controller.lastRunAtMs ? `, ${formatDateTime(controller.lastRunAtMs)}` : ""
  return `${status}, sent ${controller.lastRecordsSent}, replicas ${controller.lastRepairedReplicas}, ${controller.intervalMs} ms${lastRun}`
}

function objectRepairControllerSummary(health: NextDbHealth | undefined): string {
  const controller = health?.objectRepairController
  if (!controller?.enabled) {
    return "disabled"
  }
  const status = controller.lastError
    ? `error: ${controller.lastError}`
    : controller.lastSatisfied
      ? "satisfied"
      : "repairing"
  const lastRun = controller.lastRunAtMs ? `, ${formatDateTime(controller.lastRunAtMs)}` : ""
  return `${status}, sent ${controller.lastObjectsSent}, replicas ${controller.lastRepairedReplicas}, ${controller.intervalMs} ms${lastRun}`
}

function repairShardSummary(shards: number[] | undefined): string {
  if (!shards || shards.length === 0) {
    return "-"
  }
  return shards.join(", ")
}

function backupControllerSummary(health: NextDbHealth | undefined): string {
  const controller = health?.exportBackupController
  if (!controller?.enabled) {
    return "disabled"
  }
  const status = controller.lastError ? `error: ${controller.lastError}` : "running"
  const run = controller.lastRunId ? `, run ${controller.lastRunId}` : ""
  return `${status}${run}, ${controller.intervalMs} ms`
}

function peerHealthSummary(health: NextDbHealth | undefined): string {
  const monitor = health?.peerHealth
  if (!monitor?.enabled) {
    return "disabled"
  }
  const peers = Object.values(monitor.peers)
  if (peers.length === 0) {
    return `running, ${monitor.intervalMs} ms`
  }
  const ok = peers.filter((peer) => peer.ok).length
  return `${ok}/${peers.length} ok, ${monitor.intervalMs} ms`
}

function startupSnapshotSummary(health: NextDbHealth | undefined): string {
  const recovery = health?.startupRecovery
  if (!recovery) {
    return "-"
  }
  if (!recovery.snapshotLoaded) {
    return "none"
  }
  return `LSN ${recovery.snapshotLsn}, ${recovery.snapshotRoomCount} rooms, ${recovery.snapshotRecordHotRecordCount} hot records`
}

function startupReplaySummary(health: NextDbHealth | undefined): string {
  const recovery = health?.startupRecovery
  if (!recovery) {
    return "-"
  }
  return `${recovery.walRecordsAfterSnapshot}/${recovery.walRecordsScanned} records, high ${recovery.highestLsn}`
}

function startupSchemaWalSummary(health: NextDbHealth | undefined): string {
  const recovery = health?.startupRecovery?.schemaWalRecovery
  if (!recovery) {
    return "-"
  }
  if (!recovery.recovered) {
    return `none, history ${schemaHistoryVersionsSummary(recovery.historyVersions)}`
  }
  return `v${recovery.latestVersion ?? "-"} at LSN ${recovery.latestLsn ?? "-"}, history ${schemaHistoryVersionsSummary(recovery.historyVersions)}`
}

function startupProjectionSummary(health: NextDbHealth | undefined): string {
  const recovery = health?.startupRecovery
  if (!recovery) {
    return "-"
  }
  return `${recovery.rebuiltMessages} messages, ${recovery.rebuiltRecords} records, ${recovery.rebuiltObjectRefs} refs`
}

function startupRestoreSummary(health: NextDbHealth | undefined): string {
  const recovery = health?.startupRecovery
  if (!recovery) {
    return "-"
  }
  const restored = recovery.walRestores.filter((report) => report.restored)
  if (restored.length === 0) {
    return "none"
  }
  const archives = restored.reduce((sum, report) => sum + report.archiveFilesRestored, 0)
  return `${restored.length} shard${restored.length === 1 ? "" : "s"}, ${archives} archives`
}

function recordHotCacheSummary(health: NextDbHealth | undefined): string {
  const cache = health?.recordHotCache
  if (!cache) {
    return "-"
  }
  return `${cache.tableCount} tables, ${cache.recordCount} records, volatile ${cache.volatileRecords}, get ${cache.getHitTotal}/${cache.getTotal}, miss ${cache.getMissTotal}, hydrate ${cache.hydrateDurableTotal}, lru evict ${cache.lruEvictedTotal}, idle ${formatDurationMs(cache.durableIdleTtlMs)}`
}

function recordHotPrewarmSummary(
  source: Pick<NextDbHealth, "recordHotPrewarm" | "recordHotPrewarmLimit"> | undefined,
): string {
  const prewarm = source?.recordHotPrewarm
  if (!prewarm?.enabled) {
    return "off"
  }
  if (prewarm.lastError) {
    return `error: ${prewarm.lastError}`
  }
  const finished = prewarm.lastFinishedAtMs ? "done" : "running"
  return `${finished}, limit ${source?.recordHotPrewarmLimit ?? prewarm.limitPerTable}, found ${prewarm.totalFound}, activated ${prewarm.totalActivated}`
}

function hotRoomMaintenanceSummary(health: NextDbHealth | undefined): string {
  const status = health?.hotRoomIdleMaintenance
  if (!status) {
    return "-"
  }
  const interval = formatDurationMs(health?.hotRoomMaintenanceIntervalMs ?? 0)
  return `${interval}, last ${status.lastEvicted}, total ${status.totalEvicted}`
}

function realtimeRuntimeStateSummary(health: NextDbHealth | undefined): string {
  if (!health) {
    return "-"
  }
  return `${health.realtimeChannelStates ?? 0} states, ${health.realtimeChannelSequences ?? 0} seq`
}

function realtimeMaintenanceSummary(health: NextDbHealth | undefined): string {
  const status = health?.realtimeMaintenance
  if (!status) {
    return "-"
  }
  const interval = formatDurationMs(health?.realtimeMaintenanceIntervalMs ?? 0)
  return `${interval}, stale ${status.totalStaleMembersRemoved}, state ${status.totalOrphanStatesRemoved}, seq ${status.totalOrphanSequencesRemoved}`
}

function recordHotMaintenanceSummary(health: NextDbHealth | undefined): string {
  const cache = health?.recordHotCache
  if (!cache) {
    return "-"
  }
  const interval = formatDurationMs(health?.recordHotMaintenanceIntervalMs ?? 0)
  return `${interval}, last ${cache.durableIdleLastEvicted}, total ${cache.durableIdleTotalEvicted}`
}

function storageClassSummary(storage: NextDbHealth["recordHotCache"]["tables"][number]["storage"]): string {
  if (storage.kind === "lru") {
    return `lru ${storage.maxItems}`
  }
  return storage.kind
}

function shortRuntimeId(runtimeId: string): string {
  return runtimeId.length > 12 ? runtimeId.slice(0, 12) : runtimeId
}

function walRecordRowKey(record: NextDbWalRecord, index: number): string {
  const payload = record.payload as { path?: string; record?: { path?: string }; message?: { path?: string }; objectId?: string }
  return [
    record.lsn,
    record.shard,
    payload.path ?? payload.record?.path ?? payload.message?.path ?? payload.objectId ?? index,
  ].join(":")
}

function latestCacheInvalidation(health: NextDbHealth | undefined) {
  return health?.clientCache?.invalidations.at(-1)
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function formatDurationMs(ms: number): string {
  if (ms <= 0) {
    return "off"
  }
  if (ms < 1_000) {
    return `${ms} ms`
  }
  if (ms < 60_000) {
    return `${(ms / 1_000).toFixed(1)} sec`
  }
  return `${(ms / 60_000).toFixed(1)} min`
}

function schemaDataTargets(schema: NextDbSchema | undefined): DataExplorerTarget[] {
  const targets: DataExplorerTarget[] = []
  for (const [table, rawTable] of Object.entries(schema?.tables ?? {})) {
    targets.push({
      id: table,
      table,
      label: table,
    })
    if (isRecord(rawTable) && isRecord(rawTable.nested)) {
      for (const nested of Object.keys(rawTable.nested)) {
        targets.push({
          id: `${table}.${nested}`,
          table,
          nested,
          label: `${table}.${nested}`,
        })
      }
    }
  }
  return targets
}

function nestedKeyForEditor(target: DataExplorerTarget | undefined, parentKey: string, key: string): string {
  if (!target?.nested) {
    return key
  }
  const prefix = `${parentKey}:`
  return key.startsWith(prefix) ? key.slice(prefix.length) : key
}

function recordValuePreview(value: unknown): string {
  if (!isRecord(value)) {
    return JSON.stringify(value)
  }
  for (const key of ["title", "body", "name", "id"]) {
    const field = value[key]
    if (typeof field === "string" && field.trim()) {
      return field
    }
  }
  return Object.keys(value).slice(0, 4).join(", ")
}

function realtimeMemberSummary(member: RealtimeMember): string {
  const session = member.sessionId ? `@${member.sessionId}` : ""
  if (!isRecord(member.metadata)) {
    return `${member.userId}${session}`
  }
  const metadata = Object.entries(member.metadata)
    .slice(0, 3)
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(" ")
  return metadata ? `${member.userId}${session} ${metadata}` : `${member.userId}${session}`
}

function schemaIndexEntries(schema: NextDbSchema | undefined): Array<{ table: string; index: string; fields: string[] }> {
  const entries: Array<{ table: string; index: string; fields: string[] }> = []
  const tables = schema?.tables ?? {}
  for (const [table, rawTable] of Object.entries(tables)) {
    if (!isRecord(rawTable) || !isRecord(rawTable.indexes)) {
      continue
    }
    for (const [index, rawIndex] of Object.entries(rawTable.indexes)) {
      if (!isRecord(rawIndex) || !Array.isArray(rawIndex.fields)) {
        continue
      }
      entries.push({
        table,
        index,
        fields: rawIndex.fields.map(String),
      })
    }
  }
  return entries
}

function schemaEventEntries(schema: NextDbSchema | undefined): Array<{ name: string; summary: string }> {
  return Object.entries(schema?.events ?? {}).map(([name, event]) => ({
    name,
    summary: behaviorInputSummary(event.payload),
  }))
}

function chatLogStorageEntries(
  storagePolicy: SchemaStoragePolicyResponse | undefined,
): Array<{ path: string; bucket: string; order: string[]; liveWindow: number }> {
  return (storagePolicy?.schema.entries ?? [])
    .filter((entry) => entry.storage.kind === "chatLog")
    .map((entry) => ({
      path: entry.path,
      bucket: String(entry.storage.bucket ?? "-"),
      order: Array.isArray(entry.storage.order) ? entry.storage.order.map(String) : [],
      liveWindow: Number(entry.storage.liveWindow ?? 0),
    }))
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function behaviorMutationField(schema: NextDbSchema | undefined, behavior: string, mutation: string): FieldSchema | undefined {
  return schema?.behaviors[behavior]?.mutations[mutation]
}

function behaviorInputFields(field: FieldSchema | undefined): Array<{ name: string; field: FieldSchema }> {
  return field?.type.kind === "object" ? Object.entries(field.type.fields).map(([name, child]) => ({ name, field: child })) : []
}

function behaviorInputSummary(field: FieldSchema | undefined): string {
  if (!field) {
    return "schema unavailable"
  }
  if (field.type.kind === "object") {
    const names = Object.keys(field.type.fields)
    return names.length === 0 ? "object" : `object: ${names.join(", ")}`
  }
  return fieldTypeLabel(field.type)
}

function behaviorCapabilityRows(behavior: BehaviorManifest): Array<{ label: string; value: string }> {
  return [
    { label: "reads", value: listSummary(behavior.reads, behavior.reads === undefined ? "unrestricted" : "none") },
    { label: "records", value: scopeSummary(behavior.recordScopes, ["read", "write", "nestedRead", "nestedWrite"]) },
    { label: "objects", value: scopeSummary(behavior.objectScopes, ["read", "write"]) },
    { label: "realtime", value: scopeSummary(behavior.realtimeScopes, ["read", "write"]) },
    { label: "connections", value: scopeSummary(behavior.connectionScopes, ["read", "write"]) },
    { label: "users", value: scopeSummary(behavior.userScopes, ["publish"]) },
    { label: "events", value: scopeSummary(behavior.eventScopes, ["publish", "realtimeBroadcast"]) },
    { label: "commands", value: listSummary(behavior.commands, "unrestricted") },
  ].filter((row) => row.value !== "none")
}

function behaviorAllowsRead(
  behavior: BehaviorManifest | undefined,
  capability: BehaviorReadCapability,
): boolean {
  return behavior?.reads === undefined || behavior.reads.includes(capability)
}

function scopeSummary(scope: unknown, keys: string[]): string {
  if (!isRecord(scope)) {
    return "unrestricted"
  }
  const parts: string[] = []
  for (const key of keys) {
    const value = scope[key]
    if (Array.isArray(value) && value.length > 0) {
      parts.push(`${key}:${value.map(String).join("|")}`)
    }
  }
  return parts.length === 0 ? "none" : parts.join(" ")
}

function listSummary(values: unknown, fallback: string): string {
  return Array.isArray(values) && values.length > 0 ? values.map(String).join("|") : fallback
}

function fieldTypeLabel(fieldType: FieldType | undefined): string {
  if (!fieldType) {
    return "json"
  }
  if (fieldType.kind === "id") {
    return `id:${fieldType.entity}`
  }
  if (fieldType.kind === "objectRef") {
    return `objectRef:${fieldType.object}`
  }
  if (fieldType.kind === "text") {
    return `text:${fieldType.inlineUntil}`
  }
  if (fieldType.kind === "list") {
    return `list<${fieldTypeLabel(fieldType.item)}>`
  }
  if (fieldType.kind === "object") {
    return "object"
  }
  return fieldType.kind
}

function mergeBehaviorInput(field: FieldSchema | undefined, current: Record<string, string>): Record<string, string> {
  const entries = behaviorInputFields(field)
  if (entries.length === 0) {
    return {
      $: current.$ ?? defaultFieldInput("input", field),
    }
  }
  const next: Record<string, string> = {}
  for (const { name, field: child } of entries) {
    next[name] = current[name] ?? defaultFieldInput(name, child)
  }
  return next
}

function defaultFieldInput(name: string, field: FieldSchema | undefined): string {
  if (field?.optional) {
    return ""
  }
  if (name === "roomId") {
    return "admin-behavior"
  }
  if (name === "body") {
    return "hello from admin behavior"
  }
  switch (field?.type.kind) {
    case "boolean":
      return "false"
    case "int64":
      return "0"
    case "timeMs":
      return String(Date.now())
    case "list":
      return "[]"
    case "object":
    case "json":
      return "{}"
    case "objectRef":
      return JSON.stringify({ id: "", path: "", contentType: "", sha256: "", byteSize: 0 }, null, 2)
    case "id":
    case "string":
    case "text":
    default:
      return ""
  }
}

function parseBehaviorInput(field: FieldSchema | undefined, values: Record<string, string>): unknown {
  if (field?.type.kind === "object") {
    const input: Record<string, unknown> = {}
    for (const [name, child] of Object.entries(field.type.fields)) {
      const parsed = parseFieldValue(name, child, values[name] ?? "")
      if (parsed !== undefined || !child.optional) {
        input[name] = parsed
      }
    }
    return input
  }
  return parseFieldValue("input", field, values.$ ?? "")
}

function parseFieldValue(name: string, field: FieldSchema | undefined, rawValue: string): unknown {
  const value = rawValue.trim()
  if (!field || field.type.kind === "json") {
    return parseJsonField(name, value || "{}")
  }
  if (!value && field.optional) {
    return undefined
  }
  switch (field.type.kind) {
    case "string":
    case "text":
      return rawValue
    case "id":
      if (!value) {
        throw new Error(`${name} must be a non-empty id`)
      }
      return value
    case "int64":
    case "timeMs": {
      const numeric = Number(value)
      if (!Number.isFinite(numeric)) {
        throw new Error(`${name} must be a number`)
      }
      return numeric
    }
    case "boolean":
      return value === "true"
    case "list":
      return parseJsonField(name, value || "[]")
    case "object":
    case "objectRef":
      return parseJsonField(name, value || "{}")
    default:
      return value
  }
}

function parseJsonField(name: string, value: string): unknown {
  try {
    return JSON.parse(value)
  } catch {
    throw new Error(`${name} must be valid JSON`)
  }
}

function shallowEqual(left: Record<string, string>, right: Record<string, string>): boolean {
  const leftKeys = Object.keys(left)
  const rightKeys = Object.keys(right)
  return leftKeys.length === rightKeys.length && leftKeys.every((key) => left[key] === right[key])
}

function connectionTransportSummary(connections: ConnectionListResponse | undefined): string {
  const transports = new Set((connections?.sessions ?? []).map((session) => session.transport))
  return transports.size === 0 ? "-" : [...transports].join(", ")
}

function connectionProtocolSummary(health: NextDbHealth | undefined): string {
  const layer = health?.connectionLayer
  if (!layer) {
    return "-"
  }
  return `${layer.protocol} / ${layer.frameEncoding}`
}

function connectionCapabilitySummary(health: NextDbHealth | undefined): string {
  const transports = health?.connectionLayer?.supportedTransports ?? []
  return transports.length === 0 ? "-" : transports.join(", ")
}

function connectionDefaultTransportSummary(health: NextDbHealth | undefined): string {
  return health?.connectionLayer?.defaultTransport ?? "-"
}

function connectionWebSocketPathSummary(health: NextDbHealth | undefined): string {
  const webSocket = health?.connectionLayer?.webSocket
  if (!webSocket?.supported) {
    return "unsupported"
  }
  return webSocket.connectPath ?? health?.connectionLayer?.connectPath ?? "-"
}

function connectionJsonLineGatewaySummary(health: NextDbHealth | undefined): string {
  const custom = health?.connectionLayer?.custom
  if (!custom?.supported) {
    return "unsupported"
  }
  return custom.connectPath ?? "-"
}

function connectionWebTransportPathSummary(health: NextDbHealth | undefined): string {
  const webTransport = health?.connectionLayer?.webTransport
  if (!webTransport?.supported) {
    return "external"
  }
  return webTransport.connectPath ?? "-"
}

function localConfiguredTransportSummary(status: NextDbLocalDataStatus | undefined): string {
  if (!status) {
    return "-"
  }
  return `${status.configuredRealtimeTransportKind} / ${status.configuredConnectionTransport}`
}

function localActiveTransportSummary(status: NextDbLocalDataStatus | undefined): string {
  if (!status) {
    return "-"
  }
  return `${status.realtimeTransportKind} / ${status.connectionTransport}`
}

function connectionRoomCount(connections: ConnectionListResponse | undefined): number {
  return new Set((connections?.sessions ?? []).flatMap((session) => session.subscribedRooms)).size
}

function connectionTableCount(connections: ConnectionListResponse | undefined): number {
  return new Set((connections?.sessions ?? []).flatMap((session) => [
    ...session.subscribedTables,
    ...session.subscribedNestedTables,
  ])).size
}

function connectionQueryCount(connections: ConnectionListResponse | undefined): number {
  return new Set((connections?.sessions ?? []).flatMap((session) => session.subscribedQueries)).size
}

function connectionQueryTableCount(connections: ConnectionListResponse | undefined): number {
  return new Set((connections?.sessions ?? []).flatMap((session) => Object.keys(session.subscribedQueryTables ?? {}))).size
}

function connectionUserEventSubscriptionCount(connections: ConnectionListResponse | undefined): number {
  return (connections?.sessions ?? []).filter((session) => session.subscribedUserEvents).length
}

function connectionObjectSubscriptionCount(connections: ConnectionListResponse | undefined): number {
  return (connections?.sessions ?? []).filter((session) => session.subscribedObjects).length
}

function connectionMetadataSessionCount(connections: ConnectionListResponse | undefined): number {
  return (connections?.sessions ?? []).filter((session) => hasConnectionMetadata(session.metadata)).length
}

function connectionUserSummary(user: ConnectionListResponse["userSummaries"][number]): string {
  const transportParts = [
    user.transports.webSocket > 0 ? `${user.transports.webSocket} ws` : "",
    user.transports.webTransport > 0 ? `${user.transports.webTransport} wt` : "",
    user.transports.custom > 0 ? `${user.transports.custom} custom` : "",
  ].filter(Boolean)
  const subscriptionParts = [
    `${user.subscribedRooms.length} rooms`,
    `${user.subscribedTables.length + user.subscribedNestedTables.length} tables`,
    `${user.subscribedQueries.length} queries`,
    `${Object.keys(user.subscribedQueryTables ?? {}).length} query tables`,
    user.userEventSessions > 0 ? `${user.userEventSessions} inbox` : "",
    user.objectSessions > 0 ? `${user.objectSessions} objects` : "",
  ].filter(Boolean)
  return [...transportParts, ...subscriptionParts].join(", ")
}

function connectionEventSummary(event: ConnectionEvent): string {
  const sessionId = event.session?.sessionId ?? event.sessionId ?? "-"
  const userId = event.session?.userId ?? event.userId ?? "anonymous"
  const target = event.targetedSessionIds.length > 0 ? `, targets ${event.targetedSessionIds.length}` : ""
  const reason = event.reason ? `, ${event.reason}` : ""
  return `${event.eventType}: ${userId}/${sessionId}${target}${reason}`
}

function connectionSubscriptionSummary(session: ConnectionListResponse["sessions"][number]): string {
  const parts = [
    `${session.subscribedRooms.length} rooms`,
    `${session.subscribedTables.length} tables`,
    `${session.subscribedNestedTables.length} nested`,
    `${session.subscribedQueries.length} queries`,
    `${Object.keys(session.subscribedQueryTables ?? {}).length} query tables`,
  ]
  if (session.subscribedUserEvents) {
    parts.push("user inbox")
  }
  if (session.subscribedObjects) {
    parts.push("objects")
  }
  return parts.join(", ")
}

function connectionMetadataSummary(metadata: unknown): string {
  if (!hasConnectionMetadata(metadata)) {
    return "-"
  }
  if (metadata && typeof metadata === "object" && !Array.isArray(metadata)) {
    const record = metadata as Record<string, unknown>
    const parts = Object.entries(record)
      .slice(0, 4)
      .map(([key, value]) => `${key}: ${connectionMetadataValueSummary(value)}`)
    return parts.join(", ")
  }
  return connectionMetadataValueSummary(metadata)
}

function connectionMetadataValueSummary(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.slice(0, 3).map(connectionMetadataValueSummary).join(", ")}${value.length > 3 ? ", ..." : ""}]`
  }
  if (value && typeof value === "object") {
    return "{...}"
  }
  const text = String(value)
  return text.length > 28 ? `${text.slice(0, 25)}...` : text
}

function normalizeAdminRealtimeTransport(value: unknown): AdminRealtimeTransportKind {
  return value === "jsonl" ? "jsonl" : DEFAULT_ADMIN_TRANSPORT
}

function hasConnectionMetadata(metadata: unknown): boolean {
  if (metadata === undefined || metadata === null) {
    return false
  }
  if (typeof metadata === "object" && !Array.isArray(metadata)) {
    return Object.keys(metadata as Record<string, unknown>).length > 0
  }
  return true
}

function formatAckPolicy(policy: NextDbHealth["walReplicas"][number]["remoteAckPolicy"]): string {
  if (typeof policy === "string") {
    return policy
  }
  return `count ${policy.count}`
}

function residencyRatio(health: NextDbHealth | undefined): number {
  if (!health || health.maxHotRooms === 0) {
    return 0
  }
  return Math.min(1, health.hotRoomCount / health.maxHotRooms)
}

const rootElement = document.getElementById("root")!
const nextDbAdminGlobal = globalThis as typeof globalThis & { __nextDbAdminRoot?: Root }
const root = nextDbAdminGlobal.__nextDbAdminRoot ?? createRoot(rootElement)
nextDbAdminGlobal.__nextDbAdminRoot = root

root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
