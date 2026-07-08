export type Durability = "strict" | "relaxed" | "volatile"

const DEFAULT_REALTIME_CHANNEL_EVENT_LIMIT = 256
const DEFAULT_SEND_MESSAGE_BATCH_MAX = 128

export interface NextDbMessage {
  id: string
  roomId: string
  senderId: string
  body: string
  attachments: NextDbObjectRef[]
  createdAtMs: number
  lsn: number
  path: string
}

export interface NextDbMessageDraft {
  id: string
  roomId: string
  senderId: string
  body: string
  attachments: NextDbObjectRef[]
  createdAtMs: number
  path: string
}

export interface NextDbUserEvent {
  id: string
  userId: string
  name: string
  payload: unknown
  createdAtMs: number
  lsn: number
  path: string
}

export interface NextDbUserEventDraft {
  id: string
  userId: string
  name: string
  payload: unknown
  createdAtMs: number
  path: string
}

export interface NextDbUserProfile {
  userId: string
  displayName?: string
  metadata: unknown
  createdAtMs: number
  updatedAtMs: number
  lsn: number
  path: string
}

export interface NextDbRecord<T = unknown> {
  table: string
  key: string
  value: T
  updatedAtMs: number
  lsn: number
  path: string
}

export interface RecordResponse<T = unknown> {
  record: NextDbRecord<T>
}

export interface DeleteRecordResponse {
  table: string
  key: string
  deleted: boolean
  lsn: number
  deletedAtMs?: number
  path: string
}

export interface ListRecordsResponse<T = unknown> {
  table: string
  records: Array<NextDbRecord<T>>
  nextAfterKey?: string
  nextCursor?: string
  hasMore: boolean
}

export type NestedListOrder = "key" | "schema"

export type RecordPredicateOp = "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "contains" | "startsWith" | "exists"

export interface RecordPredicateTerm {
  field: string
  op: RecordPredicateOp
  value?: unknown
}

export interface RecordPredicate {
  all: RecordPredicateTerm[]
}

export interface RecordLiveQueryOptions {
  queryId?: string
  table: string
  parentKey?: string
  nested?: string
  indexName?: string
  value?: unknown
  values?: unknown[]
  lower?: unknown
  upper?: unknown
  lowerValues?: unknown[]
  upperValues?: unknown[]
  afterKey?: string
  afterCursor?: string
  limit?: number
  order?: NestedListOrder
  predicate?: RecordPredicate
  resultId?: string
  diff?: boolean
  persistent?: boolean
}

export interface RecordLiveQueryRemovedRecord {
  table: string
  key: string
  path: string
  deleted: boolean
  lsn?: number
  deletedAtMs?: number
}

export interface RecordLiveQueryDiff<T = unknown> {
  table: string
  added: Array<NextDbRecord<T>>
  updated: Array<NextDbRecord<T>>
  removed: RecordLiveQueryRemovedRecord[]
  keys: string[]
  nextAfterKey?: string
  nextCursor?: string
  hasMore: boolean
}

export interface RecordLiveQueryResult<T = unknown> {
  queryId: string
  response: ListRecordsResponse<T>
  currentLsn: number
  resultId: string
  diff?: RecordLiveQueryDiff<T>
}

export interface NestedSchemaOrderListOptions extends FreshnessOptions {
  limit?: number
  afterKey?: string
  afterCursor?: string
  predicate?: RecordPredicate
}

export type RecordOrderDirection = "asc" | "desc"

export interface RecordOrderTerm {
  field: string
  direction: RecordOrderDirection
}

export interface LocalOrderedRecordsResponse<T = unknown> {
  records: Array<NextDbRecord<T>>
  nextCursor?: string
  hasMore: boolean
}

interface StoredRecordOrderMetadata {
  orderId: string
  table: string
  keyPrefix: string
  order: RecordOrderTerm[]
}

interface StoredRecordOrderEntry {
  id: string
  orderId: string
  table: string
  keyPrefix: string
  cursor: string
  recordPath: string
  record: NextDbRecord
}

export interface QueryRecordsByIndexOptions extends FreshnessOptions {
  value?: unknown
  values?: unknown[]
  lower?: unknown
  upper?: unknown
  lowerValues?: unknown[]
  upperValues?: unknown[]
  limit?: number
  afterKey?: string
  afterCursor?: string
  predicate?: RecordPredicate
}

export interface LocalIndexQuery {
  fields: string[]
  values?: unknown[]
  lowerValues?: unknown[]
  upperValues?: unknown[]
  keyPrefix?: string
  limit: number
  afterKey?: string
  afterCursor?: string
}

export interface LocalIndexedRecordsResponse<T = unknown> {
  records: Array<NextDbRecord<T>>
  nextCursor?: string
  hasMore: boolean
}

export interface FreshnessOptions {
  minLsn?: number
  timeoutMs?: number
  consistency?: ReadConsistency
  recordConsistency?: RecordReadConsistency
}

export type ReadConsistency = "local" | "quorum" | "all"
export type RecordReadConsistency = "eventual" | "read-your-writes" | "strong"

export interface ListUsersOptions extends FreshnessOptions {
  limit?: number
  afterUserId?: string
}

export interface ListCachedUsersOptions {
  limit?: number
  afterUserId?: string
}

export interface ListCachedObjectsOptions {
  limit?: number
  afterId?: string
}

export interface ListCachedRoomMessagesOptions {
  limit?: number
  beforeLsn?: number
}

export interface ListCachedUserEventsOptions {
  limit?: number
  beforeLsn?: number
}

export interface ListCachedRecordsOptions {
  limit?: number
  afterKey?: string
}

export interface ListCachedNestedRecordsOptions extends ListCachedRecordsOptions {
  order?: NestedListOrder
  afterCursor?: string
}

export interface PageReadOptions extends FreshnessOptions {
  limit?: number
  afterKey?: string
  predicate?: RecordPredicate
}

export interface MessagesResponse {
  roomId: string
  source: "live" | "chatLog" | "cache"
  messages: NextDbMessage[]
}

export interface UserEventsListOptions extends FreshnessOptions {
  limit?: number
  beforeLsn?: number
  sync?: boolean
}

export interface UserEventsResponse {
  userId: string
  events: NextDbUserEvent[]
}

export interface NextDbObjectMetadata {
  id: string
  path: string
  contentType: string
  byteSize: number
  sha256: string
  createdAtMs: number
}

export interface ListObjectsOptions extends FreshnessOptions {
  limit?: number
  afterId?: string
}

export interface ListObjectsResponse {
  objects: NextDbObjectMetadata[]
  nextAfterId?: string
  hasMore: boolean
}

export interface ObjectBodyRangeOptions extends FreshnessOptions {
  start?: number
  end?: number
  suffixLength?: number
}

export interface ObjectBodyRangeResponse {
  body: Blob
  contentRange: string
  start: number
  end: number
  byteSize: number
  contentType: string
}

export interface PutObjectOptions {
  contentType?: string
  objectId?: string
  clientMutationId?: string
}

export interface DeleteObjectOptions {
  force?: boolean
  clientMutationId?: string
}

export interface DeleteObjectResponse {
  objectId: string
  deleted: boolean
  lsn: number
  deletedAtMs?: number
  path: string
}

export interface NextDbObjectRef {
  id: string
  path: string
  contentType: string
  byteSize: number
  sha256: string
}

export interface SendMessageOptions {
  durability?: Durability
  attachments?: string[]
  clientMutationId?: string
}

export interface SendMessagesItem {
  body: string
  attachments?: string[]
  clientMutationId?: string
}

export interface SendMessagesOptions {
  durability?: Durability
}

interface PendingSendMessageBatch {
  roomId: string
  userId: string
  durability: Durability
  items: PendingSendMessageBatchItem[]
  scheduled: boolean
}

interface PendingSendMessageBatchItem {
  body: string
  attachments: string[]
  clientMutationId: string
  resolve: (message: NextDbMessage) => void
  reject: (error: unknown) => void
}

export interface MessagesCreatedResponse {
  type: "messagesCreated"
  messages: NextDbMessage[]
}

export interface PublishUserEventOptions {
  durability?: Exclude<Durability, "volatile">
  clientMutationId?: string
}

export interface UpsertUserOptions {
  displayName?: string
  metadata?: unknown
  clientMutationId?: string
}

export interface UpsertRecordOptions {
  durability?: Durability
  expectedLsn?: number
  clientMutationId?: string
}

export interface UpsertManyRecordItem<T = unknown> {
  key: string
  value: T
  expectedLsn?: number
}

export interface DeleteRecordOptions {
  durability?: Durability
  expectedLsn?: number
  clientMutationId?: string
}

export type RecordTransactionOperation<T = unknown> =
  | {
      type: "upsert"
      table: string
      key: string
      value: T
      expectedLsn?: number
    }
  | {
      type: "delete"
      table: string
      key: string
      expectedLsn?: number
    }
  | {
      type: "nestedUpsert"
      table: string
      parentKey: string
      nested: string
      nestedKey: string
      value: T
      expectedLsn?: number
    }
  | {
      type: "nestedDelete"
      table: string
      parentKey: string
      nested: string
      nestedKey: string
      expectedLsn?: number
    }

export interface RecordTransactionOptions {
  durability?: Exclude<Durability, "volatile">
  clientMutationId?: string
}

export type RecordTransactionOperationResult<T = unknown> =
  | {
      type: "recordUpserted"
      record: NextDbRecord<T>
    }
  | {
      type: "recordDeleted"
      table: string
      key: string
      deletedAtMs: number
      lsn: number
      path: string
      previousRecord?: NextDbRecord
    }

export interface RecordTransactionResponse<T = unknown> {
  lsn: number
  operations: Array<RecordTransactionOperationResult<T>>
}

export interface RecordBatchResponse<T = unknown> extends RecordTransactionResponse<T> {
  transactionCount: number
}

export interface VolatilePublishResponse {
  type: "volatilePublished"
  delivered: number
}

export interface UserEventPublishResponse {
  type: "userEventPublished"
  event: NextDbUserEvent
}

export interface UserResponse {
  user: NextDbUserProfile
}

export interface ListUsersResponse {
  users: NextDbUserProfile[]
  nextAfterUserId?: string
  hasMore: boolean
}

export type PendingWriteType =
  | "sendMessage"
  | "userEvent"
  | "userProfileUpsert"
  | "recordUpsert"
  | "recordDelete"
  | "nestedRecordUpsert"
  | "nestedRecordDelete"
  | "recordTransaction"
  | "objectPut"
  | "objectDelete"

export interface PendingSendMessageWrite {
  id: string
  type: "sendMessage"
  createdAtMs: number
  attempts: number
  lastError?: string
  roomId: string
  userId: string
  body: string
  attachments: string[]
  durability: Exclude<Durability, "volatile">
  clientMutationId: string
}

export interface PendingUserEventWrite {
  id: string
  type: "userEvent"
  createdAtMs: number
  attempts: number
  lastError?: string
  userId: string
  name: string
  payload: unknown
  durability: Exclude<Durability, "volatile">
  clientMutationId: string
}

export interface PendingUserProfileUpsertWrite {
  id: string
  type: "userProfileUpsert"
  createdAtMs: number
  attempts: number
  lastError?: string
  userId: string
  displayName?: string
  metadata: unknown
  clientMutationId: string
}

export interface PendingRecordUpsertWrite<T = unknown> {
  id: string
  type: "recordUpsert"
  createdAtMs: number
  attempts: number
  lastError?: string
  table: string
  key: string
  value: T
  durability: Exclude<Durability, "volatile">
  expectedLsn?: number
  clientMutationId: string
}

export interface PendingRecordDeleteWrite {
  id: string
  type: "recordDelete"
  createdAtMs: number
  attempts: number
  lastError?: string
  table: string
  key: string
  durability: Exclude<Durability, "volatile">
  expectedLsn?: number
  clientMutationId: string
}

export interface PendingNestedRecordUpsertWrite<T = unknown> {
  id: string
  type: "nestedRecordUpsert"
  createdAtMs: number
  attempts: number
  lastError?: string
  table: string
  parentKey: string
  nested: string
  nestedKey: string
  value: T
  durability: Exclude<Durability, "volatile">
  expectedLsn?: number
  clientMutationId: string
}

export interface PendingNestedRecordDeleteWrite {
  id: string
  type: "nestedRecordDelete"
  createdAtMs: number
  attempts: number
  lastError?: string
  table: string
  parentKey: string
  nested: string
  nestedKey: string
  durability: Exclude<Durability, "volatile">
  expectedLsn?: number
  clientMutationId: string
}

export interface PendingRecordTransactionWrite<T = unknown> {
  id: string
  type: "recordTransaction"
  createdAtMs: number
  attempts: number
  lastError?: string
  operations: Array<RecordTransactionOperation<T>>
  durability: Exclude<Durability, "volatile">
  clientMutationId: string
}

export interface PendingObjectPutWrite {
  id: string
  type: "objectPut"
  createdAtMs: number
  attempts: number
  lastError?: string
  objectId: string
  contentType: string
  body: Blob
  clientMutationId: string
}

export interface PendingObjectDeleteWrite {
  id: string
  type: "objectDelete"
  createdAtMs: number
  attempts: number
  lastError?: string
  objectId: string
  force?: boolean
  clientMutationId: string
}

export type NextDbPendingWrite =
  | PendingSendMessageWrite
  | PendingUserEventWrite
  | PendingUserProfileUpsertWrite
  | PendingRecordUpsertWrite
  | PendingRecordDeleteWrite
  | PendingNestedRecordUpsertWrite
  | PendingNestedRecordDeleteWrite
  | PendingRecordTransactionWrite
  | PendingObjectPutWrite
  | PendingObjectDeleteWrite

export interface PendingWriteStats {
  total: number
  byType: Record<PendingWriteType, number>
  estimatedBytes: number
  objectPutBytes: number
  failed: number
  totalAttempts: number
  oldestCreatedAtMs?: number
  newestCreatedAtMs?: number
  maxWrites: number
  maxBytes: number
  overMaxWrites: boolean
  overMaxBytes: boolean
}

export type PendingWriteLimitKind = "maxPendingWrites" | "maxPendingWriteBytes"

export interface PendingWriteLimitDetails {
  limitKind: PendingWriteLimitKind
  writeId: string
  writeType: PendingWriteType
  currentWrites: number
  nextWrites: number
  maxWrites: number
  currentBytes: number
  writeBytes: number
  nextBytes: number
  maxBytes: number
}

export interface PendingWriteQueueStatus {
  stats: PendingWriteStats
  writes: NextDbPendingWrite[]
  autoFlush: {
    enabled: boolean
    intervalMs: number
    limit: number
    retryOnStart: boolean
    scheduled: boolean
    inFlight: boolean
  }
}

export interface DiscardPendingWriteOptions {
  removeOptimistic?: boolean
}

export interface DiscardPendingWriteResponse {
  id: string
  discarded: boolean
  removedOptimistic: boolean
  write?: NextDbPendingWrite
}

export interface ResetPendingWriteResponse {
  id: string
  reset: boolean
  write?: NextDbPendingWrite
}

export interface NextDbCacheScope {
  kind: "memory" | "indexedDb" | "custom"
  namespace: string
  name?: string
  endpoint: string
  userId?: string
}

export interface NextDbObjectCacheCoverage {
  objects: number
  byteSize: number
  cachedByteSize: number
  rangeChunks: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbRoomCacheCoverage {
  messages: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbUserCacheCoverage {
  events: number
  profile: boolean
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbRecordCacheCoverage {
  records: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbNestedRecordCacheCoverage {
  records: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbRealtimeChannelCacheCoverage {
  stateVersion?: number
  stateUpdatedAtMs?: number
  members: number
  membersUpdatedAtMs?: number
  recentEvents: number
  latestEventSequence?: number
  latestEventTimestampMs?: number
  recentSignals: number
  latestSignalSequence?: number
  latestSignalTimestampMs?: number
  activeSubscription: boolean
}

export interface NextDbCacheCoverage {
  globalCursor: number
  objects: NextDbObjectCacheCoverage
  rooms: Record<string, NextDbRoomCacheCoverage>
  users: Record<string, NextDbUserCacheCoverage>
  tables: Record<string, NextDbRecordCacheCoverage>
  nestedTables: Record<string, Record<string, NextDbNestedRecordCacheCoverage>>
  realtimeChannels: Record<string, NextDbRealtimeChannelCacheCoverage>
}

export interface NextDbLocalDataStatus {
  endpoint: string
  initialEndpoint: string
  cacheScope: NextDbCacheScope
  configuredRealtimeTransportKind: NextDbRealtimeTransportKind | "custom"
  configuredConnectionTransport: ConnectionTransport
  realtimeTransportKind: NextDbRealtimeTransportKind | "custom"
  connectionTransport: ConnectionTransport
  transportState: RealtimeTransportState | "idle"
  manuallyClosed: boolean
  lastSeenLsn: number
  objectSeenLsn: number
  roomSeenLsn: Record<string, number>
  userSeenLsn: Record<string, number>
  tableSeenLsn: Record<string, number>
  nestedTableSeenLsn: Record<string, number>
  cache: NextDbCacheStats
  coverage: NextDbCacheCoverage
  pendingWrites: PendingWriteStats
  storedSubscriptions: NextDbStoredSubscription[]
  activeSubscriptions: {
    rooms: string[]
    tables: string[]
    nestedTables: string[]
    queries: string[]
    realtimeChannels: string[]
    userEvents: boolean
    objects: boolean
  }
  persistentSubscriptions: {
    rooms: string[]
    tables: string[]
    nestedTables: string[]
    queries: string[]
    realtimeChannels: string[]
    userEvents: boolean
    objects: boolean
  }
  realtimeChannelStates: Record<string, { version: number; updatedAtMs: number }>
  realtimeChannelMembers: Record<string, { memberCount: number; updatedAtMs?: number }>
  realtimeChannelEvents: Record<string, { eventCount: number; latestSequence?: number; latestTimestampMs?: number }>
  realtimeChannelSignals: Record<string, { signalCount: number; latestSequence?: number; latestTimestampMs?: number }>
  connectionSessions: { sessionCount: number; userCount: number; updatedAtMs?: number }
  cacheMetadata?: ClientCacheMetadata
  cacheProfile?: ClientCacheProfile
}

export interface FlushPendingWritesResult {
  attempted: number
  committed: number
  remaining: number
  errors: Array<{ id: string; error: string; retryable: boolean }>
}

export interface PendingWriteAutoFlushOptions {
  enabled?: boolean
  intervalMs?: number
  limit?: number
  retryOnStart?: boolean
}

export type NextDbWalPayload =
  | {
      type: "messageCreated"
      message: NextDbMessageDraft
    }
  | {
      type: "userEventPublished"
      event: NextDbUserEventDraft
    }
  | {
      type: "userUpserted"
      user: Omit<NextDbUserProfile, "lsn">
    }
  | {
      type: "objectCommitted"
      object: NextDbObjectMetadata
      clientMutationId?: string
    }
  | {
      type: "objectDeleted"
      objectId: string
      deletedAtMs: number
      path: string
      force?: boolean
      clientMutationId?: string
    }
  | {
      type: "recordUpserted"
      record: Omit<NextDbRecord, "lsn">
    }
  | {
      type: "recordDeleted"
      record: Omit<DeleteRecordResponse, "deleted" | "lsn"> & {
        deletedAtMs: number
      }
    }
  | {
      type: "recordTransactionCommitted"
      operations: Array<
        | {
            type: "upsert"
            record: Omit<NextDbRecord, "lsn">
          }
        | {
            type: "delete"
            record: Omit<DeleteRecordResponse, "deleted" | "lsn"> & {
              deletedAtMs: number
            }
          }
      >
    }
  | {
      type: "schemaApplied"
      schema: NextDbSchema
      migration: SchemaMigrationPlan
    }
  | {
      type: "behaviorPublished"
      publish: {
        epoch: number
        loaded: number
        manifests: Array<{
          name: string
          version: string
          modulePath: string
          mutations: string[]
        }>
        publishedAtMs: number
      }
    }
  | {
      type: "hostHttpRequested"
      request: NextDbHostHttpRequestDraft
    }
  | {
      type: "hostHttpCompleted"
      requestId: string
      completedAtMs: number
    }

export interface NextDbHostHttpRequestDraft {
  requestId: string
  method: string
  url: string
  headers?: Record<string, string>
  body?: unknown
  bodyBase64?: string
  timeoutMs: number
  actorKind: ActorKind
  actorKey: string
  reminderId: string
  continuation: BehaviorContinuationPayload
  requestedAtMs: number
}

export interface NextDbWalRecord {
  lsn: number
  shard: number
  shardEpoch: number
  ownerNodeId: string
  timestampMs: number
  schemaVersion: number
  durability: Exclude<Durability, "volatile">
  payload: NextDbWalPayload
  checksum?: string
}

export interface AuditWalOptions {
  afterLsn?: number
  limit?: number
  payloadType?: NextDbWalPayload["type"]
  roomId?: string
  userId?: string
  objectId?: string
  table?: string
  recordKey?: string
  path?: string
  clientMutationId?: string
}

export interface AuditWalResponse {
  records: NextDbWalRecord[]
  nextAfterLsn: number
  hasMore: boolean
}

export type AuditTraceKind =
  | "room"
  | "user"
  | "object"
  | "record"
  | "nestedRecord"
  | "path"
  | "clientMutation"

export type AuditTraceOptions =
  | {
      kind: "room" | "user" | "object"
      id: string
      afterLsn?: number
      limit?: number
    }
  | {
      kind: "record"
      table: string
      recordKey?: string
      id?: string
      afterLsn?: number
      limit?: number
    }
  | {
      kind: "nestedRecord"
      table: string
      parentKey: string
      nested: string
      nestedKey?: string
      id?: string
      afterLsn?: number
      limit?: number
    }
  | {
      kind: "path"
      path?: string
      id?: string
      afterLsn?: number
      limit?: number
    }
  | {
      kind: "clientMutation"
      clientMutationId?: string
      id?: string
      afterLsn?: number
      limit?: number
    }

export interface AuditTraceTarget {
  kind: AuditTraceKind
  id: string
  table?: string
  recordKey?: string
  parentKey?: string
  nested?: string
  nestedKey?: string
  path?: string
  clientMutationId?: string
}

export interface AuditTraceResponse {
  target: AuditTraceTarget
  records: NextDbWalRecord[]
  nextAfterLsn: number
  hasMore: boolean
}

export type AuditReplayOptions =
  | {
      kind: "user" | "object"
      id: string
      atLsn?: number
    }
  | {
      kind: "record"
      table: string
      recordKey?: string
      id?: string
      atLsn?: number
    }
  | {
      kind: "nestedRecord"
      table: string
      parentKey: string
      nested: string
      nestedKey?: string
      id?: string
      atLsn?: number
    }

export type AuditReplayStatus = "exists" | "deleted" | "missing"

export interface AuditReplayDelete {
  table?: string
  key?: string
  objectId?: string
  path: string
  deletedAtMs: number
  force?: boolean
}

export interface AuditReplayResponse<T = unknown> {
  target: AuditTraceTarget
  atLsn: number
  status: AuditReplayStatus
  sourceLsn?: number
  record?: NextDbRecord<T>
  user?: NextDbUserProfile
  object?: NextDbObjectMetadata
  delete?: AuditReplayDelete
}

export interface SyncPullOptions {
  afterLsn?: number
  rooms?: string[]
  users?: string[]
  tables?: string[]
  nestedTables?: SyncNestedTableTarget[]
  objects?: boolean
  limit?: number
}

export interface SyncNestedTableTarget {
  table: string
  parentKey: string
  nested: string
}

export interface SyncUntilCaughtUpOptions extends SyncPullOptions {
  maxPages?: number
}

export interface SyncPullResponse {
  events: DeliveryEvent[]
  nextAfterLsn: number
  currentLsn: number
  hasMore: boolean
}

export interface SyncUntilCaughtUpResponse extends SyncPullResponse {
  pages: number
}

export interface SyncWaitOptions {
  timeoutMs?: number
  consistency?: ReadConsistency
  shardKey?: string
  shard?: number
}

export interface SyncWaitResponse {
  minLsn: number
  currentLsn: number
  caughtUp: boolean
  waitedMs: number
  consistency: ReadConsistency
  shard?: number
  remoteRequiredAcks: number
  remoteAcked: number
  remoteCaughtUp: boolean
}

export type ClientCacheInvalidationScope = "all" | "profile" | "object" | "room" | "user" | "table" | "nestedTable"

export interface ClientCacheProfile {
  version: number
  leaseTtlMs: number
  maxObjects: number
  maxObjectBytes: number
  maxRoomMessages: number
  maxUserEvents: number
  maxRecordsPerTable: number
  maxNestedPartitions: number
  maxPendingWrites: number
  maxPendingWriteBytes: number
  offlineWrites: boolean
}

export interface ClientCacheLease {
  clientId: string
  sessionId?: string
  issuedAtMs: number
  expiresAtMs: number
  profileVersion: number
}

export interface ClientCacheInvalidationEntry {
  id: string
  generation: number
  scope: ClientCacheInvalidationScope
  key?: string
  table?: string
  parentKey?: string
  nested?: string
  minValidLsn: number
  reason: string
  createdAtMs: number
}

export interface ClientCacheProfileResponse {
  runtimeId: string
  profile: ClientCacheProfile
  lease: ClientCacheLease
  invalidations: ClientCacheInvalidationEntry[]
  currentLsn: number
  schemaVersion: number
  resetRequired: boolean
}

export interface ClientCacheMetadata {
  clientId: string
  sessionId?: string
  runtimeId?: string
  profileVersion: number
  schemaVersion: number
  maxObjects?: number
  maxObjectBytes?: number
  maxRoomMessages?: number
  maxUserEvents?: number
  maxRecordsPerTable?: number
  maxNestedPartitions?: number
  maxPendingWrites?: number
  maxPendingWriteBytes?: number
  offlineWrites?: boolean
  invalidationGeneration: number
  leaseExpiresAtMs: number
  lastValidatedAtMs: number
}

export interface ClientCacheInvalidateResponse {
  entry: ClientCacheInvalidationEntry
  control: {
    profile: ClientCacheProfile
    invalidations: ClientCacheInvalidationEntry[]
  }
}

export interface ClientCacheInvalidateOptions {
  scope: ClientCacheInvalidationScope
  key?: string
  table?: string
  parentKey?: string
  nested?: string
  minValidLsn?: number
  reason?: string
}

export interface ClientCacheProfileUpdateOptions {
  expectedVersion?: number
  leaseTtlMs?: number
  maxObjects?: number
  maxObjectBytes?: number
  maxRoomMessages?: number
  maxUserEvents?: number
  maxRecordsPerTable?: number
  maxNestedPartitions?: number
  maxPendingWrites?: number
  maxPendingWriteBytes?: number
  offlineWrites?: boolean
  reason?: string
}

export interface ClientCacheProfileUpdateResponse {
  profile: ClientCacheProfile
  invalidation: ClientCacheInvalidationEntry
}

export interface EnforceLocalCacheProfileOptions {
  profile?: ClientCacheProfile
  refreshLease?: boolean
}

export interface LocalCacheProfileTrimReport {
  objects: number
  roomMessages: Record<string, number>
  userEvents: Record<string, number>
  records: Record<string, number>
  nestedRecords: Record<string, Record<string, number>>
  nestedPartitions: Record<string, number>
  total: number
}

export interface LocalCacheProfileEnforcementResult {
  profile: ClientCacheProfile
  before: NextDbCacheStats
  after: NextDbCacheStats
  removed: LocalCacheProfileTrimReport
}

export interface AdminSnapshotResponse {
  lsn: number
  roomCount: number
  recordHotTableCount: number
  recordHotRecordCount: number
}

export interface RuntimeRecordActivationOptions {
  table: string
  parentKey?: string
  nested?: string
  key?: string
  keys?: string[]
  indexName?: string
  value?: unknown
  values?: unknown[]
  lower?: unknown
  upper?: unknown
  lowerValues?: unknown[]
  upperValues?: unknown[]
  afterKey?: string
  afterCursor?: string
  order?: "key" | "schema"
  limit?: number
  predicate?: RecordPredicate
}

export interface RuntimeRecordActivationResponse {
  table: string
  parentKey?: string
  nested?: string
  requested: number
  found: number
  activated: number
  evicted: number
  actorScope?: ScopeRowsActivationResult | null
  actorScopes: ScopeRowsActivationResult[]
  before: RecordHotCacheStatus
  after: RecordHotCacheStatus
}

export type RuntimeRecordHandleActivationOptions = Omit<RuntimeRecordActivationOptions, "table" | "parentKey" | "nested">

export interface RuntimeRoomActivationOptions {
  roomId: string
  limit?: number
}

export interface RuntimeRoomActivationResponse {
  roomId: string
  requested: number
  found: number
  activated: boolean
  evicted: boolean
  beforeRoomCount: number
  afterRoomCount: number
  source: "live" | "chatLog" | "missing"
}

export interface RuntimeRoomStatus {
  roomId: string
  messages: number
  oldestLsn?: number
  newestLsn?: number
  lastAccessedMs: number
}

export type ActorKind = "room" | "scope" | "table" | "view" | "aggregate"

export interface ActorId {
  kind: ActorKind
  key: string
}

export interface ActorKernelStatus {
  totalActors: number
  roomActors: number
  kernelActors: number
  scopeRows: number
  scopeBytes: number
  scopeSubscriptionRefCount: number
  subscribedScopes: number
  lingeringScopes: number
  l1ScopeActors: number
  l3ScopeActors: number
  tableScopes: number
  tablePendingSplits: number
  kindCounts: Record<string, number>
  oldestAccessedMs?: number | null
  newestAccessedMs?: number | null
}

export interface ScopeRowsActivationResult {
  actorId: ActorId
  tableActorId: ActorId
  shardIndex: number
  created: boolean
  requested: number
  inserted: number
  updated: number
  rows: number
  bytes: number
  tableScopes: number
  tablePendingSplits: number
  scopeSplitPending: boolean
  scopeSplitRows: number
  scopeSplitBytes: number
  turnCount: number
  lastAccessedMs: number
}

export interface RuntimeActorActivationOptions {
  kind: ActorKind
  key: string
}

export interface RuntimeActorActivationResponse {
  actorId: ActorId
  shardIndex: number
  activated: boolean
  turnCount: number
  lastAccessedMs: number
  before: ActorKernelStatus
  after: ActorKernelStatus
}

export interface ActorTurnResult {
  actorId: ActorId
  shardIndex: number
  created: boolean
  turnCount: number
  lastAccessedMs: number
}

export interface ActorReminderEntry {
  actorId: ActorId
  reminderId: string
  dueAtMs: number
  payload?: unknown
}

export interface ActorReminderStatus {
  pending: number
  nextDueAtMs?: number | null
  reminders: ActorReminderEntry[]
}

export interface ActorReminderMaintenanceStatus {
  lastSweepAtMs?: number | null
  lastFired: number
  totalFired: number
}

export interface RuntimeActorReminderScheduleOptions {
  kind: ActorKind
  key: string
  reminderId?: string
  dueAtMs?: number
  delayMs?: number
  payload?: unknown
}

export interface RuntimeActorReminderCancelOptions {
  kind: ActorKind
  key: string
  reminderId: string
}

export interface RuntimeActorReminderRunDueOptions {
  limit?: number
  nowMs?: number
}

export interface RuntimeActorReminderMutationResponse {
  reminder: ActorReminderEntry
  lsn: number
  acceptedAtMs: number
  pending: ActorReminderStatus
}

export interface RuntimeActorReminderCancelResponse {
  actorId: ActorId
  reminderId: string
  cancelled: boolean
  lsn: number
  acceptedAtMs: number
  pending: ActorReminderStatus
}

export interface RuntimeActorReminderFireResult {
  reminder: ActorReminderEntry
  turn: ActorTurnResult
  behavior?: BehaviorInvokeResponse
  firedLsn: number
  firedAtMs: number
}

export interface RuntimeActorReminderRunDueResponse {
  checkedAtMs: number
  requested: number
  fired: RuntimeActorReminderFireResult[]
  pending: ActorReminderStatus
  maintenance: ActorReminderMaintenanceStatus
}

export interface ActorIdleMaintenanceStatus {
  lastSweepAtMs?: number | null
  lastEvicted: number
  totalEvicted: number
}

export interface ActorSplitMaintenanceStatus {
  lastSweepAtMs?: number | null
  lastProcessed: number
  totalProcessed: number
}

export interface ActorShardRuntimeStatus {
  shardIndex: number
  threadName: string
  targetCoreId?: number | null
  pinningRequested: boolean
  pinningSucceeded: boolean
}

export interface RealtimeMaintenanceStatus {
  lastSweepAtMs?: number | null
  lastStaleMembersRemoved: number
  lastOrphanStatesRemoved: number
  lastOrphanSequencesRemoved: number
  totalStaleMembersRemoved: number
  totalOrphanStatesRemoved: number
  totalOrphanSequencesRemoved: number
}

export interface LiveQueryMetricsStatus {
  current: number
  eventBatchMax: number
  subscribedTotal: number
  unsubscribedTotal: number
  eventBatchesTotal: number
  batchedEventsTotal: number
  refreshCandidatesTotal: number
  refreshTotal: number
  queryExecutionsTotal: number
  resultFramesTotal: number
  diffFramesTotal: number
  unchangedTotal: number
  evaluationCacheHitsTotal: number
  errorsTotal: number
}

export interface RuntimeActivationStatusResponse {
  rooms: RuntimeRoomStatus[]
  roomCount: number
  actorKernel: ActorKernelStatus
  actorShards: ActorShardRuntimeStatus[]
  maxHotRooms: number
  hotWindow: number
  hotRoomIdleTtlMs: number
  hotRoomMaintenanceIntervalMs: number
  hotRoomIdleMaintenance: ActorIdleMaintenanceStatus
  actorSplitMaintenanceIntervalMs: number
  actorSplitMaintenanceLimit: number
  actorSplitMaintenance: ActorSplitMaintenanceStatus
  actorReminderMaintenanceIntervalMs: number
  actorReminderMaintenanceLimit: number
  actorReminders: ActorReminderStatus
  actorReminderMaintenance: ActorReminderMaintenanceStatus
  recordHotMaintenanceIntervalMs: number
  recordHotPrewarmLimit: number
  recordHotPrewarm: RecordHotPrewarmStatus
  recordHotCache: RecordHotCacheStatus
}

export interface RecordHotPrewarmStatus {
  enabled: boolean
  limitPerTable: number
  lastStartedAtMs?: number | null
  lastFinishedAtMs?: number | null
  totalFound: number
  totalActivated: number
  tables: RecordHotPrewarmTableStatus[]
  lastError?: string | null
}

export interface RecordHotPrewarmTableStatus {
  table: string
  found: number
  activated: number
  beforeRecords: number
  afterRecords: number
}

export interface WalCompactResponse {
  reports: Array<{
    uptoLsn: number
    archived: number
    retained: number
    archivePath?: string
    replicas: Array<{
      path: string
      archived: number
      retained: number
      archivePath?: string
    }>
  }>
  archived: number
  retained: number
  lastSnapshotLsn: number
}

export interface WalChecksumSealFileReport {
  path: string
  records: number
  sealed: number
  alreadySealed: number
  rewritten: boolean
}

export interface WalChecksumSealReport extends WalChecksumSealFileReport {
  replicas: WalChecksumSealFileReport[]
}

export interface WalChecksumSealArchiveReport extends WalChecksumSealFileReport {
  shard: number
}

export interface WalChecksumSealResponse {
  active: WalChecksumSealReport[]
  archives: WalChecksumSealArchiveReport[]
  records: number
  sealed: number
  alreadySealed: number
  rewrittenFiles: number
}

export interface WalIntegrityFileReport {
  path: string
  kind: "active" | "archive" | string
  exists: boolean
  lineCount: number
  recordCount: number
  firstLsn?: number
  lastLsn?: number
  minTimestampMs?: number
  maxTimestampMs?: number
}

export interface WalIntegrityShardReport {
  shard: number
  activePath: string
  archiveDir: string
  fileCount: number
  recordCount: number
  firstLsn?: number
  lastLsn?: number
  files: WalIntegrityFileReport[]
}

export interface WalIntegrityGap {
  afterLsn: number
  beforeLsn: number
  missingCount: number
}

export interface WalIntegrityIssue {
  severity: "warning" | "error" | string
  code: string
  path?: string
  line?: number
  lsn?: number
  message: string
}

export interface WalIntegrityReport {
  ok: boolean
  shardCount: number
  fileCount: number
  recordCount: number
  uniqueLsnCount: number
  duplicateLsnCount: number
  checksumMissingCount: number
  checksumMismatchCount: number
  lowestLsn?: number
  highestLsn: number
  gaps: WalIntegrityGap[]
  shards: WalIntegrityShardReport[]
  issueCount: number
  issuesTruncated: boolean
  issues: WalIntegrityIssue[]
}

export interface WalArchiveRetentionOptions {
  dryRun?: boolean
  beforeLsn?: number
  beforeTimestampMs?: number
}

export interface WalArchiveRetentionFileReport {
  path: string
  shard: number
  records: number
  minLsn?: number
  maxLsn?: number
  minTimestampMs?: number
  maxTimestampMs?: number
  action: "delete" | "deleted" | "retain" | string
  reason?: string
}

export interface WalArchiveRetentionResponse {
  dryRun: boolean
  beforeLsn?: number
  beforeTimestampMs?: number
  candidates: number
  deleted: number
  retained: number
  reports: WalArchiveRetentionFileReport[]
}

export interface ExportManifestOptions {
  includeSamples?: boolean
  sampleLimit?: number
  baseLsn?: number
}

export interface ExportBundleAccessOptions {
  encryptionKey?: string
  baseLsn?: number
}

export interface ExportBundleChainVerifyOptions {
  encryptionKey?: string
}

export interface ExportBackupRunOptions {
  encryptionKey?: string
  forceFull?: boolean
  archiveObject?: boolean
  objectId?: string
  clientMutationId?: string
}

export interface ExportBackupRetentionOptions {
  dryRun?: boolean
  keepLast?: number
  beforeTimestampMs?: number
  deleteBundles?: boolean
  deleteArchiveObjects?: boolean
}

export interface ExportBackupPolicy {
  enabled: boolean
  intervalMs: number
  archiveObject: boolean
  retentionKeepLast?: number
  retentionDeleteBundles: boolean
  retentionDeleteArchiveObjects: boolean
}

export interface ExportBundleArchiveObjectOptions {
  objectId?: string
  clientMutationId?: string
}

export interface ImportBundleFromObjectOptions {
  bundleId?: string
  overwrite?: boolean
}

export interface ExportManifestResponse {
  format: string
  generatedAtMs: number
  nodeId: string
  baseLsn: number
  incremental: boolean
  currentLsn: number
  lastSnapshotLsn: number
  lastCompactionLsn: number
  schemaVersion: number
  schemaHistoryVersions: number[]
  schemaProposals: number
  clusterControl: ExportClusterControlSummary
  wal: {
    records: number
    lowestLsn?: number
    highestLsn: number
    checksumMissing: number
    checksumMismatch: number
    shards: Array<{
      shard: number
      records: number
      lowestLsn?: number
      highestLsn?: number
    }>
  }
  payloads: Record<string, number>
  tables: Record<string, number>
  rooms: Record<string, number>
  users: Record<string, number>
  objects: {
    committed: number
    deleted: number
    live: number
    liveBytes: number
  }
  encryption: ExportBundleEncryptionSummary
  samples: NextDbWalRecord[]
}

export interface ExportBundleResponse {
  id: string
  path: string
  manifestPath: string
  schemaPath: string
  schemaHistoryDir: string
  schemaHistoryVersions: number[]
  schemaProposalsPath: string
  schemaProposals: number
  clusterControlDir: string
  clusterControl: ExportClusterControlSummary
  walRecordsPath: string
  objectMetadataDir: string
  objectBlobDir: string
  walRecords: number
  objects: number
  objectBytes: number
  encrypted: boolean
  manifest: ExportManifestResponse
}

export interface ExportBundleListResponse {
  bundles: ExportBundleListEntry[]
}

export interface ExportBundleListEntry {
  id: string
  path: string
  ok: boolean
  schemaVersion?: number
  schemaHistoryVersions: number[]
  schemaProposals: number
  clusterControl: ExportClusterControlSummary
  walRecords?: number
  highestLsn?: number
  objects?: number
  objectBytes?: number
  encrypted: boolean
  problems: string[]
  manifest?: ExportManifestResponse
}

export interface ExportBundleVerifyResponse {
  id: string
  path: string
  ok: boolean
  checkedAtMs: number
  walRecords: number
  schemaVersion?: number
  schemaHistoryVersions: number[]
  schemaProposals: number
  clusterControl: ExportClusterControlSummary
  objects: number
  objectBytes: number
  encrypted: boolean
  problems: string[]
  manifest?: ExportManifestResponse
}

export interface ExportBundleChainVerifyResponse {
  ok: boolean
  checkedAtMs: number
  baseLsn: number
  highestLsn: number
  bundles: ExportBundleChainEntry[]
  problems: string[]
}

export interface ExportBundleChainEntry {
  id: string
  ok: boolean
  incremental: boolean
  baseLsn: number
  currentLsn: number
  walRecords: number
  objects: number
  encrypted: boolean
  problems: string[]
}

export interface ExportBackupRunRecord {
  id: string
  createdAtMs: number
  mode: "full" | "incremental" | string
  baseLsn: number
  currentLsn: number
  noOp: boolean
  bundleId?: string
  objectId?: string
  chainBundleIds: string[]
  chainOk?: boolean
  bundleWalRecords?: number
  bundleObjects?: number
  bundleObjectBytes?: number
  archiveBytes?: number
}

export interface ExportBackupRunListResponse {
  runs: ExportBackupRunRecord[]
}

export interface ExportBackupRetentionResponse {
  dryRun: boolean
  keepLast?: number
  beforeTimestampMs?: number
  candidates: number
  retained: number
  deletedRuns: string[]
  deletedBundles: string[]
  deletedArchiveObjects: string[]
  protectedBundles: string[]
  protectedArchiveObjects: string[]
}

export interface ExportBackupControllerState {
  enabled: boolean
  intervalMs: number
  lastRunAtMs?: number
  lastRunId?: string
  lastError?: string
}

export interface ExportBackupPolicyResponse {
  policy: ExportBackupPolicy
  controller: ExportBackupControllerState
}

export interface ExportBackupPolicyRunResponse {
  policy: ExportBackupPolicy
  backup: ExportBackupRunResponse
  retention?: ExportBackupRetentionResponse | null
}

export interface ExportBackupRunResponse {
  run: ExportBackupRunRecord
  mode: "full" | "incremental" | string
  baseLsn: number
  currentLsn: number
  noOp: boolean
  bundle?: ExportBundleResponse | null
  archived?: ExportBundleArchiveObjectResponse | null
  chain?: ExportBundleChainVerifyResponse | null
}

export interface ExportBundleArchiveObjectResponse {
  bundleId: string
  object: NextDbObjectMetadata
  files: number
  bytes: number
}

export interface ImportBundleFromObjectResponse {
  bundle: ExportBundleListEntry
  object: NextDbObjectMetadata
  files: number
  bytes: number
  overwritten: boolean
}

export interface ImportBundlePreflightResponse {
  id: string
  path: string
  ok: boolean
  checkedAtMs: number
  currentLsn: number
  requiresEmptyDatabase: boolean
  bundleWalRecords: number
  bundleHighestLsn: number
  bundleSchemaVersion?: number
  bundleSchemaHistoryVersions: number[]
  bundleSchemaProposals: number
  bundleClusterControl: ExportClusterControlSummary
  bundleObjects: number
  bundleObjectBytes: number
  bundleEncrypted: boolean
  problems: string[]
  notes: string[]
  manifest?: ExportManifestResponse
}

export interface ImportBundleRestoreResponse {
  id: string
  path: string
  restored: boolean
  restoredAtMs: number
  walRecords: number
  schemaVersion?: number
  schemaHistoryVersions: number[]
  schemaProposals: number
  clusterControl: ExportClusterControlSummary
  objects: number
  objectBytes: number
  encrypted: boolean
  currentLsn: number
  manifest?: ExportManifestResponse
}

export interface ImportBundleDeltaPreflightResponse {
  id: string
  path: string
  ok: boolean
  checkedAtMs: number
  currentLsn: number
  baseLsn: number
  bundleWalRecords: number
  bundleHighestLsn: number
  bundleSchemaVersion?: number
  bundleObjects: number
  bundleObjectBytes: number
  bundleEncrypted: boolean
  problems: string[]
  notes: string[]
  manifest?: ExportManifestResponse
}

export interface ImportBundleDeltaApplyResponse {
  id: string
  path: string
  applied: boolean
  appliedAtMs: number
  baseLsn: number
  walRecords: number
  objects: number
  objectBytes: number
  encrypted: boolean
  currentLsn: number
  manifest?: ExportManifestResponse
}

export interface ImportBundleChainRestoreOptions {
  encryptionKey?: string
}

export interface ImportBundleChainRestoreResponse {
  restored: boolean
  restoredAtMs: number
  chain: ExportBundleChainVerifyResponse
  base: ImportBundleRestoreResponse
  deltas: ImportBundleDeltaApplyResponse[]
  walRecords: number
  objects: number
  objectBytes: number
  currentLsn: number
}

export interface ExportClusterControlSummary {
  topologyOverrides: number
  topologyLogEntries: number
  topologyProposals: number
  handoffWorkflows: number
  topologyLeaseTerm: number
}

export interface ExportBundleEncryptionSummary {
  encrypted: boolean
  algorithm?: string
  keyDerivation?: string
  encryptedFiles: number
}

export type WalRemoteAckPolicy = "all" | "quorum" | "none" | { count: number }

export interface WalRemoteReplicaStatus {
  url: string
  ok: boolean
  highestAckedLsn: number
  lastAttemptMs?: number
  lastSuccessMs?: number
  lastErrorMs?: number
  lastError?: string
  ackedBatches: number
  failedBatches: number
}

export interface WalWriterStatus {
  shard: number
  batchMax: number
  batchWaitMs: number
  queueCapacity: number
  queueDepth: number
  localBatches: number
  localFailedBatches: number
  localRecords: number
  localBytes: number
  localSyncs: number
  localTotalWriteMs: number
  localTotalSyncMs: number
  localLastBatchRecords: number
  localLastBatchBytes: number
  localLastBatchSync: boolean
  localLastBatchStartedAtMs?: number
  localLastBatchFinishedAtMs?: number
  localLastBatchWriteMs: number
  localLastBatchSyncMs: number
  remoteAckPolicy: WalRemoteAckPolicy
  remoteRequiredAcks: number
  remoteReplicaCount: number
  remoteReplicas: WalRemoteReplicaStatus[]
}

export interface WalRestoreReport {
  primary: string
  replicasChecked: string[]
  restored: boolean
  restoredFrom: string | null
  archiveFilesRestored: number
}

export interface WalReplayReport {
  shard: number
  path: string
  sinceLsn: number
  highestLsn: number
  scannedRecords: number
  recordsAfterSnapshot: number
}

export interface StartupRecoveryReport {
  snapshotLoaded: boolean
  snapshotLsn: number
  snapshotSchemaVersion: number | null
  snapshotRoomCount: number
  snapshotRecordHotTableCount: number
  snapshotRecordHotRecordCount: number
  schemaWalRecovery: {
    recovered: boolean
    latestLsn?: number | null
    latestVersion?: number | null
    historyVersions: number[]
  }
  walRestores: WalRestoreReport[]
  walReplay: WalReplayReport[]
  walRecordsScanned: number
  walRecordsAfterSnapshot: number
  highestLsn: number
  rebuiltMessages: number
  rebuiltRecords: number
  rebuiltObjectRefs: number
}

export type RecordHotStorageClass =
  | { kind: "actorPartition" }
  | { kind: "resident" }
  | { kind: "lru"; maxItems: number }

export interface RecordHotTableStatus {
  table: string
  storage: RecordHotStorageClass
  maxItems: number | null
  records: number
  volatileRecords: number
  getTotal: number
  getHitTotal: number
  getMissTotal: number
  listTotal: number
  listRecordsTotal: number
  hydrateDurableTotal: number
  hydrateDurableSkippedVolatileTotal: number
  upsertTotal: number
  deleteTotal: number
  evictTotal: number
  lruEvictedTotal: number
}

export interface RecordHotCacheStatus {
  tables: RecordHotTableStatus[]
  tableCount: number
  recordCount: number
  volatileRecords: number
  durableIdleTtlMs: number
  durableIdleLastSweepAtMs?: number | null
  durableIdleLastEvicted: number
  durableIdleTotalEvicted: number
  getTotal: number
  getHitTotal: number
  getMissTotal: number
  listTotal: number
  listRecordsTotal: number
  hydrateDurableTotal: number
  hydrateDurableSkippedVolatileTotal: number
  upsertTotal: number
  deleteTotal: number
  evictTotal: number
  lruEvictedTotal: number
}

export type ShardRole = "owner" | "replica" | "unassigned"

export interface ClusterNode {
  id: string
  url?: string
}

export interface ClusterShard {
  shard: number
  epoch: number
  owner: string
  replicas: string[]
  role: ShardRole
  ownerUrl?: string
  replicaUrls: string[]
}

export interface ClusterTopology {
  nodeId: string
  nodeUrl?: string
  shardCount: number
  enforceOwnership: boolean
  nodes: ClusterNode[]
  shards: ClusterShard[]
}

export interface ShardRoute {
  key: string
  shard: number
  epoch: number
  owner: string
  ownerUrl?: string
  replicas: string[]
  replicaUrls: string[]
  localRole: ShardRole
  localAcceptsWrites: boolean
}

export interface ShardControl {
  shard: number
  frozen: boolean
  reason?: string
  frozenAtMs?: number
}

export interface ShardControlResponse {
  control: ShardControl
}

export interface HandoffPlanResponse {
  shard: number
  currentOwner: string
  targetOwner: string
  targetOwnerUrl?: string
  currentEpoch: number
  nextEpoch: number
  currentShardLsn: number
  targetAckedLsn: number
  targetCaughtUp: boolean
  frozen: boolean
  ready: boolean
  requiredEnv: Record<string, string>
  steps: string[]
}

export interface FailoverPlanResponse {
  shard: number
  currentOwner: string
  targetOwner: string
  targetOwnerUrl?: string
  currentEpoch: number
  nextEpoch: number
  currentShardLsn: number
  localLsn: number
  ownerLastSeenOkLsn?: number
  ownerHealthy: boolean
  targetIsLocal: boolean
  targetIsReplica: boolean
  targetCaughtUp: boolean
  ready: boolean
  reason?: string
  requiredOverride: ClusterShardOverride & { shard: number }
  requiredAcks: number
  ownerPeer?: PeerHealthStatus
  steps: string[]
}

export interface FailoverProposalResponse {
  plan: FailoverPlanResponse
  proposal: TopologyProposal
  topology: ClusterTopology
  overrides: Record<string, ClusterShardOverride>
}

export interface ClusterShardOverride {
  owner?: string
  epoch?: number
  replicas?: string[]
}

export interface TopologyOverrideResponse {
  overrides: Record<string, ClusterShardOverride>
  topology: ClusterTopology
}

export interface TopologyLogEntry {
  id: string
  timestampMs: number
  nodeId: string
  reason: string
  request: ClusterShardOverride & { shard: number }
  overrides: Record<string, ClusterShardOverride>
}

export interface TopologyLogResponse {
  entries: TopologyLogEntry[]
}

export interface TopologyLease {
  currentTerm: number
  holderNodeId?: string
  proposalId?: string
  expiresAtMs?: number
}

export type TopologyProposalPhase = "prepared" | "committed" | "failed" | "aborted"

export interface TopologyProposal {
  id: string
  createdAtMs: number
  updatedAtMs: number
  proposedBy: string
  term: number
  leaseExpiresAtMs: number
  reason: string
  phase: TopologyProposalPhase
  request: ClusterShardOverride & { shard: number }
  prepareAcks: TopologyPropagationResult[]
  commitResults: TopologyPropagationResult[]
  requiredAcks: number
  lastError?: string
}

export interface TopologyProposalResponse {
  proposal: TopologyProposal
  topology: ClusterTopology
  overrides: Record<string, ClusterShardOverride>
}

export interface TopologyProposalListResponse {
  proposals: TopologyProposal[]
}

export type HandoffWorkflowPhase = "waitingForCatchUp" | "readyToReconfigure" | "applied" | "aborted"

export interface HandoffWorkflow {
  id: string
  shard: number
  currentOwner: string
  targetOwner: string
  currentEpoch: number
  nextEpoch: number
  phase: HandoffWorkflowPhase
  createdAtMs: number
  updatedAtMs: number
  currentShardLsn: number
  targetAckedLsn: number
  lastError?: string
  requiredEnv: Record<string, string>
}

export interface HandoffWorkflowResponse {
  workflow: HandoffWorkflow
  plan: HandoffPlanResponse
}

export interface TopologyPropagationResult {
  nodeId: string
  url: string
  applied: boolean
  status?: number
  error?: string
}

export interface HandoffApplyResponse {
  workflow: HandoffWorkflow
  topology: ClusterTopology
  overrides: Record<string, ClusterShardOverride>
  propagation: TopologyPropagationResult[]
}

export interface HandoffAutoResponse {
  workflow: HandoffWorkflow
  plan: HandoffPlanResponse
  applied: boolean
  apply?: HandoffApplyResponse
}

export interface HandoffWorkflowListResponse {
  workflows: HandoffWorkflow[]
}

export interface HandoffControllerState {
  enabled: boolean
  intervalMs: number
  lastRunAtMs?: number
  lastWorkflowId?: string
  lastAppliedWorkflowId?: string
  lastError?: string
}

export interface FailoverControllerState {
  enabled: boolean
  intervalMs: number
  lastRunAtMs?: number
  lastShard?: number
  lastProposalId?: string
  lastCommittedProposalId?: string
  lastError?: string
}

export interface WalRepairControllerState {
  enabled: boolean
  intervalMs: number
  lastRunAtMs?: number
  lastShards: number[]
  lastRecordsSent: number
  lastRepairedReplicas: number
  lastSatisfied: boolean
  lastError?: string
}

export interface ObjectRepairControllerState {
  enabled: boolean
  intervalMs: number
  lastRunAtMs?: number
  lastShards: number[]
  lastObjectsSent: number
  lastRepairedReplicas: number
  lastSatisfied: boolean
  lastError?: string
}

export interface PeerHealthMonitorState {
  enabled: boolean
  intervalMs: number
  lastRunAtMs?: number
  peers: Record<string, PeerHealthStatus>
}

export interface PeerHealthStatus {
  nodeId: string
  url: string
  ok: boolean
  status?: number
  acceptingWrites?: boolean
  currentLsn?: number
  lastSeenOkLsn?: number
  latencyMs?: number
  lastCheckedAtMs: number
  lastSeenOkAtMs?: number
  error?: string
}

export interface RuntimeLimits {
  maxObjectBytes: number
  maxMessageBytes: number
  maxUserEventBytes: number
  maxRecordValueBytes: number
  maxLiveQueriesPerConnection: number
  maxLiveQueriesPerTablePerConnection: number
  maxLiveQueriesPerUser: number
  maxLiveQueryResultRows: number
}

export interface NextDbConnectionLayerCapabilities {
  protocol: "nextdb.realtime.v1"
  frameEncoding: "json"
  connectPath: string
  supportedTransports: ConnectionTransport[]
  defaultTransport: ConnectionTransport
  webSocket: {
    supported: boolean
    connectPath?: string
  }
  webTransport: {
    supported: boolean
    connectPath?: string | null
  }
  custom: {
    supported: boolean
    connectPath?: string | null
  }
}

export type NextDbRealtimeTransportSupportStatus = "supported" | "unsupported" | "custom"

export interface NextDbRealtimeTransportCompatibility {
  requestedKind: NextDbRealtimeTransportKind | "custom"
  requestedTransport: ConnectionTransport
  supported: boolean
  status: NextDbRealtimeTransportSupportStatus
  supportedTransports: ConnectionTransport[]
  defaultTransport: ConnectionTransport
  fallbackTransport?: ConnectionTransport
  reason?: string
}

export type NextDbRealtimeTransportFallbackTarget = "none" | "websocket" | "jsonl"

export interface NextDbConnectCompatibleRealtimeOptions {
  requestedKind?: NextDbRealtimeTransportKind | "custom"
  fallbackTo?: NextDbRealtimeTransportFallbackTarget
}

export interface NextDbConnectCompatibleRealtimeResult extends NextDbRealtimeTransportCompatibility {
  activeKind: NextDbRealtimeTransportKind | "custom"
  activeTransport: ConnectionTransport
  fallbackApplied: boolean
  connected: boolean
}

export interface BehaviorRuntimeCounterSnapshot {
  invocations: number
  handleMessageInvocations: number
  unknownMessageInvocations: number
  successes: number
  guestErrors: number
  commandRejections: number
  instanceCreateErrors: number
  instancesCreated: number
  instancesReused: number
  instancesReturned: number
  instancesDiscarded: number
  poolErrors: number
}

export interface BehaviorRuntimeConfig {
  fuelEnabled: boolean
  instancePoolMax: number
  poolingTotalCoreInstances: number
  poolingTotalMemories: number
  poolingTotalTables: number
}

export interface BehaviorRuntimeBehaviorStatus {
  name: string
  version: string
  epoch: number
  pooledInstances: number
  instancePoolMax: number
  maxFuel: number
  fuelEnabled: boolean
  abiEncoding: BehaviorAbiEncoding
  counters: BehaviorRuntimeCounterSnapshot
}

export interface BehaviorRuntimeStatus {
  epoch: number
  behaviorCount: number
  fuelEnabled: boolean
  instancePoolMax: number
  pooledInstances: number
  counters: BehaviorRuntimeCounterSnapshot
  config: BehaviorRuntimeConfig
  behaviors: BehaviorRuntimeBehaviorStatus[]
}

export function realtimeTransportCompatibility(
  health: Pick<NextDbHealth, "connectionLayer">,
  requestedKind: NextDbRealtimeTransportKind | "custom",
): NextDbRealtimeTransportCompatibility {
  const requestedTransport = connectionTransportParam(requestedKind)
  const layer = health.connectionLayer
  const supportedTransports = layer.supportedTransports ?? []
  if (requestedKind === "custom") {
    return {
      requestedKind,
      requestedTransport,
      supported: true,
      status: "custom",
      supportedTransports,
      defaultTransport: layer.defaultTransport,
      fallbackTransport: supportedTransports[0] ?? layer.defaultTransport,
      reason: "custom realtime transports are application-owned and may terminate at an external gateway",
    }
  }
  if (supportedTransports.includes(requestedTransport)) {
    return {
      requestedKind,
      requestedTransport,
      supported: true,
      status: "supported",
      supportedTransports,
      defaultTransport: layer.defaultTransport,
    }
  }
  return {
    requestedKind,
    requestedTransport,
    supported: false,
    status: "unsupported",
    supportedTransports,
    defaultTransport: layer.defaultTransport,
    fallbackTransport: supportedTransports[0] ?? layer.defaultTransport,
    reason: `${requestedTransport} is not advertised by this node's connectionLayer.supportedTransports`,
  }
}

export interface ClusterRouteOptions {
  key?: string
  roomId?: string
  table?: string
  recordKey?: string
  objectId?: string
}

export interface NextDbHealth {
  ok: boolean
  runtimeId: string
  draining: boolean
  acceptingWrites: boolean
  runtimeDrain: RuntimeDrainState
  runtimeWrites: RuntimeWriteState
  behaviorRuntime: BehaviorRuntimeStatus
  adminAuthEnabled: boolean
  clientAuthEnabled: boolean
  clientUserAuthEnabled: boolean
  nodeId: string
  clusterEnforceOwnership: boolean
  clusterTopology: ClusterTopology
  topologyOverrides: Record<string, ClusterShardOverride>
  topologyLog: string
  topologyLease: TopologyLease
  topologyLeaseMs: number
  clientCache: {
    profile: ClientCacheProfile
    invalidations: ClientCacheInvalidationEntry[]
  }
  clientCacheControl: string
  shardControls: ShardControl[]
  handoffWorkflows: HandoffWorkflow[]
  handoffController: HandoffControllerState
  failoverController: FailoverControllerState
  walRepairController: WalRepairControllerState
  objectRepairController: ObjectRepairControllerState
  exportBackupController: ExportBackupControllerState
  peerHealth: PeerHealthMonitorState
  roomCount: number
  hotRoomCount: number
  actorKernel: ActorKernelStatus
  actorShards: ActorShardRuntimeStatus[]
  maxHotRooms: number
  hotWindow: number
  hotRoomIdleTtlMs: number
  hotRoomMaintenanceIntervalMs: number
  hotRoomIdleMaintenance: ActorIdleMaintenanceStatus
  actorSplitMaintenanceIntervalMs: number
  actorSplitMaintenanceLimit: number
  actorSplitMaintenance: ActorSplitMaintenanceStatus
  actorReminderMaintenanceIntervalMs: number
  actorReminderMaintenanceLimit: number
  actorReminders: ActorReminderStatus
  actorReminderMaintenance: ActorReminderMaintenanceStatus
  currentLsn: number
  lastSnapshotLsn: number
  lastCompactionLsn: number
  startupRecovery: StartupRecoveryReport
  checkpointEveryLsn: number
  checkpointInFlight: boolean
  autoCompactWal: boolean
  objectGcGraceMs: number
  limits: RuntimeLimits
  chatLog: string
  recordHotCache: RecordHotCacheStatus
  recordHotMaintenanceIntervalMs: number
  recordHotPrewarmLimit: number
  recordHotPrewarm: RecordHotPrewarmStatus
  objectStore: string
  objectRemoteReplicaCount: number
  objectRemoteReplicas: string[]
  connectionCount: number
  connectedUsers: number
  realtimeChannels: number
  realtimeChannelStates: number
  realtimeChannelSequences: number
  realtimeMaintenanceIntervalMs: number
  realtimeMaintenance: RealtimeMaintenanceStatus
  liveQueries: LiveQueryMetricsStatus
  connectionLayer: NextDbConnectionLayerCapabilities
  schema: string
  wal: string
  walShardCount: number
  walPaths: string[]
  walReplicaCount: number
  walRemoteReplicaCount: number
  walReplicas: Array<{
    shard: number
    epoch: number
    primary: string
    replicas: string[]
    remoteReplicas: string[]
    remoteAckPolicy: WalRemoteAckPolicy
    remoteRequiredAcks: number
    remoteStatus: WalWriterStatus
    owner: string
    role: ShardRole
  }>
}

export interface NextDbReadinessCheck {
  name: string
  ok: boolean
  detail: string
}

export interface NextDbReadiness {
  ok: boolean
  readReady: boolean
  writeReady: boolean
  realtimeReady: boolean
  acceptingWrites: boolean
  draining: boolean
  runtimeDrain: RuntimeDrainState
  runtimeWrites: RuntimeWriteState
  currentLsn: number
  runtimeId: string
  nodeId: string
  walShardCount: number
  localWritableShards: number
  checkedAtMs: number
  checks: NextDbReadinessCheck[]
}

export interface RuntimeDrainState {
  draining: boolean
  reason?: string
  updatedAtMs?: number
}

export interface RuntimeWriteState {
  inFlight: number
  lastStartedAtMs?: number
  lastFinishedAtMs?: number
}

export interface RuntimePrepareRestartOptions {
  reason?: string
  snapshot?: boolean
  compactWal?: boolean
  waitForWritesMs?: number
}

export interface RuntimePrepareRestartResponse {
  drain: RuntimeDrainState
  runtimeWrites: RuntimeWriteState
  writesQuiesced: boolean
  writeWaitTimedOut: boolean
  waitedForWritesMs: number
  readyForRestart: boolean
  snapshot?: AdminSnapshotResponse
  compactWal?: WalCompactResponse
  currentLsn: number
  preparedAtMs: number
}

export type ProjectionRebuildPhase = "idle" | "running" | "succeeded" | "failed"

export interface ProjectionRebuildStatus {
  phase: ProjectionRebuildPhase
  runId?: string
  background: boolean
  startedAtMs?: number
  finishedAtMs?: number
  messages?: number
  records?: number
  objectRefs?: number
  error?: string
}

export interface ProjectionRebuildResponse {
  messages: number
  records: number
  objectRefs: number
  phase: ProjectionRebuildPhase
  runId?: string
  background: boolean
  startedAtMs?: number
  finishedAtMs?: number
  error?: string
}

export interface ProjectionRebuildOptions {
  background?: boolean
}

export interface ObjectReferences {
  objectId: string
  objectExists: boolean
  dangling: boolean
  refCount: number
  sources: string[]
}

export interface ObjectGcResponse {
  dryRun: boolean
  force: boolean
  graceMs: number
  deleted: string[]
  retained: string[]
  protected: string[]
}

export interface BehaviorManifest {
  name: string
  version: string
  modulePath: string
  abiEncoding?: BehaviorAbiEncoding
  mutations: string[]
  inputs?: Record<string, FieldSchema>
  reads?: BehaviorReadCapability[]
  recordScopes?: BehaviorRecordScopes
  objectScopes?: BehaviorObjectScopes
  realtimeScopes?: BehaviorRealtimeScopes
  connectionScopes?: BehaviorConnectionScopes
  userScopes?: BehaviorUserScopes
  eventScopes?: BehaviorEventScopes
  hostHttpScopes?: BehaviorHostHttpScopes
  commands?: BehaviorCommandCapability[]
  maxFuel?: number
}

export type BehaviorAbiEncoding = "json" | "postcard" | "postcardTypedSchema"

export interface BehaviorRecordScopes {
  read?: string[]
  write?: string[]
  nestedRead?: string[]
  nestedWrite?: string[]
}

export interface BehaviorObjectScopes {
  read?: string[]
  write?: string[]
}

export interface BehaviorRealtimeScopes {
  read?: string[]
  write?: string[]
}

export interface BehaviorConnectionScopes {
  read?: string[]
  write?: string[]
}

export interface BehaviorUserScopes {
  read?: string[]
  publish?: string[]
}

export interface BehaviorEventScopes {
  publish?: string[]
  realtimeBroadcast?: string[]
}

export interface BehaviorHostHttpScopes {
  allowUrlPrefixes: string[]
}

export type BehaviorReadCapability =
  | "records"
  | "nestedRecords"
  | "latestMessages"
  | "objects"
  | "objectBodies"
  | "realtimeChannelMembers"
  | "realtimeChannelStates"
  | "connectionSessions"
  | "auditTraces"
  | "auditReplays"

export type BehaviorCommandCapability =
  | "sendMessage"
  | "publishVolatile"
  | "publishUserVolatile"
  | "publishUserEvent"
  | "putObject"
  | "deleteObject"
  | "upsertRecord"
  | "deleteRecord"
  | "recordTransaction"
  | "broadcastRealtimeChannel"
  | "updateRealtimeChannelState"
  | "updateRealtimePresence"
  | "disconnectConnections"
  | "activateRuntimeRecords"
  | "evictRuntimeRecords"
  | "activateRuntimeRoom"
  | "evictRuntimeRoom"
  | "scheduleActorReminder"
  | "requestHostHttp"

export interface BehaviorInvokeRequest {
  behavior: string
  mutation: string
  userId?: string
  clientMutationId?: string
  input: unknown
  read?: BehaviorReadPlan
  context?: unknown
}

export interface BehaviorRuntimeContext {
  timestampMs: number
  sender: {
    kind: "user" | "system"
    userId?: string
    behavior: string
    mutation: string
    clientMutationId?: string
  }
  rngSeed: string
  [key: string]: unknown
}

export interface BehaviorContinuationPayload {
  type: "behaviorContinuation"
  behavior: string
  mutation: string
  userId?: string
  clientMutationId?: string
  input?: unknown
  read?: BehaviorReadPlan
  context?: unknown
  callChainId?: string
  callDepth?: number
  maxDepth?: number
  deadlineMs?: number
  path?: string[]
}

export interface BehaviorReadPlan {
  records?: Array<{ table: string; key: string }>
  nestedRecords?: Array<{ table: string; parentKey: string; nested: string; nestedKey: string }>
  latestMessages?: Array<{ roomId: string; limit?: number }>
  objects?: Array<{ objectId: string }>
  objectBodies?: Array<{ objectId: string }>
  realtimeChannelMembers?: Array<{ channelId: string }>
  realtimeChannelStates?: Array<{ channelId: string }>
  connectionSessions?: Array<{ userId?: string; sessionId?: string; transport?: ConnectionTransport }>
  auditTraces?: AuditTraceOptions[]
  auditReplays?: AuditReplayOptions[]
}

export interface BehaviorInvokeResponse {
  output: {
    commands: unknown[]
    result: unknown
  }
  metadata: {
    behavior: string
    behaviorVersion: string
    epoch: number
  }
  committed: Array<{ type: string; [key: string]: unknown }>
}

export interface BehaviorReloadResponse {
  loaded: number
  epoch: number
  publishedLsn: number
}

export interface RealtimeMember {
  userId: string
  sessionId?: string
  metadata: unknown
  joinedAtMs: number
  updatedAtMs: number
}

export interface RealtimeJoinResponse {
  channelId: string
  member: RealtimeMember
  members: RealtimeMember[]
}

export interface RealtimeLeaveResponse {
  channelId: string
  removed: boolean
  members: RealtimeMember[]
}

export interface RealtimePresenceUpdateResponse {
  channelId: string
  member: RealtimeMember
  members: RealtimeMember[]
  sequence: number
  delivered: number
}

export interface RealtimeMembersResponse {
  channelId: string
  members: RealtimeMember[]
}

export interface RealtimeMembersSnapshotView<TMetadata = unknown> {
  channelId: string
  snapshot?: RealtimeMembersResponse & { members: Array<RealtimeMember & { metadata: TMetadata }> }
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface RealtimeChannelSummary {
  channelId: string
  memberCount: number
  sequence: number
  stateVersion: number
  stateUpdatedAtMs?: number
  members: RealtimeMember[]
}

export interface RealtimeChannelListResponse {
  channels: RealtimeChannelSummary[]
  total: number
}

export interface RealtimeSignal {
  channelId: string
  fromUserId: string
  toUserId: string
  kind: "offer" | "answer" | "ice" | "gameInput" | "statePatch" | "custom" | string
  payload: unknown
  sequence: number
  timestampMs: number
}

export interface RealtimeChannelSignalsOptions {
  limit?: number
  kind?: RealtimeSignal["kind"]
}

export interface RealtimeChannelSignalsSnapshotView {
  channelId: string
  signals: RealtimeSignal[]
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export type RealtimeMemberJoinedEvent = {
  channelId: string
  member: RealtimeMember
}

export type RealtimeMemberLeftEvent = {
  channelId: string
  members: RealtimeMember[]
}

export type RealtimeMemberUpdatedEvent = {
  channelId: string
  member: RealtimeMember
  sequence: number
  timestampMs: number
}

export type RealtimeMemberEvent = RealtimeMemberJoinedEvent | RealtimeMemberLeftEvent | RealtimeMemberUpdatedEvent

export interface RealtimeSignalResponse {
  channelId: string
  sequence: number
  timestampMs: number
  delivered: boolean
  deliveredSessions: number
}

export interface RealtimeChannelEvent {
  channelId: string
  fromUserId: string
  kind: "gameInput" | "statePatch" | "voice" | "video" | "custom" | string
  payload: unknown
  sequence: number
  timestampMs: number
}

export interface RealtimeChannelEventsOptions {
  limit?: number
  kind?: RealtimeChannelEvent["kind"]
}

export interface RealtimeChannelEventsSnapshotView {
  channelId: string
  events: RealtimeChannelEvent[]
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export type RealtimeBinaryBody = Blob | ArrayBuffer | Uint8Array | string

export interface RealtimeBinaryFrameOptions {
  contentType?: string
  codec?: string
  timestampMs?: number
  metadata?: unknown
  includeSelf?: boolean
}

export interface RealtimeBinaryFramePayload {
  dataBase64: string
  byteLength: number
  contentType?: string
  codec?: string
  timestampMs: number
  metadata?: unknown
}

export interface RealtimeChannelStateSnapshot<T = unknown> {
  channelId: string
  version: number
  state: T
  updatedAtMs: number
}

export interface RealtimeChannelStateEvent<T = unknown> {
  channelId: string
  fromUserId: string
  state: RealtimeChannelStateSnapshot<T>
  sequence: number
  timestampMs: number
}

export interface RealtimeChannelStateResponse<T = unknown> {
  channelId: string
  state: RealtimeChannelStateSnapshot<T>
}

export interface RealtimeChannelStateUpdateResponse<T = unknown> {
  channelId: string
  state: RealtimeChannelStateSnapshot<T>
  sequence: number
  delivered: number
}

export interface RealtimeChannelStateSnapshotView<T = unknown> {
  channelId: string
  snapshot?: RealtimeChannelStateSnapshot<T>
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface RealtimeBroadcastResponse {
  channelId: string
  sequence: number
  delivered: number
}

export type ConnectionTransport = "webSocket" | "webTransport" | "custom"

export interface ConnectionSession {
  sessionId: string
  userId?: string
  transport: ConnectionTransport
  metadata: unknown
  connectedAtMs: number
  lastSeenAtMs: number
  subscribedRooms: string[]
  subscribedTables: string[]
  subscribedNestedTables: string[]
  subscribedQueries: string[]
  subscribedQueryTables: Record<string, number>
  subscribedUserEvents: boolean
  subscribedObjects: boolean
}

export interface ListConnectionsOptions {
  userId?: string
  transport?: ConnectionTransport
}

export interface ConnectionListResponse {
  sessions: ConnectionSession[]
  total: number
  users: number
  transports: Record<ConnectionTransport, number>
  userSummaries: ConnectionUserSummary[]
}

export interface WatchConnectionsOptions extends ListConnectionsOptions {
  immediate?: boolean
}

export interface ConnectionListSnapshotView {
  connections?: ConnectionListResponse
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface ConnectionUserSummary {
  userId: string
  sessionCount: number
  sessionIds: string[]
  transports: Record<ConnectionTransport, number>
  subscribedRooms: string[]
  subscribedTables: string[]
  subscribedNestedTables: string[]
  subscribedQueries: string[]
  subscribedQueryTables: Record<string, number>
  userEventSessions: number
  objectSessions: number
  lastSeenAtMs: number
}

export interface ConnectionDisconnectRequest {
  userId?: string
  sessionId?: string
  reason?: string
}

export interface ConnectionDisconnectResponse {
  userId?: string
  sessionId?: string
  reason: string
  targeted: number
  targetedSessionIds: string[]
}

export type ConnectionEventType =
  | "connected"
  | "disconnected"
  | "subscriptionsUpdated"
  | "metadataUpdated"
  | "disconnectRequested"

export interface ConnectionEvent {
  eventType: ConnectionEventType
  timestampMs: number
  session?: ConnectionSession
  userId?: string
  sessionId?: string
  reason?: string
  targetedSessionIds: string[]
}

export interface ObjectSchema {
  fields: Record<string, FieldSchema>
}

export interface ReadVisibilityPolicy {
  all?: ReadVisibilityRule[]
}

export type ReadVisibilityRule =
  | { kind: "fieldEqualsUserId"; field: string }

export interface TableSchema {
  storage: SchemaStorageClass
  fields: Record<string, FieldSchema>
  nested: Record<string, NestedTableSchema>
  readVisibility?: ReadVisibilityPolicy
  indexes: Record<string, IndexSchema>
}

export interface NestedTableSchema {
  storage: SchemaStorageClass
  fields: Record<string, FieldSchema>
  readVisibility?: ReadVisibilityPolicy
  indexes: Record<string, IndexSchema>
}

export interface IndexSchema {
  fields: string[]
}

export interface BehaviorSchema {
  mutations: Record<string, FieldSchema>
}

export interface EventSchema {
  payload: FieldSchema
}

export interface FieldSchema {
  type: FieldType
  optional?: boolean
}

export type FieldType =
  | { kind: "string" }
  | { kind: "text"; inlineUntil: number }
  | { kind: "int64" }
  | { kind: "timeMs" }
  | { kind: "boolean" }
  | { kind: "id"; entity: string }
  | { kind: "objectRef"; object: string }
  | { kind: "list"; item: FieldType }
  | { kind: "object"; fields: Record<string, FieldSchema> }
  | { kind: "json" }

export type SchemaStorageClass =
  | { kind: "actorPartition" }
  | { kind: "resident" }
  | { kind: "lru"; maxItems: number }
  | { kind: "disk" }
  | { kind: "chatLog"; bucket: string; order: string[]; liveWindow: number }

export interface NextDbSchema {
  name: string
  version: number
  objects: Record<string, ObjectSchema>
  tables: Record<string, TableSchema>
  events: Record<string, EventSchema>
  behaviors: Record<string, BehaviorSchema>
}

export interface SchemaTypescriptResponse {
  typescript: string
}

export interface SchemaHistoryEntry {
  version: number
  name: string
  current: boolean
  objectCount: number
  tableCount: number
  eventCount: number
  behaviorCount: number
}

export interface SchemaHistoryResponse {
  entries: SchemaHistoryEntry[]
}

export interface SchemaValidationReport {
  ok: boolean
  errors: string[]
}

export interface SchemaReloadResponse {
  name: string
  version: number
  report: SchemaValidationReport
  migration: SchemaMigrationPlan
}

export interface SchemaApplyResponse extends SchemaReloadResponse {
  applied: boolean
  persisted: boolean
  replayRebuild: boolean
  breakingReplayAllowed: boolean
  projectionRebuilt: boolean
  backgroundReplayRunId?: string
  backgroundReplayPhase?: SchemaReplayApplyPhase
  schemaAuditLsn?: number
  peerPreflight?: SchemaPeerPreflightReport
  projectionStatus: RecordProjectionStatus
}

export type SchemaReplayApplyPhase =
  | "idle"
  | "running"
  | "committing"
  | "succeeded"
  | "failed"
  | "cancelled"

export interface SchemaReplayApplyStatus {
  phase: SchemaReplayApplyPhase
  runId?: string
  resumedFromRunId?: string
  targetVersion?: number
  expectedVersion?: number
  schema?: NextDbSchema
  allowBreakingReplay: boolean
  replayRebuild: boolean
  projectionRebuild: boolean
  resumeEligible: boolean
  resumeReason?: string
  startedAtMs?: number
  finishedAtMs?: number
  schemaAuditLsn?: number
  projectionStatus?: RecordProjectionStatus
  error?: string
}

export interface SchemaPeerPreflightReport {
  requiredAcks: number
  acked: number
  replicas: SchemaPeerPreflightResult[]
}

export interface SchemaPeerPreflightResult {
  nodeId?: string
  url: string
  ok: boolean
  status?: number
  error?: string
}

export type SchemaProposalPhase = "prepared" | "committed" | "failed" | "aborted"

export interface SchemaProposal {
  id: string
  createdAtMs: number
  updatedAtMs: number
  proposedBy: string
  reason: string
  phase: SchemaProposalPhase
  expectedVersion?: number
  allowBreakingReplay: boolean
  schema: NextDbSchema
  report: SchemaValidationReport
  migration: SchemaMigrationPlan
  projectionRebuilt: boolean
  projectionStatus: RecordProjectionStatus
  prepareAcks: TopologyPropagationResult[]
  commitAcks: TopologyPropagationResult[]
  requiredAcks: number
  schemaAuditLsn?: number
  peerPreflight?: SchemaPeerPreflightReport
  lastError?: string
}

export interface SchemaProposalResponse {
  proposal: SchemaProposal
}

export interface SchemaProposalListResponse {
  proposals: SchemaProposal[]
}

export interface SchemaMigrationPlan {
  fromVersion: number
  toVersion: number
  compatible: boolean
  errors: string[]
  warnings: string[]
  requiresReplayRebuild: boolean
  replaySafeBreakingChanges: string[]
  unsafeBreakingChanges: string[]
  projectionRebuildRequired: boolean
  projectionRebuildReasons: string[]
}

export interface SchemaStoragePolicyResponse {
  hotWindow: number
  maxHotRooms: number
  schema: {
    entries: StoragePolicyEntry[]
  }
}

export interface StoragePolicyEntry {
  path: string
  storage: { kind: string; [key: string]: unknown }
  physicalRole: string
}

export interface RecordProjectionStatus {
  records: number
  keyOrderEntries: number
  recentEntries: number
  indexEntries: number
  partitionEntries: number
  orderEntries: number
}

export type DeliveryEvent =
  | {
      type: "messageCreated"
      roomId: string
      message: NextDbMessage
    }
  | {
      type: "volatileRoomEvent"
      roomId: string
      name: string
      payload: unknown
    }
  | {
      type: "volatileUserEvent"
      userId: string
      name: string
      payload: unknown
    }
  | {
      type: "userEvent"
      userId: string
      event: NextDbUserEvent
    }
  | {
      type: "userUpserted"
      userId: string
      user: NextDbUserProfile
    }
  | {
      type: "recordUpserted"
      table: string
      key: string
      record: NextDbRecord
    }
  | {
      type: "recordDeleted"
      table: string
      key: string
      deletedAtMs: number
      lsn: number
      path: string
      previousRecord?: NextDbRecord
    }
  | {
      type: "objectCommitted"
      object: NextDbObjectMetadata
      lsn: number
    }
  | {
      type: "objectDeleted"
      objectId: string
      deletedAtMs: number
      lsn: number
      path: string
      force?: boolean
    }

export type TableDeliveryEvent = Extract<DeliveryEvent, { type: "recordUpserted" | "recordDeleted" }>
export type RoomDeliveryEvent = Extract<DeliveryEvent, { type: "messageCreated" | "volatileRoomEvent" }>
export type UserDeliveryEvent = Extract<DeliveryEvent, { type: "userEvent" | "userUpserted" | "volatileUserEvent" }>
export type ObjectDeliveryEvent = Extract<DeliveryEvent, { type: "objectCommitted" | "objectDeleted" }>

export type CacheChangeSource = "mutation" | "realtime" | "sync" | "offline" | "cacheInvalidation" | "manual"

export type NextDbCacheChange =
  | {
      type: "messageUpserted"
      source: CacheChangeSource
      roomId: string
      key: string
      lsn: number
      message: NextDbMessage
    }
  | {
      type: "userEventUpserted"
      source: Exclude<CacheChangeSource, "offline">
      userId: string
      key: string
      lsn: number
      event: NextDbUserEvent
    }
  | {
      type: "userProfileUpserted"
      source: Exclude<CacheChangeSource, "cacheInvalidation" | "manual">
      userId: string
      lsn: number
      user: NextDbUserProfile
    }
  | {
      type: "userProfileDeleted"
      source: Extract<CacheChangeSource, "cacheInvalidation" | "manual">
      userId: string
    }
  | {
      type: "recordUpserted"
      source: CacheChangeSource
      table: string
      key: string
      lsn: number
      record: NextDbRecord
    }
  | {
      type: "recordDeleted"
      source: CacheChangeSource
      table: string
      key: string
      lsn: number
      deletedAtMs?: number
      path?: string
    }
  | {
      type: "tableSnapshotApplied"
      source: Extract<CacheChangeSource, "sync">
      table: string
      lsn: number
    }
  | {
      type: "objectUpserted"
      source: CacheChangeSource
      objectId: string
      metadata: NextDbObjectMetadata
    }
  | {
      type: "objectDeleted"
      source: CacheChangeSource
      objectId: string
    }
  | {
      type: "objectsInvalidated"
      source: Extract<CacheChangeSource, "manual">
    }
  | {
      type: "roomInvalidated"
      source: Extract<CacheChangeSource, "cacheInvalidation" | "manual">
      roomId: string
      minValidLsn?: number
    }
  | {
      type: "userInvalidated"
      source: Extract<CacheChangeSource, "cacheInvalidation" | "manual">
      userId: string
      minValidLsn?: number
    }
  | {
      type: "tableInvalidated"
      source: Extract<CacheChangeSource, "cacheInvalidation" | "manual">
      table: string
      minValidLsn?: number
    }
  | {
      type: "allInvalidated"
      source: Extract<CacheChangeSource, "cacheInvalidation" | "manual">
      minValidLsn?: number
    }
  | {
      type: "cacheProfileUpdated"
      source: Extract<CacheChangeSource, "cacheInvalidation">
    }
  | {
      type: "cacheProfileEnforced"
      source: Extract<CacheChangeSource, "manual" | "cacheInvalidation">
      result: LocalCacheProfileEnforcementResult
    }
  | {
      type: "realtimeChannelStateUpdated"
      source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">
      channelId: string
      state: RealtimeChannelStateSnapshot
    }
  | {
      type: "realtimeChannelStateCleared"
      source: Extract<CacheChangeSource, "manual">
      channelId: string
    }
  | {
      type: "realtimeChannelMembersUpdated"
      source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">
      channelId: string
      members: RealtimeMember[]
    }
  | {
      type: "realtimeChannelMembersCleared"
      source: Extract<CacheChangeSource, "manual">
      channelId: string
    }
  | {
      type: "realtimeChannelEventReceived"
      source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">
      channelId: string
      event: RealtimeChannelEvent
    }
  | {
      type: "realtimeChannelEventsCleared"
      source: Extract<CacheChangeSource, "manual">
      channelId: string
    }
  | {
      type: "realtimeChannelSignalReceived"
      source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">
      channelId: string
      signal: RealtimeSignal
    }
  | {
      type: "realtimeChannelSignalsCleared"
      source: Extract<CacheChangeSource, "manual">
      channelId: string
    }
  | {
      type: "connectionSessionsUpdated"
      source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">
      connections: ConnectionListResponse
    }
  | {
      type: "connectionSessionsCleared"
      source: Extract<CacheChangeSource, "manual">
    }
  | {
      type: "pendingWriteQueued"
      source: Extract<CacheChangeSource, "offline">
      write: NextDbPendingWrite
      stats: PendingWriteStats
    }
  | {
      type: "pendingWriteRejected"
      source: Extract<CacheChangeSource, "offline">
      write: NextDbPendingWrite
      limit: PendingWriteLimitDetails
      stats: PendingWriteStats
    }
  | {
      type: "pendingWriteReset"
      source: Extract<CacheChangeSource, "manual">
      write: NextDbPendingWrite
      stats: PendingWriteStats
    }
  | {
      type: "pendingWriteDiscarded"
      source: Extract<CacheChangeSource, "manual">
      write: NextDbPendingWrite
      removedOptimistic: boolean
      stats: PendingWriteStats
    }
  | {
      type: "pendingWritesCleared"
      source: Extract<CacheChangeSource, "manual">
      removed: number
      stats: PendingWriteStats
    }
  | {
      type: "pendingWriteCommitted"
      source: Extract<CacheChangeSource, "sync">
      write: NextDbPendingWrite
      stats: PendingWriteStats
    }
  | {
      type: "pendingWriteFailed"
      source: Extract<CacheChangeSource, "sync">
      write: NextDbPendingWrite
      error: string
      retryable: boolean
      stats: PendingWriteStats
    }

export interface TableSubscriptionKeyRange {
  lowerKey?: string
  upperKey?: string
}

export interface TableSubscriptionIndexPrefix {
  indexName: string
  fields?: string[]
  values: unknown[]
}

export interface SubscriptionOptions {
  catchUp?: boolean
  catchUpLimit?: number
  persistent?: boolean
  keyRange?: TableSubscriptionKeyRange
  indexPrefix?: TableSubscriptionIndexPrefix
  serverSnapshot?: boolean
  snapshotLimit?: number
}

export type StoredSubscriptionKind = "room" | "table" | "nestedTable" | "query" | "userEvents" | "objects"

export type NextDbStoredSubscription =
  | {
      id: string
      kind: "room"
      roomId: string
      options: SubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "table"
      table: string
      options: SubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "nestedTable"
      table: string
      parentKey: string
      nested: string
      options: SubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "query"
      query: Extract<ClientFrame, { type: "subscribeQuery" }>
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "userEvents"
      userId: string
      options: SubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "objects"
      options: SubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }

type NextDbStoredSubscriptionDraft =
  | {
      id: string
      kind: "room"
      roomId: string
      options: SubscriptionOptions
    }
  | {
      id: string
      kind: "table"
      table: string
      options: SubscriptionOptions
    }
  | {
      id: string
      kind: "nestedTable"
      table: string
      parentKey: string
      nested: string
      options: SubscriptionOptions
    }
  | {
      id: string
      kind: "query"
      query: Extract<ClientFrame, { type: "subscribeQuery" }>
    }
  | {
      id: string
      kind: "userEvents"
      userId: string
      options: SubscriptionOptions
    }
  | {
      id: string
      kind: "objects"
      options: SubscriptionOptions
    }

export type CacheSnapshotSource = CacheChangeSource | "cache"

export interface WatchOptions extends SubscriptionOptions {
  limit?: number
  immediate?: boolean
  hydrate?: boolean
  hydrateLimit?: number
  hydrateMaxPages?: number
}

export interface RoomMessagesSnapshot {
  roomId: string
  messages: NextDbMessage[]
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface UserEventsSnapshot {
  userId: string
  events: NextDbUserEvent[]
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface TableRecordsSnapshot<T = unknown> {
  table: string
  records: Array<NextDbRecord<T>>
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface RecordSnapshot<T = unknown> {
  table: string
  key: string
  record?: NextDbRecord<T>
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface ObjectListSnapshot {
  objects: NextDbObjectMetadata[]
  nextAfterId?: string
  hasMore: boolean
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface ObjectSnapshot {
  objectId: string
  metadata?: NextDbObjectMetadata
  cachedBodyAvailable: boolean
  cachedBody?: Blob
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface ObjectWatchOptions extends WatchOptions {
  includeBody?: boolean
}

export interface PendingWritesSnapshot {
  queue: PendingWriteQueueStatus
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export interface LocalDataStatusSnapshot {
  status: NextDbLocalDataStatus
  pendingQueue: PendingWriteQueueStatus
  source: CacheSnapshotSource
  change?: NextDbCacheChange
}

export type ServerFrame =
  | {
      type: "hello"
      userId?: string
      sessionId: string
    }
  | {
      type: "subscribed"
      roomId: string
    }
  | {
      type: "unsubscribed"
      roomId: string
    }
  | {
      type: "tableSubscribed"
      table: string
    }
  | {
      type: "tableSnapshot"
      table: string
      lowerKey?: string
      upperKey?: string
      indexName?: string
      indexValues?: string
      response: ListRecordsResponse
      currentLsn: number
    }
  | {
      type: "nestedTableSnapshot"
      table: string
      parentKey: string
      nested: string
      response: ListRecordsResponse
      currentLsn: number
    }
  | {
      type: "tableUnsubscribed"
      table: string
    }
  | {
      type: "querySubscribed"
      queryId: string
    }
  | {
      type: "queryUnsubscribed"
      queryId: string
    }
  | {
      type: "queryResult"
      queryId: string
      response: ListRecordsResponse
      currentLsn: number
      resultId: string
    }
  | {
      type: "queryDiff"
      queryId: string
      diff: RecordLiveQueryDiff
      currentLsn: number
      resultId: string
    }
  | {
      type: "queryUnchanged"
      queryId: string
      resultId: string
      currentLsn: number
    }
  | {
      type: "objectsSubscribed"
    }
  | {
      type: "userEventsUnsubscribed"
    }
  | {
      type: "objectsUnsubscribed"
    }
  | {
      type: "connectionMetadataUpdated"
      session: ConnectionSession
    }
  | {
      type: "cacheInvalidated"
      invalidation: ClientCacheInvalidationEntry
    }
  | {
      type: "connectionEventsSubscribed"
    }
  | {
      type: "connectionEventsUnsubscribed"
    }
  | {
      type: "aggregateCountSubscribed"
      snapshot: AggregateCountSnapshot
    }
  | {
      type: "aggregateCountUnsubscribed"
      table: string
    }
  | {
      type: "aggregateCountUpdated"
      update: AggregateCountUpdate
    }
  | {
      type: "aggregateSumSubscribed"
      snapshot: AggregateSumSnapshot
    }
  | {
      type: "aggregateSumUnsubscribed"
      table: string
      field: string
    }
  | {
      type: "aggregateSumUpdated"
      update: AggregateSumUpdate
    }
  | {
      type: "aggregatePresenceSubscribed"
      snapshot: AggregatePresenceSnapshot
    }
  | {
      type: "aggregatePresenceUnsubscribed"
      channelId: string
    }
  | {
      type: "aggregatePresenceUpdated"
      update: AggregatePresenceUpdate
    }
  | {
      type: "connectionEvent"
      event: ConnectionEvent
    }
  | {
      type: "connectionClosing"
      reason: string
    }
  | {
      type: "event"
      event: DeliveryEvent
    }
  | {
      type: "events"
      events: DeliveryEvent[]
    }
  | {
      type: "subscriptionCatchUp"
      rooms: string[]
      users: string[]
      tables: string[]
      nestedTables?: SyncNestedTableTarget[]
      objects: boolean
      nextAfterLsn: number
      currentLsn: number
      hasMore: boolean
    }
  | {
      type: "error"
      message: string
    }

export type ClientFrame =
  | {
      type: "subscribeRoom"
      roomId: string
      afterLsn?: number
      catchUpLimit?: number
    }
  | {
      type: "unsubscribeRoom"
      roomId: string
    }
  | {
      type: "subscribeTable"
      table: string
      lowerKey?: string
      upperKey?: string
      indexName?: string
      indexValues?: string
      snapshotLimit?: number
      afterLsn?: number
      catchUpLimit?: number
    }
  | {
      type: "unsubscribeTable"
      table: string
      lowerKey?: string
      upperKey?: string
      indexName?: string
      indexValues?: string
    }
  | {
      type: "subscribeNestedTable"
      table: string
      parentKey: string
      nested: string
      snapshotLimit?: number
      afterLsn?: number
      catchUpLimit?: number
    }
  | {
      type: "unsubscribeNestedTable"
      table: string
      parentKey: string
      nested: string
    }
  | {
      type: "subscribeQuery"
      queryId: string
      table: string
      parentKey?: string
      nested?: string
      indexName?: string
      value?: string
      values?: string
      lower?: string
      upper?: string
      lowerValues?: string
      upperValues?: string
      afterKey?: string
      afterCursor?: string
      limit?: number
      order?: NestedListOrder
      predicate?: RecordPredicate
      resultId?: string
      diff?: boolean
    }
  | {
      type: "unsubscribeQuery"
      queryId: string
    }
  | {
      type: "subscribeUserEvents"
      afterLsn?: number
      catchUpLimit?: number
    }
  | {
      type: "unsubscribeUserEvents"
    }
  | {
      type: "subscribeObjects"
      afterLsn?: number
      catchUpLimit?: number
    }
  | {
      type: "unsubscribeObjects"
    }
  | {
      type: "updateConnectionMetadata"
      metadata: unknown
    }
  | {
      type: "subscribeConnectionEvents"
    }
  | {
      type: "unsubscribeConnectionEvents"
    }
  | {
      type: "subscribeAggregateCount"
      table: string
    }
  | {
      type: "unsubscribeAggregateCount"
      table: string
    }
  | {
      type: "subscribeAggregateSum"
      table: string
      field: string
    }
  | {
      type: "unsubscribeAggregateSum"
      table: string
      field: string
    }
  | {
      type: "subscribeAggregatePresence"
      channelId: string
    }
  | {
      type: "unsubscribeAggregatePresence"
      channelId: string
    }

export type AggregateCountSnapshot = {
  table: string
  count: number
  currentLsn: number
}

export type AggregateCountUpdate = {
  table: string
  count: number
  lsn: number
}

export type AggregateCountEvent = {
  table: string
  count: number
  lsn: number
  source: "snapshot" | "update"
}

export type AggregateSumSnapshot = {
  table: string
  field: string
  sum: number
  currentLsn: number
}

export type AggregateSumUpdate = {
  table: string
  field: string
  sum: number
  lsn: number
}

export type AggregateSumEvent = {
  table: string
  field: string
  sum: number
  lsn: number
  source: "snapshot" | "update"
}

export type AggregatePresenceSnapshot = {
  channelId: string
  memberCount: number
  userCount: number
  currentLsn: number
  updatedAtMs: number
}

export type AggregatePresenceUpdate = {
  channelId: string
  memberCount: number
  userCount: number
  currentLsn: number
  updatedAtMs: number
}

export type AggregatePresenceEvent = {
  channelId: string
  memberCount: number
  userCount: number
  lsn: number
  updatedAtMs: number
  source: "snapshot" | "update"
}

export type RealtimeTransportState = "connecting" | "open" | "closed"

export interface NextDbRealtimeTransport {
  readonly state: RealtimeTransportState
  send(frame: ClientFrame): void
  close(): void
  onOpen(listener: () => void): void
  onFrame(listener: (frame: ServerFrame) => void): void
  onError(listener: (error?: unknown) => void): void
  onClose(listener: () => void): void
}

export interface NextDbRealtimeTransportContext {
  url: URL
}

export type NextDbRealtimeTransportFactory = (context: NextDbRealtimeTransportContext) => NextDbRealtimeTransport
export type NextDbRealtimeTransportKind = "websocket" | "webtransport" | "jsonl"

export function encodeRealtimeClientFrame(frame: ClientFrame): string {
  return JSON.stringify(frame)
}

export function encodeRealtimeClientFrameJsonLine(frame: ClientFrame): string {
  return `${encodeRealtimeClientFrame(frame)}\n`
}

export function decodeRealtimeServerFrame(payload: string): ServerFrame {
  return JSON.parse(payload) as ServerFrame
}

export class RealtimeServerFrameJsonLineDecoder {
  private buffer = ""

  push(chunk: string, options: { flush?: boolean } = {}): ServerFrame[] {
    this.buffer += chunk
    const frames: ServerFrame[] = []
    let start = 0
    while (true) {
      const newline = this.buffer.indexOf("\n", start)
      if (newline === -1) {
        break
      }
      const line = this.buffer.slice(start, newline)
      const frame = decodeRealtimeServerFrameLine(line)
      if (frame !== undefined) {
        frames.push(frame)
      }
      start = newline + 1
    }
    this.buffer = this.buffer.slice(start)
    if (options.flush && this.buffer.trim() !== "") {
      frames.push(decodeRealtimeServerFrame(this.buffer.trim()))
      this.buffer = ""
    }
    return frames
  }
}

function decodeRealtimeServerFrameLine(line: string): ServerFrame | undefined {
  const trimmed = line.trim()
  if (trimmed === "") {
    return undefined
  }
  return decodeRealtimeServerFrame(trimmed)
}

export interface NextDbLocalCache {
  putObject(metadata: NextDbObjectMetadata, body?: Blob): Promise<void>
  getObjectMetadata(objectId: string): Promise<NextDbObjectMetadata | undefined>
  getObjectBody(objectId: string): Promise<Blob | undefined>
  putObjectBodyRange(metadata: NextDbObjectMetadata, range: ObjectBodyRangeResponse): Promise<void>
  getObjectBodyRange(metadata: NextDbObjectMetadata, start: number, end: number): Promise<ObjectBodyRangeResponse | undefined>
  listObjects(limit: number, afterId?: string): Promise<NextDbObjectMetadata[]>
  deleteObject(objectId: string): Promise<boolean>
  trimObjects(maxObjects: number, maxBytes: number): Promise<number>
  putRoomMessages(roomId: string, messages: NextDbMessage[]): Promise<void>
  getRoomMessages(roomId: string, limit: number, beforeLsn?: number): Promise<NextDbMessage[]>
  deleteRoomMessage(roomId: string, messageId: string): Promise<boolean>
  putUserEvents(userId: string, events: NextDbUserEvent[]): Promise<void>
  getUserEvents(userId: string, limit: number, beforeLsn?: number): Promise<NextDbUserEvent[]>
  clearUserEvents(userId: string): Promise<number>
  trimUserEvents(userId: string, keepLatest: number): Promise<number>
  putUserProfile(profile: NextDbUserProfile): Promise<void>
  getUserProfile(userId: string): Promise<NextDbUserProfile | undefined>
  listUserProfiles(limit: number, afterUserId?: string): Promise<NextDbUserProfile[]>
  deleteUserProfile(userId: string): Promise<boolean>
  putRecords(records: NextDbRecord[]): Promise<void>
  getRecord<T = unknown>(table: string, key: string): Promise<NextDbRecord<T> | undefined>
  listRecords<T = unknown>(table: string, limit: number, afterKey?: string): Promise<Array<NextDbRecord<T>>>
  listRecordsByKeyPrefix<T = unknown>(
    table: string,
    keyPrefix: string,
    limit: number,
    afterKey?: string,
  ): Promise<Array<NextDbRecord<T>>>
  listRecordsBySchemaOrder<T = unknown>(
    table: string,
    keyPrefix: string,
    order: RecordOrderTerm[],
    limit: number,
    afterCursor?: string,
  ): Promise<LocalOrderedRecordsResponse<T>>
  queryRecordsByIndex<T = unknown>(
    table: string,
    query: LocalIndexQuery,
  ): Promise<LocalIndexedRecordsResponse<T>>
  deleteRecord(table: string, key: string): Promise<boolean>
  clearRecordsByKeyPrefix(table: string, keyPrefix: string): Promise<number>
  trimRecordsByKeyPrefix(table: string, keyPrefix: string, keepLatest: number): Promise<number>
  trimNestedTablePartitions(table: string, keepPartitions: number, keepLatestPerPartition: number): Promise<number>
  getGlobalCursor(): Promise<number>
  setGlobalCursor(lsn: number): Promise<void>
  getObjectCursor(): Promise<number>
  setObjectCursor(lsn: number): Promise<void>
  getRoomCursor(roomId: string): Promise<number>
  setRoomCursor(roomId: string, lsn: number): Promise<void>
  getUserCursor(userId: string): Promise<number>
  setUserCursor(userId: string, lsn: number): Promise<void>
  getTableCursor(table: string): Promise<number>
  setTableCursor(table: string, lsn: number): Promise<void>
  getNestedTableCursor(table: string, parentKey: string, nested: string): Promise<number>
  setNestedTableCursor(table: string, parentKey: string, nested: string, lsn: number): Promise<void>
  getMetadata(): Promise<ClientCacheMetadata | undefined>
  setMetadata(metadata: ClientCacheMetadata): Promise<void>
  putPendingWrite(write: NextDbPendingWrite): Promise<void>
  listPendingWrites(limit?: number): Promise<NextDbPendingWrite[]>
  deletePendingWrite(id: string): Promise<void>
  clearPendingWrites(): Promise<number>
  putSubscription(subscription: NextDbStoredSubscription): Promise<void>
  listSubscriptions(): Promise<NextDbStoredSubscription[]>
  deleteSubscription(id: string): Promise<void>
  clearSubscriptions(): Promise<number>
  stats(): Promise<NextDbCacheStats>
  clearAll(): Promise<number>
  clearObjects(): Promise<number>
  clearRoom(roomId: string): Promise<number>
  clearTable(table: string): Promise<number>
  trimRoom(roomId: string, keepLatest: number): Promise<number>
  trimTable(table: string, keepLatest: number): Promise<number>
}

export interface NextDbCacheStats {
  totalObjects: number
  totalObjectBytes: number
  totalObjectCachedBytes: number
  totalObjectRangeChunks: number
  totalMessages: number
  totalUserEvents: number
  totalUserProfiles: number
  totalRecords: number
  pendingWrites: number
  subscriptions: number
  rooms: Record<string, number>
  users: Record<string, number>
  tables: Record<string, number>
  nestedTables: Record<string, Record<string, number>>
}

interface CachedObjectBodyRange {
  objectId: string
  start: number
  end: number
  byteSize: number
  contentType: string
  sha256: string
  body: Blob
}

export class MemoryLocalCache implements NextDbLocalCache {
  private readonly objectMetadata = new Map<string, NextDbObjectMetadata>()
  private readonly objectBodies = new Map<string, Blob>()
  private readonly objectBodyRanges = new Map<string, CachedObjectBodyRange[]>()
  private readonly rooms = new Map<string, NextDbMessage[]>()
  private readonly userEvents = new Map<string, NextDbUserEvent[]>()
  private readonly userProfiles = new Map<string, NextDbUserProfile>()
  private readonly records = new Map<string, Map<string, NextDbRecord>>()
  private readonly recordOrderMetadata = new Map<string, { table: string; keyPrefix: string; order: RecordOrderTerm[] }>()
  private readonly recordOrders = new Map<string, Map<string, NextDbRecord>>()
  private globalCursor = 0
  private objectCursor = 0
  private readonly roomCursors = new Map<string, number>()
  private readonly userCursors = new Map<string, number>()
  private readonly tableCursors = new Map<string, number>()
  private readonly nestedTableCursors = new Map<string, number>()
  private readonly pendingWrites = new Map<string, NextDbPendingWrite>()
  private readonly subscriptions = new Map<string, NextDbStoredSubscription>()
  private metadata?: ClientCacheMetadata

  async putObject(metadata: NextDbObjectMetadata, body?: Blob): Promise<void> {
    const previous = this.objectMetadata.get(metadata.id)
    this.objectMetadata.set(metadata.id, metadata)
    this.discardStaleObjectRanges(metadata)
    if (body !== undefined) {
      this.objectBodies.set(metadata.id, body)
      this.objectBodyRanges.delete(metadata.id)
    } else if (previous !== undefined && !objectMetadataContentMatches(previous, metadata)) {
      this.objectBodies.delete(metadata.id)
    }
  }

  async getObjectMetadata(objectId: string): Promise<NextDbObjectMetadata | undefined> {
    return this.objectMetadata.get(objectId)
  }

  async getObjectBody(objectId: string): Promise<Blob | undefined> {
    return this.objectBodies.get(objectId)
  }

  async putObjectBodyRange(metadata: NextDbObjectMetadata, range: ObjectBodyRangeResponse): Promise<void> {
    if (!objectBodyRangeMatchesMetadata(range, metadata)) {
      return
    }
    const ranges = this.objectBodyRanges.get(metadata.id) ?? []
    const retained = ranges.filter((entry) => entry.start !== range.start || entry.end !== range.end)
    retained.push({
      objectId: metadata.id,
      start: range.start,
      end: range.end,
      byteSize: metadata.byteSize,
      contentType: metadata.contentType,
      sha256: metadata.sha256,
      body: range.body,
    })
    retained.sort((left, right) => left.start - right.start || left.end - right.end)
    this.objectBodyRanges.set(metadata.id, retained)
  }

  async getObjectBodyRange(
    metadata: NextDbObjectMetadata,
    start: number,
    end: number,
  ): Promise<ObjectBodyRangeResponse | undefined> {
    const ranges = this.objectBodyRanges.get(metadata.id) ?? []
    const cached = ranges.find((entry) =>
      entry.start <= start &&
      entry.end >= end &&
      entry.byteSize === metadata.byteSize &&
      entry.contentType === metadata.contentType &&
      entry.sha256 === metadata.sha256
    )
    if (cached === undefined) {
      return undefined
    }
    const offsetStart = start - cached.start
    const offsetEnd = end - cached.start + 1
    return {
      body: cached.body.slice(offsetStart, offsetEnd, metadata.contentType),
      contentRange: objectContentRange(start, end, metadata.byteSize),
      start,
      end,
      byteSize: metadata.byteSize,
      contentType: metadata.contentType,
    }
  }

  async listObjects(limit: number, afterId?: string): Promise<NextDbObjectMetadata[]> {
    return [...this.objectMetadata.values()]
      .filter((object) => afterId === undefined || object.id > afterId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .slice(0, limit)
  }

  async deleteObject(objectId: string): Promise<boolean> {
    const deletedMetadata = this.objectMetadata.delete(objectId)
    const deletedBody = this.objectBodies.delete(objectId)
    const deletedRanges = this.objectBodyRanges.delete(objectId)
    return deletedMetadata || deletedBody || deletedRanges
  }

  async trimObjects(maxObjects: number, maxBytes: number): Promise<number> {
    const deleteIds = selectObjectIdsToTrim(
      [...this.objectMetadata.values()].map((object) => ({
        ...object,
        cachedBytes: this.objectCachedBytes(object.id),
      })),
      maxObjects,
      maxBytes,
    )
    for (const objectId of deleteIds) {
      this.objectMetadata.delete(objectId)
      this.objectBodies.delete(objectId)
      this.objectBodyRanges.delete(objectId)
    }
    return deleteIds.length
  }

  async putRoomMessages(roomId: string, messages: NextDbMessage[]): Promise<void> {
    const existing = this.rooms.get(roomId) ?? []
    const byId = new Map(existing.map((message) => [message.id, message]))
    for (const message of messages) {
      byId.set(message.id, message)
    }
    const merged = [...byId.values()].sort((left, right) => right.lsn - left.lsn)
    this.rooms.set(roomId, merged.slice(0, 10_000))
  }

  async getRoomMessages(roomId: string, limit: number, beforeLsn?: number): Promise<NextDbMessage[]> {
    const messages = this.rooms.get(roomId) ?? []
    return messages
      .filter((message) => beforeLsn === undefined || message.lsn < beforeLsn)
      .slice(0, limit)
  }

  async deleteRoomMessage(roomId: string, messageId: string): Promise<boolean> {
    const messages = this.rooms.get(roomId) ?? []
    const retained = messages.filter((message) => message.id !== messageId)
    this.rooms.set(roomId, retained)
    return retained.length !== messages.length
  }

  async putUserEvents(userId: string, events: NextDbUserEvent[]): Promise<void> {
    const existing = this.userEvents.get(userId) ?? []
    const byId = new Map(existing.map((event) => [event.id, event]))
    for (const event of events) {
      byId.set(event.id, event)
    }
    const merged = [...byId.values()].sort((left, right) => right.lsn - left.lsn)
    this.userEvents.set(userId, merged.slice(0, 10_000))
  }

  async getUserEvents(userId: string, limit: number, beforeLsn?: number): Promise<NextDbUserEvent[]> {
    const events = this.userEvents.get(userId) ?? []
    return events
      .filter((event) => beforeLsn === undefined || event.lsn < beforeLsn)
      .slice(0, limit)
  }

  async clearUserEvents(userId: string): Promise<number> {
    const removed = this.userEvents.get(userId)?.length ?? 0
    this.userEvents.delete(userId)
    this.userCursors.delete(userId)
    return removed
  }

  async trimUserEvents(userId: string, keepLatest: number): Promise<number> {
    const events = this.userEvents.get(userId) ?? []
    const keep = Math.max(0, keepLatest)
    const retained = events.slice(0, keep)
    this.userEvents.set(userId, retained)
    return Math.max(0, events.length - retained.length)
  }

  async putUserProfile(profile: NextDbUserProfile): Promise<void> {
    this.userProfiles.set(profile.userId, profile)
  }

  async getUserProfile(userId: string): Promise<NextDbUserProfile | undefined> {
    return this.userProfiles.get(userId)
  }

  async listUserProfiles(limit: number, afterUserId?: string): Promise<NextDbUserProfile[]> {
    return [...this.userProfiles.values()]
      .filter((profile) => afterUserId === undefined || profile.userId > afterUserId)
      .sort((left, right) => left.userId.localeCompare(right.userId))
      .slice(0, limit)
  }

  async deleteUserProfile(userId: string): Promise<boolean> {
    return this.userProfiles.delete(userId)
  }

  async putRecords(records: NextDbRecord[]): Promise<void> {
    for (const record of records) {
      const table = this.records.get(record.table) ?? new Map<string, NextDbRecord>()
      table.set(record.key, record)
      this.records.set(record.table, table)
      this.updateRecordOrderEntries(record)
    }
  }

  async getRecord<T = unknown>(table: string, key: string): Promise<NextDbRecord<T> | undefined> {
    return this.records.get(table)?.get(key) as NextDbRecord<T> | undefined
  }

  async listRecords<T = unknown>(table: string, limit: number, afterKey?: string): Promise<Array<NextDbRecord<T>>> {
    return [...(this.records.get(table)?.values() ?? [])]
      .filter((record) => afterKey === undefined || record.key > afterKey)
      .sort((left, right) => left.key.localeCompare(right.key))
      .slice(0, limit) as Array<NextDbRecord<T>>
  }

  async listRecordsByKeyPrefix<T = unknown>(
    table: string,
    keyPrefix: string,
    limit: number,
    afterKey?: string,
  ): Promise<Array<NextDbRecord<T>>> {
    return [...(this.records.get(table)?.values() ?? [])]
      .filter((record) => record.key.startsWith(keyPrefix))
      .filter((record) => afterKey === undefined || record.key > afterKey)
      .sort((left, right) => left.key.localeCompare(right.key))
      .slice(0, limit) as Array<NextDbRecord<T>>
  }

  async listRecordsBySchemaOrder<T = unknown>(
    table: string,
    keyPrefix: string,
    order: RecordOrderTerm[],
    limit: number,
    afterCursor?: string,
  ): Promise<LocalOrderedRecordsResponse<T>> {
    const orderId = localOrderId(table, keyPrefix, order)
    if (!this.recordOrderMetadata.has(orderId)) {
      this.recordOrderMetadata.set(orderId, { table, keyPrefix, order })
      const ordered = new Map<string, NextDbRecord>()
      for (const record of this.records.get(table)?.values() ?? []) {
        if (record.key.startsWith(keyPrefix)) {
          ordered.set(localRecordOrderCursor(record, order), record)
        }
      }
      this.recordOrders.set(orderId, ordered)
    }
    const ordered = this.recordOrders.get(orderId) ?? new Map<string, NextDbRecord>()
    const rows = [...ordered.entries()]
      .filter(([cursor]) => afterCursor === undefined || cursor > afterCursor)
      .sort(([left], [right]) => left.localeCompare(right))
    const page = rows.slice(0, limit)
    return {
      records: page.map(([, record]) => record as NextDbRecord<T>),
      nextCursor: page.at(-1)?.[0],
      hasMore: rows.length > limit,
    }
  }

  async queryRecordsByIndex<T = unknown>(
    table: string,
    query: LocalIndexQuery,
  ): Promise<LocalIndexedRecordsResponse<T>> {
    const records = [...(this.records.get(table)?.values() ?? [])]
      .filter((record) => query.keyPrefix === undefined || record.key.startsWith(query.keyPrefix))
    return localQueryRecordsByIndex<T>(records, query)
  }

  async deleteRecord(table: string, key: string): Promise<boolean> {
    const records = this.records.get(table)
    if (!records) {
      return false
    }
    const existing = records.get(key)
    const deleted = records.delete(key)
    if (deleted) {
      this.deleteRecordOrderEntries(existing?.path)
    }
    return deleted
  }

  async clearRecordsByKeyPrefix(table: string, keyPrefix: string): Promise<number> {
    const records = this.records.get(table)
    if (!records) {
      return 0
    }
    let removed = 0
    for (const [key, record] of [...records.entries()]) {
      if (key.startsWith(keyPrefix)) {
        records.delete(key)
        this.deleteRecordOrderEntries(record.path)
        removed += 1
      }
    }
    return removed
  }

  async trimRecordsByKeyPrefix(table: string, keyPrefix: string, keepLatest: number): Promise<number> {
    const records = this.records.get(table)
    if (!records) {
      return 0
    }
    const keep = Math.max(0, keepLatest)
    const retained = new Set(
      [...records.values()]
        .filter((record) => record.key.startsWith(keyPrefix))
        .sort((left, right) => right.lsn - left.lsn || left.key.localeCompare(right.key))
        .slice(0, keep)
        .map((record) => record.path),
    )
    let removed = 0
    for (const [key, record] of [...records.entries()]) {
      if (record.key.startsWith(keyPrefix) && !retained.has(record.path)) {
        records.delete(key)
        this.deleteRecordOrderEntries(record.path)
        removed += 1
      }
    }
    return removed
  }

  async trimNestedTablePartitions(
    table: string,
    keepPartitions: number,
    keepLatestPerPartition: number,
  ): Promise<number> {
    const records = this.records.get(table)
    if (!records) {
      return 0
    }
    const partitionLsn = new Map<string, number>()
    for (const record of records.values()) {
      const keyPrefix = nestedRecordKeyPrefix(record)
      if (keyPrefix === undefined) {
        continue
      }
      partitionLsn.set(keyPrefix, Math.max(partitionLsn.get(keyPrefix) ?? 0, record.lsn))
    }
    const keep = Math.max(0, keepPartitions)
    const retained = new Set(
      [...partitionLsn.entries()]
        .sort(([leftPrefix, leftLsn], [rightPrefix, rightLsn]) => rightLsn - leftLsn || leftPrefix.localeCompare(rightPrefix))
        .slice(0, keep)
        .map(([keyPrefix]) => keyPrefix),
    )
    let removed = 0
    for (const [key, record] of [...records.entries()]) {
      const keyPrefix = nestedRecordKeyPrefix(record)
      if (keyPrefix !== undefined && !retained.has(keyPrefix)) {
        records.delete(key)
        this.deleteRecordOrderEntries(record.path)
        removed += 1
      }
    }
    for (const keyPrefix of retained) {
      if (keepLatestPerPartition > 0) {
        removed += await this.trimRecordsByKeyPrefix(table, keyPrefix, keepLatestPerPartition)
      }
    }
    for (const keyPrefix of partitionLsn.keys()) {
      if (!retained.has(keyPrefix)) {
        this.nestedTableCursors.delete(nestedTableCursorKey(table, keyPrefix.slice(0, -1)))
      }
    }
    return removed
  }

  async getGlobalCursor(): Promise<number> {
    return this.globalCursor
  }

  async setGlobalCursor(lsn: number): Promise<void> {
    this.globalCursor = Math.max(0, lsn)
  }

  async getObjectCursor(): Promise<number> {
    return this.objectCursor
  }

  async setObjectCursor(lsn: number): Promise<void> {
    this.objectCursor = Math.max(0, lsn)
  }

  async getRoomCursor(roomId: string): Promise<number> {
    return this.roomCursors.get(roomId) ?? 0
  }

  async setRoomCursor(roomId: string, lsn: number): Promise<void> {
    this.roomCursors.set(roomId, Math.max(0, lsn))
  }

  async getUserCursor(userId: string): Promise<number> {
    return this.userCursors.get(userId) ?? 0
  }

  async setUserCursor(userId: string, lsn: number): Promise<void> {
    this.userCursors.set(userId, Math.max(0, lsn))
  }

  async getTableCursor(table: string): Promise<number> {
    return this.tableCursors.get(table) ?? 0
  }

  async setTableCursor(table: string, lsn: number): Promise<void> {
    this.tableCursors.set(table, Math.max(0, lsn))
  }

  async getNestedTableCursor(table: string, parentKey: string, nested: string): Promise<number> {
    return this.nestedTableCursors.get(nestedTableCursorId(table, parentKey, nested)) ?? 0
  }

  async setNestedTableCursor(table: string, parentKey: string, nested: string, lsn: number): Promise<void> {
    this.nestedTableCursors.set(nestedTableCursorId(table, parentKey, nested), Math.max(0, lsn))
  }

  async getMetadata(): Promise<ClientCacheMetadata | undefined> {
    return this.metadata
  }

  async setMetadata(metadata: ClientCacheMetadata): Promise<void> {
    this.metadata = metadata
  }

  async putPendingWrite(write: NextDbPendingWrite): Promise<void> {
    this.pendingWrites.set(write.id, write)
  }

  async listPendingWrites(limit = Number.MAX_SAFE_INTEGER): Promise<NextDbPendingWrite[]> {
    return [...this.pendingWrites.values()]
      .sort((left, right) => left.createdAtMs - right.createdAtMs || left.id.localeCompare(right.id))
      .slice(0, limit)
  }

  async deletePendingWrite(id: string): Promise<void> {
    this.pendingWrites.delete(id)
  }

  async clearPendingWrites(): Promise<number> {
    const count = this.pendingWrites.size
    this.pendingWrites.clear()
    return count
  }

  async putSubscription(subscription: NextDbStoredSubscription): Promise<void> {
    this.subscriptions.set(subscription.id, subscription)
  }

  async listSubscriptions(): Promise<NextDbStoredSubscription[]> {
    return [...this.subscriptions.values()]
      .sort((left, right) => left.createdAtMs - right.createdAtMs || left.id.localeCompare(right.id))
  }

  async deleteSubscription(id: string): Promise<void> {
    this.subscriptions.delete(id)
  }

  async clearSubscriptions(): Promise<number> {
    const count = this.subscriptions.size
    this.subscriptions.clear()
    return count
  }

  async stats(): Promise<NextDbCacheStats> {
    const rooms: Record<string, number> = {}
    const users: Record<string, number> = {}
    const tables: Record<string, number> = {}
    const nestedTables: Record<string, Record<string, number>> = {}
    let totalObjectBytes = 0
    let totalObjectCachedBytes = 0
    let totalObjectRangeChunks = 0
    let totalMessages = 0
    let totalRecords = 0
    for (const object of this.objectMetadata.values()) {
      totalObjectBytes += object.byteSize
      totalObjectCachedBytes += this.objectBodies.get(object.id)?.size ?? 0
      const ranges = this.objectBodyRanges.get(object.id) ?? []
      totalObjectRangeChunks += ranges.length
      for (const range of ranges) {
        totalObjectCachedBytes += range.body.size
      }
    }
    for (const [roomId, messages] of this.rooms) {
      rooms[roomId] = messages.length
      totalMessages += messages.length
    }
    let totalUserEvents = 0
    for (const [userId, events] of this.userEvents) {
      users[userId] = events.length
      totalUserEvents += events.length
    }
    for (const [table, records] of this.records) {
      tables[table] = records.size
      totalRecords += records.size
      for (const record of records.values()) {
        const keyPrefix = nestedRecordKeyPrefix(record)
        if (keyPrefix !== undefined) {
          nestedTables[table] ??= {}
          nestedTables[table][keyPrefix] = (nestedTables[table][keyPrefix] ?? 0) + 1
        }
      }
    }
    return {
      totalObjects: this.objectMetadata.size,
      totalObjectBytes,
      totalObjectCachedBytes,
      totalObjectRangeChunks,
      totalMessages,
      totalUserEvents,
      totalUserProfiles: this.userProfiles.size,
      totalRecords,
      pendingWrites: this.pendingWrites.size,
      subscriptions: this.subscriptions.size,
      rooms,
      users,
      tables,
      nestedTables,
    }
  }

  async clearAll(): Promise<number> {
    const stats = await this.stats()
    this.objectMetadata.clear()
    this.objectBodies.clear()
    this.objectBodyRanges.clear()
    this.rooms.clear()
    this.userEvents.clear()
    this.userProfiles.clear()
    this.records.clear()
    this.recordOrderMetadata.clear()
    this.recordOrders.clear()
    this.globalCursor = 0
    this.objectCursor = 0
    this.roomCursors.clear()
    this.userCursors.clear()
    this.tableCursors.clear()
    this.nestedTableCursors.clear()
    this.pendingWrites.clear()
    this.subscriptions.clear()
    this.metadata = undefined
    return stats.totalObjects + stats.totalMessages + stats.totalUserEvents + stats.totalUserProfiles + stats.totalRecords + stats.pendingWrites + stats.subscriptions
  }

  async clearObjects(): Promise<number> {
    const removed = this.objectMetadata.size
    this.objectMetadata.clear()
    this.objectBodies.clear()
    this.objectBodyRanges.clear()
    this.objectCursor = 0
    return removed
  }

  async clearRoom(roomId: string): Promise<number> {
    const removed = this.rooms.get(roomId)?.length ?? 0
    this.rooms.delete(roomId)
    this.roomCursors.delete(roomId)
    return removed
  }

  async clearTable(table: string): Promise<number> {
    const removed = this.records.get(table)?.size ?? 0
    this.records.delete(table)
    for (const [orderId, metadata] of this.recordOrderMetadata) {
      if (metadata.table === table) {
        this.recordOrderMetadata.delete(orderId)
        this.recordOrders.delete(orderId)
      }
    }
    this.tableCursors.delete(table)
    for (const key of [...this.nestedTableCursors.keys()]) {
      if (key.startsWith(`${nestedTableCursorIdPrefix(table)}:`)) {
        this.nestedTableCursors.delete(key)
      }
    }
    return removed
  }

  async trimRoom(roomId: string, keepLatest: number): Promise<number> {
    const messages = this.rooms.get(roomId) ?? []
    const keep = Math.max(0, keepLatest)
    const retained = messages.slice(0, keep)
    this.rooms.set(roomId, retained)
    return Math.max(0, messages.length - retained.length)
  }

  async trimTable(table: string, keepLatest: number): Promise<number> {
    const records = this.records.get(table)
    if (!records) {
      return 0
    }
    const keep = Math.max(0, keepLatest)
    const retained = new Set(
      [...records.values()]
        .sort((left, right) => right.lsn - left.lsn || left.key.localeCompare(right.key))
        .slice(0, keep)
        .map((record) => record.path),
    )
    let removed = 0
    for (const [key, record] of [...records.entries()]) {
      if (!retained.has(record.path)) {
        records.delete(key)
        this.deleteRecordOrderEntries(record.path)
        removed += 1
      }
    }
    return removed
  }

  private updateRecordOrderEntries(record: NextDbRecord): void {
    for (const [orderId, metadata] of this.recordOrderMetadata) {
      if (metadata.table !== record.table || !record.key.startsWith(metadata.keyPrefix)) {
        continue
      }
      const ordered = this.recordOrders.get(orderId) ?? new Map<string, NextDbRecord>()
      for (const [cursor, existing] of ordered) {
        if (existing.path === record.path) {
          ordered.delete(cursor)
        }
      }
      ordered.set(localRecordOrderCursor(record, metadata.order), record)
      this.recordOrders.set(orderId, ordered)
    }
  }

  private deleteRecordOrderEntries(path: string | undefined): void {
    if (path === undefined) {
      return
    }
    for (const ordered of this.recordOrders.values()) {
      for (const [cursor, record] of ordered) {
        if (record.path === path) {
          ordered.delete(cursor)
        }
      }
    }
  }

  private discardStaleObjectRanges(metadata: NextDbObjectMetadata): void {
    const ranges = this.objectBodyRanges.get(metadata.id)
    if (ranges === undefined) {
      return
    }
    const retained = ranges.filter((entry) =>
      entry.byteSize === metadata.byteSize &&
      entry.contentType === metadata.contentType &&
      entry.sha256 === metadata.sha256
    )
    if (retained.length === 0) {
      this.objectBodyRanges.delete(metadata.id)
    } else if (retained.length !== ranges.length) {
      this.objectBodyRanges.set(metadata.id, retained)
    }
  }

  private objectCachedBytes(objectId: string): number {
    let bytes = this.objectBodies.get(objectId)?.size ?? 0
    for (const range of this.objectBodyRanges.get(objectId) ?? []) {
      bytes += range.body.size
    }
    return bytes
  }
}

export class IndexedDbLocalCache implements NextDbLocalCache {
  private readonly dbName: string
  private openPromise?: Promise<IDBDatabase>

  constructor(dbName = "nextdb-client") {
    this.dbName = dbName
  }

  async putObject(metadata: NextDbObjectMetadata, body?: Blob): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    const previous = await idbGet(db, "objectMetadata", metadata.id) as NextDbObjectMetadata | undefined
    await idbObjectTransaction(db, "readwrite", (metadataStore, bodyStore, rangeStore) => {
      metadataStore.put(metadata)
      deleteStaleObjectBodyRangeRows(rangeStore, metadata)
      if (body !== undefined) {
        bodyStore.put({ id: metadata.id, body })
        deleteObjectBodyRangeRows(rangeStore, metadata.id)
      } else if (previous !== undefined && !objectMetadataContentMatches(previous, metadata)) {
        bodyStore.delete(metadata.id)
      }
    })
  }

  async getObjectMetadata(objectId: string): Promise<NextDbObjectMetadata | undefined> {
    if (!hasIndexedDb()) {
      return undefined
    }

    const db = await this.open()
    return idbGet(db, "objectMetadata", objectId) as Promise<NextDbObjectMetadata | undefined>
  }

  async getObjectBody(objectId: string): Promise<Blob | undefined> {
    if (!hasIndexedDb()) {
      return undefined
    }

    const db = await this.open()
    const row = await idbGet(db, "objectBodies", objectId)
    return isObjectBodyRecord(row) ? row.body : undefined
  }

  async putObjectBodyRange(metadata: NextDbObjectMetadata, range: ObjectBodyRangeResponse): Promise<void> {
    if (!hasIndexedDb() || !objectBodyRangeMatchesMetadata(range, metadata)) {
      return
    }

    const db = await this.open()
    await idbObjectRangeTransaction(db, "readwrite", (store) => {
      store.put({
        id: objectBodyRangeCacheId(metadata.id, range.start, range.end),
        objectId: metadata.id,
        start: range.start,
        end: range.end,
        byteSize: metadata.byteSize,
        contentType: metadata.contentType,
        sha256: metadata.sha256,
        body: range.body,
      })
    })
  }

  async getObjectBodyRange(
    metadata: NextDbObjectMetadata,
    start: number,
    end: number,
  ): Promise<ObjectBodyRangeResponse | undefined> {
    if (!hasIndexedDb()) {
      return undefined
    }

    const db = await this.open()
    return idbGetObjectBodyRange(db, metadata, start, end)
  }

  async listObjects(limit: number, afterId?: string): Promise<NextDbObjectMetadata[]> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return idbListObjects(db, limit, afterId)
  }

  async deleteObject(objectId: string): Promise<boolean> {
    if (!hasIndexedDb()) {
      return false
    }

    const db = await this.open()
    const existing = await idbGet(db, "objectMetadata", objectId)
    await idbObjectTransaction(db, "readwrite", (metadataStore, bodyStore, rangeStore) => {
      metadataStore.delete(objectId)
      bodyStore.delete(objectId)
      deleteObjectBodyRangeRows(rangeStore, objectId)
    })
    return existing !== undefined
  }

  async trimObjects(maxObjects: number, maxBytes: number): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const objects = await idbListAllObjects(db)
    const cachedBytes = await idbObjectCachedBytesById(db)
    const deleteIds = selectObjectIdsToTrim(
      objects.map((object) => ({
        ...object,
        cachedBytes: cachedBytes.get(object.id) ?? 0,
      })),
      maxObjects,
      maxBytes,
    )
    if (deleteIds.length === 0) {
      return 0
    }
    await idbObjectTransaction(db, "readwrite", (metadataStore, bodyStore, rangeStore) => {
      for (const objectId of deleteIds) {
        metadataStore.delete(objectId)
        bodyStore.delete(objectId)
        deleteObjectBodyRangeRows(rangeStore, objectId)
      }
    })
    return deleteIds.length
  }

  async putRoomMessages(roomId: string, messages: NextDbMessage[]): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbTransaction(db, "readwrite", (store) => {
      for (const message of messages) {
        store.put({
          ...message,
          roomId,
        })
      }
    })
  }

  async getRoomMessages(roomId: string, limit: number, beforeLsn?: number): Promise<NextDbMessage[]> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return new Promise((resolve, reject) => {
      const transaction = db.transaction("messages", "readonly")
      const store = transaction.objectStore("messages")
      const index = store.index("byRoomLsn")
      const upper = beforeLsn === undefined ? Number.MAX_SAFE_INTEGER : beforeLsn - 1
      const range = IDBKeyRange.bound([roomId, 0], [roomId, upper])
      const request = index.openCursor(range, "prev")
      const messages: NextDbMessage[] = []

      request.onerror = () => reject(request.error)
      request.onsuccess = () => {
        const cursor = request.result
        if (!cursor || messages.length >= limit) {
          resolve(messages)
          return
        }
        messages.push(fromStoredMessage(cursor.value))
        cursor.continue()
      }
    })
  }

  async deleteRoomMessage(_roomId: string, messageId: string): Promise<boolean> {
    if (!hasIndexedDb()) {
      return false
    }

    const db = await this.open()
    const existing = await idbGet(db, "messages", messageId)
    if (!existing) {
      return false
    }
    await idbTransaction(db, "readwrite", (store) => {
      store.delete(messageId)
    })
    return true
  }

  async putUserEvents(userId: string, events: NextDbUserEvent[]): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbUserEventTransaction(db, "readwrite", (store) => {
      for (const event of events) {
        store.put({
          ...event,
          userId,
        })
      }
    })
  }

  async getUserEvents(userId: string, limit: number, beforeLsn?: number): Promise<NextDbUserEvent[]> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return new Promise((resolve, reject) => {
      const transaction = db.transaction("userEvents", "readonly")
      const store = transaction.objectStore("userEvents")
      const index = store.index("byUserLsn")
      const upper = beforeLsn === undefined ? Number.MAX_SAFE_INTEGER : beforeLsn - 1
      const range = IDBKeyRange.bound([userId, 0], [userId, upper])
      const request = index.openCursor(range, "prev")
      const events: NextDbUserEvent[] = []

      request.onerror = () => reject(request.error)
      request.onsuccess = () => {
        const cursor = request.result
        if (!cursor || events.length >= limit) {
          resolve(events)
          return
        }
        events.push(cursor.value as NextDbUserEvent)
        cursor.continue()
      }
      transaction.onerror = () => reject(transaction.error)
    })
  }

  async clearUserEvents(userId: string): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const removed = await deleteUserEvents(db, userId)
    await this.setUserCursor(userId, 0)
    return removed
  }

  async trimUserEvents(userId: string, keepLatest: number): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return trimUserEvents(db, userId, Math.max(0, keepLatest))
  }

  async putUserProfile(profile: NextDbUserProfile): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbNamedTransaction(db, "userProfiles", "readwrite", (store) => {
      store.put(profile)
    })
  }

  async getUserProfile(userId: string): Promise<NextDbUserProfile | undefined> {
    if (!hasIndexedDb()) {
      return undefined
    }

    const db = await this.open()
    return idbGet(db, "userProfiles", userId) as Promise<NextDbUserProfile | undefined>
  }

  async listUserProfiles(limit: number, afterUserId?: string): Promise<NextDbUserProfile[]> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return idbListUserProfiles(db, limit, afterUserId)
  }

  async deleteUserProfile(userId: string): Promise<boolean> {
    if (!hasIndexedDb()) {
      return false
    }

    const db = await this.open()
    const existing = await idbGet(db, "userProfiles", userId)
    await idbNamedTransaction(db, "userProfiles", "readwrite", (store) => {
      store.delete(userId)
    })
    return existing !== undefined
  }

  async putRecords(records: NextDbRecord[]): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbRecordTransaction(db, "readwrite", (store) => {
      for (const record of records) {
        store.put(record)
      }
    })
    await idbUpdateRecordOrderEntries(db, records)
  }

  async getRecord<T = unknown>(table: string, key: string): Promise<NextDbRecord<T> | undefined> {
    if (!hasIndexedDb()) {
      return undefined
    }

    const db = await this.open()
    return idbGetRecordByTableKey<T>(db, table, key)
  }

  async listRecords<T = unknown>(table: string, limit: number, afterKey?: string): Promise<Array<NextDbRecord<T>>> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return new Promise((resolve, reject) => {
      const transaction = db.transaction("records", "readonly")
      const store = transaction.objectStore("records")
      const index = store.index("byTableKey")
      const lower = afterKey === undefined ? "" : afterKey
      const range = afterKey === undefined
        ? IDBKeyRange.bound([table, ""], [table, "\uffff"])
        : IDBKeyRange.bound([table, lower], [table, "\uffff"], true, false)
      const request = index.openCursor(range)
      const records: Array<NextDbRecord<T>> = []

      request.onerror = () => reject(request.error)
      request.onsuccess = () => {
        const cursor = request.result
        if (!cursor || records.length >= limit) {
          resolve(records)
          return
        }
        records.push(cursor.value as NextDbRecord<T>)
        cursor.continue()
      }
      transaction.onerror = () => reject(transaction.error)
    })
  }

  async listRecordsByKeyPrefix<T = unknown>(
    table: string,
    keyPrefix: string,
    limit: number,
    afterKey?: string,
  ): Promise<Array<NextDbRecord<T>>> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return new Promise((resolve, reject) => {
      const transaction = db.transaction("records", "readonly")
      const store = transaction.objectStore("records")
      const index = store.index("byTableKey")
      const lower = afterKey === undefined ? keyPrefix : afterKey
      const range = IDBKeyRange.bound(
        [table, lower],
        [table, `${keyPrefix}\uffff`],
        afterKey !== undefined,
        false,
      )
      const request = index.openCursor(range)
      const records: Array<NextDbRecord<T>> = []

      request.onerror = () => reject(request.error)
      request.onsuccess = () => {
        const cursor = request.result
        if (!cursor || records.length >= limit) {
          resolve(records)
          return
        }
        const record = cursor.value as NextDbRecord<T>
        if (record.key.startsWith(keyPrefix)) {
          records.push(record)
        }
        cursor.continue()
      }
      transaction.onerror = () => reject(transaction.error)
    })
  }

  async listRecordsBySchemaOrder<T = unknown>(
    table: string,
    keyPrefix: string,
    order: RecordOrderTerm[],
    limit: number,
    afterCursor?: string,
  ): Promise<LocalOrderedRecordsResponse<T>> {
    if (!hasIndexedDb()) {
      return { records: [], hasMore: false }
    }

    const db = await this.open()
    const orderId = await idbMaterializeRecordOrder(db, table, keyPrefix, order)
    return idbListRecordOrder<T>(db, orderId, limit, afterCursor)
  }

  async queryRecordsByIndex<T = unknown>(
    table: string,
    query: LocalIndexQuery,
  ): Promise<LocalIndexedRecordsResponse<T>> {
    if (!hasIndexedDb()) {
      return { records: [], hasMore: false }
    }

    const db = await this.open()
    return idbQueryRecordsByIndex<T>(db, table, query)
  }

  async deleteRecord(table: string, key: string): Promise<boolean> {
    if (!hasIndexedDb()) {
      return false
    }

    const db = await this.open()
    const existing = await idbGetRecordByTableKey(db, table, key)
    if (!existing) {
      return false
    }
    await idbRecordTransaction(db, "readwrite", (store) => {
      store.delete(existing.path)
    })
    await idbDeleteRecordOrderEntries(db, existing.path)
    return true
  }

  async clearRecordsByKeyPrefix(table: string, keyPrefix: string): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return deleteTableRecordsByKeyPrefix(db, table, keyPrefix)
  }

  async trimRecordsByKeyPrefix(table: string, keyPrefix: string, keepLatest: number): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return trimTableRecordsByKeyPrefix(db, table, keyPrefix, Math.max(0, keepLatest))
  }

  async getGlobalCursor(): Promise<number> {
    return this.getCursor("global")
  }

  async setGlobalCursor(lsn: number): Promise<void> {
    await this.setCursor("global", lsn)
  }

  async getObjectCursor(): Promise<number> {
    return this.getCursor("objects")
  }

  async setObjectCursor(lsn: number): Promise<void> {
    await this.setCursor("objects", lsn)
  }

  async getRoomCursor(roomId: string): Promise<number> {
    return this.getCursor(`room:${roomId}`)
  }

  async setRoomCursor(roomId: string, lsn: number): Promise<void> {
    await this.setCursor(`room:${roomId}`, lsn)
  }

  async getUserCursor(userId: string): Promise<number> {
    return this.getCursor(`user:${userId}`)
  }

  async setUserCursor(userId: string, lsn: number): Promise<void> {
    await this.setCursor(`user:${userId}`, lsn)
  }

  async getTableCursor(table: string): Promise<number> {
    return this.getCursor(`table:${table}`)
  }

  async setTableCursor(table: string, lsn: number): Promise<void> {
    await this.setCursor(`table:${table}`, lsn)
  }

  async getNestedTableCursor(table: string, parentKey: string, nested: string): Promise<number> {
    return this.getCursor(nestedTableCursorId(table, parentKey, nested))
  }

  async setNestedTableCursor(table: string, parentKey: string, nested: string, lsn: number): Promise<void> {
    await this.setCursor(nestedTableCursorId(table, parentKey, nested), lsn)
  }

  async getMetadata(): Promise<ClientCacheMetadata | undefined> {
    if (!hasIndexedDb()) {
      return undefined
    }

    const db = await this.open()
    const value = await idbGet(db, "metadata", "cache")
    return isMetadataRecord(value) ? value.metadata : undefined
  }

  async setMetadata(metadata: ClientCacheMetadata): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbNamedTransaction(db, "metadata", "readwrite", (store) => {
      store.put({ key: "cache", metadata })
    })
  }

  async putPendingWrite(write: NextDbPendingWrite): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbPendingWriteTransaction(db, "readwrite", (store) => {
      store.put(write)
    })
  }

  async listPendingWrites(limit = Number.MAX_SAFE_INTEGER): Promise<NextDbPendingWrite[]> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return idbListPendingWrites(db, limit)
  }

  async deletePendingWrite(id: string): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbPendingWriteTransaction(db, "readwrite", (store) => {
      store.delete(id)
    })
  }

  async clearPendingWrites(): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const count = await idbCount(db, "pendingWrites")
    await idbPendingWriteTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    return count
  }

  async putSubscription(subscription: NextDbStoredSubscription): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbSubscriptionTransaction(db, "readwrite", (store) => {
      store.put(subscription)
    })
  }

  async listSubscriptions(): Promise<NextDbStoredSubscription[]> {
    if (!hasIndexedDb()) {
      return []
    }

    const db = await this.open()
    return idbListSubscriptions(db)
  }

  async deleteSubscription(id: string): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbSubscriptionTransaction(db, "readwrite", (store) => {
      store.delete(id)
    })
  }

  async clearSubscriptions(): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const count = await idbCount(db, "subscriptions")
    await idbSubscriptionTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    return count
  }

  async stats(): Promise<NextDbCacheStats> {
    if (!hasIndexedDb()) {
      return { totalObjects: 0, totalObjectBytes: 0, totalObjectCachedBytes: 0, totalObjectRangeChunks: 0, totalMessages: 0, totalUserEvents: 0, totalUserProfiles: 0, totalRecords: 0, pendingWrites: 0, subscriptions: 0, rooms: {}, users: {}, tables: {}, nestedTables: {} }
    }

    const db = await this.open()
    const objectCache = await idbObjectCacheStats(db)
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(["objectMetadata", "messages", "userEvents", "userProfiles", "records"], "readonly")
      const objectStore = transaction.objectStore("objectMetadata")
      const messageStore = transaction.objectStore("messages")
      const userEventStore = transaction.objectStore("userEvents")
      const userProfileStore = transaction.objectStore("userProfiles")
      const recordStore = transaction.objectStore("records")
      const objectRequest = objectStore.openCursor()
      const request = messageStore.openCursor()
      const userEventRequest = userEventStore.openCursor()
      const userProfileRequest = userProfileStore.openCursor()
      const recordRequest = recordStore.openCursor()
      const rooms: Record<string, number> = {}
      const users: Record<string, number> = {}
      const tables: Record<string, number> = {}
      const nestedTables: Record<string, Record<string, number>> = {}
      let totalObjects = 0
      let totalObjectBytes = 0
      let totalMessages = 0
      let totalUserEvents = 0
      let totalUserProfiles = 0
      let totalRecords = 0
      let objectsDone = false
      let messagesDone = false
      let userEventsDone = false
      let userProfilesDone = false
      let recordsDone = false

      const maybeResolve = () => {
        if (objectsDone && messagesDone && userEventsDone && userProfilesDone && recordsDone) {
          Promise.all([
            this.listPendingWrites(),
            this.listSubscriptions(),
          ])
            .then(([pendingWrites, subscriptions]) => {
              resolve({
                totalObjects,
                totalObjectBytes,
                totalObjectCachedBytes: objectCache.bytes,
                totalObjectRangeChunks: objectCache.rangeChunks,
                totalMessages,
                totalUserEvents,
                totalUserProfiles,
                totalRecords,
                pendingWrites: pendingWrites.length,
                subscriptions: subscriptions.length,
                rooms,
                users,
                tables,
                nestedTables,
              })
            })
            .catch(reject)
        }
      }

      objectRequest.onerror = () => reject(objectRequest.error)
      objectRequest.onsuccess = () => {
        const cursor = objectRequest.result
        if (!cursor) {
          objectsDone = true
          maybeResolve()
          return
        }
        const object = cursor.value as NextDbObjectMetadata
        totalObjects += 1
        totalObjectBytes += object.byteSize
        cursor.continue()
      }
      request.onerror = () => reject(request.error)
      request.onsuccess = () => {
        const cursor = request.result
        if (!cursor) {
          messagesDone = true
          maybeResolve()
          return
        }
        const message = fromStoredMessage(cursor.value)
        rooms[message.roomId] = (rooms[message.roomId] ?? 0) + 1
        totalMessages += 1
        cursor.continue()
      }
      userEventRequest.onerror = () => reject(userEventRequest.error)
      userEventRequest.onsuccess = () => {
        const cursor = userEventRequest.result
        if (!cursor) {
          userEventsDone = true
          maybeResolve()
          return
        }
        const event = cursor.value as NextDbUserEvent
        users[event.userId] = (users[event.userId] ?? 0) + 1
        totalUserEvents += 1
        cursor.continue()
      }
      userProfileRequest.onerror = () => reject(userProfileRequest.error)
      userProfileRequest.onsuccess = () => {
        const cursor = userProfileRequest.result
        if (!cursor) {
          userProfilesDone = true
          maybeResolve()
          return
        }
        totalUserProfiles += 1
        cursor.continue()
      }
      recordRequest.onerror = () => reject(recordRequest.error)
      recordRequest.onsuccess = () => {
        const cursor = recordRequest.result
        if (!cursor) {
          recordsDone = true
          maybeResolve()
          return
        }
        const record = cursor.value as NextDbRecord
        tables[record.table] = (tables[record.table] ?? 0) + 1
        const keyPrefix = nestedRecordKeyPrefix(record)
        if (keyPrefix !== undefined) {
          nestedTables[record.table] ??= {}
          nestedTables[record.table][keyPrefix] = (nestedTables[record.table][keyPrefix] ?? 0) + 1
        }
        totalRecords += 1
        cursor.continue()
      }
      transaction.onerror = () => reject(transaction.error)
    })
  }

  async clearAll(): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const objectCount = await idbCount(db, "objectMetadata")
    const messageCount = await idbCount(db, "messages")
    const userEventCount = await idbCount(db, "userEvents")
    const userProfileCount = await idbCount(db, "userProfiles")
    const recordCount = await idbCount(db, "records")
    const pendingCount = await idbCount(db, "pendingWrites")
    const subscriptionCount = await idbCount(db, "subscriptions")
    const count = objectCount + messageCount + userEventCount + userProfileCount + recordCount + pendingCount + subscriptionCount
    await idbObjectTransaction(db, "readwrite", (metadataStore, bodyStore, rangeStore) => {
      metadataStore.clear()
      bodyStore.clear()
      rangeStore.clear()
    })
    await idbTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    await idbUserEventTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    await idbNamedTransaction(db, "userProfiles", "readwrite", (store) => {
      store.clear()
    })
    await idbRecordTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    await idbCursorTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    await idbPendingWriteTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    await idbSubscriptionTransaction(db, "readwrite", (store) => {
      store.clear()
    })
    await idbRecordOrderTransaction(db, "readwrite", (metadata, orders) => {
      metadata.clear()
      orders.clear()
    })
    await idbNamedTransaction(db, "metadata", "readwrite", (store) => {
      store.clear()
    })
    return count
  }

  async clearObjects(): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const count = await idbCount(db, "objectMetadata")
    await idbObjectTransaction(db, "readwrite", (metadataStore, bodyStore, rangeStore) => {
      metadataStore.clear()
      bodyStore.clear()
      rangeStore.clear()
    })
    await this.setObjectCursor(0)
    return count
  }

  async clearRoom(roomId: string): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const removed = await deleteRoomMessages(db, roomId)
    await this.setRoomCursor(roomId, 0)
    return removed
  }

  async clearTable(table: string): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    const removed = await deleteTableRecords(db, table)
    await idbDeleteRecordOrdersForTable(db, table)
    await this.setTableCursor(table, 0)
    await deleteNestedTableCursorsForLogicalTable(db, table)
    return removed
  }

  async trimRoom(roomId: string, keepLatest: number): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return trimRoomMessages(db, roomId, Math.max(0, keepLatest))
  }

  async trimTable(table: string, keepLatest: number): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return trimTableRecords(db, table, Math.max(0, keepLatest))
  }

  async trimNestedTablePartitions(
    table: string,
    keepPartitions: number,
    keepLatestPerPartition: number,
  ): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return trimNestedTableRecordPartitions(
      db,
      table,
      Math.max(0, keepPartitions),
      Math.max(0, keepLatestPerPartition),
    )
  }

  private open(): Promise<IDBDatabase> {
    this.openPromise ??= new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 12)

      request.onerror = () => reject(request.error)
      request.onupgradeneeded = () => {
        const db = request.result
        if (!db.objectStoreNames.contains("messages")) {
          const store = db.createObjectStore("messages", { keyPath: "id" })
          store.createIndex("byRoomLsn", ["roomId", "lsn"])
        }
        if (!db.objectStoreNames.contains("userEvents")) {
          const store = db.createObjectStore("userEvents", { keyPath: "id" })
          store.createIndex("byUserLsn", ["userId", "lsn"])
        }
        if (!db.objectStoreNames.contains("objectMetadata")) {
          db.createObjectStore("objectMetadata", { keyPath: "id" })
        }
        if (!db.objectStoreNames.contains("objectBodies")) {
          db.createObjectStore("objectBodies", { keyPath: "id" })
        }
        if (!db.objectStoreNames.contains("objectBodyRanges")) {
          const store = db.createObjectStore("objectBodyRanges", { keyPath: "id" })
          store.createIndex("byObjectStart", ["objectId", "start"])
        } else {
          const store = request.transaction?.objectStore("objectBodyRanges")
          if (store && !store.indexNames.contains("byObjectStart")) {
            store.createIndex("byObjectStart", ["objectId", "start"])
          }
        }
        if (!db.objectStoreNames.contains("userProfiles")) {
          db.createObjectStore("userProfiles", { keyPath: "userId" })
        }
        if (!db.objectStoreNames.contains("records")) {
          const store = db.createObjectStore("records", { keyPath: "path" })
          store.createIndex("byTableKey", ["table", "key"])
          store.createIndex("byTableLsn", ["table", "lsn"])
        } else {
          const store = request.transaction?.objectStore("records")
          if (store && !store.indexNames.contains("byTableLsn")) {
            store.createIndex("byTableLsn", ["table", "lsn"])
          }
        }
        if (!db.objectStoreNames.contains("cursors")) {
          db.createObjectStore("cursors", { keyPath: "key" })
        }
        if (!db.objectStoreNames.contains("pendingWrites")) {
          const store = db.createObjectStore("pendingWrites", { keyPath: "id" })
          store.createIndex("byCreatedAt", "createdAtMs")
        }
        if (!db.objectStoreNames.contains("metadata")) {
          db.createObjectStore("metadata", { keyPath: "key" })
        }
        if (!db.objectStoreNames.contains("subscriptions")) {
          const store = db.createObjectStore("subscriptions", { keyPath: "id" })
          store.createIndex("byKind", "kind")
          store.createIndex("byUpdatedAt", "updatedAtMs")
        }
        if (!db.objectStoreNames.contains("recordOrderMetadata")) {
          db.createObjectStore("recordOrderMetadata", { keyPath: "orderId" })
        }
        if (!db.objectStoreNames.contains("recordOrders")) {
          const store = db.createObjectStore("recordOrders", { keyPath: "id" })
          store.createIndex("byOrderCursor", ["orderId", "cursor"])
          store.createIndex("byRecordPath", "recordPath")
          store.createIndex("byTable", "table")
        }
      }
      request.onsuccess = () => resolve(request.result)
    })
    return this.openPromise
  }

  private async getCursor(key: string): Promise<number> {
    if (!hasIndexedDb()) {
      return 0
    }

    const db = await this.open()
    return idbGetCursor(db, key)
  }

  private async setCursor(key: string, lsn: number): Promise<void> {
    if (!hasIndexedDb()) {
      return
    }

    const db = await this.open()
    await idbCursorTransaction(db, "readwrite", (store) => {
      if (lsn <= 0) {
        store.delete(key)
      } else {
        store.put({ key, lsn })
      }
    })
  }
}

export class WebSocketRealtimeTransport implements NextDbRealtimeTransport {
  private readonly socket: WebSocket
  private openListener?: () => void
  private frameListener?: (frame: ServerFrame) => void
  private errorListener?: (error?: unknown) => void
  private closeListener?: () => void

  constructor(url: URL) {
    this.socket = new WebSocket(url)
    this.socket.onopen = () => this.openListener?.()
    this.socket.onmessage = (message) => {
      this.frameListener?.(decodeRealtimeServerFrame(String(message.data)))
    }
    this.socket.onerror = (event) => this.errorListener?.(event)
    this.socket.onclose = () => this.closeListener?.()
  }

  get state(): RealtimeTransportState {
    if (this.socket.readyState === WebSocket.OPEN) {
      return "open"
    }
    if (this.socket.readyState === WebSocket.CLOSED || this.socket.readyState === WebSocket.CLOSING) {
      return "closed"
    }
    return "connecting"
  }

  send(frame: ClientFrame): void {
    this.socket.send(encodeRealtimeClientFrame(frame))
  }

  close(): void {
    this.socket.close()
  }

  onOpen(listener: () => void): void {
    this.openListener = listener
  }

  onFrame(listener: (frame: ServerFrame) => void): void {
    this.frameListener = listener
  }

  onError(listener: (error?: unknown) => void): void {
    this.errorListener = listener
  }

  onClose(listener: () => void): void {
    this.closeListener = listener
  }
}

export class WebTransportRealtimeTransport implements NextDbRealtimeTransport {
  private readonly transport: WebTransport
  private readonly encoder = new TextEncoder()
  private readonly decoder = new TextDecoder()
  private stateValue: RealtimeTransportState = "connecting"
  private writer?: WritableStreamDefaultWriter<Uint8Array>
  private writeChain: Promise<void> = Promise.resolve()
  private queuedFrames: ClientFrame[] = []
  private openListener?: () => void
  private frameListener?: (frame: ServerFrame) => void
  private errorListener?: (error?: unknown) => void
  private closeListener?: () => void

  constructor(url: URL, options?: WebTransportOptions) {
    const ctor = globalThis.WebTransport
    if (ctor === undefined) {
      throw new Error("WebTransport is not available in this runtime")
    }
    this.transport = new ctor(webTransportUrl(url).toString(), options)
    void this.start()
  }

  get state(): RealtimeTransportState {
    return this.stateValue
  }

  send(frame: ClientFrame): void {
    if (this.stateValue !== "open" || this.writer === undefined) {
      this.queuedFrames.push(frame)
      return
    }
    this.writeFrame(frame)
  }

  close(): void {
    this.finishClosed()
    this.writer?.close().catch((error) => this.errorListener?.(error))
    this.transport.close()
  }

  onOpen(listener: () => void): void {
    this.openListener = listener
  }

  onFrame(listener: (frame: ServerFrame) => void): void {
    this.frameListener = listener
  }

  onError(listener: (error?: unknown) => void): void {
    this.errorListener = listener
  }

  onClose(listener: () => void): void {
    this.closeListener = listener
  }

  private async start(): Promise<void> {
    try {
      await this.transport.ready
      if (this.stateValue === "closed") {
        return
      }
      const stream = await this.transport.createBidirectionalStream()
      this.writer = stream.writable.getWriter()
      this.stateValue = "open"
      this.openListener?.()
      for (const frame of this.queuedFrames.splice(0)) {
        this.writeFrame(frame)
      }
      void this.readJsonLines(stream.readable)
      void this.watchIncomingBidirectionalStreams()
      void this.watchIncomingUnidirectionalStreams()
      await this.transport.closed
      this.finishClosed()
    } catch (error) {
      this.errorListener?.(error)
      this.finishClosed()
    }
  }

  private writeFrame(frame: ClientFrame): void {
    const writer = this.writer
    if (writer === undefined) {
      this.queuedFrames.push(frame)
      return
    }
    const payload = this.encoder.encode(encodeRealtimeClientFrameJsonLine(frame))
    this.writeChain = this.writeChain
      .then(() => writer.write(payload))
      .catch((error) => {
        this.errorListener?.(error)
        this.finishClosed()
      })
  }

  private async watchIncomingBidirectionalStreams(): Promise<void> {
    const reader = this.transport.incomingBidirectionalStreams.getReader()
    try {
      while (this.stateValue !== "closed") {
        const item = await reader.read()
        if (item.done) {
          break
        }
        void this.readJsonLines(item.value.readable)
      }
    } catch (error) {
      if (this.stateValue !== "closed") {
        this.errorListener?.(error)
      }
    } finally {
      reader.releaseLock()
    }
  }

  private async watchIncomingUnidirectionalStreams(): Promise<void> {
    const reader = this.transport.incomingUnidirectionalStreams.getReader()
    try {
      while (this.stateValue !== "closed") {
        const item = await reader.read()
        if (item.done) {
          break
        }
        void this.readJsonLines(item.value)
      }
    } catch (error) {
      if (this.stateValue !== "closed") {
        this.errorListener?.(error)
      }
    } finally {
      reader.releaseLock()
    }
  }

  private async readJsonLines(readable: ReadableStream<Uint8Array>): Promise<void> {
    const reader = readable.getReader()
    const decoder = new RealtimeServerFrameJsonLineDecoder()
    try {
      while (this.stateValue !== "closed") {
        const item = await reader.read()
        if (item.done) {
          break
        }
        for (const frame of decoder.push(this.decoder.decode(item.value, { stream: true }))) {
          this.frameListener?.(frame)
        }
      }
      for (const frame of decoder.push(this.decoder.decode(), { flush: true })) {
        this.frameListener?.(frame)
      }
    } catch (error) {
      if (this.stateValue !== "closed") {
        this.errorListener?.(error)
      }
    } finally {
      reader.releaseLock()
    }
  }

  private finishClosed(): void {
    if (this.stateValue === "closed") {
      return
    }
    this.stateValue = "closed"
    this.closeListener?.()
  }
}

export function webTransportRealtimeTransport(options?: WebTransportOptions): NextDbRealtimeTransportFactory {
  return ({ url }) => new WebTransportRealtimeTransport(url, options)
}

export interface JsonLineHttpRealtimeTransportOptions {
  fetch?: typeof fetch
  connectPath?: string
  requestInit?: Omit<RequestInit, "body" | "method" | "signal"> & { duplex?: "half" }
}

export class JsonLineHttpRealtimeTransport implements NextDbRealtimeTransport {
  private readonly url: URL
  private readonly fetchImpl: typeof fetch
  private readonly requestInit?: JsonLineHttpRealtimeTransportOptions["requestInit"]
  private readonly abortController = new AbortController()
  private readonly encoder = new TextEncoder()
  private readonly decoder = new TextDecoder()
  private stateValue: RealtimeTransportState = "connecting"
  private controller?: ReadableStreamDefaultController<Uint8Array>
  private queuedFrames: ClientFrame[] = []
  private openListener?: () => void
  private frameListener?: (frame: ServerFrame) => void
  private errorListener?: (error?: unknown) => void
  private closeListener?: () => void

  constructor(url: URL, options: JsonLineHttpRealtimeTransportOptions = {}) {
    this.url = jsonLineHttpUrl(url, options.connectPath)
    this.fetchImpl = options.fetch ?? globalThis.fetch
    this.requestInit = options.requestInit
    if (this.fetchImpl === undefined) {
      throw new Error("fetch is not available in this runtime")
    }
    void this.start()
  }

  get state(): RealtimeTransportState {
    return this.stateValue
  }

  send(frame: ClientFrame): void {
    if (this.stateValue !== "open" || this.controller === undefined) {
      this.queuedFrames.push(frame)
      return
    }
    this.enqueueFrame(frame)
  }

  close(): void {
    if (this.stateValue === "closed") {
      return
    }
    try {
      this.controller?.close()
    } catch {
      // The request stream may already be closed by the runtime.
    }
    this.abortController.abort()
    this.finishClosed()
  }

  onOpen(listener: () => void): void {
    this.openListener = listener
  }

  onFrame(listener: (frame: ServerFrame) => void): void {
    this.frameListener = listener
  }

  onError(listener: (error?: unknown) => void): void {
    this.errorListener = listener
  }

  onClose(listener: () => void): void {
    this.closeListener = listener
  }

  private async start(): Promise<void> {
    try {
      const body = new ReadableStream<Uint8Array>({
        start: (controller) => {
          this.controller = controller
          queueMicrotask(() => this.finishOpen())
        },
        cancel: () => this.finishClosed(),
      })
      const response = await this.fetchImpl(this.url, {
        ...this.requestInit,
        method: "POST",
        headers: jsonLineHttpHeaders(this.requestInit?.headers),
        body,
        duplex: "half",
        signal: this.abortController.signal,
      } as RequestInit & { duplex: "half" })
      if (!response.ok) {
        throw new Error(`jsonl realtime connect failed with status ${response.status}`)
      }
      if (response.body === null) {
        throw new Error("jsonl realtime response body is not readable")
      }
      await this.readJsonLines(response.body)
      this.finishClosed()
    } catch (error) {
      if (this.stateValue !== "closed") {
        this.errorListener?.(error)
        this.finishClosed()
      }
    }
  }

  private finishOpen(): void {
    if (this.stateValue !== "connecting" || this.controller === undefined) {
      return
    }
    this.stateValue = "open"
    this.openListener?.()
    for (const frame of this.queuedFrames.splice(0)) {
      this.enqueueFrame(frame)
    }
  }

  private enqueueFrame(frame: ClientFrame): void {
    const controller = this.controller
    if (controller === undefined) {
      this.queuedFrames.push(frame)
      return
    }
    try {
      controller.enqueue(this.encoder.encode(encodeRealtimeClientFrameJsonLine(frame)))
    } catch (error) {
      this.errorListener?.(error)
      this.finishClosed()
    }
  }

  private async readJsonLines(readable: ReadableStream<Uint8Array>): Promise<void> {
    const reader = readable.getReader()
    const decoder = new RealtimeServerFrameJsonLineDecoder()
    try {
      while (this.stateValue !== "closed") {
        const item = await reader.read()
        if (item.done) {
          break
        }
        for (const frame of decoder.push(this.decoder.decode(item.value, { stream: true }))) {
          this.frameListener?.(frame)
        }
      }
      for (const frame of decoder.push(this.decoder.decode(), { flush: true })) {
        this.frameListener?.(frame)
      }
    } finally {
      reader.releaseLock()
    }
  }

  private finishClosed(): void {
    if (this.stateValue === "closed") {
      return
    }
    this.stateValue = "closed"
    this.closeListener?.()
  }
}

export function jsonLineHttpRealtimeTransport(
  options?: JsonLineHttpRealtimeTransportOptions,
): NextDbRealtimeTransportFactory {
  return ({ url }) => new JsonLineHttpRealtimeTransport(url, options)
}

export interface NextDbClientOptions {
  endpoint: string
  wsEndpoint?: string
  replicaEndpoints?: string[]
  clientId?: string
  authToken?: string
  adminToken?: string
  userId?: string
  sessionId?: string
  schemaVersion?: number
  cache?: NextDbLocalCache
  cacheNamespace?: string
  realtimeTransportKind?: NextDbRealtimeTransportKind
  realtimeTransport?: NextDbRealtimeTransportFactory
  webTransportOptions?: WebTransportOptions
  connectionMetadata?: unknown
  offlineWrites?: boolean
  autoFlushPendingWrites?: boolean | PendingWriteAutoFlushOptions
  autoRestoreSubscriptions?: boolean
}

export class NextDbClient {
  private readonly initialEndpoint: string
  private activeEndpoint: string
  private readonly explicitWsEndpoint?: string
  private readonly knownEndpoints: string[]
  private readonly clientId: string
  private readonly authToken?: string
  private readonly adminToken?: string
  private readonly userId?: string
  private readonly sessionId?: string
  private schemaVersion?: number
  private readonly cache: NextDbLocalCache
  private readonly cacheScope: NextDbCacheScope
  private readonly configuredRealtimeTransportKind: NextDbRealtimeTransportKind | "custom"
  private readonly configuredRealtimeTransportFactory: NextDbRealtimeTransportFactory
  private realtimeTransportKind: NextDbRealtimeTransportKind | "custom"
  private realtimeTransportFactory: NextDbRealtimeTransportFactory
  private readonly offlineWrites: boolean
  private connectionMetadata?: unknown
  private transport?: NextDbRealtimeTransport
  private manuallyClosed = false
  private lastSeenLsn = 0
  private objectSeenLsn = 0
  private readonly roomSeenLsn = new Map<string, number>()
  private readonly userSeenLsn = new Map<string, number>()
  private readonly tableSeenLsn = new Map<string, number>()
  private readonly tableSeenEventIds = new Map<string, { lsn: number; ids: Set<string> }>()
  private readonly tableCaughtUpLsn = new Map<string, number>()
  private readonly tableAppliedEventIds = new Map<string, Map<string, number>>()
  private readonly nestedTableSeenLsn = new Map<string, number>()
  private readonly nestedTableSeenEventIds = new Map<string, { lsn: number; ids: Set<string> }>()
  private readonly nestedTableCaughtUpLsn = new Map<string, number>()
  private readonly nestedTableAppliedEventIds = new Map<string, Map<string, number>>()
  private recoverPromise?: Promise<void>
  private cursorHydratePromise?: Promise<void>
  private cacheControlPromise?: Promise<ClientCacheProfileResponse>
  private clientCacheProfile?: ClientCacheProfile
  private readonly nestedOrderCache = new Map<string, Promise<RecordOrderTerm[] | undefined>>()
  private readonly recordIndexCache = new Map<string, Promise<string[] | undefined>>()
  private readonly volatileRecordOverlays = new Set<string>()
  private readonly roomListeners = new Map<string, Set<(event: RoomDeliveryEvent) => void>>()
  private readonly tableListeners = new Map<string, Set<{
    listener: (event: TableDeliveryEvent) => void
    options: SubscriptionOptions
  }>>()
  private readonly queryListeners = new Map<string, Set<(event: RecordLiveQueryResult) => void>>()
  private readonly querySubscriptions = new Map<string, Extract<ClientFrame, { type: "subscribeQuery" }>>()
  private readonly queryResults = new Map<string, ListRecordsResponse>()
  private readonly userListeners = new Set<(event: UserDeliveryEvent) => void>()
  private readonly objectListeners = new Set<(event: ObjectDeliveryEvent) => void>()
  private readonly connectionEventListeners = new Set<(event: ConnectionEvent) => void>()
  private readonly aggregateCountListeners = new Map<string, Set<(event: AggregateCountEvent) => void>>()
  private readonly aggregateSumListeners = new Map<string, Set<(event: AggregateSumEvent) => void>>()
  private readonly aggregatePresenceListeners = new Map<string, Set<(event: AggregatePresenceEvent) => void>>()
  private readonly cacheChangeListeners = new Set<(event: NextDbCacheChange) => void>()
  private cachedClusterTopology?: ClusterTopology
  private clusterTopologyPromise?: Promise<ClusterTopology>
  private readonly realtimeChannelStates = new Map<string, RealtimeChannelStateSnapshot>()
  private readonly realtimeChannelMemberSnapshots = new Map<string, RealtimeMember[]>()
  private readonly realtimeChannelEventSnapshots = new Map<string, RealtimeChannelEvent[]>()
  private readonly realtimeChannelSignalSnapshots = new Map<string, RealtimeSignal[]>()
  private readonly connectionSessionSnapshots = new Map<string, ConnectionSession>()
  private connectionSessionsLoaded = false
  private readonly activeTableSubscriptions = new Map<string, {
    table: string
    options: SubscriptionOptions
    count: number
  }>()
  private readonly activeNestedTableSubscriptions = new Map<string, {
    table: string
    parentKey: string
    nested: string
    logicalTable: string
    options: SubscriptionOptions
    count: number
  }>()
  private readonly persistentRoomSubscriptions = new Map<string, SubscriptionOptions>()
  private readonly persistentTableSubscriptions = new Map<string, {
    table: string
    options: SubscriptionOptions
  }>()
  private readonly persistentNestedTableSubscriptions = new Map<string, {
    table: string
    parentKey: string
    nested: string
    logicalTable: string
    options: SubscriptionOptions
  }>()
  private readonly persistentQuerySubscriptions = new Map<string, Extract<ClientFrame, { type: "subscribeQuery" }>>()
  private persistentUserSubscription?: { userId: string; options: SubscriptionOptions }
  private persistentObjectSubscription?: SubscriptionOptions
  private readonly pendingRoomSubscriptions = new Map<string, { afterLsn?: number; catchUpLimit?: number }>()
  private readonly pendingTableSubscriptions = new Map<string, Extract<ClientFrame, { type: "subscribeTable" }>>()
  private readonly pendingNestedTableSubscriptions = new Map<string, Extract<ClientFrame, { type: "subscribeNestedTable" }>>()
  private readonly pendingQuerySubscriptions = new Map<string, Extract<ClientFrame, { type: "subscribeQuery" }>>()
  private pendingUserSubscription?: { afterLsn?: number; catchUpLimit?: number }
  private pendingObjectSubscription?: { afterLsn?: number; catchUpLimit?: number }
  private readonly pendingAggregateCountSubscriptions = new Set<string>()
  private readonly pendingAggregateSumSubscriptions = new Set<string>()
  private readonly pendingAggregatePresenceSubscriptions = new Set<string>()
  private readonly joinedRealtimeChannels = new Map<string, { metadata: unknown }>()
  private userSubscriptionActive = false
  private objectSubscriptionActive = false
  private connectionEventsSubscriptionActive = false
  private pendingWriteClock = 0
  private pendingWriteAutoFlush?: Required<PendingWriteAutoFlushOptions>
  private pendingWriteFlushTimer?: ReturnType<typeof setTimeout>
  private pendingWriteFlushPromise?: Promise<FlushPendingWritesResult>
  private readonly pendingSendMessageBatches = new Map<string, PendingSendMessageBatch>()

  constructor(options: NextDbClientOptions) {
    this.initialEndpoint = options.endpoint.replace(/\/$/, "")
    this.activeEndpoint = this.initialEndpoint
    this.explicitWsEndpoint = options.wsEndpoint?.replace(/\/$/, "")
    this.knownEndpoints = uniqueEndpoints([
      this.initialEndpoint,
      ...(options.replicaEndpoints ?? []),
    ])
    this.clientId = options.clientId ?? nextClientId("client")
    this.authToken = options.authToken
    this.adminToken = options.adminToken
    this.userId = options.userId
    this.sessionId = options.sessionId
    this.schemaVersion = options.schemaVersion
    this.connectionMetadata = options.connectionMetadata
    const cacheNamespace = normalizeCacheNamespace(options.cacheNamespace)
    if (options.cache !== undefined) {
      this.cache = options.cache
      this.cacheScope = {
        kind: "custom",
        namespace: cacheNamespace,
        endpoint: this.initialEndpoint,
        userId: this.userId,
      }
    } else if (hasIndexedDb()) {
      const name = defaultIndexedDbCacheName(this.initialEndpoint, this.userId, cacheNamespace)
      this.cache = new IndexedDbLocalCache(name)
      this.cacheScope = {
        kind: "indexedDb",
        namespace: cacheNamespace,
        name,
        endpoint: this.initialEndpoint,
        userId: this.userId,
      }
    } else {
      this.cache = new MemoryLocalCache()
      this.cacheScope = {
        kind: "memory",
        namespace: cacheNamespace,
        endpoint: this.initialEndpoint,
        userId: this.userId,
      }
    }
    this.configuredRealtimeTransportKind = options.realtimeTransportKind ?? (options.realtimeTransport ? "custom" : "websocket")
    this.configuredRealtimeTransportFactory = options.realtimeTransport ?? defaultRealtimeTransportFactory(
      this.configuredRealtimeTransportKind === "custom" ? "websocket" : this.configuredRealtimeTransportKind,
      options.webTransportOptions,
    )
    this.realtimeTransportKind = this.configuredRealtimeTransportKind
    this.realtimeTransportFactory = this.configuredRealtimeTransportFactory
    this.offlineWrites = options.offlineWrites ?? false
    this.pendingWriteAutoFlush = normalizePendingWriteAutoFlush(options.autoFlushPendingWrites)
    if (this.pendingWriteAutoFlush.enabled && this.pendingWriteAutoFlush.retryOnStart) {
      this.schedulePendingWriteFlush(0)
    }
    if (options.autoRestoreSubscriptions) {
      void this.restoreSubscriptions().catch((error) => {
        console.error("nextdb subscription auto-restore failed", error)
      })
    }
  }

  room(roomId: string): RoomHandle {
    return new RoomHandle(this, roomId)
  }

  table(table: string): TableHandle {
    return new TableHandle(this, table)
  }

  nestedTable(table: string, parentKey: string, nested: string): NestedTableHandle {
    return new NestedTableHandle(this, table, parentKey, nested)
  }

  objectStore(objectName: string): ObjectStoreHandle {
    return new ObjectStoreHandle(this, objectName)
  }

  withSchemaVersion(schemaVersion: number): this {
    if (!Number.isInteger(schemaVersion) || schemaVersion < 0) {
      throw new Error("schemaVersion must be a non-negative integer")
    }
    this.schemaVersion = schemaVersion
    return this
  }

  onCacheChange(listener: (event: NextDbCacheChange) => void): () => void {
    this.cacheChangeListeners.add(listener)
    return () => {
      this.cacheChangeListeners.delete(listener)
    }
  }

  async sendMessage(
    roomId: string,
    body: string,
    optionsOrDurability: SendMessageOptions | Durability = "strict",
  ): Promise<NextDbMessage> {
    const userId = this.userId
    if (!userId) {
      throw new Error("sendMessage requires userId in NextDbClientOptions")
    }

    const options =
      typeof optionsOrDurability === "string"
        ? { durability: optionsOrDurability }
        : optionsOrDurability

    const request = {
      roomId,
      userId,
      body,
      attachments: options.attachments ?? [],
      durability: options.durability ?? "strict",
      clientMutationId: options.clientMutationId ?? nextClientId("mutation"),
    }

    try {
      if (request.attachments.length === 0 && request.body.trim().length > 0) {
        return await this.commitSendMessageBatched(request)
      }
      return await this.commitSendMessage(request)
    } catch (error) {
      if (request.durability === "volatile" || !this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingMessage({
        ...request,
        durability: request.durability,
      }, error)
    }
  }

  async sendMessages(
    roomId: string,
    messagesOrBodies: Array<string | SendMessagesItem>,
    optionsOrDurability: SendMessagesOptions | Durability = "strict",
  ): Promise<NextDbMessage[]> {
    const userId = this.userId
    if (!userId) {
      throw new Error("sendMessages requires userId in NextDbClientOptions")
    }

    const options =
      typeof optionsOrDurability === "string"
        ? { durability: optionsOrDurability }
        : optionsOrDurability

    return this.commitSendMessages({
      roomId,
      userId,
      messages: messagesOrBodies.map((item) =>
        typeof item === "string"
          ? { body: item, attachments: [] }
          : {
            body: item.body,
            attachments: item.attachments ?? [],
            clientMutationId: item.clientMutationId,
          }),
      durability: options.durability ?? "strict",
    })
  }

  async publishVolatile(roomId: string, name: string, payload: unknown): Promise<VolatilePublishResponse> {
    return this.post<VolatilePublishResponse>("/v1/mutate", {
      type: "publishVolatile",
      roomId,
      name,
      payload,
    })
  }

  async publishUserVolatile(userId: string, name: string, payload: unknown): Promise<VolatilePublishResponse> {
    return this.post<VolatilePublishResponse>("/v1/mutate", {
      type: "publishUserVolatile",
      userId,
      name,
      payload,
    })
  }

  async publishUserEvent(
    userId: string,
    name: string,
    payload: unknown,
    optionsOrDurability: PublishUserEventOptions | Exclude<Durability, "volatile"> = "strict",
  ): Promise<NextDbUserEvent> {
    const options = typeof optionsOrDurability === "string"
      ? { durability: optionsOrDurability }
      : optionsOrDurability
    const request = {
      userId,
      name,
      payload,
      durability: options.durability ?? "strict",
      clientMutationId: options.clientMutationId ?? nextClientId("mutation"),
    }
    try {
      return await this.commitUserEvent(request)
    } catch (error) {
      if (!this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingUserEvent(request, error)
    }
  }

  async listUserEvents(
    userId: string,
    options: UserEventsListOptions = {},
  ): Promise<NextDbUserEvent[]> {
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    if (requiresReadQuorum(options)) {
      const response = await this.readUserEventsWithQuorum(userId, limit, options.beforeLsn, options)
      await this.putUserEventsCached(userId, response.events)
      response.events.forEach((event) => this.advanceUserEvent(event))
      return response.events
    }
    if (options.sync !== false && options.beforeLsn === undefined) {
      await this.hydrateCursorsFor({ users: [userId] })
      await this.syncUntilCaughtUp({
        afterLsn: await this.minimumSeenUserLsn([userId]),
        users: [userId],
        limit,
      })
    }
    const cached = await this.cache.getUserEvents(userId, limit, options.beforeLsn)
    if (
      cached.length >= limit
      && cached.every((event) => freshnessSatisfied(options, event.lsn))
    ) {
      return cached
    }
    if (options.sync === false && options.minLsn === undefined) {
      return cached
    }
    await this.ensureFreshness(options, userId)
    const response = await this.fetchUserEvents(userId, limit, options.beforeLsn)
    await this.putUserEventsCached(userId, response.events)
    response.events.forEach((event) => this.advanceUserEvent(event))
    return response.events
  }

  async listCachedUserEvents(
    userId: string,
    options: ListCachedUserEventsOptions = {},
  ): Promise<NextDbUserEvent[]> {
    const limit = normalizePageLimit(options.limit)
    return (await this.cache.getUserEvents(userId, limit, options.beforeLsn)).slice(0, limit)
  }

  async listCurrentUserEvents(
    options: UserEventsListOptions = {},
  ): Promise<NextDbUserEvent[]> {
    return this.listUserEvents(this.requireUserId("listCurrentUserEvents"), options)
  }

  async listCachedCurrentUserEvents(
    options: ListCachedUserEventsOptions = {},
  ): Promise<NextDbUserEvent[]> {
    return this.listCachedUserEvents(this.requireUserId("listCachedCurrentUserEvents"), options)
  }

  async getCachedUser(userId = this.requireUserId("getCachedUser")): Promise<NextDbUserProfile | undefined> {
    return this.cache.getUserProfile(userId)
  }

  async getUser(userId = this.requireUserId("getUser"), options: FreshnessOptions = {}): Promise<NextDbUserProfile> {
    await this.reconcileCacheControl()
    if (requiresReadQuorum(options)) {
      const user = await this.readUserWithQuorum(userId, options)
      await this.putUserProfileCached(user, "sync")
      return user
    }
    await this.ensureFreshness(options, userId)
    const cached = await this.cache.getUserProfile(userId)
    if (cached !== undefined && freshnessSatisfied(options, cached.lsn)) {
      return cached
    }
    const response = await this.get<UserResponse>(`/v1/users/${encodeURIComponent(userId)}`)
    await this.putUserProfileCached(response.user, "sync")
    return response.user
  }

  async upsertUser(
    userId = this.requireUserId("upsertUser"),
    profile: UpsertUserOptions = {},
  ): Promise<NextDbUserProfile> {
    const request = {
      userId,
      displayName: profile.displayName,
      metadata: profile.metadata ?? {},
      clientMutationId: profile.clientMutationId ?? nextClientId("mutation"),
    }
    try {
      return await this.commitUserProfileUpsert(request)
    } catch (error) {
      if (!this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingUserProfileUpsert(request, error)
    }
  }

  async listUsers(options: ListUsersOptions = {}): Promise<ListUsersResponse> {
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    const params = new URLSearchParams()
    params.set("limit", String(limit))
    if (options.afterUserId !== undefined) {
      params.set("afterUserId", options.afterUserId)
    }
    if (requiresReadQuorum(options)) {
      const response = await this.readUserListWithQuorum(params, limit, options)
      await this.putUsersFromListResponse(response)
      return response
    }
    await this.ensureFreshness(options)
    const cached = await this.cache.listUserProfiles(limit, options.afterUserId)
    if (
      cached.length >= limit
      && cached.every((user) => freshnessSatisfied(options, user.lsn))
    ) {
      return {
        users: cached,
        nextAfterUserId: cached.at(-1)?.userId,
        hasMore: false,
      }
    }
    const suffix = params.size > 0 ? `?${params}` : ""
    const response = await this.get<ListUsersResponse>(`/v1/admin/users${suffix}`)
    await this.putUsersFromListResponse(response)
    return response
  }

  async listCachedUsers(options: ListCachedUsersOptions = {}): Promise<ListUsersResponse> {
    const limit = normalizePageLimit(options.limit)
    const users = await this.cache.listUserProfiles(limit + 1, options.afterUserId)
    const page = users.slice(0, limit)
    return {
      users: page,
      nextAfterUserId: page.at(-1)?.userId,
      hasMore: users.length > limit,
    }
  }

  private async putUsersFromListResponse(response: ListUsersResponse): Promise<void> {
    for (const user of response.users) {
      await this.putUserProfileCached(user, "sync")
    }
  }

  realtimeChannel(channelId: string): RealtimeChannelHandle {
    return new RealtimeChannelHandle(this, channelId)
  }

  async joinRealtimeChannel(channelId: string, metadata: unknown = {}): Promise<RealtimeJoinResponse> {
    this.requireUserId("joinRealtimeChannel")
    await this.ensureRealtimeTransportOpen()
    const response = await this.postRealtimeChannelJoin(channelId, metadata, true)
    this.joinedRealtimeChannels.set(channelId, { metadata })
    this.rememberRealtimeChannelMembers(response.channelId, response.members, "mutation")
    this.refreshRealtimeChannelState(channelId).catch((error) => {
      console.error(`nextdb realtime channel ${channelId} state hydrate failed`, error)
    })
    return response
  }

  async leaveRealtimeChannel(channelId: string): Promise<RealtimeLeaveResponse> {
    const userId = this.requireUserId("leaveRealtimeChannel")
    const response = await this.post<RealtimeLeaveResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/leave`, {
      userId,
      sessionId: this.sessionId,
    })
    this.joinedRealtimeChannels.delete(channelId)
    this.forgetRealtimeChannelState(channelId, "manual")
    this.forgetRealtimeChannelMembers(channelId, "manual")
    this.forgetRealtimeChannelEvents(channelId, "manual")
    this.forgetRealtimeChannelSignals(channelId, "manual")
    return response
  }

  async updateRealtimePresence(channelId: string, metadata: unknown): Promise<RealtimePresenceUpdateResponse> {
    const userId = this.requireUserId("updateRealtimePresence")
    const response = await this.post<RealtimePresenceUpdateResponse>(
      `/v1/realtime/channels/${encodeURIComponent(channelId)}/presence`,
      {
        userId,
        sessionId: this.sessionId,
        metadata,
      },
    )
    if (this.joinedRealtimeChannels.has(channelId)) {
      this.joinedRealtimeChannels.set(channelId, { metadata })
    }
    this.rememberRealtimeChannelMembers(response.channelId, response.members, "mutation")
    return response
  }

  async realtimeChannelMembers(channelId: string): Promise<RealtimeMembersResponse> {
    const response = await this.get<RealtimeMembersResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/members`)
    this.rememberRealtimeChannelMembers(response.channelId, response.members, "sync")
    return response
  }

  cachedRealtimeChannelMembers<TMetadata = unknown>(channelId: string): (RealtimeMembersResponse & { members: Array<RealtimeMember & { metadata: TMetadata }> }) | undefined {
    const members = this.realtimeChannelMemberSnapshots.get(channelId)
    if (!members) {
      return undefined
    }
    return {
      channelId,
      members: members as Array<RealtimeMember & { metadata: TMetadata }>,
    }
  }

  watchRealtimeChannelMembers<TMetadata = unknown>(
    channelId: string,
    listener: (snapshot: RealtimeMembersSnapshotView<TMetadata>) => void,
    options: WatchOptions = {},
  ): () => void {
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      if (closed) {
        return
      }
      listener({
        channelId,
        snapshot: this.cachedRealtimeChannelMembers<TMetadata>(channelId),
        source,
        change,
      })
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    this.refreshRealtimeChannelMembers(channelId).catch((error) => {
      console.error(`nextdb realtime channel ${channelId} members refresh failed`, error)
    })
    const stopCache = this.onCacheChange((change) => {
      if (
        (change.type === "realtimeChannelMembersUpdated" || change.type === "realtimeChannelMembersCleared") &&
        change.channelId === channelId
      ) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated") {
        emit(change.source, change)
      }
    })
    const stopUserEvents = this.userId ? this.onUserEvent(() => undefined, options) : () => undefined
    return () => {
      closed = true
      stopCache()
      stopUserEvents()
    }
  }

  async realtimeChannelState<T = unknown>(channelId: string): Promise<RealtimeChannelStateResponse<T>> {
    const response = await this.get<RealtimeChannelStateResponse<T>>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`)
    this.rememberRealtimeChannelState(response.state, "sync", true)
    return response
  }

  async updateRealtimeChannelState<T = unknown>(
    channelId: string,
    state: T,
    options: { expectedVersion?: number } = {},
  ): Promise<RealtimeChannelStateUpdateResponse<T>> {
    const fromUserId = this.requireUserId("updateRealtimeChannelState")
    const response = await this.post<RealtimeChannelStateUpdateResponse<T>>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`, {
      fromUserId,
      state,
      expectedVersion: options.expectedVersion,
    })
    this.rememberRealtimeChannelState(response.state, "mutation", false)
    return response
  }

  cachedRealtimeChannelState<T = unknown>(channelId: string): RealtimeChannelStateSnapshot<T> | undefined {
    return this.realtimeChannelStates.get(channelId) as RealtimeChannelStateSnapshot<T> | undefined
  }

  watchRealtimeChannelState<T = unknown>(
    channelId: string,
    listener: (snapshot: RealtimeChannelStateSnapshotView<T>) => void,
    options: WatchOptions = {},
  ): () => void {
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      if (closed) {
        return
      }
      listener({
        channelId,
        snapshot: this.cachedRealtimeChannelState<T>(channelId),
        source,
        change,
      })
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    this.refreshRealtimeChannelState(channelId).catch((error) => {
      console.error(`nextdb realtime channel ${channelId} state refresh failed`, error)
    })
    const stopCache = this.onCacheChange((change) => {
      if (
        (change.type === "realtimeChannelStateUpdated" || change.type === "realtimeChannelStateCleared") &&
        change.channelId === channelId
      ) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated") {
        emit(change.source, change)
      }
    })
    const stopUserEvents = this.userId ? this.onUserEvent(() => undefined, options) : () => undefined
    return () => {
      closed = true
      stopCache()
      stopUserEvents()
    }
  }

  cachedRealtimeChannelEvents(
    channelId: string,
    options: RealtimeChannelEventsOptions = {},
  ): RealtimeChannelEvent[] {
    const limit = normalizePageLimit(options.limit ?? DEFAULT_REALTIME_CHANNEL_EVENT_LIMIT)
    const events = this.realtimeChannelEventSnapshots.get(channelId) ?? []
    const filtered = options.kind === undefined
      ? events
      : events.filter((event) => event.kind === options.kind)
    return filtered.slice(-limit)
  }

  watchRealtimeChannelEvents(
    channelId: string,
    listener: (snapshot: RealtimeChannelEventsSnapshotView) => void,
    options: WatchOptions & RealtimeChannelEventsOptions = {},
  ): () => void {
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      if (closed) {
        return
      }
      listener({
        channelId,
        events: this.cachedRealtimeChannelEvents(channelId, options),
        source,
        change,
      })
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (
        (change.type === "realtimeChannelEventReceived" || change.type === "realtimeChannelEventsCleared") &&
        change.channelId === channelId
      ) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated") {
        emit(change.source, change)
      }
    })
    const stopUserEvents = this.userId ? this.onUserEvent(() => undefined, options) : () => undefined
    return () => {
      closed = true
      stopCache()
      stopUserEvents()
    }
  }

  cachedRealtimeChannelSignals(
    channelId: string,
    options: RealtimeChannelSignalsOptions = {},
  ): RealtimeSignal[] {
    const limit = normalizePageLimit(options.limit ?? DEFAULT_REALTIME_CHANNEL_EVENT_LIMIT)
    const signals = this.realtimeChannelSignalSnapshots.get(channelId) ?? []
    const filtered = options.kind === undefined
      ? signals
      : signals.filter((signal) => signal.kind === options.kind)
    return filtered.slice(-limit)
  }

  watchRealtimeChannelSignals(
    channelId: string,
    listener: (snapshot: RealtimeChannelSignalsSnapshotView) => void,
    options: WatchOptions & RealtimeChannelSignalsOptions = {},
  ): () => void {
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      if (closed) {
        return
      }
      listener({
        channelId,
        signals: this.cachedRealtimeChannelSignals(channelId, options),
        source,
        change,
      })
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (
        (change.type === "realtimeChannelSignalReceived" || change.type === "realtimeChannelSignalsCleared") &&
        change.channelId === channelId
      ) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated") {
        emit(change.source, change)
      }
    })
    const stopUserEvents = this.userId ? this.onUserEvent(() => undefined, options) : () => undefined
    return () => {
      closed = true
      stopCache()
      stopUserEvents()
    }
  }

  async listRealtimeChannels(): Promise<RealtimeChannelListResponse> {
    return this.get<RealtimeChannelListResponse>("/v1/realtime/channels")
  }

  async listConnections(userIdOrOptions?: string | ListConnectionsOptions): Promise<ConnectionListResponse> {
    const options = typeof userIdOrOptions === "string" ? { userId: userIdOrOptions } : userIdOrOptions ?? {}
    const params = new URLSearchParams()
    if (options.userId !== undefined) {
      params.set("userId", options.userId)
    }
    if (options.transport !== undefined) {
      params.set("transport", options.transport)
    }
    const suffix = params.size > 0 ? `?${params}` : ""
    const response = await this.get<ConnectionListResponse>(`/v1/admin/connections${suffix}`)
    this.rememberConnectionList(response, options, "sync")
    return response
  }

  cachedConnections(options: ListConnectionsOptions = {}): ConnectionListResponse | undefined {
    if (!this.connectionSessionsLoaded && this.connectionSessionSnapshots.size === 0) {
      return undefined
    }
    return buildConnectionListResponse(
      [...this.connectionSessionSnapshots.values()],
      options,
    )
  }

  watchConnections(
    listener: (snapshot: ConnectionListSnapshotView) => void,
    options: WatchConnectionsOptions = {},
  ): () => void {
    let closed = false
    const listOptions: ListConnectionsOptions = {
      ...(options.userId === undefined ? {} : { userId: options.userId }),
      ...(options.transport === undefined ? {} : { transport: options.transport }),
    }
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      if (closed) {
        return
      }
      listener({
        connections: this.cachedConnections(listOptions),
        source,
        change,
      })
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    this.listConnections(listOptions).catch((error) => {
      console.error("nextdb connection watcher refresh failed", error)
    })
    const stopEvents = this.onConnectionEvent(() => undefined)
    const stopCache = this.onCacheChange((change) => {
      if (
        change.type === "connectionSessionsUpdated" ||
        change.type === "connectionSessionsCleared" ||
        change.type === "allInvalidated"
      ) {
        emit(change.source, change)
      }
    })
    return () => {
      closed = true
      stopEvents()
      stopCache()
    }
  }

  async disconnectConnections(request: ConnectionDisconnectRequest): Promise<ConnectionDisconnectResponse> {
    return this.post<ConnectionDisconnectResponse>("/v1/admin/connections/disconnect", request)
  }

  async sendRealtimeSignal(
    channelId: string,
    toUserId: string,
    kind: RealtimeSignal["kind"],
    payload: unknown,
  ): Promise<RealtimeSignalResponse> {
    const fromUserId = this.requireUserId("sendRealtimeSignal")
    return this.post<RealtimeSignalResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/signal`, {
      fromUserId,
      toUserId,
      kind,
      payload,
    })
  }

  async broadcastRealtimeEvent(
    channelId: string,
    kind: RealtimeChannelEvent["kind"],
    payload: unknown,
    options: { includeSelf?: boolean } = {},
  ): Promise<RealtimeBroadcastResponse> {
    const fromUserId = this.requireUserId("broadcastRealtimeEvent")
    return this.post<RealtimeBroadcastResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/broadcast`, {
      fromUserId,
      kind,
      payload,
      includeSelf: options.includeSelf,
    })
  }

  async putObject(
    body: Blob | ArrayBuffer | Uint8Array | string,
    contentTypeOrOptions: string | PutObjectOptions = "application/octet-stream",
  ): Promise<NextDbObjectMetadata> {
    const options = typeof contentTypeOrOptions === "string"
      ? { contentType: contentTypeOrOptions }
      : contentTypeOrOptions
    const contentType = options.contentType ?? "application/octet-stream"
    const payload = normalizeObjectBody(body)
    const cachedBody = objectBodyBlob(body, contentType)
    const objectId = options.objectId ?? nextObjectId()
    const clientMutationId = options.clientMutationId ?? (this.offlineWrites ? nextClientId("mutation") : undefined)
    const params = new URLSearchParams({
      contentType,
      objectId,
    })
    if (clientMutationId !== undefined) {
      params.set("clientMutationId", clientMutationId)
    }
    let metadata: NextDbObjectMetadata
    try {
      metadata = await this.putObjectWithRetry(params, payload, contentType)
    } catch (error) {
      if (!this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingObjectPut(objectId, cachedBody, contentType, clientMutationId ?? nextClientId("mutation"), error)
    }
    const cacheableBody = await objectBodyMatchesMetadata(cachedBody, metadata)
    await this.putObjectCached(metadata, cacheableBody ? cachedBody : undefined)
    this.emitCacheChange({
      type: "objectUpserted",
      source: "mutation",
      objectId: metadata.id,
      metadata,
    })
    return metadata
  }

  private async putObjectTo(
    endpoint: string,
    params: URLSearchParams,
    payload: BodyInit,
    contentType: string,
  ): Promise<NextDbObjectMetadata> {
    const response = await fetch(`${endpoint}/v1/objects?${params}`, {
      method: "POST",
      headers: {
        "content-type": contentType,
        ...this.authHeaders(),
      },
      body: payload,
    })
    return parseResponse<NextDbObjectMetadata>(response)
  }

  private async putObjectWithRetry(
    params: URLSearchParams,
    payload: BodyInit,
    contentType: string,
  ): Promise<NextDbObjectMetadata> {
    try {
      return await this.putObjectTo(this.activeEndpoint, params, payload, contentType)
    } catch (error) {
      const ownerEndpoint = ownerRetryEndpoint(error)
      if (ownerEndpoint && !sameEndpoint(ownerEndpoint, this.activeEndpoint)) {
        return this.putObjectTo(ownerEndpoint, params, payload, contentType)
      }
      const drainEndpoint = await this.drainingRetryEndpoint(error)
      if (!drainEndpoint) {
        throw error
      }
      return this.putObjectTo(drainEndpoint, params, payload, contentType)
    }
  }

  async getObjectMetadata(objectId: string, options: FreshnessOptions = {}): Promise<NextDbObjectMetadata> {
    await this.reconcileCacheControl()
    if (requiresReadQuorum(options)) {
      const metadata = await this.readObjectMetadataWithQuorum(objectId, options)
      await this.putObjectCached(metadata)
      this.emitCacheChange({
        type: "objectUpserted",
        source: "sync",
        objectId: metadata.id,
        metadata,
      })
      return metadata
    }
    await this.ensureFreshness(options, objectId)
    const cached = await this.cache.getObjectMetadata(objectId)
    if (cached !== undefined && freshnessSatisfied(options, undefined)) {
      return cached
    }
    const metadata = await this.get<NextDbObjectMetadata>(`/v1/objects/${encodeURIComponent(objectId)}/metadata`)
    await this.putObjectCached(metadata)
    this.emitCacheChange({
      type: "objectUpserted",
      source: "sync",
      objectId: metadata.id,
      metadata,
    })
    return metadata
  }

  async getCachedObjectMetadata(objectId: string): Promise<NextDbObjectMetadata | undefined> {
    return this.cache.getObjectMetadata(objectId)
  }

  async getObjectBody(objectId: string, options: FreshnessOptions = {}): Promise<Blob> {
    await this.reconcileCacheControl()
    if (requiresReadQuorum(options)) {
      const metadata = await this.readObjectMetadataWithQuorum(objectId, options)
      const body = await this.readObjectBodyWithQuorum(objectId, metadata, options)
      await this.putObjectCached(metadata, body)
      this.emitCacheChange({
        type: "objectUpserted",
        source: "sync",
        objectId: metadata.id,
        metadata,
      })
      return body
    }
    await this.ensureFreshness(options, objectId)
    const cached = await this.cache.getObjectBody(objectId)
    if (cached !== undefined && freshnessSatisfied(options, undefined)) {
      return cached
    }
    const response = await fetch(`${this.activeEndpoint}/v1/objects/${encodeURIComponent(objectId)}/body`, {
      headers: this.authHeaders(),
    })
    if (!response.ok) {
      const payload = await response.json().catch(() => undefined)
      throw new Error(payload?.error ?? `NextDB object request failed with ${response.status}`)
    }
    const body = await response.blob()
    const metadata = await this.getObjectMetadata(objectId, options)
    await this.putObjectCached(metadata, body)
    this.emitCacheChange({
      type: "objectUpserted",
      source: "sync",
      objectId: metadata.id,
      metadata,
    })
    return body
  }

  async getCachedObjectBody(objectId: string): Promise<Blob | undefined> {
    return this.cache.getObjectBody(objectId)
  }

  async getObjectBodyRange(objectId: string, options: ObjectBodyRangeOptions): Promise<ObjectBodyRangeResponse> {
    await this.reconcileCacheControl()
    const range = objectRangeHeader(options)
    if (requiresReadQuorum(options)) {
      const metadata = await this.readObjectMetadataWithQuorum(objectId, options)
      return this.readObjectBodyRangeWithQuorum(objectId, metadata, options)
    }
    await this.ensureFreshness(options, objectId)
    if (freshnessSatisfied(options, undefined)) {
      const cached = await this.cachedObjectBodyRange(objectId, options)
      if (cached !== undefined) {
        return cached
      }
    }
    const response = await this.fetchObjectBodyRangeFrom(
      this.activeEndpoint,
      `/v1/objects/${encodeURIComponent(objectId)}/body`,
      range,
    )
    await this.putObjectBodyRangeCached(objectId, response, options)
    return response
  }

  async getObjectReferences(objectId: string): Promise<ObjectReferences> {
    return this.get<ObjectReferences>(`/v1/objects/${encodeURIComponent(objectId)}/references`)
  }

  async deleteObject(objectId: string, options: DeleteObjectOptions = {}): Promise<DeleteObjectResponse> {
    const params = new URLSearchParams()
    if (options.force !== undefined) {
      params.set("force", String(options.force))
    }
    const clientMutationId = options.clientMutationId ?? (this.offlineWrites ? nextClientId("mutation") : undefined)
    if (clientMutationId !== undefined) {
      params.set("clientMutationId", clientMutationId)
    }
    const suffix = params.size > 0 ? `?${params}` : ""
    let response: DeleteObjectResponse
    try {
      response = await this.deleteOwnerAware<DeleteObjectResponse>(
        `/v1/objects/${encodeURIComponent(objectId)}${suffix}`,
      )
    } catch (error) {
      if (!this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingObjectDelete(objectId, options.force, clientMutationId ?? nextClientId("mutation"), error)
    }
    await this.cache.deleteObject(objectId)
    this.emitCacheChange({
      type: "objectDeleted",
      source: "mutation",
      objectId,
    })
    return response
  }

  async listObjects(options: ListObjectsOptions = {}): Promise<ListObjectsResponse> {
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    const params = new URLSearchParams()
    params.set("limit", String(limit))
    if (options.afterId !== undefined) {
      params.set("afterId", options.afterId)
    }
    if (requiresReadQuorum(options)) {
      const response = await this.readObjectListWithQuorum(params, limit, options)
      await this.putObjectsFromListResponse(response)
      return response
    }
    await this.ensureFreshness(options)
    const cached = await this.cache.listObjects(limit, options.afterId)
    if (cached.length >= limit && freshnessSatisfied(options, undefined)) {
      return {
        objects: cached,
        nextAfterId: cached.at(-1)?.id,
        hasMore: false,
      }
    }
    const suffix = params.size > 0 ? `?${params}` : ""
    const response = await this.get<ListObjectsResponse>(`/v1/objects${suffix}`)
    await this.putObjectsFromListResponse(response)
    return response
  }

  async listCachedObjects(options: ListCachedObjectsOptions = {}): Promise<ListObjectsResponse> {
    const limit = normalizePageLimit(options.limit)
    const objects = await this.cache.listObjects(limit + 1, options.afterId)
    const page = objects.slice(0, limit)
    return {
      objects: page,
      nextAfterId: page.at(-1)?.id,
      hasMore: objects.length > limit,
    }
  }

  private async putObjectsFromListResponse(response: ListObjectsResponse): Promise<void> {
    for (const object of response.objects) {
      await this.putObjectCached(object)
    }
    for (const object of response.objects) {
      this.emitCacheChange({
        type: "objectUpserted",
        source: "sync",
        objectId: object.id,
        metadata: object,
      })
    }
  }

  async gcObjects(options: { dryRun?: boolean; force?: boolean; graceMs?: number } = {}): Promise<ObjectGcResponse> {
    const params = new URLSearchParams({
      dryRun: String(options.dryRun ?? true),
    })
    if (options.force !== undefined) {
      params.set("force", String(options.force))
    }
    if (options.graceMs !== undefined) {
      params.set("graceMs", String(options.graceMs))
    }
    return this.post<ObjectGcResponse>(`/v1/admin/objects/gc?${params}`, {})
  }

  async listBehaviors(): Promise<BehaviorManifest[]> {
    return this.get<BehaviorManifest[]>("/v1/behaviors")
  }

  async reloadBehaviors(): Promise<BehaviorReloadResponse> {
    return this.post<BehaviorReloadResponse>("/v1/admin/behaviors/reload", {})
  }

  async invokeBehavior(request: Omit<BehaviorInvokeRequest, "userId"> & { userId?: string }): Promise<BehaviorInvokeResponse> {
    return this.post<BehaviorInvokeResponse>("/v1/behaviors/invoke", {
      ...request,
      userId: request.userId ?? this.userId,
    })
  }

  async getSchema(): Promise<NextDbSchema> {
    return this.get<NextDbSchema>("/v1/schema")
  }

  async schemaHistory(): Promise<SchemaHistoryResponse> {
    return this.get<SchemaHistoryResponse>("/v1/schema/history")
  }

  async getSchemaVersion(version: number): Promise<NextDbSchema> {
    return this.get<NextDbSchema>(`/v1/schema/history/${encodeURIComponent(String(version))}`)
  }

  private nestedSchemaOrder(table: string, nested: string): Promise<RecordOrderTerm[] | undefined> {
    const cacheKey = `${table}.${nested}`
    let promise = this.nestedOrderCache.get(cacheKey)
    if (promise === undefined) {
      promise = this.getSchema()
        .then((schema) => parseNestedSchemaOrder(schema, table, nested))
        .catch(() => undefined)
      this.nestedOrderCache.set(cacheKey, promise)
    }
    return promise
  }

  private recordIndexFields(table: string, indexName: string): Promise<string[] | undefined> {
    const cacheKey = `${table}:${indexName}`
    let promise = this.recordIndexCache.get(cacheKey)
    if (promise === undefined) {
      promise = this.getSchema()
        .then((schema) => parseRecordIndexFields(schema, table, indexName))
        .catch(() => undefined)
      this.recordIndexCache.set(cacheKey, promise)
    }
    return promise
  }

  async generateTypescriptSchema(): Promise<string> {
    const response = await this.get<SchemaTypescriptResponse>("/v1/schema/typescript")
    return response.typescript
  }

  async validateSchema(): Promise<SchemaValidationReport> {
    return this.get<SchemaValidationReport>("/v1/schema/validate")
  }

  async schemaMigrationPlan(): Promise<SchemaMigrationPlan> {
    return this.get<SchemaMigrationPlan>("/v1/schema/migration-plan")
  }

  async getStoragePolicy(): Promise<SchemaStoragePolicyResponse> {
    return this.get<SchemaStoragePolicyResponse>("/v1/schema/storage-policy")
  }

  async getProjectionStatus(): Promise<RecordProjectionStatus> {
    return this.get<RecordProjectionStatus>("/v1/admin/projections/status")
  }

  async reloadSchema(): Promise<SchemaReloadResponse> {
    return this.post<SchemaReloadResponse>("/v1/admin/schema/reload", {})
  }

  async schemaReplayApplyStatus(): Promise<SchemaReplayApplyStatus> {
    return this.get<SchemaReplayApplyStatus>("/v1/admin/schema/replay/status")
  }

  async retrySchemaReplayApply(): Promise<SchemaApplyResponse> {
    return this.post<SchemaApplyResponse>("/v1/admin/schema/replay/retry", {})
  }

  async resumeSchemaReplayApply(): Promise<SchemaApplyResponse> {
    return this.post<SchemaApplyResponse>("/v1/admin/schema/replay/resume", {})
  }

  async cancelSchemaReplayApply(): Promise<SchemaReplayApplyStatus> {
    return this.post<SchemaReplayApplyStatus>("/v1/admin/schema/replay/cancel", {})
  }

  async applySchema(schema: NextDbSchema, options: { dryRun?: boolean; expectedVersion?: number; allowBreakingReplay?: boolean; backgroundReplay?: boolean } = {}): Promise<SchemaApplyResponse> {
    return this.post<SchemaApplyResponse>("/v1/admin/schema/apply", {
      schema,
      dryRun: options.dryRun ?? false,
      expectedVersion: options.expectedVersion,
      allowBreakingReplay: options.allowBreakingReplay ?? false,
      backgroundReplay: options.backgroundReplay ?? false,
    })
  }

  async schemaProposals(): Promise<SchemaProposalListResponse> {
    return this.get<SchemaProposalListResponse>("/v1/admin/schema/proposals")
  }

  async startSchemaProposal(
    schema: NextDbSchema,
    options: { expectedVersion?: number; reason?: string; allowBreakingReplay?: boolean } = {},
  ): Promise<SchemaProposalResponse> {
    return this.post<SchemaProposalResponse>("/v1/admin/schema/proposals", {
      schema,
      expectedVersion: options.expectedVersion,
      allowBreakingReplay: options.allowBreakingReplay ?? false,
      reason: options.reason,
    })
  }

  async commitSchemaProposal(proposalId: string): Promise<SchemaProposalResponse> {
    return this.post<SchemaProposalResponse>(
      `/v1/admin/schema/proposals/${encodeURIComponent(proposalId)}/commit`,
      {},
    )
  }

  async abortSchemaProposal(proposalId: string): Promise<SchemaProposalResponse> {
    return this.post<SchemaProposalResponse>(
      `/v1/admin/schema/proposals/${encodeURIComponent(proposalId)}/abort`,
      {},
    )
  }

  async createSnapshot(): Promise<AdminSnapshotResponse> {
    return this.post<AdminSnapshotResponse>("/v1/admin/snapshot", {})
  }

  async runtimeActivationStatus(): Promise<RuntimeActivationStatusResponse> {
    return this.get<RuntimeActivationStatusResponse>("/v1/admin/runtime/activation")
  }

  async activateRuntimeRecords(options: RuntimeRecordActivationOptions): Promise<RuntimeRecordActivationResponse> {
    return this.post<RuntimeRecordActivationResponse>("/v1/admin/runtime/activate-records", options)
  }

  async activateRuntimeActor(options: RuntimeActorActivationOptions): Promise<RuntimeActorActivationResponse> {
    return this.post<RuntimeActorActivationResponse>("/v1/admin/runtime/activate-actor", options)
  }

  async scheduleActorReminder(options: RuntimeActorReminderScheduleOptions): Promise<RuntimeActorReminderMutationResponse> {
    return this.post<RuntimeActorReminderMutationResponse>("/v1/admin/runtime/reminders", options)
  }

  async cancelActorReminder(options: RuntimeActorReminderCancelOptions): Promise<RuntimeActorReminderCancelResponse> {
    return this.post<RuntimeActorReminderCancelResponse>("/v1/admin/runtime/reminders/cancel", options)
  }

  async runDueActorReminders(options: RuntimeActorReminderRunDueOptions = {}): Promise<RuntimeActorReminderRunDueResponse> {
    return this.post<RuntimeActorReminderRunDueResponse>("/v1/admin/runtime/reminders/run-due", options)
  }

  async evictRuntimeRecords(options: RuntimeRecordActivationOptions): Promise<RuntimeRecordActivationResponse> {
    return this.post<RuntimeRecordActivationResponse>("/v1/admin/runtime/evict-records", options)
  }

  async activateRuntimeRoom(options: RuntimeRoomActivationOptions): Promise<RuntimeRoomActivationResponse> {
    return this.post<RuntimeRoomActivationResponse>("/v1/admin/runtime/activate-room", options)
  }

  async evictRuntimeRoom(options: RuntimeRoomActivationOptions): Promise<RuntimeRoomActivationResponse> {
    return this.post<RuntimeRoomActivationResponse>("/v1/admin/runtime/evict-room", options)
  }

  async exportManifest(options: ExportManifestOptions = {}): Promise<ExportManifestResponse> {
    const params = new URLSearchParams()
    if (options.includeSamples !== undefined) {
      params.set("includeSamples", String(options.includeSamples))
    }
    if (options.sampleLimit !== undefined) {
      params.set("sampleLimit", String(options.sampleLimit))
    }
    if (options.baseLsn !== undefined) {
      params.set("baseLsn", String(options.baseLsn))
    }
    const suffix = params.size > 0 ? `?${params}` : ""
    return this.get<ExportManifestResponse>(`/v1/admin/export/manifest${suffix}`)
  }

  async createExportBundle(options: ExportBundleAccessOptions = {}): Promise<ExportBundleResponse> {
    return this.post<ExportBundleResponse>("/v1/admin/export/bundle", {
      encryptionKey: options.encryptionKey,
      baseLsn: options.baseLsn,
    })
  }

  async runExportBackup(options: ExportBackupRunOptions = {}): Promise<ExportBackupRunResponse> {
    return this.post<ExportBackupRunResponse>("/v1/admin/export/backup/run", {
      encryptionKey: options.encryptionKey,
      forceFull: options.forceFull,
      archiveObject: options.archiveObject,
      objectId: options.objectId,
      clientMutationId: options.clientMutationId,
    })
  }

  async listExportBackupRuns(): Promise<ExportBackupRunListResponse> {
    return this.get<ExportBackupRunListResponse>("/v1/admin/export/backup/runs")
  }

  async getExportBackupPolicy(): Promise<ExportBackupPolicyResponse> {
    return this.get<ExportBackupPolicyResponse>("/v1/admin/export/backup/policy")
  }

  async setExportBackupPolicy(policy: ExportBackupPolicy): Promise<ExportBackupPolicyResponse> {
    return this.post<ExportBackupPolicyResponse>("/v1/admin/export/backup/policy", policy)
  }

  async runExportBackupPolicy(): Promise<ExportBackupPolicyRunResponse> {
    return this.post<ExportBackupPolicyRunResponse>("/v1/admin/export/backup/policy/run", {})
  }

  async retainExportBackups(options: ExportBackupRetentionOptions = {}): Promise<ExportBackupRetentionResponse> {
    return this.post<ExportBackupRetentionResponse>("/v1/admin/export/backup/retention", {
      dryRun: options.dryRun,
      keepLast: options.keepLast,
      beforeTimestampMs: options.beforeTimestampMs,
      deleteBundles: options.deleteBundles,
      deleteArchiveObjects: options.deleteArchiveObjects,
    })
  }

  async listExportBundles(): Promise<ExportBundleListResponse> {
    return this.get<ExportBundleListResponse>("/v1/admin/export/bundles")
  }

  async verifyExportBundle(bundleId: string, options: ExportBundleAccessOptions = {}): Promise<ExportBundleVerifyResponse> {
    return this.post<ExportBundleVerifyResponse>(
      `/v1/admin/export/bundles/${encodeURIComponent(bundleId)}/verify`,
      { encryptionKey: options.encryptionKey },
    )
  }

  async verifyExportBundleChain(
    bundleIds: string[],
    options: ExportBundleChainVerifyOptions = {},
  ): Promise<ExportBundleChainVerifyResponse> {
    return this.post<ExportBundleChainVerifyResponse>("/v1/admin/export/bundles/verify-chain", {
      bundleIds,
      encryptionKey: options.encryptionKey,
    })
  }

  async archiveExportBundleToObject(
    bundleId: string,
    options: ExportBundleArchiveObjectOptions = {},
  ): Promise<ExportBundleArchiveObjectResponse> {
    return this.post<ExportBundleArchiveObjectResponse>(
      `/v1/admin/export/bundles/${encodeURIComponent(bundleId)}/archive-object`,
      {
        objectId: options.objectId,
        clientMutationId: options.clientMutationId,
      },
    )
  }

  async importBundleFromObject(
    objectId: string,
    options: ImportBundleFromObjectOptions = {},
  ): Promise<ImportBundleFromObjectResponse> {
    return this.post<ImportBundleFromObjectResponse>(
      `/v1/admin/import/bundles/from-object/${encodeURIComponent(objectId)}`,
      {
        bundleId: options.bundleId,
        overwrite: options.overwrite,
      },
    )
  }

  async importBundlePreflight(bundleId: string, options: ExportBundleAccessOptions = {}): Promise<ImportBundlePreflightResponse> {
    return this.post<ImportBundlePreflightResponse>(
      `/v1/admin/import/bundles/${encodeURIComponent(bundleId)}/preflight`,
      { encryptionKey: options.encryptionKey },
    )
  }

  async importBundleDeltaPreflight(bundleId: string, options: ExportBundleAccessOptions = {}): Promise<ImportBundleDeltaPreflightResponse> {
    return this.post<ImportBundleDeltaPreflightResponse>(
      `/v1/admin/import/bundles/${encodeURIComponent(bundleId)}/preflight-delta`,
      { encryptionKey: options.encryptionKey },
    )
  }

  async restoreImportBundle(bundleId: string, options: ExportBundleAccessOptions = {}): Promise<ImportBundleRestoreResponse> {
    return this.post<ImportBundleRestoreResponse>(
      `/v1/admin/import/bundles/${encodeURIComponent(bundleId)}/restore`,
      { encryptionKey: options.encryptionKey },
    )
  }

  async applyImportBundleDelta(bundleId: string, options: ExportBundleAccessOptions = {}): Promise<ImportBundleDeltaApplyResponse> {
    return this.post<ImportBundleDeltaApplyResponse>(
      `/v1/admin/import/bundles/${encodeURIComponent(bundleId)}/apply-delta`,
      { encryptionKey: options.encryptionKey },
    )
  }

  async restoreImportBundleChain(
    bundleIds: string[],
    options: ImportBundleChainRestoreOptions = {},
  ): Promise<ImportBundleChainRestoreResponse> {
    return this.post<ImportBundleChainRestoreResponse>("/v1/admin/import/bundles/restore-chain", {
      bundleIds,
      encryptionKey: options.encryptionKey,
    })
  }

  async compactWal(): Promise<WalCompactResponse> {
    return this.post<WalCompactResponse>("/v1/admin/wal/compact", {})
  }

  async walIntegrity(): Promise<WalIntegrityReport> {
    return this.get<WalIntegrityReport>("/v1/admin/wal/integrity")
  }

  async sealWalChecksums(): Promise<WalChecksumSealResponse> {
    return this.post<WalChecksumSealResponse>("/v1/admin/wal/seal-checksums", {})
  }

  async retainWalArchives(options: WalArchiveRetentionOptions = {}): Promise<WalArchiveRetentionResponse> {
    const params = new URLSearchParams({
      dryRun: String(options.dryRun ?? true),
    })
    if (options.beforeLsn !== undefined) {
      params.set("beforeLsn", String(options.beforeLsn))
    }
    if (options.beforeTimestampMs !== undefined) {
      params.set("beforeTimestampMs", String(options.beforeTimestampMs))
    }
    return this.post<WalArchiveRetentionResponse>(`/v1/admin/wal/archive/retention?${params}`, {})
  }

  async clusterTopology(): Promise<ClusterTopology> {
    let topology: ClusterTopology
    try {
      topology = await this.get<ClusterTopology>("/v1/cluster/topology")
    } catch (error) {
      if (!(error instanceof NextDbHttpError) || error.status !== 404) {
        throw error
      }
      topology = (await this.health()).clusterTopology
    }
    this.cachedClusterTopology = topology
    return topology
  }

  async clusterShardForKey(key: string): Promise<number> {
    const topology = await this.clusterTopologyForRouting()
    const shard = await localShardIndex(key, topology.shardCount)
    if (shard !== undefined) {
      return shard
    }
    return (await this.clusterRoute({ key })).shard
  }

  async clusterRoute(options: ClusterRouteOptions): Promise<ShardRoute> {
    const params = new URLSearchParams()
    if (options.key !== undefined) {
      params.set("key", options.key)
    }
    if (options.roomId !== undefined) {
      params.set("roomId", options.roomId)
    }
    if (options.table !== undefined) {
      params.set("table", options.table)
    }
    if (options.recordKey !== undefined) {
      params.set("recordKey", options.recordKey)
    }
    if (options.objectId !== undefined) {
      params.set("objectId", options.objectId)
    }
    try {
      return await this.get<ShardRoute>(`/v1/cluster/route?${params}`)
    } catch (error) {
      if (!(error instanceof NextDbHttpError) || error.status !== 404) {
        throw error
      }
      const topology = await this.clusterTopologyForRouting()
      const key = clusterRouteKey(options)
      const shard = await localShardIndex(key, topology.shardCount)
      if (shard === undefined) {
        throw error
      }
      const shardTopology = topology.shards[shard]
      const owner = shardTopology?.owner ?? topology.nodeId
      const localRole = shardTopology?.role ?? "owner"
      return {
        key,
        shard,
        epoch: shardTopology?.epoch ?? 1,
        owner,
        ownerUrl: shardTopology?.ownerUrl,
        replicas: shardTopology?.replicas ?? [],
        replicaUrls: shardTopology?.replicaUrls ?? [],
        localRole,
        localAcceptsWrites: !topology.enforceOwnership || localRole === "owner",
      }
    }
  }

  private async clusterTopologyForRouting(): Promise<ClusterTopology> {
    if (this.cachedClusterTopology !== undefined) {
      return this.cachedClusterTopology
    }
    if (this.clusterTopologyPromise === undefined) {
      this.clusterTopologyPromise = this.clusterTopology().finally(() => {
        this.clusterTopologyPromise = undefined
      })
    }
    return this.clusterTopologyPromise
  }

  async freezeShard(shard: number, reason?: string): Promise<ShardControlResponse> {
    return this.post<ShardControlResponse>(`/v1/admin/cluster/shards/${encodeURIComponent(String(shard))}/freeze`, {
      reason,
    })
  }

  async unfreezeShard(shard: number): Promise<ShardControlResponse> {
    return this.post<ShardControlResponse>(`/v1/admin/cluster/shards/${encodeURIComponent(String(shard))}/unfreeze`, {})
  }

  async handoffPlan(shard: number, targetOwner: string): Promise<HandoffPlanResponse> {
    return this.post<HandoffPlanResponse>("/v1/admin/cluster/handoff/plan", {
      shard,
      targetOwner,
    })
  }

  async failoverPlan(shard: number, targetOwner?: string): Promise<FailoverPlanResponse> {
    return this.post<FailoverPlanResponse>("/v1/admin/cluster/failover/plan", {
      shard,
      targetOwner,
    })
  }

  async startFailoverProposal(shard: number, targetOwner?: string): Promise<FailoverProposalResponse> {
    return this.post<FailoverProposalResponse>("/v1/admin/cluster/failover/proposals", {
      shard,
      targetOwner,
    })
  }

  async topologyOverrides(): Promise<TopologyOverrideResponse> {
    return this.get<TopologyOverrideResponse>("/v1/admin/cluster/topology/overrides")
  }

  async topologyLog(): Promise<TopologyLogResponse> {
    return this.get<TopologyLogResponse>("/v1/admin/cluster/topology/log")
  }

  async topologyProposals(): Promise<TopologyProposalListResponse> {
    return this.get<TopologyProposalListResponse>("/v1/admin/cluster/topology/proposals")
  }

  async startTopologyProposal(
    shard: number,
    override: ClusterShardOverride,
    reason?: string,
  ): Promise<TopologyProposalResponse> {
    return this.post<TopologyProposalResponse>("/v1/admin/cluster/topology/proposals", {
      shard,
      ...override,
      reason,
    })
  }

  async commitTopologyProposal(proposalId: string): Promise<TopologyProposalResponse> {
    return this.post<TopologyProposalResponse>(
      `/v1/admin/cluster/topology/proposals/${encodeURIComponent(proposalId)}/commit`,
      {},
    )
  }

  async retryTopologyProposal(proposalId: string): Promise<TopologyProposalResponse> {
    return this.post<TopologyProposalResponse>(
      `/v1/admin/cluster/topology/proposals/${encodeURIComponent(proposalId)}/retry`,
      {},
    )
  }

  async abortTopologyProposal(proposalId: string): Promise<TopologyProposalResponse> {
    return this.post<TopologyProposalResponse>(
      `/v1/admin/cluster/topology/proposals/${encodeURIComponent(proposalId)}/abort`,
      {},
    )
  }

  async cleanupTopologyLease(): Promise<{ cleared: boolean; lease: TopologyLease }> {
    return this.post<{ cleared: boolean; lease: TopologyLease }>("/v1/admin/cluster/topology/lease/cleanup", {})
  }

  async applyTopologyOverride(shard: number, override: ClusterShardOverride): Promise<TopologyOverrideResponse> {
    return this.post<TopologyOverrideResponse>("/v1/admin/cluster/topology/overrides", {
      shard,
      ...override,
    })
  }

  async listHandoffWorkflows(): Promise<HandoffWorkflowListResponse> {
    return this.get<HandoffWorkflowListResponse>("/v1/admin/cluster/handoff/workflows")
  }

  async startHandoffWorkflow(shard: number, targetOwner: string): Promise<HandoffWorkflowResponse> {
    return this.post<HandoffWorkflowResponse>("/v1/admin/cluster/handoff/workflows", {
      shard,
      targetOwner,
    })
  }

  async stepHandoffWorkflow(workflowId: string): Promise<HandoffWorkflowResponse> {
    return this.post<HandoffWorkflowResponse>(
      `/v1/admin/cluster/handoff/workflows/${encodeURIComponent(workflowId)}/step`,
      {},
    )
  }

  async autoHandoffWorkflow(workflowId: string): Promise<HandoffAutoResponse> {
    return this.post<HandoffAutoResponse>(
      `/v1/admin/cluster/handoff/workflows/${encodeURIComponent(workflowId)}/auto`,
      {},
    )
  }

  async abortHandoffWorkflow(workflowId: string): Promise<HandoffWorkflowResponse> {
    return this.post<HandoffWorkflowResponse>(
      `/v1/admin/cluster/handoff/workflows/${encodeURIComponent(workflowId)}/abort`,
      {},
    )
  }

  async applyHandoffWorkflow(workflowId: string): Promise<HandoffApplyResponse> {
    return this.post<HandoffApplyResponse>(
      `/v1/admin/cluster/handoff/workflows/${encodeURIComponent(workflowId)}/apply`,
      {},
    )
  }

  async health(): Promise<NextDbHealth> {
    return this.get<NextDbHealth>("/v1/health")
  }

  async readiness(): Promise<NextDbReadiness> {
    return this.get<NextDbReadiness>("/v1/ready")
  }

  async realtimeTransportCompatibility(
    requestedKind: NextDbRealtimeTransportKind | "custom" = this.configuredRealtimeTransportKind,
  ): Promise<NextDbRealtimeTransportCompatibility> {
    return realtimeTransportCompatibility(await this.health(), requestedKind)
  }

  async connectCompatibleRealtime(
    options: NextDbConnectCompatibleRealtimeOptions = {},
  ): Promise<NextDbConnectCompatibleRealtimeResult> {
    const requestedKind = options.requestedKind ?? this.configuredRealtimeTransportKind
    const fallbackTo = options.fallbackTo ?? "websocket"
    const compatibility = await this.realtimeTransportCompatibility(requestedKind)
    let activeKind = requestedKind
    let fallbackApplied = false
    let connected = false

    if (!compatibility.supported && compatibility.status === "unsupported") {
      if (fallbackTo === "websocket" && compatibility.supportedTransports.includes("webSocket")) {
        activeKind = "websocket"
        fallbackApplied = true
      } else if (fallbackTo === "jsonl" && compatibility.supportedTransports.includes("custom")) {
        activeKind = "jsonl"
        fallbackApplied = true
      } else {
        return {
          ...compatibility,
          activeKind: this.realtimeTransportKind,
          activeTransport: connectionTransportParam(this.realtimeTransportKind),
          fallbackApplied: false,
          connected: false,
        }
      }
    }
    if (activeKind === "custom" && this.configuredRealtimeTransportKind !== "custom") {
      return {
        ...compatibility,
        supported: false,
        activeKind: this.realtimeTransportKind,
        activeTransport: connectionTransportParam(this.realtimeTransportKind),
        fallbackApplied: false,
        connected: false,
        reason: "custom realtime transport requires NextDbClientOptions.realtimeTransport",
      }
    }

    this.useRealtimeTransportKind(activeKind)
    this.ensureSocket()
    connected = true
    return {
      ...compatibility,
      fallbackTransport: fallbackApplied ? connectionTransportParam(activeKind) : compatibility.fallbackTransport,
      activeKind: this.realtimeTransportKind,
      activeTransport: connectionTransportParam(this.realtimeTransportKind),
      fallbackApplied,
      connected,
    }
  }

  async metrics(): Promise<string> {
    return this.getText("/v1/metrics")
  }

  async getRuntimeDrain(): Promise<RuntimeDrainState> {
    return this.get<RuntimeDrainState>("/v1/admin/runtime/drain")
  }

  async setRuntimeDraining(draining: boolean, reason?: string): Promise<RuntimeDrainState> {
    return this.post<RuntimeDrainState>("/v1/admin/runtime/drain", { draining, reason })
  }

  async prepareRestart(options: RuntimePrepareRestartOptions = {}): Promise<RuntimePrepareRestartResponse> {
    return this.post<RuntimePrepareRestartResponse>("/v1/admin/runtime/prepare-restart", options)
  }

  async projectionRebuildStatus(): Promise<ProjectionRebuildStatus> {
    return this.get<ProjectionRebuildStatus>("/v1/admin/projections/rebuild/status")
  }

  async rebuildProjections(options: ProjectionRebuildOptions = {}): Promise<ProjectionRebuildResponse> {
    return this.post<ProjectionRebuildResponse>("/v1/admin/projections/rebuild", {
      background: options.background ?? false,
    })
  }

  async auditWal(options: AuditWalOptions = {}): Promise<AuditWalResponse> {
    const params = new URLSearchParams()
    if (options.afterLsn !== undefined) {
      params.set("afterLsn", String(options.afterLsn))
    }
    if (options.limit !== undefined) {
      params.set("limit", String(options.limit))
    }
    if (options.payloadType !== undefined) {
      params.set("payloadType", options.payloadType)
    }
    if (options.roomId !== undefined) {
      params.set("roomId", options.roomId)
    }
    if (options.userId !== undefined) {
      params.set("userId", options.userId)
    }
    if (options.objectId !== undefined) {
      params.set("objectId", options.objectId)
    }
    if (options.table !== undefined) {
      params.set("table", options.table)
    }
    if (options.recordKey !== undefined) {
      params.set("recordKey", options.recordKey)
    }
    if (options.path !== undefined) {
      params.set("path", options.path)
    }
    if (options.clientMutationId !== undefined) {
      params.set("clientMutationId", options.clientMutationId)
    }
    const suffix = params.size > 0 ? `?${params}` : ""
    return this.get<AuditWalResponse>(`/v1/audit/wal${suffix}`)
  }

  async traceEntity(options: AuditTraceOptions): Promise<AuditTraceResponse> {
    const params = new URLSearchParams({ kind: options.kind })
    if ("id" in options && options.id !== undefined) {
      params.set("id", options.id)
    }
    if (options.afterLsn !== undefined) {
      params.set("afterLsn", String(options.afterLsn))
    }
    if (options.limit !== undefined) {
      params.set("limit", String(options.limit))
    }
    if ("table" in options && options.table !== undefined) {
      params.set("table", options.table)
    }
    if ("recordKey" in options && options.recordKey !== undefined) {
      params.set("recordKey", options.recordKey)
    }
    if ("parentKey" in options && options.parentKey !== undefined) {
      params.set("parentKey", options.parentKey)
    }
    if ("nested" in options && options.nested !== undefined) {
      params.set("nested", options.nested)
    }
    if ("nestedKey" in options && options.nestedKey !== undefined) {
      params.set("nestedKey", options.nestedKey)
    }
    if ("path" in options && options.path !== undefined) {
      params.set("path", options.path)
    }
    if ("clientMutationId" in options && options.clientMutationId !== undefined) {
      params.set("clientMutationId", options.clientMutationId)
    }
    return this.get<AuditTraceResponse>(`/v1/audit/trace?${params}`)
  }

  async replayEntity<T = unknown>(options: AuditReplayOptions): Promise<AuditReplayResponse<T>> {
    const params = new URLSearchParams({ kind: options.kind })
    if ("id" in options && options.id !== undefined) {
      params.set("id", options.id)
    }
    if (options.atLsn !== undefined) {
      params.set("atLsn", String(options.atLsn))
    }
    if ("table" in options && options.table !== undefined) {
      params.set("table", options.table)
    }
    if ("recordKey" in options && options.recordKey !== undefined) {
      params.set("recordKey", options.recordKey)
    }
    if ("parentKey" in options && options.parentKey !== undefined) {
      params.set("parentKey", options.parentKey)
    }
    if ("nested" in options && options.nested !== undefined) {
      params.set("nested", options.nested)
    }
    if ("nestedKey" in options && options.nestedKey !== undefined) {
      params.set("nestedKey", options.nestedKey)
    }
    return this.get<AuditReplayResponse<T>>(`/v1/audit/replay?${params}`)
  }

  async waitForLsn(minLsn: number, options: SyncWaitOptions = {}): Promise<SyncWaitResponse> {
    const params = new URLSearchParams({
      minLsn: String(Math.max(0, Math.floor(minLsn))),
    })
    if (options.timeoutMs !== undefined) {
      params.set("timeoutMs", String(Math.max(0, Math.floor(options.timeoutMs))))
    }
    if (options.consistency !== undefined) {
      params.set("consistency", options.consistency)
    }
    if (options.shardKey !== undefined) {
      params.set("shardKey", options.shardKey)
    }
    if (options.shard !== undefined) {
      params.set("shard", String(Math.max(0, Math.floor(options.shard))))
    }
    const response = await this.get<SyncWaitResponse>(`/v1/sync/wait?${params}`)
    if (response.caughtUp) {
      this.advanceLsn(response.currentLsn)
    }
    return response
  }

  private async ensureFreshness(options?: FreshnessOptions, shardKey?: string): Promise<void> {
    if (options?.minLsn === undefined) {
      return
    }
    if (options.consistency !== undefined && options.consistency !== "local" && shardKey === undefined) {
      throw new Error(`NextDB ${options.consistency} freshness requires a single shard target`)
    }
    const result = await this.waitForLsn(options.minLsn, {
      timeoutMs: options.timeoutMs,
      consistency: options.consistency,
      shardKey,
    })
    if (!result.caughtUp) {
      throw new Error(
        `NextDB node did not catch up to LSN ${options.minLsn} with ${result.consistency} consistency; current LSN is ${result.currentLsn}, remote acks ${result.remoteAcked}/${result.remoteRequiredAcks}`,
      )
    }
  }

  async syncPull(options: SyncPullOptions = {}): Promise<SyncPullResponse> {
    await this.reconcileCacheControl()
    const params = new URLSearchParams()
    if (options.afterLsn !== undefined) {
      params.set("afterLsn", String(options.afterLsn))
    }
    if (options.rooms !== undefined && options.rooms.length > 0) {
      params.set("rooms", options.rooms.join(","))
    }
    if (options.users !== undefined && options.users.length > 0) {
      params.set("users", options.users.join(","))
    }
    if (options.tables !== undefined && options.tables.length > 0) {
      params.set("tables", options.tables.join(","))
    }
    if (options.nestedTables !== undefined && options.nestedTables.length > 0) {
      params.set("nestedTables", options.nestedTables.map(syncNestedTableTargetParam).join(","))
    }
    if (options.objects !== undefined) {
      params.set("objects", String(options.objects))
    }
    if (options.limit !== undefined) {
      params.set("limit", String(options.limit))
    }

    const suffix = params.size > 0 ? `?${params}` : ""
    const response = await this.get<SyncPullResponse>(`/v1/sync/pull${suffix}`)
    await this.applySyncEvents(response.events, options.nestedTables ?? [])
    this.advanceLsn(response.nextAfterLsn)
    return response
  }

  async syncUntilCaughtUp(options: SyncUntilCaughtUpOptions = {}): Promise<SyncUntilCaughtUpResponse> {
    await this.reconcileCacheControl()
    const maxPages = Math.max(1, options.maxPages ?? 100)
    let afterLsn = options.afterLsn ?? await this.cache.getGlobalCursor()
    let currentLsn = afterLsn
    let hasMore = false
    let pages = 0
    const events: DeliveryEvent[] = []

    while (pages < maxPages) {
      const page = await this.syncPull({
        ...options,
        afterLsn,
      })
      pages += 1
      events.push(...page.events)
      currentLsn = page.currentLsn
      hasMore = page.hasMore
      if (!page.hasMore || page.nextAfterLsn <= afterLsn) {
        afterLsn = page.nextAfterLsn
        break
      }
      afterLsn = page.nextAfterLsn
    }

    this.advanceLsn(afterLsn)
    if (!hasMore) {
      await this.markSyncTargetsCaughtUp(options, currentLsn)
    }
    return {
      events,
      nextAfterLsn: afterLsn,
      currentLsn,
      hasMore,
      pages,
    }
  }

  async syncSubscribedRooms(limit = 500): Promise<SyncPullResponse | undefined> {
    const rooms = [...this.roomListeners.keys()]
    if (rooms.length === 0) {
      return undefined
    }
    await this.hydrateCursorsFor({ rooms })
    return this.syncUntilCaughtUp({
      afterLsn: await this.minimumSeenRoomLsn(rooms),
      rooms,
      limit,
    })
  }

  async syncSubscribedTables(limit = 500): Promise<SyncPullResponse | undefined> {
    const tables = this.activeTableNames()
    const nestedTables = this.activeNestedTableSubscriptionTargets()
    if (tables.length === 0 && nestedTables.length === 0) {
      return undefined
    }
    await this.hydrateCursorsFor({ tables, nestedTables })
    return this.syncUntilCaughtUp({
      afterLsn: Math.min(
        await this.minimumSeenTableLsn(tables),
        await this.minimumSeenNestedTableLsn(nestedTables),
      ),
      tables,
      nestedTables,
      limit,
    })
  }

  async syncCurrentUserEvents(options: { limit?: number; maxPages?: number } = {}): Promise<SyncUntilCaughtUpResponse | undefined> {
    if (!this.userId) {
      return undefined
    }
    await this.hydrateCursorsFor({ users: [this.userId] })
    return this.syncUntilCaughtUp({
      afterLsn: await this.minimumSeenUserLsn([this.userId]),
      users: [this.userId],
      limit: options.limit,
      maxPages: options.maxPages,
    })
  }

  async syncRoom(roomId: string, options: { limit?: number; maxPages?: number } = {}): Promise<SyncUntilCaughtUpResponse> {
    await this.hydrateCursorsFor({ rooms: [roomId] })
    return this.syncUntilCaughtUp({
      afterLsn: await this.minimumSeenRoomLsn([roomId]),
      rooms: [roomId],
      limit: options.limit,
      maxPages: options.maxPages,
    })
  }

  async syncTable(table: string, options: { limit?: number; maxPages?: number } = {}): Promise<SyncUntilCaughtUpResponse> {
    await this.hydrateCursorsFor({ tables: [table] })
    return this.syncUntilCaughtUp({
      afterLsn: await this.minimumSeenTableLsn([table]),
      tables: [table],
      limit: options.limit,
      maxPages: options.maxPages,
    })
  }

  async syncObjects(options: { limit?: number; maxPages?: number } = {}): Promise<SyncUntilCaughtUpResponse> {
    await this.hydrateCursorsFor({ objects: true })
    return this.syncUntilCaughtUp({
      afterLsn: await this.minimumSeenObjectLsn(),
      objects: true,
      limit: options.limit,
      maxPages: options.maxPages,
    })
  }

  async cacheStats(): Promise<NextDbCacheStats> {
    return this.cache.stats()
  }

  async cacheCoverage(): Promise<NextDbCacheCoverage> {
    const [cache, pendingWrites, storedSubscriptions] = await Promise.all([
      this.cache.stats(),
      this.cache.listPendingWrites(),
      this.cache.listSubscriptions(),
    ])
    return this.buildCacheCoverage(cache, pendingWrites, storedSubscriptions)
  }

  async localDataStatus(): Promise<NextDbLocalDataStatus> {
    const activeRooms = this.activeRoomSubscriptionIds()
    const activeTables = this.activeTableSubscriptionIds()
    const activeTableNames = this.activeTableNames()
    const activeNestedTables = this.activeNestedTableSubscriptionIds()
    const activeQueries = this.activeQuerySubscriptions().map(([queryId]) => queryId)
    const activeUsers = this.userId && (this.userListeners.size > 0 || this.persistentUserSubscription)
      ? [this.userId]
      : []
    const activeObjects = this.objectListeners.size > 0 || this.persistentObjectSubscription !== undefined
    await this.hydrateCursorsFor({
      rooms: activeRooms,
      users: activeUsers,
      tables: activeTableNames,
      objects: activeObjects,
    })
    const [cache, pendingWrites, storedSubscriptions, cacheMetadata] = await Promise.all([
      this.cache.stats(),
      this.pendingWriteStats(),
      this.cache.listSubscriptions(),
      this.cache.getMetadata(),
    ])
    const pendingWriteRows = await this.cache.listPendingWrites()
    const coverage = await this.buildCacheCoverage(cache, pendingWriteRows, storedSubscriptions)
    return {
      endpoint: this.activeEndpoint,
      initialEndpoint: this.initialEndpoint,
      cacheScope: this.cacheScope,
      configuredRealtimeTransportKind: this.configuredRealtimeTransportKind,
      configuredConnectionTransport: connectionTransportParam(this.configuredRealtimeTransportKind),
      realtimeTransportKind: this.realtimeTransportKind,
      connectionTransport: connectionTransportParam(this.realtimeTransportKind),
      transportState: this.transport?.state ?? "idle",
      manuallyClosed: this.manuallyClosed,
      lastSeenLsn: this.lastSeenLsn,
      objectSeenLsn: this.objectSeenLsn,
      roomSeenLsn: mapToRecord(this.roomSeenLsn),
      userSeenLsn: mapToRecord(this.userSeenLsn),
      tableSeenLsn: mapToRecord(this.tableSeenLsn),
      nestedTableSeenLsn: mapToRecord(this.nestedTableSeenLsn),
      cache,
      coverage,
      pendingWrites,
      storedSubscriptions,
      activeSubscriptions: {
        rooms: activeRooms,
        tables: activeTables,
        nestedTables: activeNestedTables,
        queries: activeQueries,
        realtimeChannels: [...this.joinedRealtimeChannels.keys()],
        userEvents: activeUsers.length > 0,
        objects: activeObjects,
      },
      persistentSubscriptions: {
        rooms: [...this.persistentRoomSubscriptions.keys()],
        tables: [...this.persistentTableSubscriptions.keys()],
        nestedTables: [...this.persistentNestedTableSubscriptions.values()].map(nestedTableSubscriptionLabel),
        queries: [...this.persistentQuerySubscriptions.keys()],
        realtimeChannels: [],
        userEvents: this.persistentUserSubscription !== undefined,
        objects: this.persistentObjectSubscription !== undefined,
      },
      realtimeChannelStates: mapRealtimeChannelStateVersions(this.realtimeChannelStates),
      realtimeChannelMembers: mapRealtimeChannelMemberSummaries(this.realtimeChannelMemberSnapshots),
      realtimeChannelEvents: mapRealtimeChannelEventSummaries(this.realtimeChannelEventSnapshots),
      realtimeChannelSignals: mapRealtimeChannelSignalSummaries(this.realtimeChannelSignalSnapshots),
      connectionSessions: connectionSessionStatus(this.connectionSessionSnapshots),
      cacheMetadata,
      cacheProfile: this.clientCacheProfile,
    }
  }

  watchLocalDataStatus(
    listener: (snapshot: LocalDataStatusSnapshot) => void,
    options: Pick<WatchOptions, "limit" | "immediate"> = {},
  ): () => void {
    const pendingLimit = normalizePageLimit(options.limit)
    let closed = false
    let scheduled = false
    let latestSource: CacheSnapshotSource = "cache"
    let latestChange: NextDbCacheChange | undefined
    const emitNow = () => {
      scheduled = false
      const source = latestSource
      const change = latestChange
      latestChange = undefined
      Promise.all([
        this.localDataStatus(),
        this.pendingWriteQueueStatus(pendingLimit),
      ])
        .then(([status, pendingQueue]) => {
          if (!closed) {
            listener({ status, pendingQueue, source, change })
          }
        })
        .catch((error) => console.error("nextdb local data watcher failed", error))
    }
    const schedule = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      latestSource = source
      latestChange = change
      if (scheduled) {
        return
      }
      scheduled = true
      queueMicrotask(emitNow)
    }
    if (options.immediate ?? true) {
      schedule("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      schedule(change.source, change)
    })
    return () => {
      closed = true
      stopCache()
    }
  }

  private async buildCacheCoverage(
    cache: NextDbCacheStats,
    pendingWrites: NextDbPendingWrite[],
    storedSubscriptions: NextDbStoredSubscription[],
  ): Promise<NextDbCacheCoverage> {
    const activeRooms = new Set(this.activeRoomSubscriptionIds())
    const activeTables = new Set(this.activeTableSubscriptionIds())
    const activeNestedTables = new Set(this.activeNestedTableSubscriptionTargets().map(nestedCoverageKeyFromTarget))
    const activeUsers = this.userId && (this.userListeners.size > 0 || this.persistentUserSubscription)
      ? new Set([this.userId])
      : new Set<string>()
    const activeObjects = this.objectListeners.size > 0 || this.persistentObjectSubscription !== undefined
    const persistentRooms = new Set(this.persistentRoomSubscriptions.keys())
    const persistentTables = new Set(this.persistentTableSubscriptions.keys())
    const persistentNestedTables = new Set([...this.persistentNestedTableSubscriptions.values()].map(nestedCoverageKeyFromTarget))
    const persistentUsers = this.persistentUserSubscription ? new Set([this.persistentUserSubscription.userId]) : new Set<string>()
    const persistentObjects = this.persistentObjectSubscription !== undefined
    const storedRooms = new Set<string>()
    const storedTables = new Set<string>()
    const storedNestedTables = new Set<string>()
    const storedUsers = new Set<string>()
    let storedObjects = false

    for (const subscription of storedSubscriptions) {
      if (subscription.kind === "room") {
        storedRooms.add(subscription.roomId)
      } else if (subscription.kind === "table") {
        storedTables.add(tableSubscriptionTargetId(subscription.table, subscription.options))
      } else if (subscription.kind === "nestedTable") {
        storedNestedTables.add(nestedCoverageKeyFromTarget(subscription))
      } else if (subscription.kind === "userEvents") {
        storedUsers.add(subscription.userId)
      } else if (subscription.kind === "objects") {
        storedObjects = true
      }
    }

    const pendingRooms = new Map<string, number>()
    const pendingUsers = new Map<string, number>()
    const pendingTables = new Map<string, number>()
    const pendingNestedTables = new Map<string, number>()
    let pendingObjects = 0

    for (const write of pendingWrites) {
      if (write.type === "sendMessage") {
        incrementCount(pendingRooms, write.roomId)
      } else if (write.type === "userEvent" || write.type === "userProfileUpsert") {
        incrementCount(pendingUsers, write.userId)
      } else if (write.type === "recordUpsert" || write.type === "recordDelete") {
        incrementCount(pendingTables, write.table)
      } else if (write.type === "nestedRecordUpsert" || write.type === "nestedRecordDelete") {
        incrementCount(pendingNestedTables, nestedCoverageKey(write.table, write.parentKey, write.nested))
      } else if (write.type === "recordTransaction") {
        for (const operation of write.operations) {
          if (operation.type === "upsert" || operation.type === "delete") {
            incrementCount(pendingTables, operation.table)
          } else {
            incrementCount(pendingNestedTables, nestedCoverageKey(operation.table, operation.parentKey, operation.nested))
          }
        }
      } else if (write.type === "objectPut" || write.type === "objectDelete") {
        pendingObjects += 1
      }
    }

    const profiles = await this.cache.listUserProfiles(Number.MAX_SAFE_INTEGER)
    const userProfiles = new Set(profiles.map((profile) => profile.userId))
    const roomIds = sortedStrings([
      ...Object.keys(cache.rooms),
      ...pendingRooms.keys(),
      ...activeRooms,
      ...persistentRooms,
      ...storedRooms,
    ])
    const userIds = sortedStrings([
      ...Object.keys(cache.users),
      ...userProfiles,
      ...pendingUsers.keys(),
      ...activeUsers,
      ...persistentUsers,
      ...storedUsers,
    ])
    const tableIds = sortedStrings([
      ...Object.keys(cache.tables),
      ...pendingTables.keys(),
      ...activeTables,
      ...persistentTables,
      ...storedTables,
    ])
    const nestedIds = new Set<string>()
    for (const [logicalTable, partitions] of Object.entries(cache.nestedTables)) {
      for (const keyPrefix of Object.keys(partitions)) {
        nestedIds.add(nestedCoverageKeyFromLogical(logicalTable, parentKeyFromNestedRecordPrefix(keyPrefix)))
      }
    }
    for (const key of pendingNestedTables.keys()) {
      nestedIds.add(key)
    }
    for (const key of activeNestedTables) {
      nestedIds.add(key)
    }
    for (const key of persistentNestedTables) {
      nestedIds.add(key)
    }
    for (const key of storedNestedTables) {
      nestedIds.add(key)
    }

    const rooms: Record<string, NextDbRoomCacheCoverage> = {}
    for (const roomId of roomIds) {
      rooms[roomId] = {
        messages: cache.rooms[roomId] ?? 0,
        cursor: await this.cache.getRoomCursor(roomId),
        pendingWrites: pendingRooms.get(roomId) ?? 0,
        activeSubscription: activeRooms.has(roomId),
        persistentSubscription: persistentRooms.has(roomId),
        storedSubscription: storedRooms.has(roomId),
      }
    }

    const users: Record<string, NextDbUserCacheCoverage> = {}
    for (const userId of userIds) {
      users[userId] = {
        events: cache.users[userId] ?? 0,
        profile: userProfiles.has(userId),
        cursor: await this.cache.getUserCursor(userId),
        pendingWrites: pendingUsers.get(userId) ?? 0,
        activeSubscription: activeUsers.has(userId),
        persistentSubscription: persistentUsers.has(userId),
        storedSubscription: storedUsers.has(userId),
      }
    }

    const tables: Record<string, NextDbRecordCacheCoverage> = {}
    for (const table of tableIds) {
      tables[table] = {
        records: cache.tables[table] ?? 0,
        cursor: await this.cache.getTableCursor(table),
        pendingWrites: pendingTables.get(table) ?? 0,
        activeSubscription: activeTables.has(table),
        persistentSubscription: persistentTables.has(table),
        storedSubscription: storedTables.has(table),
      }
    }

    const nestedTables: Record<string, Record<string, NextDbNestedRecordCacheCoverage>> = {}
    for (const key of sortedStrings([...nestedIds])) {
      const parsed = parseNestedCoverageKey(key)
      if (!parsed) {
        continue
      }
      const records = cache.nestedTables[parsed.logicalTable]?.[nestedRecordPrefix(parsed.parentKey)] ?? 0
      nestedTables[parsed.logicalTable] ??= {}
      nestedTables[parsed.logicalTable][parsed.parentKey] = {
        records,
        cursor: await this.cache.getNestedTableCursor(parsed.table, parsed.parentKey, parsed.nested),
        pendingWrites: pendingNestedTables.get(key) ?? 0,
        activeSubscription: activeNestedTables.has(key),
        persistentSubscription: persistentNestedTables.has(key),
        storedSubscription: storedNestedTables.has(key),
      }
    }

    const realtimeChannelIds = sortedStrings([
      ...this.joinedRealtimeChannels.keys(),
      ...this.realtimeChannelStates.keys(),
      ...this.realtimeChannelMemberSnapshots.keys(),
      ...this.realtimeChannelEventSnapshots.keys(),
      ...this.realtimeChannelSignalSnapshots.keys(),
    ])
    const realtimeChannels: Record<string, NextDbRealtimeChannelCacheCoverage> = {}
    for (const channelId of realtimeChannelIds) {
      const state = this.realtimeChannelStates.get(channelId)
      const members = this.realtimeChannelMemberSnapshots.get(channelId) ?? []
      const events = this.realtimeChannelEventSnapshots.get(channelId) ?? []
      const signals = this.realtimeChannelSignalSnapshots.get(channelId) ?? []
      const latestEvent = events.at(-1)
      const latestSignal = signals.at(-1)
      realtimeChannels[channelId] = {
        stateVersion: state?.version,
        stateUpdatedAtMs: state?.updatedAtMs,
        members: members.length,
        membersUpdatedAtMs: latestRealtimeMemberUpdatedAtMs(members),
        recentEvents: events.length,
        latestEventSequence: latestEvent?.sequence,
        latestEventTimestampMs: latestEvent?.timestampMs,
        recentSignals: signals.length,
        latestSignalSequence: latestSignal?.sequence,
        latestSignalTimestampMs: latestSignal?.timestampMs,
        activeSubscription: this.joinedRealtimeChannels.has(channelId),
      }
    }

    return {
      globalCursor: await this.cache.getGlobalCursor(),
      objects: {
        objects: cache.totalObjects,
        byteSize: cache.totalObjectBytes,
        cachedByteSize: cache.totalObjectCachedBytes,
        rangeChunks: cache.totalObjectRangeChunks,
        cursor: await this.cache.getObjectCursor(),
        pendingWrites: pendingObjects,
        activeSubscription: activeObjects,
        persistentSubscription: persistentObjects,
        storedSubscription: storedObjects,
      },
      rooms,
      users,
      tables,
      nestedTables,
      realtimeChannels,
    }
  }

  async listStoredSubscriptions(): Promise<NextDbStoredSubscription[]> {
    return this.cache.listSubscriptions()
  }

  async clearStoredSubscriptions(): Promise<number> {
    this.clearPersistentSubscriptionIntents()
    return this.cache.clearSubscriptions()
  }

  private clearPersistentSubscriptionIntents(): void {
    const rooms = [...this.persistentRoomSubscriptions.keys()]
    const tables = [...this.persistentTableSubscriptions.entries()]
    const nestedTables = [...this.persistentNestedTableSubscriptions.entries()]
    const queries = [...this.persistentQuerySubscriptions.keys()]
    const hadUserSubscription = this.persistentUserSubscription !== undefined
    const hadObjectSubscription = this.persistentObjectSubscription !== undefined

    this.persistentRoomSubscriptions.clear()
    this.persistentTableSubscriptions.clear()
    this.persistentNestedTableSubscriptions.clear()
    this.persistentQuerySubscriptions.clear()
    this.persistentUserSubscription = undefined
    this.persistentObjectSubscription = undefined

    for (const roomId of rooms) {
      this.pendingRoomSubscriptions.delete(roomId)
      if (!this.roomListeners.has(roomId)) {
        this.unsubscribeWhenConnected({ type: "unsubscribeRoom", roomId })
      }
    }
    for (const [id, subscription] of tables) {
      this.pendingTableSubscriptions.delete(id)
      if (!this.activeTableSubscriptions.has(id)) {
        this.unsubscribeWhenConnected(unsubscribeTableFrame(subscription.table, subscription.options))
      }
    }
    for (const [id, subscription] of nestedTables) {
      this.pendingNestedTableSubscriptions.delete(id)
      if (!this.activeNestedTableSubscriptions.has(id)) {
        this.unsubscribeWhenConnected({
          type: "unsubscribeNestedTable",
          table: subscription.table,
          parentKey: subscription.parentKey,
          nested: subscription.nested,
        })
      }
    }
    for (const queryId of queries) {
      this.pendingQuerySubscriptions.delete(queryId)
      if (!this.querySubscriptions.has(queryId)) {
        this.queryResults.delete(queryId)
        this.unsubscribeWhenConnected({ type: "unsubscribeQuery", queryId })
      }
    }
    if (hadUserSubscription && this.userListeners.size === 0) {
      this.unsubscribeWhenConnected({ type: "unsubscribeUserEvents" })
      this.userSubscriptionActive = false
      this.pendingUserSubscription = undefined
    }
    if (hadObjectSubscription && this.objectListeners.size === 0) {
      this.unsubscribeWhenConnected({ type: "unsubscribeObjects" })
      this.objectSubscriptionActive = false
      this.pendingObjectSubscription = undefined
    }
  }

  async restoreSubscriptions(): Promise<NextDbStoredSubscription[]> {
    const subscriptions = await this.cache.listSubscriptions()
    for (const subscription of subscriptions) {
      await this.activateStoredSubscription(subscription)
    }
    if (subscriptions.length > 0) {
      this.ensureSocket()
    }
    return subscriptions
  }

  async refreshCacheLease(): Promise<ClientCacheProfileResponse> {
    return this.reconcileCacheControl(true)
  }

  async enforceLocalCacheProfile(
    options: EnforceLocalCacheProfileOptions = {},
  ): Promise<LocalCacheProfileEnforcementResult> {
    const profile = options.profile ?? (await this.reconcileCacheControl(options.refreshLease ?? false)).profile
    const result = await this.enforceCacheProfile(profile)
    this.emitCacheChange({
      type: "cacheProfileEnforced",
      source: "manual",
      result,
    })
    return result
  }

  async invalidateClientCaches(options: ClientCacheInvalidateOptions): Promise<ClientCacheInvalidateResponse> {
    return this.post<ClientCacheInvalidateResponse>("/v1/admin/cache/invalidate", options)
  }

  async updateClientCacheProfile(
    options: ClientCacheProfileUpdateOptions,
  ): Promise<ClientCacheProfileUpdateResponse> {
    return this.post<ClientCacheProfileUpdateResponse>("/v1/admin/cache/profile", options)
  }

  async clearCache(): Promise<number> {
    this.clearPersistentSubscriptionIntents()
    const removed = await this.cache.clearAll()
    this.lastSeenLsn = 0
    this.objectSeenLsn = 0
    this.roomSeenLsn.clear()
    this.userSeenLsn.clear()
    this.tableSeenLsn.clear()
    this.tableSeenEventIds.clear()
    this.tableCaughtUpLsn.clear()
    this.tableAppliedEventIds.clear()
    this.nestedTableSeenLsn.clear()
    this.nestedTableSeenEventIds.clear()
    this.nestedTableCaughtUpLsn.clear()
    this.nestedTableAppliedEventIds.clear()
    this.emitCacheChange({
      type: "allInvalidated",
      source: "manual",
      minValidLsn: 0,
    })
    return removed
  }

  async clearObjectCache(): Promise<number> {
    const removed = await this.cache.clearObjects()
    this.objectSeenLsn = 0
    this.emitCacheChange({
      type: "objectsInvalidated",
      source: "manual",
    })
    return removed
  }

  async clearRoomCache(roomId: string): Promise<number> {
    const removed = await this.cache.clearRoom(roomId)
    this.roomSeenLsn.delete(roomId)
    this.emitCacheChange({
      type: "roomInvalidated",
      source: "manual",
      roomId,
      minValidLsn: 0,
    })
    return removed
  }

  async trimRoomCache(roomId: string, keepLatest: number): Promise<number> {
    return this.cache.trimRoom(roomId, keepLatest)
  }

  async clearUserEventCache(userId = this.requireUserId("clearUserEventCache")): Promise<number> {
    const removed = await this.cache.clearUserEvents(userId)
    this.userSeenLsn.delete(userId)
    this.emitCacheChange({
      type: "userInvalidated",
      source: "manual",
      userId,
      minValidLsn: 0,
    })
    return removed
  }

  async clearUserProfileCache(userId = this.requireUserId("clearUserProfileCache")): Promise<number> {
    const removed = await this.cache.deleteUserProfile(userId)
    if (removed) {
      this.emitCacheChange({
        type: "userProfileDeleted",
        source: "manual",
        userId,
      })
    }
    return removed ? 1 : 0
  }

  async clearUserCache(userId = this.requireUserId("clearUserCache")): Promise<number> {
    const removedEvents = await this.cache.clearUserEvents(userId)
    const removedProfile = await this.cache.deleteUserProfile(userId)
    this.userSeenLsn.delete(userId)
    this.emitCacheChange({
      type: "userInvalidated",
      source: "manual",
      userId,
      minValidLsn: 0,
    })
    if (removedProfile) {
      this.emitCacheChange({
        type: "userProfileDeleted",
        source: "manual",
        userId,
      })
    }
    return removedEvents + (removedProfile ? 1 : 0)
  }

  async clearTableCache(table: string): Promise<number> {
    const removed = await this.cache.clearTable(table)
    this.tableSeenLsn.delete(table)
    this.tableSeenEventIds.delete(table)
    this.tableCaughtUpLsn.delete(table)
    this.tableAppliedEventIds.delete(table)
    this.clearNestedCursorStateForLogicalTable(table)
    this.emitCacheChange({
      type: "tableInvalidated",
      source: "manual",
      table,
      minValidLsn: 0,
    })
    return removed
  }

  async clearNestedTableCache(table: string, parentKey: string, nested: string): Promise<number> {
    const logicalTable = nestedRecordTable(table, nested)
    const removed = await this.cache.clearRecordsByKeyPrefix(logicalTable, nestedRecordPrefix(parentKey))
    await this.cache.setTableCursor(logicalTable, 0)
    await this.cache.setNestedTableCursor(table, parentKey, nested, 0)
    this.tableSeenLsn.delete(logicalTable)
    this.tableSeenEventIds.delete(logicalTable)
    this.tableCaughtUpLsn.delete(logicalTable)
    this.tableAppliedEventIds.delete(logicalTable)
    this.clearNestedCursorState(table, parentKey, nested)
    this.emitCacheChange({
      type: "tableInvalidated",
      source: "manual",
      table: logicalTable,
      minValidLsn: 0,
    })
    return removed
  }

  async listPendingWrites(limit?: number): Promise<NextDbPendingWrite[]> {
    return this.cache.listPendingWrites(limit)
  }

  async pendingWriteQueueStatus(limit = 100): Promise<PendingWriteQueueStatus> {
    const [writes, stats] = await Promise.all([
      this.cache.listPendingWrites(limit),
      this.pendingWriteStats(),
    ])
    const autoFlush = this.pendingWriteAutoFlush ?? defaultPendingWriteAutoFlush()
    return {
      stats,
      writes,
      autoFlush: {
        enabled: autoFlush.enabled,
        intervalMs: autoFlush.intervalMs,
        limit: autoFlush.limit,
        retryOnStart: autoFlush.retryOnStart,
        scheduled: this.pendingWriteFlushTimer !== undefined,
        inFlight: this.pendingWriteFlushPromise !== undefined,
      },
    }
  }

  async pendingWriteStats(): Promise<PendingWriteStats> {
    const [writes, metadata] = await Promise.all([
      this.cache.listPendingWrites(),
      this.cache.getMetadata(),
    ])
    const byType: Record<PendingWriteType, number> = {
      sendMessage: 0,
      userEvent: 0,
      userProfileUpsert: 0,
      recordUpsert: 0,
      recordDelete: 0,
      nestedRecordUpsert: 0,
      nestedRecordDelete: 0,
      recordTransaction: 0,
      objectPut: 0,
      objectDelete: 0,
    }
    let estimatedBytes = 0
    let objectPutBytes = 0
    let failed = 0
    let totalAttempts = 0
    let oldestCreatedAtMs: number | undefined
    let newestCreatedAtMs: number | undefined
    for (const write of writes) {
      byType[write.type] += 1
      estimatedBytes += estimatePendingWriteBytes(write)
      if (write.type === "objectPut") {
        objectPutBytes += write.body.size
      }
      if (write.lastError !== undefined) {
        failed += 1
      }
      totalAttempts += write.attempts
      oldestCreatedAtMs = oldestCreatedAtMs === undefined ? write.createdAtMs : Math.min(oldestCreatedAtMs, write.createdAtMs)
      newestCreatedAtMs = newestCreatedAtMs === undefined ? write.createdAtMs : Math.max(newestCreatedAtMs, write.createdAtMs)
    }
    const maxWrites = this.clientCacheProfile?.maxPendingWrites ?? metadata?.maxPendingWrites ?? 0
    const maxBytes = this.clientCacheProfile?.maxPendingWriteBytes ?? metadata?.maxPendingWriteBytes ?? 0
    return {
      total: writes.length,
      byType,
      estimatedBytes,
      objectPutBytes,
      failed,
      totalAttempts,
      oldestCreatedAtMs,
      newestCreatedAtMs,
      maxWrites,
      maxBytes,
      overMaxWrites: maxWrites > 0 && writes.length > maxWrites,
      overMaxBytes: maxBytes > 0 && estimatedBytes > maxBytes,
    }
  }

  watchPendingWrites(
    listener: (snapshot: PendingWritesSnapshot) => void,
    options: Pick<WatchOptions, "limit" | "immediate"> = {},
  ): () => void {
    const limit = normalizePageLimit(options.limit)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.pendingWriteQueueStatus(limit)
        .then((queue) => {
          if (!closed) {
            listener({ queue, source, change })
          }
        })
        .catch((error) => console.error("nextdb pending write watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (isPendingWriteChange(change)) {
        emit(change.source, change)
      }
    })
    return () => {
      closed = true
      stopCache()
    }
  }

  async clearPendingWrites(): Promise<number> {
    const removed = await this.cache.clearPendingWrites()
    if (removed > 0) {
      await this.emitPendingWriteChange({
        type: "pendingWritesCleared",
        source: "manual",
        removed,
      })
    }
    return removed
  }

  async discardPendingWrite(
    id: string,
    options: DiscardPendingWriteOptions = {},
  ): Promise<DiscardPendingWriteResponse> {
    const write = await this.findPendingWrite(id)
    if (write === undefined) {
      return { id, discarded: false, removedOptimistic: false }
    }

    await this.cache.deletePendingWrite(id)
    const removedOptimistic = options.removeOptimistic === true
      ? await this.removeOptimisticPendingWrite(write)
      : false
    await this.emitPendingWriteChange({
      type: "pendingWriteDiscarded",
      source: "manual",
      write,
      removedOptimistic,
    })
    return { id, discarded: true, removedOptimistic, write }
  }

  async resetPendingWrite(id: string): Promise<ResetPendingWriteResponse> {
    const write = await this.findPendingWrite(id)
    if (write === undefined) {
      return { id, reset: false }
    }

    const reset = {
      ...write,
      attempts: 0,
      lastError: undefined,
    } as NextDbPendingWrite
    await this.cache.putPendingWrite(reset)
    await this.emitPendingWriteChange({
      type: "pendingWriteReset",
      source: "manual",
      write: reset,
    })
    return { id, reset: true, write: reset }
  }

  startPendingWriteAutoFlush(options: PendingWriteAutoFlushOptions = {}): void {
    this.pendingWriteAutoFlush = normalizePendingWriteAutoFlush({
      ...this.pendingWriteAutoFlush,
      ...options,
      enabled: true,
    })
    if (this.pendingWriteAutoFlush.retryOnStart) {
      this.schedulePendingWriteFlush(0)
    }
  }

  stopPendingWriteAutoFlush(): void {
    this.pendingWriteAutoFlush = {
      ...(this.pendingWriteAutoFlush ?? defaultPendingWriteAutoFlush()),
      enabled: false,
    }
    if (this.pendingWriteFlushTimer !== undefined) {
      clearTimeout(this.pendingWriteFlushTimer)
      this.pendingWriteFlushTimer = undefined
    }
  }

  async flushPendingWrites(limit = 100): Promise<FlushPendingWritesResult> {
    this.pendingWriteFlushPromise ??= this.flushPendingWritesOnce(limit)
      .finally(() => {
        this.pendingWriteFlushPromise = undefined
      })
    return this.pendingWriteFlushPromise
  }

  private async findPendingWrite(id: string): Promise<NextDbPendingWrite | undefined> {
    const writes = await this.cache.listPendingWrites()
    return writes.find((write) => write.id === id)
  }

  private async removeOptimisticPendingWrite(write: NextDbPendingWrite): Promise<boolean> {
    if (write.type === "sendMessage") {
      const removed = await this.cache.deleteRoomMessage(write.roomId, write.id)
      if (removed) {
        this.emitCacheChange({
          type: "roomInvalidated",
          source: "manual",
          roomId: write.roomId,
        })
      }
      return removed
    }

    if (write.type === "userProfileUpsert") {
      const cached = await this.cache.getUserProfile(write.userId)
      if (cached?.lsn !== 0) {
        return false
      }
      const removed = await this.cache.deleteUserProfile(write.userId)
      if (removed) {
        this.emitCacheChange({
          type: "userProfileDeleted",
          source: "manual",
          userId: write.userId,
        })
      }
      return removed
    }

    if (write.type === "recordUpsert") {
      return this.removeOptimisticRecord(write.table, write.key)
    }

    if (write.type === "nestedRecordUpsert") {
      return this.removeOptimisticRecord(
        nestedRecordTable(write.table, write.nested),
        nestedRecordKey(write.parentKey, write.nestedKey),
      )
    }

    if (write.type === "objectPut") {
      const removed = await this.cache.deleteObject(write.objectId)
      if (removed) {
        this.emitCacheChange({
          type: "objectDeleted",
          source: "manual",
          objectId: write.objectId,
        })
      }
      return removed
    }

    return false
  }

  private async removeOptimisticRecord(table: string, key: string): Promise<boolean> {
    const cached = await this.cache.getRecord(table, key)
    if (cached?.lsn !== 0) {
      return false
    }

    const removed = await this.cache.deleteRecord(table, key)
    if (removed) {
      this.emitRecordDeleted("manual", {
        table,
        key,
        lsn: 0,
        path: cached.path,
      })
    }
    return removed
  }

  private async flushPendingWritesOnce(limit = 100): Promise<FlushPendingWritesResult> {
    const writes = await this.cache.listPendingWrites(limit)
    let committed = 0
    const errors: Array<{ id: string; error: string; retryable: boolean }> = []

    for (const write of writes) {
      const attempt = {
        ...write,
        attempts: write.attempts + 1,
        lastError: undefined,
      } as NextDbPendingWrite
      await this.cache.putPendingWrite(attempt)
      try {
        if (write.type === "sendMessage") {
          await this.commitSendMessage(write)
          await this.cache.deleteRoomMessage(write.roomId, write.id)
        } else if (write.type === "userEvent") {
          await this.commitUserEvent(write)
        } else if (write.type === "userProfileUpsert") {
          await this.commitUserProfileUpsert(write)
        } else if (write.type === "recordUpsert") {
          await this.commitRecordUpsert(write.table, write.key, write.value, write.durability, write.expectedLsn, write.clientMutationId)
        } else if (write.type === "recordDelete") {
          await this.commitRecordDelete(write.table, write.key, write.durability, write.expectedLsn, write.clientMutationId)
        } else if (write.type === "nestedRecordUpsert") {
          await this.commitNestedRecordUpsert(
            write.table,
            write.parentKey,
            write.nested,
            write.nestedKey,
            write.value,
            write.durability,
            write.expectedLsn,
            write.clientMutationId,
          )
        } else if (write.type === "nestedRecordDelete") {
          await this.commitNestedRecordDelete(
            write.table,
            write.parentKey,
            write.nested,
            write.nestedKey,
            write.durability,
            write.expectedLsn,
            write.clientMutationId,
          )
        } else if (write.type === "recordTransaction") {
          await this.commitRecordTransaction(write.operations, {
            durability: write.durability,
            clientMutationId: write.clientMutationId,
          })
        } else if (write.type === "objectPut") {
          await this.commitObjectPut(write.objectId, write.body, write.contentType, write.clientMutationId)
        } else {
          await this.commitObjectDelete(write.objectId, write.force, write.clientMutationId)
        }
        await this.cache.deletePendingWrite(write.id)
        await this.emitPendingWriteChange({
          type: "pendingWriteCommitted",
          source: "sync",
          write,
        })
        committed += 1
      } catch (error) {
        const message = errorMessage(error)
        const retryable = isRetryablePendingWriteError(error)
        const failedWrite = {
          ...attempt,
          lastError: message,
        } as NextDbPendingWrite
        await this.cache.putPendingWrite(failedWrite)
        await this.emitPendingWriteChange({
          type: "pendingWriteFailed",
          source: "sync",
          write: failedWrite,
          error: message,
          retryable,
        })
        errors.push({ id: write.id, error: message, retryable })
        if (isNetworkFailure(error)) {
          break
        }
      }
    }

    return {
      attempted: committed + errors.length,
      committed,
      remaining: (await this.cache.listPendingWrites()).length,
      errors,
    }
  }

  private schedulePendingWriteFlush(delayMs?: number): void {
    const options = this.pendingWriteAutoFlush
    if (!options?.enabled || this.manuallyClosed) {
      return
    }
    if (this.pendingWriteFlushTimer !== undefined) {
      return
    }
    this.pendingWriteFlushTimer = setTimeout(() => {
      this.pendingWriteFlushTimer = undefined
      void this.runPendingWriteAutoFlush()
    }, Math.max(0, Math.floor(delayMs ?? options.intervalMs)))
  }

  private async runPendingWriteAutoFlush(): Promise<void> {
    const options = this.pendingWriteAutoFlush
    if (!options?.enabled || this.manuallyClosed) {
      return
    }
    const pending = await this.cache.listPendingWrites(1)
    if (pending.length === 0) {
      return
    }
    const result = await this.flushPendingWrites(options.limit)
      .catch((error) => ({
        attempted: 0,
        committed: 0,
        remaining: 1,
        errors: [{ id: "flush", error: errorMessage(error), retryable: isRetryablePendingWriteError(error) }],
      }))
    if (result.remaining > 0 && result.errors.some((error) => error.retryable)) {
      this.schedulePendingWriteFlush(options.intervalMs)
    }
  }

  async latestMessages(roomId: string, limitOrOptions: number | (FreshnessOptions & { limit?: number }) = 50): Promise<MessagesResponse> {
    const options = typeof limitOrOptions === "number" ? { limit: limitOrOptions } : limitOrOptions
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    if (requiresReadQuorum(options)) {
      const response = await this.readRoomMessagesWithQuorum(roomId, limit, undefined, options)
      await this.putRoomMessagesCached(roomId, response.messages)
      this.advanceMessages(response.messages)
      return response
    }
    await this.ensureFreshness(options, roomId)
    const cached = await this.cache.getRoomMessages(roomId, limit)
    if (cached.length >= limit && freshnessSatisfied(options, maxLsn(cached))) {
      this.advanceMessages(cached)
      return {
        roomId,
        source: "live",
        messages: cached,
      }
    }

    const response = await this.get<MessagesResponse>(`/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?limit=${limit}`)
    await this.putRoomMessagesCached(roomId, response.messages)
    this.advanceMessages(response.messages)
    return response
  }

  async listCachedRoomMessages(
    roomId: string,
    options: ListCachedRoomMessagesOptions = {},
  ): Promise<MessagesResponse> {
    const limit = normalizePageLimit(options.limit)
    return {
      roomId,
      source: "cache",
      messages: (await this.cache.getRoomMessages(roomId, limit, options.beforeLsn)).slice(0, limit),
    }
  }

  async messagesBefore(roomId: string, beforeLsn: number, limitOrOptions: number | (FreshnessOptions & { limit?: number }) = 50): Promise<MessagesResponse> {
    const options = typeof limitOrOptions === "number" ? { limit: limitOrOptions } : limitOrOptions
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    if (requiresReadQuorum(options)) {
      const response = await this.readRoomMessagesWithQuorum(roomId, limit, beforeLsn, options)
      await this.putRoomMessagesCached(roomId, response.messages)
      this.advanceMessages(response.messages)
      return response
    }
    await this.ensureFreshness(options, roomId)
    const cached = await this.cache.getRoomMessages(roomId, limit, beforeLsn)
    if (cached.length >= limit && freshnessSatisfied(options, maxLsn(cached))) {
      this.advanceMessages(cached)
      return {
        roomId,
        source: "live",
        messages: cached,
      }
    }

    const params = new URLSearchParams({
      limit: String(limit),
      beforeLsn: String(beforeLsn),
    })
    const response = await this.get<MessagesResponse>(`/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?${params}`)
    await this.putRoomMessagesCached(roomId, response.messages)
    this.advanceMessages(response.messages)
    return response
  }

  async upsertRecord<T = unknown>(
    table: string,
    key: string,
    value: T,
    optionsOrDurability: UpsertRecordOptions | Durability = "strict",
  ): Promise<NextDbRecord<T>> {
    const options =
      typeof optionsOrDurability === "string"
        ? { durability: optionsOrDurability }
        : optionsOrDurability
    const durability = options.durability ?? "strict"
    const clientMutationId = options.clientMutationId ?? nextClientId("mutation")
    const expectedLsn = options.expectedLsn ?? (this.offlineWrites
      ? (await this.cache.getRecord(table, key))?.lsn
      : undefined)
    try {
      return await this.commitRecordUpsert(table, key, value, durability, expectedLsn, clientMutationId)
    } catch (error) {
      if (durability === "volatile" || !this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingRecord(table, key, value, durability, expectedLsn, clientMutationId, error)
    }
  }

  async deleteRecord(
    table: string,
    key: string,
    optionsOrDurability: DeleteRecordOptions | Durability = "strict",
  ): Promise<DeleteRecordResponse> {
    const options =
      typeof optionsOrDurability === "string"
        ? { durability: optionsOrDurability }
        : optionsOrDurability
    const durability = options.durability ?? "strict"
    const clientMutationId = options.clientMutationId ?? nextClientId("mutation")
    const expectedLsn = options.expectedLsn ?? (this.offlineWrites
      ? (await this.cache.getRecord(table, key))?.lsn
      : undefined)
    try {
      return await this.commitRecordDelete(table, key, durability, expectedLsn, clientMutationId)
    } catch (error) {
      if (durability === "volatile" || !this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingRecordDelete(table, key, durability, expectedLsn, clientMutationId, error)
    }
  }

  async upsertNestedRecord<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    value: T,
    optionsOrDurability: UpsertRecordOptions | Durability = "strict",
  ): Promise<NextDbRecord<T>> {
    const options =
      typeof optionsOrDurability === "string"
        ? { durability: optionsOrDurability }
        : optionsOrDurability
    const durability = options.durability ?? "strict"
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    const expectedLsn = options.expectedLsn ?? (this.offlineWrites
      ? (await this.cache.getRecord(logicalTable, logicalKey))?.lsn
      : undefined)
    const clientMutationId = options.clientMutationId ?? nextClientId("mutation")
    try {
      return await this.commitNestedRecordUpsert(table, parentKey, nested, nestedKey, value, durability, expectedLsn, clientMutationId)
    } catch (error) {
      if (durability === "volatile" || !this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingNestedRecord(
        table,
        parentKey,
        nested,
        nestedKey,
        value,
        durability,
        expectedLsn,
        clientMutationId,
        error,
      )
    }
  }

  async deleteNestedRecord(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    optionsOrDurability: DeleteRecordOptions | Durability = "strict",
  ): Promise<DeleteRecordResponse> {
    const options =
      typeof optionsOrDurability === "string"
        ? { durability: optionsOrDurability }
        : optionsOrDurability
    const durability = options.durability ?? "strict"
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    const expectedLsn = options.expectedLsn ?? (this.offlineWrites
      ? (await this.cache.getRecord(logicalTable, logicalKey))?.lsn
      : undefined)
    const clientMutationId = options.clientMutationId ?? nextClientId("mutation")
    try {
      return await this.commitNestedRecordDelete(table, parentKey, nested, nestedKey, durability, expectedLsn, clientMutationId)
    } catch (error) {
      if (durability === "volatile" || !this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingNestedRecordDelete(
        table,
        parentKey,
        nested,
        nestedKey,
        durability,
        expectedLsn,
        clientMutationId,
        error,
      )
    }
  }

  async getNestedRecord<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    options: FreshnessOptions = {},
  ): Promise<NextDbRecord<T>> {
    await this.reconcileCacheControl()
    const params = new URLSearchParams()
    setRecordReadConsistencyParams(params, options)
    const query = params.size === 0 ? "" : `?${params}`
    const path = `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(parentKey)}/${encodeURIComponent(nested)}/${encodeURIComponent(nestedKey)}${query}`
    if (requiresReadQuorum(options)) {
      const record = await this.readRecordWithQuorum<T>(
        { key: recordShardKey(table, parentKey) },
        path,
        options,
        `${table}/${parentKey}/${nested}/${nestedKey}`,
      )
      await this.putAuthoritativeRecordsCached([record])
      this.advanceRecord(record)
      return record
    }
    await this.ensureFreshness(options, recordShardKey(table, parentKey))
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    const cached = await this.cache.getRecord<T>(logicalTable, logicalKey)
    if (
      cached !== undefined &&
      !recordReadConsistencyRequiresServer(options) &&
      !this.volatileRecordOverlays.has(recordOverlayKey(logicalTable, logicalKey)) &&
      freshnessSatisfied(options, cached.lsn)
    ) {
      return cached
    }
    const response = await this.get<RecordResponse<T>>(path)
    await this.putAuthoritativeRecordsCached([response.record])
    this.advanceRecord(response.record)
    return response.record
  }

  async getCachedNestedRecord<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
  ): Promise<NextDbRecord<T> | undefined> {
    return this.cache.getRecord<T>(nestedRecordTable(table, nested), nestedRecordKey(parentKey, nestedKey))
  }

  async listNestedRecords<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    limitOrOptions: number | (FreshnessOptions & { limit?: number; afterKey?: string; afterCursor?: string; predicate?: RecordPredicate }) = 50,
    afterKey?: string,
    order: NestedListOrder = "key",
    afterCursor?: string,
  ): Promise<ListRecordsResponse<T>> {
    const options = typeof limitOrOptions === "number" ? { limit: limitOrOptions, afterKey, afterCursor } : limitOrOptions
    await this.reconcileCacheControl()
    await this.ensureFreshness(options, recordShardKey(table, parentKey))
    const limit = normalizePageLimit(options.limit)
    const readAfterKey = options.afterKey
    const readAfterCursor = options.afterCursor
    const logicalTable = nestedRecordTable(table, nested)
    const prefix = nestedRecordPrefix(parentKey)
    const canUseLocalRecordPage = !this.hasVolatileRecordOverlayForTable(logicalTable)
    if (!recordReadConsistencyRequiresServer(options) && canUseLocalRecordPage && order === "key" && options.predicate === undefined) {
      const cached = await this.cache.listRecordsByKeyPrefix<T>(
        logicalTable,
        prefix,
        limit,
        readAfterKey ? nestedRecordKey(parentKey, readAfterKey) : undefined,
      )
      if (cached.length >= limit && freshnessSatisfied(options, maxLsn(cached))) {
        this.advanceRecords(cached)
        return {
          table: logicalTable,
          records: cached,
          nextAfterKey: nestedKeyFromLogicalKey(parentKey, cached.at(-1)?.key),
          hasMore: false,
        }
      }
    } else if (!recordReadConsistencyRequiresServer(options) && canUseLocalRecordPage && options.predicate === undefined) {
      const schemaOrder = await this.nestedSchemaOrder(table, nested)
      if (schemaOrder !== undefined) {
        const cached = await this.cache.listRecordsBySchemaOrder<T>(
          logicalTable,
          prefix,
          schemaOrder,
          limit,
          readAfterCursor,
        )
        if (cached.records.length >= limit && freshnessSatisfied(options, maxLsn(cached.records))) {
          this.advanceRecords(cached.records)
          return {
            table: logicalTable,
            records: cached.records,
            nextAfterKey: nestedKeyFromLogicalKey(parentKey, cached.records.at(-1)?.key),
            nextCursor: cached.nextCursor,
            hasMore: cached.hasMore,
          }
        }
      }
    }

    const params = new URLSearchParams({ limit: String(limit) })
    setRecordReadConsistencyParams(params, options)
    if (readAfterKey !== undefined) {
      params.set("afterKey", readAfterKey)
    }
    if (readAfterCursor !== undefined) {
      params.set("afterCursor", readAfterCursor)
    }
    if (order !== "key") {
      params.set("order", order)
    }
    setRecordPredicateParam(params, options.predicate)
    const response = await this.get<ListRecordsResponse<T>>(
      `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(parentKey)}/${encodeURIComponent(nested)}?${params}`,
    )
    await this.putRecordsCached(response.records)
    this.advanceRecords(response.records)
    return response
  }

  async listCachedNestedRecords<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    options: ListCachedNestedRecordsOptions = {},
  ): Promise<ListRecordsResponse<T>> {
    const limit = normalizePageLimit(options.limit)
    const logicalTable = nestedRecordTable(table, nested)
    const prefix = nestedRecordPrefix(parentKey)
    if (options.order === "schema") {
      const schemaOrder = await this.nestedSchemaOrder(table, nested)
      if (schemaOrder !== undefined) {
        const cached = await this.cache.listRecordsBySchemaOrder<T>(
          logicalTable,
          prefix,
          schemaOrder,
          limit + 1,
          options.afterCursor,
        )
        const page = cached.records.slice(0, limit)
        return {
          table: logicalTable,
          records: page,
          nextAfterKey: nestedKeyFromLogicalKey(parentKey, page.at(-1)?.key),
          nextCursor: cached.nextCursor,
          hasMore: cached.records.length > limit || cached.hasMore,
        }
      }
    }
    const records = await this.cache.listRecordsByKeyPrefix<T>(
      logicalTable,
      prefix,
      limit + 1,
      options.afterKey ? nestedRecordKey(parentKey, options.afterKey) : undefined,
    )
    const page = records.slice(0, limit)
    return {
      table: logicalTable,
      records: page,
      nextAfterKey: nestedKeyFromLogicalKey(parentKey, page.at(-1)?.key),
      hasMore: records.length > limit,
    }
  }

  async recordTransaction<T = unknown>(
    operations: Array<RecordTransactionOperation<T>>,
    options: RecordTransactionOptions = {},
  ): Promise<RecordTransactionResponse<T>> {
    const request = {
      operations,
      durability: options.durability ?? "strict",
      clientMutationId: options.clientMutationId ?? nextClientId("mutation"),
    }
    try {
      return await this.commitRecordTransaction(request.operations, request)
    } catch (error) {
      if (!this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.enqueuePendingRecordTransaction(request, error)
    }
  }

  async recordBatch<T = unknown>(
    operations: Array<RecordTransactionOperation<T>>,
    options: RecordTransactionOptions = {},
  ): Promise<RecordBatchResponse<T>> {
    const request = {
      operations,
      durability: options.durability ?? "strict",
      clientMutationId: options.clientMutationId ?? nextClientId("mutation"),
    }
    try {
      return await this.commitRecordBatch(request.operations, request)
    } catch (error) {
      if (!this.offlineWrites || !isNetworkFailure(error)) {
        throw error
      }
      return this.commitRecordBatchViaTransactions(request.operations, request)
    }
  }

  async getRecord<T = unknown>(table: string, key: string, options: FreshnessOptions = {}): Promise<NextDbRecord<T>> {
    await this.reconcileCacheControl()
    const params = new URLSearchParams()
    setRecordReadConsistencyParams(params, options)
    const query = params.size === 0 ? "" : `?${params}`
    const path = `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(key)}${query}`
    if (requiresReadQuorum(options)) {
      const record = await this.readRecordWithQuorum<T>(
        { table, recordKey: key },
        path,
        options,
        `${table}/${key}`,
      )
      await this.putAuthoritativeRecordsCached([record])
      this.advanceRecord(record)
      return record
    }
    await this.ensureFreshness(options, recordShardKey(table, key))
    const cached = await this.cache.getRecord<T>(table, key)
    if (
      cached !== undefined &&
      !recordReadConsistencyRequiresServer(options) &&
      !this.volatileRecordOverlays.has(recordOverlayKey(table, key)) &&
      freshnessSatisfied(options, cached.lsn)
    ) {
      return cached
    }
    const response = await this.get<RecordResponse<T>>(path)
    await this.putAuthoritativeRecordsCached([response.record])
    this.advanceRecord(response.record)
    return response.record
  }

  async getCachedRecord<T = unknown>(table: string, key: string): Promise<NextDbRecord<T> | undefined> {
    return this.cache.getRecord<T>(table, key)
  }

  async listRecords<T = unknown>(table: string, limitOrOptions: number | PageReadOptions = 50, afterKey?: string): Promise<ListRecordsResponse<T>> {
    const options = typeof limitOrOptions === "number" ? { limit: limitOrOptions, afterKey } : limitOrOptions
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    const params = new URLSearchParams({ limit: String(limit) })
    setRecordReadConsistencyParams(params, options)
    if (options.afterKey !== undefined) {
      params.set("afterKey", options.afterKey)
    }
    setRecordPredicateParam(params, options.predicate)
    if (requiresReadQuorum(options)) {
      const response = await this.readRecordListWithQuorum<T>(
        `/v1/records/${encodeURIComponent(table)}`,
        params,
        table,
        limit,
        options,
      )
      await this.putRecordsCached(response.records)
      this.advanceRecords(response.records)
      return response
    }
    await this.ensureFreshness(options)
    if (!recordReadConsistencyRequiresServer(options) && !this.hasVolatileRecordOverlayForTable(table) && options.predicate === undefined) {
      const cached = await this.cache.listRecords<T>(table, limit, options.afterKey)
      if (cached.length >= limit && freshnessSatisfied(options, maxLsn(cached))) {
        this.advanceRecords(cached)
        return {
          table,
          records: cached,
          nextAfterKey: cached.at(-1)?.key,
          hasMore: false,
        }
      }
    }

    const response = await this.get<ListRecordsResponse<T>>(`/v1/records/${encodeURIComponent(table)}?${params}`)
    await this.putRecordsCached(response.records)
    this.advanceRecords(response.records)
    return response
  }

  async listCachedRecords<T = unknown>(
    table: string,
    options: ListCachedRecordsOptions = {},
  ): Promise<ListRecordsResponse<T>> {
    const limit = normalizePageLimit(options.limit)
    const records = await this.cache.listRecords<T>(table, limit + 1, options.afterKey)
    const page = records.slice(0, limit)
    return {
      table,
      records: page,
      nextAfterKey: page.at(-1)?.key,
      hasMore: records.length > limit,
    }
  }

  async queryRecordsByIndex<T = unknown>(
    table: string,
    indexName: string,
    options: QueryRecordsByIndexOptions,
  ): Promise<ListRecordsResponse<T>> {
    await this.reconcileCacheControl()
    const limit = normalizePageLimit(options.limit)
    const rangeQuery = isIndexRangeQuery(options)
    const params = new URLSearchParams()
    setRecordReadConsistencyParams(params, options)
    setIndexQueryParams(params, options, "queryRecordsByIndex")
    if (options.limit !== undefined) {
      params.set("limit", String(limit))
    }
    if (options.afterKey !== undefined) {
      params.set("afterKey", options.afterKey)
    }
    if (options.afterCursor !== undefined) {
      params.set("afterCursor", options.afterCursor)
    }
    setRecordPredicateParam(params, options.predicate)
    const fields = rangeQuery ? await this.recordIndexFields(table, indexName) : undefined
    if (requiresReadQuorum(options)) {
      let merge: RecordListQuorumMerge = { kind: "key" }
      if (rangeQuery) {
        if (fields === undefined) {
          throw new Error(`NextDB index read quorum requires schema fields for ${table}.${indexName}`)
        }
        merge = { kind: "indexRange", fields }
      }
      const response = await this.readRecordListWithQuorum<T>(
        `/v1/records/${encodeURIComponent(table)}/indexes/${encodeURIComponent(indexName)}`,
        params,
        table,
        limit,
        options,
        merge,
      )
      await this.putRecordsCached(response.records)
      this.advanceRecords(response.records)
      return response
    }
    await this.ensureFreshness(options)
    const resolvedFields = fields ?? await this.recordIndexFields(table, indexName)
    const localQuery = resolvedFields === undefined
      ? undefined
      : localIndexQueryFromOptions(resolvedFields, options, limit, "queryRecordsByIndex")
    if (
      localQuery !== undefined &&
      !recordReadConsistencyRequiresServer(options) &&
      options.predicate === undefined &&
      !this.hasVolatileRecordOverlayForTable(table)
    ) {
      const cached = await this.cache.queryRecordsByIndex<T>(table, {
        ...localQuery,
        afterKey: rangeQuery ? undefined : options.afterKey,
        afterCursor: rangeQuery ? options.afterCursor : undefined,
      })
      if (cached.records.length >= limit && freshnessSatisfied(options, maxLsn(cached.records))) {
        const records = cached.records.slice(0, limit)
        this.advanceRecords(records)
        return {
          table,
          records,
          nextAfterKey: records.at(-1)?.key,
          nextCursor: cached.nextCursor,
          hasMore: cached.hasMore,
        }
      }
    }

    const response = await this.get<ListRecordsResponse<T>>(
      `/v1/records/${encodeURIComponent(table)}/indexes/${encodeURIComponent(indexName)}?${params}`,
    )
    await this.putRecordsCached(response.records)
    this.advanceRecords(response.records)
    return response
  }

  async queryNestedRecordsByIndex<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    indexName: string,
    options: QueryRecordsByIndexOptions,
  ): Promise<ListRecordsResponse<T>> {
    await this.reconcileCacheControl()
    await this.ensureFreshness(options, recordShardKey(table, parentKey))
    const limit = normalizePageLimit(options.limit)
    const logicalTable = nestedRecordTable(table, nested)
    const prefix = nestedRecordPrefix(parentKey)
    const rangeQuery = isIndexRangeQuery(options)
    const fields = await this.recordIndexFields(logicalTable, indexName)
    const localQuery = fields === undefined
      ? undefined
      : localIndexQueryFromOptions(fields, options, limit, "queryNestedRecordsByIndex")
    if (
      localQuery !== undefined &&
      !recordReadConsistencyRequiresServer(options) &&
      options.predicate === undefined &&
      !this.hasVolatileRecordOverlayForTable(logicalTable)
    ) {
      const cached = await this.cache.queryRecordsByIndex<T>(logicalTable, {
        ...localQuery,
        keyPrefix: prefix,
        afterKey: rangeQuery || options.afterKey === undefined ? undefined : nestedRecordKey(parentKey, options.afterKey),
        afterCursor: rangeQuery ? options.afterCursor : undefined,
      })
      if (cached.records.length >= limit && freshnessSatisfied(options, maxLsn(cached.records))) {
        const records = cached.records.slice(0, limit)
        this.advanceRecords(records)
        return {
          table: logicalTable,
          records,
          nextAfterKey: nestedKeyFromLogicalKey(parentKey, records.at(-1)?.key),
          nextCursor: cached.nextCursor,
          hasMore: cached.hasMore,
        }
      }
    }

    const params = new URLSearchParams()
    setRecordReadConsistencyParams(params, options)
    setIndexQueryParams(params, options, "queryNestedRecordsByIndex")
    if (options.limit !== undefined) {
      params.set("limit", String(limit))
    }
    if (options.afterKey !== undefined) {
      params.set("afterKey", options.afterKey)
    }
    if (options.afterCursor !== undefined) {
      params.set("afterCursor", options.afterCursor)
    }
    setRecordPredicateParam(params, options.predicate)
    const response = await this.get<ListRecordsResponse<T>>(
      `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(parentKey)}/${encodeURIComponent(nested)}/indexes/${encodeURIComponent(indexName)}?${params}`,
    )
    await this.putRecordsCached(response.records)
    this.advanceRecords(response.records)
    return response
  }

  subscribeRoom(
    roomId: string,
    listener: (event: RoomDeliveryEvent) => void,
    options: SubscriptionOptions = {},
  ): () => void {
    const listeners = this.roomListeners.get(roomId) ?? new Set()
    listeners.add(listener)
    this.roomListeners.set(roomId, listeners)
    this.ensureSocket()
    void this.sendRoomSubscription(roomId, options)
    if (options.persistent) {
      this.persistentRoomSubscriptions.set(roomId, subscriptionOptionsForStorage(options))
      void this.storeSubscription({
        id: storedRoomSubscriptionId(roomId),
        kind: "room",
        roomId,
        options: subscriptionOptionsForStorage(options),
      })
    }

    return () => {
      const current = this.roomListeners.get(roomId)
      current?.delete(listener)
      if (!current || current.size === 0) {
        this.roomListeners.delete(roomId)
        this.pendingRoomSubscriptions.delete(roomId)
        this.persistentRoomSubscriptions.delete(roomId)
        if (options.persistent) {
          void this.cache.deleteSubscription(storedRoomSubscriptionId(roomId))
        }
        this.sendWhenReady({
          type: "unsubscribeRoom",
          roomId,
        })
      }
    }
  }

  subscribeTable(
    table: string,
    listener: (event: TableDeliveryEvent) => void,
    options: SubscriptionOptions = {},
  ): () => void {
    const targetId = tableSubscriptionTargetId(table, options)
    const storedOptions = subscriptionOptionsForStorage(options)
    const listenerEntry = { listener, options: storedOptions }
    const listeners = this.tableListeners.get(table) ?? new Set()
    listeners.add(listenerEntry)
    this.tableListeners.set(table, listeners)
    this.ensureSocket()
    void this.sendTableSubscription(table, options)
    const active = this.activeTableSubscriptions.get(targetId)
    this.activeTableSubscriptions.set(targetId, {
      table,
      options: storedOptions,
      count: (active?.count ?? 0) + 1,
    })
    if (options.persistent) {
      this.persistentTableSubscriptions.set(targetId, {
        table,
        options: storedOptions,
      })
      void this.storeSubscription({
        id: storedTableSubscriptionId(targetId),
        kind: "table",
        table,
        options: storedOptions,
      })
    }

    return () => {
      const current = this.tableListeners.get(table)
      current?.delete(listenerEntry)
      const active = this.activeTableSubscriptions.get(targetId)
      if (active && active.count > 1) {
        this.activeTableSubscriptions.set(targetId, {
          ...active,
          count: active.count - 1,
        })
      } else {
        this.activeTableSubscriptions.delete(targetId)
      }
      if (!current || current.size === 0) {
        this.tableListeners.delete(table)
      }
      this.pendingTableSubscriptions.delete(targetId)
      if (options.persistent) {
        this.persistentTableSubscriptions.delete(targetId)
        void this.cache.deleteSubscription(storedTableSubscriptionId(targetId))
      }
      if (!this.activeTableSubscriptions.has(targetId) && !this.persistentTableSubscriptions.has(targetId)) {
        this.sendWhenReady(unsubscribeTableFrame(table, storedOptions))
      }
    }
  }

  subscribeNestedTable(
    table: string,
    parentKey: string,
    nested: string,
    listener: (event: TableDeliveryEvent) => void,
    options: SubscriptionOptions = {},
  ): () => void {
    const logicalTable = nestedRecordTable(table, nested)
    const prefix = nestedRecordPrefix(parentKey)
    const subscriptionId = storedNestedTableSubscriptionId(table, parentKey, nested)
    const storedOptions = subscriptionOptionsForStorage(options)
    const wrappedListener = (event: TableDeliveryEvent) => {
      if (event.key.startsWith(prefix)) {
        listener(event)
      }
    }
    const listenerEntry = { listener: wrappedListener, options: {} }
    const listeners = this.tableListeners.get(logicalTable) ?? new Set()
    listeners.add(listenerEntry)
    this.tableListeners.set(logicalTable, listeners)
    this.ensureSocket()
    void this.sendNestedTableSubscription(table, parentKey, nested, options)

    const active = this.activeNestedTableSubscriptions.get(subscriptionId)
    this.activeNestedTableSubscriptions.set(subscriptionId, {
      table,
      parentKey,
      nested,
      logicalTable,
      options: storedOptions,
      count: (active?.count ?? 0) + 1,
    })

    if (options.persistent) {
      this.persistentNestedTableSubscriptions.set(subscriptionId, {
        table,
        parentKey,
        nested,
        logicalTable,
        options: storedOptions,
      })
      void this.storeSubscription({
        id: subscriptionId,
        kind: "nestedTable",
        table,
        parentKey,
        nested,
        options: storedOptions,
      })
    }

    return () => {
      const current = this.tableListeners.get(logicalTable)
      current?.delete(listenerEntry)
      if (!current || current.size === 0) {
        this.tableListeners.delete(logicalTable)
      }
      const active = this.activeNestedTableSubscriptions.get(subscriptionId)
      if (active && active.count > 1) {
        this.activeNestedTableSubscriptions.set(subscriptionId, {
          ...active,
          count: active.count - 1,
        })
      } else {
        this.activeNestedTableSubscriptions.delete(subscriptionId)
      }
      if (options.persistent) {
        this.persistentNestedTableSubscriptions.delete(subscriptionId)
        void this.cache.deleteSubscription(subscriptionId)
      }
      this.pendingNestedTableSubscriptions.delete(subscriptionId)
      if (!this.activeNestedTableSubscriptions.has(subscriptionId) && !this.persistentNestedTableSubscriptions.has(subscriptionId)) {
        this.sendWhenReady({
          type: "unsubscribeNestedTable",
          table,
          parentKey,
          nested,
        })
      }
    }
  }

  subscribeQuery<T = unknown>(
    options: RecordLiveQueryOptions,
    listener: (event: RecordLiveQueryResult<T>) => void,
  ): () => void {
    const queryId = options.queryId ?? nextClientId("query")
    const subscription = subscribeQueryFrame({ ...options, queryId })
    const listeners = this.queryListeners.get(queryId) ?? new Set()
    listeners.add(listener as (event: RecordLiveQueryResult) => void)
    this.queryListeners.set(queryId, listeners)
    this.querySubscriptions.set(queryId, subscription)
    this.ensureSocket()
    this.sendWhenReady(subscription)
    if (options.persistent) {
      this.persistentQuerySubscriptions.set(queryId, subscription)
      void this.storeSubscription({
        id: storedQuerySubscriptionId(queryId),
        kind: "query",
        query: subscription,
      })
    }

    return () => {
      const current = this.queryListeners.get(queryId)
      current?.delete(listener as (event: RecordLiveQueryResult) => void)
      if (!current || current.size === 0) {
        this.queryListeners.delete(queryId)
        this.querySubscriptions.delete(queryId)
        this.queryResults.delete(queryId)
        this.pendingQuerySubscriptions.delete(queryId)
        this.persistentQuerySubscriptions.delete(queryId)
        if (options.persistent) {
          void this.cache.deleteSubscription(storedQuerySubscriptionId(queryId))
        }
        this.sendWhenReady({
          type: "unsubscribeQuery",
          queryId,
        })
      }
    }
  }

  subscribeAggregateCount(
    table: string,
    listener: (event: AggregateCountEvent) => void,
  ): () => void {
    const listeners = this.aggregateCountListeners.get(table) ?? new Set()
    listeners.add(listener)
    this.aggregateCountListeners.set(table, listeners)
    this.ensureSocket()
    this.sendWhenReady({ type: "subscribeAggregateCount", table })

    return () => {
      const current = this.aggregateCountListeners.get(table)
      current?.delete(listener)
      if (!current || current.size === 0) {
        this.aggregateCountListeners.delete(table)
        this.pendingAggregateCountSubscriptions.delete(table)
        this.sendWhenReady({ type: "unsubscribeAggregateCount", table })
      }
    }
  }

  subscribeAggregateSum(
    table: string,
    field: string,
    listener: (event: AggregateSumEvent) => void,
  ): () => void {
    const subscriptionId = aggregateSumSubscriptionId(table, field)
    const listeners = this.aggregateSumListeners.get(subscriptionId) ?? new Set()
    listeners.add(listener)
    this.aggregateSumListeners.set(subscriptionId, listeners)
    this.ensureSocket()
    this.sendWhenReady({ type: "subscribeAggregateSum", table, field })

    return () => {
      const current = this.aggregateSumListeners.get(subscriptionId)
      current?.delete(listener)
      if (!current || current.size === 0) {
        this.aggregateSumListeners.delete(subscriptionId)
        this.pendingAggregateSumSubscriptions.delete(subscriptionId)
        this.sendWhenReady({ type: "unsubscribeAggregateSum", table, field })
      }
    }
  }

  subscribeAggregatePresence(
    channelId: string,
    listener: (event: AggregatePresenceEvent) => void,
  ): () => void {
    const listeners = this.aggregatePresenceListeners.get(channelId) ?? new Set()
    listeners.add(listener)
    this.aggregatePresenceListeners.set(channelId, listeners)
    this.ensureSocket()
    this.sendWhenReady({ type: "subscribeAggregatePresence", channelId })

    return () => {
      const current = this.aggregatePresenceListeners.get(channelId)
      current?.delete(listener)
      if (!current || current.size === 0) {
        this.aggregatePresenceListeners.delete(channelId)
        this.pendingAggregatePresenceSubscriptions.delete(channelId)
        this.sendWhenReady({ type: "unsubscribeAggregatePresence", channelId })
      }
    }
  }

  watchRoomMessages(
    roomId: string,
    listener: (snapshot: RoomMessagesSnapshot) => void,
    options: WatchOptions = {},
  ): () => void {
    const limit = normalizePageLimit(options.limit)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.cache.getRoomMessages(roomId, limit)
        .then((messages) => {
          if (!closed) {
            listener({ roomId, messages, source, change })
          }
        })
        .catch((error) => console.error("nextdb room watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (change.type === "messageUpserted" && change.roomId === roomId) {
        emit(change.source, change)
      }
      if (change.type === "roomInvalidated" && change.roomId === roomId) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    const stopSubscription = this.subscribeRoom(roomId, () => undefined, options)
    return () => {
      closed = true
      stopCache()
      stopSubscription()
    }
  }

  watchTableRecords<T = unknown>(
    table: string,
    listener: (snapshot: TableRecordsSnapshot<T>) => void,
    options: WatchOptions = {},
  ): () => void {
    const limit = normalizePageLimit(options.limit)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.listCachedTableRecords<T>(table, options, limit)
        .then((records) => {
          if (!closed) {
            listener({ table, records, source, change })
          }
        })
        .catch((error) => console.error("nextdb table watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if ((change.type === "recordUpserted" || change.type === "recordDeleted") && change.table === table) {
        emit(change.source, change)
      }
      if (change.type === "tableInvalidated" && change.table === table) {
        emit(change.source, change)
      }
      if (change.type === "tableSnapshotApplied" && change.table === table) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    let stopSubscription: (() => void) | undefined
    this.startHydratedTableSubscription(
      table,
      options,
      () => !closed,
      () => emit("sync"),
      (stop) => {
        stopSubscription = stop
      },
    )
    return () => {
      closed = true
      stopCache()
      stopSubscription?.()
    }
  }

  watchRecord<T = unknown>(
    table: string,
    key: string,
    listener: (snapshot: RecordSnapshot<T>) => void,
    options: WatchOptions = {},
  ): () => void {
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.cache.getRecord<T>(table, key)
        .then((record) => {
          if (!closed) {
            listener({ table, key, record, source, change })
          }
        })
        .catch((error) => console.error("nextdb record watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if ((change.type === "recordUpserted" || change.type === "recordDeleted") && change.table === table && change.key === key) {
        emit(change.source, change)
      }
      if (change.type === "tableInvalidated" && change.table === table) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    const stopSubscription = this.subscribeTable(table, () => undefined, options)
    return () => {
      closed = true
      stopCache()
      stopSubscription()
    }
  }

  watchNestedRecords<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    listener: (snapshot: TableRecordsSnapshot<T>) => void,
    options: WatchOptions = {},
  ): () => void {
    const logicalTable = nestedRecordTable(table, nested)
    const keyPrefix = nestedRecordPrefix(parentKey)
    const limit = normalizePageLimit(options.limit)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.cache.listRecordsByKeyPrefix<T>(logicalTable, keyPrefix, limit)
        .then((records) => {
          if (!closed) {
            listener({ table: logicalTable, records, source, change })
          }
        })
        .catch((error) => console.error("nextdb nested table watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (
        (change.type === "recordUpserted" || change.type === "recordDeleted") &&
        change.table === logicalTable &&
        change.key.startsWith(keyPrefix)
      ) {
        emit(change.source, change)
      }
      if (change.type === "tableInvalidated" && change.table === logicalTable) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    let stopSubscription: (() => void) | undefined
    this.startHydratedNestedTableSubscription(
      table,
      parentKey,
      nested,
      options,
      () => !closed,
      () => emit("sync"),
      (stop) => {
        stopSubscription = stop
      },
    )
    return () => {
      closed = true
      stopCache()
      stopSubscription?.()
    }
  }

  private async listCachedTableRecords<T>(
    table: string,
    options: SubscriptionOptions,
    limit: number,
  ): Promise<Array<NextDbRecord<T>>> {
    if (options.keyRange?.lowerKey === undefined && options.keyRange?.upperKey === undefined) {
      return this.cache.listRecords<T>(table, limit)
    }
    const records = await this.cache.listRecords<T>(table, Number.MAX_SAFE_INTEGER)
    return records
      .filter((record) => tableRecordMatchesSubscription(record, options))
      .slice(0, limit)
  }

  private startHydratedTableSubscription(
    table: string,
    options: WatchOptions,
    isActive: () => boolean,
    onHydrated: () => void,
    setStop: (stop: () => void) => void,
  ): void {
    const startSubscription = () => {
      if (isActive()) {
        setStop(this.subscribeTable(table, () => undefined, options))
      }
    }
    if (options.hydrate === false) {
      startSubscription()
      return
    }
    if (tableSubscriptionSupportsServerSnapshot(options)) {
      startSubscription()
      return
    }
    void this.syncTable(table, {
      limit: options.hydrateLimit,
      maxPages: options.hydrateMaxPages,
    })
      .then(() => {
        if (isActive()) {
          onHydrated()
        }
      })
      .catch((error) => console.error("nextdb table watcher hydration failed", error))
      .finally(startSubscription)
  }

  private startHydratedNestedTableSubscription(
    table: string,
    parentKey: string,
    nested: string,
    options: WatchOptions,
    isActive: () => boolean,
    onHydrated: () => void,
    setStop: (stop: () => void) => void,
  ): void {
    const startSubscription = () => {
      if (isActive()) {
        setStop(this.subscribeNestedTable(table, parentKey, nested, () => undefined, options))
      }
    }
    if (options.hydrate === false) {
      startSubscription()
      return
    }
    if (options.serverSnapshot === true) {
      startSubscription()
      return
    }
    const target = { table, parentKey, nested }
    void this.hydrateCursorsFor({ nestedTables: [target] })
      .then(() => this.syncUntilCaughtUp({
        afterLsn: this.nestedTableSeenLsn.get(nestedTableCursorId(table, parentKey, nested)) ?? 0,
        nestedTables: [target],
        limit: options.hydrateLimit,
        maxPages: options.hydrateMaxPages,
      }))
      .then(() => {
        if (isActive()) {
          onHydrated()
        }
      })
      .catch((error) => console.error("nextdb nested table watcher hydration failed", error))
      .finally(startSubscription)
  }

  watchNestedRecord<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    listener: (snapshot: RecordSnapshot<T>) => void,
    options: WatchOptions = {},
  ): () => void {
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.cache.getRecord<T>(logicalTable, logicalKey)
        .then((record) => {
          if (!closed) {
            listener({ table: logicalTable, key: logicalKey, record, source, change })
          }
        })
        .catch((error) => console.error("nextdb nested record watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if ((change.type === "recordUpserted" || change.type === "recordDeleted") && change.table === logicalTable && change.key === logicalKey) {
        emit(change.source, change)
      }
      if (change.type === "tableInvalidated" && change.table === logicalTable) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    const stopSubscription = this.subscribeNestedTable(table, parentKey, nested, () => undefined, options)
    return () => {
      closed = true
      stopCache()
      stopSubscription()
    }
  }

  watchCurrentUserEvents(
    listener: (snapshot: UserEventsSnapshot) => void,
    options: WatchOptions = {},
  ): () => void {
    const userId = this.requireUserId("watchCurrentUserEvents")
    const limit = normalizePageLimit(options.limit)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.cache.getUserEvents(userId, limit)
        .then((events) => {
          if (!closed) {
            listener({ userId, events, source, change })
          }
        })
        .catch((error) => console.error("nextdb user event watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (change.type === "userEventUpserted" && change.userId === userId) {
        emit(change.source, change)
      }
      if (change.type === "userInvalidated" && change.userId === userId) {
        emit(change.source, change)
      }
      if (change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    const stopSubscription = this.onUserEvent(() => undefined, options)
    return () => {
      closed = true
      stopCache()
      stopSubscription()
    }
  }

  watchObjects(
    listener: (snapshot: ObjectListSnapshot) => void,
    options: WatchOptions = {},
  ): () => void {
    const limit = normalizePageLimit(options.limit)
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      this.listCachedObjects({ limit })
        .then((page) => {
          if (!closed) {
            listener({ ...page, source, change })
          }
        })
        .catch((error) => console.error("nextdb object watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if (change.type === "objectUpserted" || change.type === "objectDeleted" || change.type === "objectsInvalidated" || change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    const stopSubscription = this.subscribeObjects(() => undefined, options)
    return () => {
      closed = true
      stopCache()
      stopSubscription()
    }
  }

  watchObject(
    objectId: string,
    listener: (snapshot: ObjectSnapshot) => void,
    options: ObjectWatchOptions = {},
  ): () => void {
    let closed = false
    const emit = (source: CacheSnapshotSource, change?: NextDbCacheChange) => {
      Promise.all([
        this.cache.getObjectMetadata(objectId),
        this.cache.getObjectBody(objectId),
      ])
        .then(([metadata, cachedBody]) => {
          if (!closed) {
            listener({
              objectId,
              metadata,
              cachedBodyAvailable: cachedBody !== undefined,
              cachedBody: options.includeBody === true ? cachedBody : undefined,
              source,
              change,
            })
          }
        })
        .catch((error) => console.error("nextdb object detail watcher failed", error))
    }
    if (options.immediate ?? true) {
      emit("cache")
    }
    const stopCache = this.onCacheChange((change) => {
      if ((change.type === "objectUpserted" || change.type === "objectDeleted") && change.objectId === objectId) {
        emit(change.source, change)
      }
      if (change.type === "objectsInvalidated" || change.type === "allInvalidated" || change.type === "cacheProfileEnforced") {
        emit(change.source, change)
      }
    })
    const stopSubscription = this.subscribeObjects(() => undefined, options)
    return () => {
      closed = true
      stopCache()
      stopSubscription()
    }
  }

  onUserEvent(listener: (event: UserDeliveryEvent) => void, options: SubscriptionOptions = {}): () => void {
    if (!this.userId) {
      throw new Error("onUserEvent requires userId in NextDbClientOptions")
    }
    const needsSubscription = this.userListeners.size === 0
    this.userListeners.add(listener)
    this.ensureSocket()
    if (needsSubscription) {
      void this.sendUserSubscription(options)
    }
    if (options.persistent) {
      const storedOptions = subscriptionOptionsForStorage(options)
      this.persistentUserSubscription = { userId: this.userId, options: storedOptions }
      void this.storeSubscription({
        id: storedUserSubscriptionId(this.userId),
        kind: "userEvents",
        userId: this.userId,
        options: storedOptions,
      })
    }
    return () => {
      this.userListeners.delete(listener)
      if (this.userListeners.size === 0) {
        if (options.persistent && this.userId) {
          this.persistentUserSubscription = undefined
          void this.cache.deleteSubscription(storedUserSubscriptionId(this.userId))
        }
        if (!this.persistentUserSubscription) {
          this.sendWhenReady({ type: "unsubscribeUserEvents" })
          this.userSubscriptionActive = false
          this.pendingUserSubscription = undefined
        }
      }
    }
  }

  subscribeObjects(listener: (event: ObjectDeliveryEvent) => void, options: SubscriptionOptions = {}): () => void {
    const needsSubscription = this.objectListeners.size === 0
    this.objectListeners.add(listener)
    this.ensureSocket()
    if (needsSubscription) {
      void this.sendObjectSubscription(options)
    }
    if (options.persistent) {
      const storedOptions = subscriptionOptionsForStorage(options)
      this.persistentObjectSubscription = storedOptions
      void this.storeSubscription({
        id: storedObjectSubscriptionId(),
        kind: "objects",
        options: storedOptions,
      })
    }
    return () => {
      this.objectListeners.delete(listener)
      if (this.objectListeners.size === 0) {
        this.sendWhenReady({ type: "unsubscribeObjects" })
        this.objectSubscriptionActive = false
        this.persistentObjectSubscription = undefined
        if (options.persistent) {
          void this.cache.deleteSubscription(storedObjectSubscriptionId())
        }
      }
    }
  }

  onConnectionEvent(listener: (event: ConnectionEvent) => void): () => void {
    const needsSubscription = this.connectionEventListeners.size === 0
    this.connectionEventListeners.add(listener)
    this.ensureSocket()
    if (needsSubscription) {
      this.sendWhenReady({ type: "subscribeConnectionEvents" })
    }
    return () => {
      this.connectionEventListeners.delete(listener)
      if (this.connectionEventListeners.size === 0) {
        this.sendWhenReady({ type: "unsubscribeConnectionEvents" })
        this.connectionEventsSubscriptionActive = false
      }
    }
  }

  updateConnectionMetadata(metadata: unknown = {}): void {
    this.connectionMetadata = metadata === undefined ? {} : metadata
    this.sendWhenReady({
      type: "updateConnectionMetadata",
      metadata: this.connectionMetadata,
    })
  }

  private async activateStoredSubscription(subscription: NextDbStoredSubscription): Promise<void> {
    if (subscription.kind === "room") {
      this.persistentRoomSubscriptions.set(subscription.roomId, subscription.options)
      void this.sendRoomSubscription(
        subscription.roomId,
        subscription.options,
        () => this.persistentRoomSubscriptions.has(subscription.roomId),
      )
      return
    }
    if (subscription.kind === "table") {
      const targetId = tableSubscriptionTargetId(subscription.table, subscription.options)
      this.persistentTableSubscriptions.set(targetId, {
        table: subscription.table,
        options: subscription.options,
      })
      void this.sendTableSubscription(
        subscription.table,
        subscription.options,
        () => this.persistentTableSubscriptions.has(targetId),
      )
      return
    }
    if (subscription.kind === "nestedTable") {
      const logicalTable = nestedRecordTable(subscription.table, subscription.nested)
      this.persistentNestedTableSubscriptions.set(subscription.id, {
        table: subscription.table,
        parentKey: subscription.parentKey,
        nested: subscription.nested,
        logicalTable,
        options: subscription.options,
      })
      void this.sendNestedTableSubscription(
        subscription.table,
        subscription.parentKey,
        subscription.nested,
        subscription.options,
        () => this.persistentNestedTableSubscriptions.has(subscription.id),
      )
      return
    }
    if (subscription.kind === "query") {
      this.persistentQuerySubscriptions.set(subscription.query.queryId, subscription.query)
      const query = await this.hydrateStoredQueryBaseline(subscription.query)
      if (!this.persistentQuerySubscriptions.has(query.queryId)) {
        return
      }
      this.persistentQuerySubscriptions.set(query.queryId, query)
      this.pendingQuerySubscriptions.set(query.queryId, query)
      this.sendWhenReady(query)
      return
    }
    if (subscription.kind === "userEvents") {
      if (!this.userId || subscription.userId !== this.userId) {
        return
      }
      this.persistentUserSubscription = {
        userId: subscription.userId,
        options: subscription.options,
      }
      void this.sendUserSubscription(
        subscription.options,
        () => this.persistentUserSubscription?.userId === subscription.userId,
      )
      return
    }
    if (subscription.kind === "objects") {
      this.persistentObjectSubscription = subscription.options
      void this.sendObjectSubscription(
        subscription.options,
        () => this.persistentObjectSubscription !== undefined,
      )
    }
  }

  private async storeSubscription(
    subscription: NextDbStoredSubscriptionDraft,
  ): Promise<void> {
    const existing = (await this.cache.listSubscriptions()).find((row) => row.id === subscription.id)
    const now = Date.now()
    await this.cache.putSubscription({
      ...subscription,
      createdAtMs: existing?.createdAtMs ?? now,
      updatedAtMs: now,
    } as NextDbStoredSubscription)
  }

  private async hydrateStoredQueryBaseline(
    query: Extract<ClientFrame, { type: "subscribeQuery" }>,
  ): Promise<Extract<ClientFrame, { type: "subscribeQuery" }>> {
    if (query.resultId === undefined || query.diff === false) {
      return query
    }
    const baseline = await this.queryResponseFromCache(query)
    if (!baseline) {
      return { ...query, resultId: undefined }
    }
    this.queryResults.set(query.queryId, baseline)
    return query
  }

  private async queryResponseFromCache(
    query: Extract<ClientFrame, { type: "subscribeQuery" }>,
  ): Promise<ListRecordsResponse | undefined> {
    if (query.predicate !== undefined) {
      return undefined
    }
    const limit = normalizePageLimit(query.limit)
    const table = query.nested ? nestedRecordTable(query.table, query.nested) : query.table
    const parentKey = query.parentKey
    const prefix = parentKey === undefined ? undefined : nestedRecordPrefix(parentKey)
    if (this.hasVolatileRecordOverlayForTable(table)) {
      return undefined
    }

    if (query.indexName !== undefined) {
      const fields = await this.recordIndexFields(table, query.indexName)
      if (fields === undefined) {
        return undefined
      }
      const localQuery = localIndexQueryFromFrame(fields, query, limit)
      if (localQuery === undefined) {
        return undefined
      }
      const localQueryForCache: LocalIndexQuery = {
        ...localQuery,
        keyPrefix: prefix,
        afterKey: prefix === undefined || localQuery.afterKey === undefined
          ? localQuery.afterKey
          : nestedRecordKey(parentKey ?? "", localQuery.afterKey),
      }
      const cached = await this.cache.queryRecordsByIndex(table, {
        ...localQueryForCache,
      })
      if (cached.records.length < limit) {
        return undefined
      }
      return {
        table,
        records: cached.records.slice(0, limit),
        nextAfterKey: parentKey ? nestedKeyFromLogicalKey(parentKey, cached.records.at(limit - 1)?.key) : cached.records.at(limit - 1)?.key,
        nextCursor: cached.nextCursor,
        hasMore: cached.hasMore,
      }
    }

    if (prefix !== undefined) {
      const nestedParentKey = parentKey ?? ""
      if (query.order === "schema") {
        const schemaOrder = await this.nestedSchemaOrder(query.table, query.nested ?? "")
        if (schemaOrder === undefined) {
          return undefined
        }
        const cached = await this.cache.listRecordsBySchemaOrder(
          table,
          prefix,
          schemaOrder,
          limit,
          query.afterCursor,
        )
        if (cached.records.length < limit) {
          return undefined
        }
        return {
          table,
          records: cached.records,
          nextAfterKey: nestedKeyFromLogicalKey(nestedParentKey, cached.records.at(-1)?.key),
          nextCursor: cached.nextCursor,
          hasMore: cached.hasMore,
        }
      }
      const cached = await this.cache.listRecordsByKeyPrefix(
        table,
        prefix,
        limit,
        query.afterKey ? nestedRecordKey(nestedParentKey, query.afterKey) : undefined,
      )
      if (cached.length < limit) {
        return undefined
      }
      return {
        table,
        records: cached,
        nextAfterKey: nestedKeyFromLogicalKey(nestedParentKey, cached.at(-1)?.key),
        hasMore: false,
      }
    }

    const cached = await this.cache.listRecords(table, limit, query.afterKey)
    if (cached.length < limit) {
      return undefined
    }
    return {
      table,
      records: cached,
      nextAfterKey: cached.at(-1)?.key,
      hasMore: false,
    }
  }

  private async sendRoomSubscription(
    roomId: string,
    options: SubscriptionOptions,
    shouldSend: () => boolean = () => true,
  ): Promise<void> {
    if (!shouldSend()) {
      return
    }
    const catchUp = options.catchUp ?? true
    if (!catchUp) {
      this.sendWhenReady({
        type: "subscribeRoom",
        roomId,
      })
      return
    }

    await this.hydrateCursorsFor({ rooms: [roomId] })
    if (!shouldSend()) {
      return
    }
    this.sendWhenReady({
      type: "subscribeRoom",
      roomId,
      afterLsn: this.roomSeenLsn.get(roomId) ?? 0,
      catchUpLimit: options.catchUpLimit,
    })
  }

  private async sendTableSubscription(
    table: string,
    options: SubscriptionOptions,
    shouldSend: () => boolean = () => true,
  ): Promise<void> {
    if (!shouldSend()) {
      return
    }
    if (tableSubscriptionSupportsServerSnapshot(options)) {
      this.sendWhenReady(tableSubscriptionFrame(table, options))
      return
    }
    const catchUp = options.catchUp ?? true
    if (!catchUp) {
      this.sendWhenReady(tableSubscriptionFrame(table, options))
      return
    }

    await this.hydrateCursorsFor({ tables: [table] })
    if (!shouldSend()) {
      return
    }
    this.sendWhenReady({
      ...tableSubscriptionFrame(table, options),
      afterLsn: this.tableSeenLsn.get(table) ?? 0,
      catchUpLimit: options.catchUpLimit,
    })
  }

  private async sendNestedTableSubscription(
    table: string,
    parentKey: string,
    nested: string,
    options: SubscriptionOptions,
    shouldSend: () => boolean = () => true,
  ): Promise<void> {
    if (!shouldSend()) {
      return
    }
    if (options.serverSnapshot === true) {
      this.sendWhenReady({
        type: "subscribeNestedTable",
        table,
        parentKey,
        nested,
        snapshotLimit: normalizePageLimit(options.snapshotLimit),
      })
      return
    }
    const catchUp = options.catchUp ?? true
    if (!catchUp) {
      this.sendWhenReady({
        type: "subscribeNestedTable",
        table,
        parentKey,
        nested,
      })
      return
    }

    const cursorId = nestedTableCursorId(table, parentKey, nested)
    await this.hydrateCursorsFor({ nestedTables: [{ table, parentKey, nested }] })
    if (!shouldSend()) {
      return
    }
    this.sendWhenReady({
      type: "subscribeNestedTable",
      table,
      parentKey,
      nested,
      afterLsn: this.nestedTableSeenLsn.get(cursorId) ?? 0,
      catchUpLimit: options.catchUpLimit,
    })
  }

  private async sendUserSubscription(
    options: SubscriptionOptions,
    shouldSend: () => boolean = () => true,
  ): Promise<void> {
    if (!this.userId) {
      return
    }
    if (!shouldSend()) {
      return
    }
    const catchUp = options.catchUp ?? true
    if (!catchUp) {
      this.sendWhenReady({
        type: "subscribeUserEvents",
      })
      return
    }

    await this.hydrateCursorsFor({ users: [this.userId] })
    if (!shouldSend()) {
      return
    }
    this.sendWhenReady({
      type: "subscribeUserEvents",
      afterLsn: this.userSeenLsn.get(this.userId) ?? 0,
      catchUpLimit: options.catchUpLimit,
    })
  }

  private async sendObjectSubscription(
    options: SubscriptionOptions,
    shouldSend: () => boolean = () => true,
  ): Promise<void> {
    if (!shouldSend()) {
      return
    }
    const catchUp = options.catchUp ?? true
    if (!catchUp) {
      this.sendWhenReady({
        type: "subscribeObjects",
      })
      return
    }

    await this.hydrateCursorsFor({ objects: true })
    if (!shouldSend()) {
      return
    }
    this.sendWhenReady({
      type: "subscribeObjects",
      afterLsn: this.objectSeenLsn,
      catchUpLimit: options.catchUpLimit,
    })
  }

  close(): void {
    this.manuallyClosed = true
    if (this.pendingWriteFlushTimer !== undefined) {
      clearTimeout(this.pendingWriteFlushTimer)
      this.pendingWriteFlushTimer = undefined
    }
    this.pendingRoomSubscriptions.clear()
    this.pendingTableSubscriptions.clear()
    this.pendingNestedTableSubscriptions.clear()
    this.pendingUserSubscription = undefined
    this.pendingObjectSubscription = undefined
    this.pendingAggregateCountSubscriptions.clear()
    this.pendingAggregateSumSubscriptions.clear()
    this.pendingAggregatePresenceSubscriptions.clear()
    this.joinedRealtimeChannels.clear()
    this.userSubscriptionActive = false
    this.objectSubscriptionActive = false
    this.connectionEventsSubscriptionActive = false
    this.transport?.close()
    this.transport = undefined
  }

  private useRealtimeTransportKind(kind: NextDbRealtimeTransportKind | "custom"): void {
    if (kind === "custom" && this.configuredRealtimeTransportKind !== "custom") {
      throw new Error("custom realtime transport requires NextDbClientOptions.realtimeTransport")
    }
    const factory = kind === this.configuredRealtimeTransportKind
      ? this.configuredRealtimeTransportFactory
      : defaultRealtimeTransportFactory(kind === "custom" ? "websocket" : kind)
    const changed = kind !== this.realtimeTransportKind || factory !== this.realtimeTransportFactory
    this.realtimeTransportKind = kind
    this.realtimeTransportFactory = factory
    if (!changed) {
      return
    }
    const transport = this.transport
    if (transport && transport.state !== "closed") {
      transport.close()
    }
    if (this.transport === transport) {
      this.transport = undefined
    }
    this.userSubscriptionActive = false
    this.objectSubscriptionActive = false
    this.connectionEventsSubscriptionActive = false
  }

  private ensureSocket(): void {
    this.manuallyClosed = false
    if (this.transport && this.transport.state !== "closed") {
      return
    }

    const url = new URL("/v1/connect", this.currentWsEndpoint())
    if (this.userId) {
      url.searchParams.set("userId", this.userId)
    }
    if (this.sessionId) {
      url.searchParams.set("sessionId", this.sessionId)
    }
    if (this.authToken) {
      url.searchParams.set("authToken", this.authToken)
    }
    if (this.adminToken) {
      url.searchParams.set("adminToken", this.adminToken)
    }
    if (this.schemaVersion !== undefined) {
      url.searchParams.set("schemaVersion", String(this.schemaVersion))
    }
    const connectMetadata = this.connectionMetadata === undefined
      ? undefined
      : serializeConnectionMetadata(this.connectionMetadata)
    if (connectMetadata !== undefined) {
      url.searchParams.set("metadata", connectMetadata)
    }
    url.searchParams.set("transport", connectionTransportParam(this.realtimeTransportKind))

    const transport = this.realtimeTransportFactory({ url })
    this.transport = transport
    this.userSubscriptionActive = false
    this.objectSubscriptionActive = false
    this.connectionEventsSubscriptionActive = false
    let reconnectScheduled = false
    const scheduleReconnect = () => {
        if (reconnectScheduled || this.manuallyClosed) {
          return
        }
        reconnectScheduled = true
      setTimeout(() => {
        if (
          this.roomListeners.size > 0 ||
          this.persistentRoomSubscriptions.size > 0 ||
          this.activeTableSubscriptions.size > 0 ||
          this.persistentTableSubscriptions.size > 0 ||
          this.activeNestedTableSubscriptions.size > 0 ||
          this.persistentNestedTableSubscriptions.size > 0 ||
          this.queryListeners.size > 0 ||
          this.persistentQuerySubscriptions.size > 0 ||
          this.userListeners.size > 0 ||
          this.persistentUserSubscription !== undefined ||
          this.objectListeners.size > 0 ||
          this.persistentObjectSubscription !== undefined ||
          this.joinedRealtimeChannels.size > 0 ||
          this.connectionEventListeners.size > 0 ||
          this.aggregateCountListeners.size > 0 ||
          this.aggregateSumListeners.size > 0 ||
          this.aggregatePresenceListeners.size > 0
        ) {
          for (const roomId of this.activeRoomSubscriptionIds()) {
            this.pendingRoomSubscriptions.set(roomId, this.persistentRoomSubscriptions.get(roomId) ?? {})
          }
          for (const [id, subscription] of this.activeTableSubscriptionsForReconnect()) {
            this.pendingTableSubscriptions.set(id, {
              ...tableSubscriptionFrame(subscription.table, subscription.options),
              afterLsn: this.tableSeenLsn.get(subscription.table) ?? 0,
              catchUpLimit: subscription.options.catchUpLimit,
            })
          }
          for (const [id, subscription] of this.activeNestedTableSubscriptionsForReconnect()) {
            this.pendingNestedTableSubscriptions.set(id, this.nestedTableSubscriptionFrame(subscription))
          }
          for (const [queryId, subscription] of this.activeQuerySubscriptions()) {
            this.pendingQuerySubscriptions.set(queryId, subscription)
          }
          if (this.userId && (this.userListeners.size > 0 || this.persistentUserSubscription)) {
            this.pendingUserSubscription = this.persistentUserSubscription?.options ?? {}
          }
          if (this.objectListeners.size > 0 || this.persistentObjectSubscription) {
            this.pendingObjectSubscription = this.persistentObjectSubscription ?? {}
          }
          for (const table of this.aggregateCountListeners.keys()) {
            this.pendingAggregateCountSubscriptions.add(table)
          }
          for (const subscriptionId of this.aggregateSumListeners.keys()) {
            this.pendingAggregateSumSubscriptions.add(subscriptionId)
          }
          for (const channelId of this.aggregatePresenceListeners.keys()) {
            this.pendingAggregatePresenceSubscriptions.add(channelId)
          }
          this.recoverEndpointForRealtime()
            .catch((error) => {
              console.error("nextdb realtime endpoint recovery failed", error)
            })
            .finally(() => this.ensureSocket())
        }
      }, 500)
    }
    transport.onOpen(() => {
      this.schedulePendingWriteFlush(0)
      this.recoverSubscribedTargets()
        .catch((error) => {
          console.error("nextdb subscription recovery failed", error)
        })
        .finally(() => {
          if (this.transport !== transport || transport.state !== "open") {
            return
          }
          for (const [roomId, options] of this.pendingRoomSubscriptions) {
            transport.send({ type: "subscribeRoom", roomId, ...options })
          }
          for (const [, frame] of this.pendingTableSubscriptions) {
            transport.send(frame)
          }
          for (const [, subscription] of this.pendingNestedTableSubscriptions) {
            transport.send(subscription)
          }
          for (const [, subscription] of this.pendingQuerySubscriptions) {
            transport.send(this.prepareQuerySubscriptionForSend(subscription))
          }
          for (const table of this.pendingAggregateCountSubscriptions) {
            transport.send({ type: "subscribeAggregateCount", table })
          }
          for (const subscriptionId of this.pendingAggregateSumSubscriptions) {
            const subscription = aggregateSumSubscriptionFromId(subscriptionId)
            transport.send({
              type: "subscribeAggregateSum",
              table: subscription.table,
              field: subscription.field,
            })
          }
          for (const channelId of this.pendingAggregatePresenceSubscriptions) {
            transport.send({ type: "subscribeAggregatePresence", channelId })
          }
          if (this.userId && this.pendingUserSubscription && !this.userSubscriptionActive) {
            transport.send({ type: "subscribeUserEvents", ...this.pendingUserSubscription })
            this.userSubscriptionActive = true
          } else if (this.userId && (this.userListeners.size > 0 || this.persistentUserSubscription) && !this.userSubscriptionActive) {
            void this.sendUserSubscription(this.persistentUserSubscription?.options ?? {})
          }
          if (this.pendingObjectSubscription && !this.objectSubscriptionActive) {
            transport.send({ type: "subscribeObjects", ...this.pendingObjectSubscription })
            this.objectSubscriptionActive = true
          } else if ((this.objectListeners.size > 0 || this.persistentObjectSubscription) && !this.objectSubscriptionActive) {
            void this.sendObjectSubscription(this.persistentObjectSubscription ?? {})
          }
          if (this.connectionEventListeners.size > 0 && !this.connectionEventsSubscriptionActive) {
            transport.send({ type: "subscribeConnectionEvents" })
            this.connectionEventsSubscriptionActive = true
          }
          const currentMetadata = this.connectionMetadata === undefined
            ? undefined
            : serializeConnectionMetadata(this.connectionMetadata)
          if (currentMetadata !== undefined && currentMetadata !== connectMetadata) {
            transport.send({ type: "updateConnectionMetadata", metadata: this.connectionMetadata })
          }
          void this.rejoinRealtimeChannels().catch((error) => {
            console.error("nextdb realtime channel rejoin failed", error)
          })
          this.pendingRoomSubscriptions.clear()
          this.pendingTableSubscriptions.clear()
          this.pendingNestedTableSubscriptions.clear()
          this.pendingQuerySubscriptions.clear()
          this.pendingAggregateCountSubscriptions.clear()
          this.pendingAggregateSumSubscriptions.clear()
          this.pendingAggregatePresenceSubscriptions.clear()
          this.pendingUserSubscription = undefined
          this.pendingObjectSubscription = undefined
        })
    })
    transport.onFrame((frame) => {
      this.handleFrame(frame).catch((error) => {
        console.error("nextdb frame handling failed", error)
      })
    })
    transport.onError(() => {
      if (this.transport === transport) {
        this.transport = undefined
      }
      if (transport.state === "open") {
        transport.close()
      }
      scheduleReconnect()
    })
    transport.onClose(() => {
      if (this.manuallyClosed) {
        return
      }
      if (this.transport === transport) {
        this.transport = undefined
      }
      scheduleReconnect()
    })
  }

  private async handleFrame(frame: ServerFrame): Promise<void> {
    if (frame.type === "error") {
      if (frame.message.includes("connection lagged")) {
        await this.recoverSubscribedTargets()
      }
      return
    }

    if (frame.type === "subscriptionCatchUp") {
      await this.handleSubscriptionCatchUp(frame)
      return
    }

    if (frame.type === "tableSnapshot") {
      await this.applyTableSnapshot(frame)
      return
    }

    if (frame.type === "nestedTableSnapshot") {
      await this.applyNestedTableSnapshot(frame)
      return
    }

    if (frame.type === "queryResult") {
      await this.putRecordsCached(frame.response.records)
      this.advanceRecords(frame.response.records)
      this.advanceLsn(frame.currentLsn)
      this.queryResults.set(frame.queryId, frame.response)
      this.rememberQueryResultId(frame.queryId, frame.resultId)
      this.dispatchQueryResult(frame)
      return
    }

    if (frame.type === "queryDiff") {
      const previousQueryResult = this.queryResults.get(frame.queryId)
      if (!previousQueryResult) {
        const subscription =
          this.querySubscriptions.get(frame.queryId) ??
          this.pendingQuerySubscriptions.get(frame.queryId) ??
          this.persistentQuerySubscriptions.get(frame.queryId)
        if (subscription) {
          this.sendWhenReady({ ...subscription, resultId: undefined })
        }
        return
      }
      const changedRecords = [...frame.diff.added, ...frame.diff.updated]
      if (changedRecords.length > 0) {
        await this.putRecordsCached(changedRecords)
        this.advanceRecords(changedRecords)
      }
      for (const removed of frame.diff.removed) {
        if (!removed.deleted) {
          continue
        }
        await this.cache.deleteRecord(removed.table, removed.key)
        this.emitRecordDeleted("realtime", {
          table: removed.table,
          key: removed.key,
          lsn: removed.lsn ?? frame.currentLsn,
          deletedAtMs: removed.deletedAtMs,
          path: removed.path,
        })
      }
      this.advanceLsn(frame.currentLsn)
      const response = mergeLiveQueryDiff(previousQueryResult, frame.diff)
      this.queryResults.set(frame.queryId, response)
      this.rememberQueryResultId(frame.queryId, frame.resultId)
      this.dispatchQueryResult({
        queryId: frame.queryId,
        response,
        currentLsn: frame.currentLsn,
        resultId: frame.resultId,
        diff: frame.diff,
      })
      return
    }

    if (frame.type === "queryUnchanged") {
      this.advanceLsn(frame.currentLsn)
      this.rememberQueryResultId(frame.queryId, frame.resultId)
      return
    }

    if (frame.type === "cacheInvalidated") {
      await this.applyRealtimeCacheInvalidation(frame.invalidation)
      return
    }

    if (frame.type === "connectionMetadataUpdated") {
      this.rememberConnectionSession(frame.session, "realtime")
      return
    }

    if (frame.type === "connectionEvent") {
      this.applyConnectionEvent(frame.event, "realtime")
      for (const listener of this.connectionEventListeners) {
        listener(frame.event)
      }
      return
    }

    if (frame.type === "aggregateCountSubscribed") {
      this.dispatchAggregateCount({
        table: frame.snapshot.table,
        count: frame.snapshot.count,
        lsn: frame.snapshot.currentLsn,
        source: "snapshot",
      })
      return
    }

    if (frame.type === "aggregateCountUpdated") {
      this.dispatchAggregateCount({
        table: frame.update.table,
        count: frame.update.count,
        lsn: frame.update.lsn,
        source: "update",
      })
      return
    }

    if (frame.type === "aggregateSumSubscribed") {
      this.dispatchAggregateSum({
        table: frame.snapshot.table,
        field: frame.snapshot.field,
        sum: frame.snapshot.sum,
        lsn: frame.snapshot.currentLsn,
        source: "snapshot",
      })
      return
    }

    if (frame.type === "aggregateSumUpdated") {
      this.dispatchAggregateSum({
        table: frame.update.table,
        field: frame.update.field,
        sum: frame.update.sum,
        lsn: frame.update.lsn,
        source: "update",
      })
      return
    }

    if (frame.type === "aggregatePresenceSubscribed") {
      this.dispatchAggregatePresence({
        channelId: frame.snapshot.channelId,
        memberCount: frame.snapshot.memberCount,
        userCount: frame.snapshot.userCount,
        lsn: frame.snapshot.currentLsn,
        updatedAtMs: frame.snapshot.updatedAtMs,
        source: "snapshot",
      })
      return
    }

    if (frame.type === "aggregatePresenceUpdated") {
      this.dispatchAggregatePresence({
        channelId: frame.update.channelId,
        memberCount: frame.update.memberCount,
        userCount: frame.update.userCount,
        lsn: frame.update.currentLsn,
        updatedAtMs: frame.update.updatedAtMs,
        source: "update",
      })
      return
    }

    if (frame.type === "events") {
      await this.handleRealtimeEvents(frame.events)
      return
    }

    if (frame.type === "event") {
      await this.handleRealtimeEvent(frame.event)
    }
  }

  private async applyTableSnapshot(
    frame: Extract<ServerFrame, { type: "tableSnapshot" }>,
  ): Promise<void> {
    const fullTableSnapshot = frame.lowerKey === undefined &&
      frame.upperKey === undefined &&
      frame.indexName === undefined
    await this.putAuthoritativeRecordsCached(frame.response.records)
    if (fullTableSnapshot) {
      this.advanceRecords(frame.response.records)
    }
    for (const record of frame.response.records) {
      this.emitRecordCached("sync", record)
    }
    if (fullTableSnapshot) {
      await this.markTableCaughtUp(frame.table, frame.currentLsn)
    }
    this.advanceLsn(frame.currentLsn)
    this.emitCacheChange({
      type: "tableSnapshotApplied",
      source: "sync",
      table: frame.table,
      lsn: frame.currentLsn,
    })
  }

  private async applyNestedTableSnapshot(
    frame: Extract<ServerFrame, { type: "nestedTableSnapshot" }>,
  ): Promise<void> {
    const target = {
      table: frame.table,
      parentKey: frame.parentKey,
      nested: frame.nested,
    }
    await this.putAuthoritativeRecordsCached(frame.response.records)
    for (const record of frame.response.records) {
      this.emitRecordCached("sync", record)
    }
    await this.markNestedTableCaughtUp(target, frame.currentLsn)
    this.advanceLsn(frame.currentLsn)
    this.emitCacheChange({
      type: "tableSnapshotApplied",
      source: "sync",
      table: nestedRecordTable(frame.table, frame.nested),
      lsn: frame.currentLsn,
    })
  }

  private async handleRealtimeEvents(events: DeliveryEvent[]): Promise<void> {
    const pendingMessages = new Map<string, NextDbMessage[]>()
    const flushMessages = async () => {
      for (const [roomId, messages] of pendingMessages) {
        await this.putRoomMessagesCached(roomId, messages)
        for (const message of messages) {
          this.advanceMessage(message)
          this.emitMessageCached("realtime", message)
          this.dispatchEvent({ type: "messageCreated", roomId, message })
        }
      }
      pendingMessages.clear()
    }

    for (const event of orderedRealtimeDeliveryEvents(events)) {
      if (event.type === "messageCreated") {
        if (!isCacheableMessage(event.message)) {
          await flushMessages()
          this.dispatchEvent(event)
          continue
        }
        if (this.hasSeenRoomLsn(event.roomId, event.message.lsn)) {
          continue
        }
        const messages = pendingMessages.get(event.roomId) ?? []
        messages.push(event.message)
        pendingMessages.set(event.roomId, messages)
        continue
      }

      await flushMessages()
      await this.handleRealtimeEvent(event)
    }
    await flushMessages()
  }

  private async handleRealtimeEvent(event: DeliveryEvent): Promise<void> {
    if (event.type === "messageCreated") {
      if (!isCacheableMessage(event.message)) {
        this.dispatchEvent(event)
        return
      }
      if (this.hasSeenRoomLsn(event.roomId, event.message.lsn)) {
        return
      }
      await this.putRoomMessagesCached(event.roomId, [event.message])
      this.advanceMessage(event.message)
      this.emitMessageCached("realtime", event.message)
      this.dispatchEvent(event)
      return
    }

    if (event.type === "recordUpserted") {
      if (!isCacheableRecord(event.record)) {
        await this.rememberVolatileRecordOverlay(event.record)
        this.dispatchEvent(event)
        return
      }
      const nestedTargets = this.activeNestedTableSubscriptionTargetsForEvent(event)
      if (this.hasSeenTableEvent(event, nestedTargets)) {
        return
      }
      await this.putAuthoritativeRecordsCached([event.record])
      this.advanceRecord(event.record, nestedTargets)
      this.emitRecordCached("realtime", event.record)
    }

    if (event.type === "recordDeleted") {
      if (!isCacheableRecordDelete(event)) {
        await this.forgetVolatileRecordOverlay(event)
        this.dispatchEvent(event)
        return
      }
      const nestedTargets = this.activeNestedTableSubscriptionTargetsForEvent(event)
      if (this.hasSeenTableEvent(event, nestedTargets)) {
        return
      }
      await this.cache.deleteRecord(event.table, event.key)
      this.advanceRecordDelete(event, nestedTargets)
      this.emitRecordDeleted("realtime", event)
    }

    if (event.type === "userEvent") {
      if (this.hasSeenUserLsn(event.userId, event.event.lsn)) {
        return
      }
      await this.putUserEventsCached(event.userId, [event.event])
      this.advanceUserEvent(event.event)
      this.emitUserEventCached("realtime", event.event)
      for (const listener of this.userListeners) {
        listener(event)
      }
      return
    }

    if (event.type === "userUpserted") {
      if (this.hasSeenUserLsn(event.userId, event.user.lsn)) {
        return
      }
      await this.putUserProfileCached(event.user, "realtime")
      for (const listener of this.userListeners) {
        listener(event)
      }
      return
    }

    if (event.type === "volatileUserEvent") {
      if (event.name === "realtime.channel.state") {
        const stateEvent = event.payload as RealtimeChannelStateEvent
        this.rememberRealtimeChannelState(stateEvent.state, "realtime", false)
      } else if (event.name === "realtime.channel.memberJoined") {
        const memberEvent = event.payload as RealtimeMemberJoinedEvent
        this.upsertRealtimeChannelMember(memberEvent.channelId, memberEvent.member, "realtime")
      } else if (event.name === "realtime.channel.memberUpdated") {
        const memberEvent = event.payload as RealtimeMemberUpdatedEvent
        this.upsertRealtimeChannelMember(memberEvent.channelId, memberEvent.member, "realtime")
      } else if (event.name === "realtime.channel.memberLeft") {
        const memberEvent = event.payload as RealtimeMemberLeftEvent
        this.rememberRealtimeChannelMembers(memberEvent.channelId, memberEvent.members, "realtime")
      } else if (event.name === "realtime.channel.event") {
        const channelEvent = event.payload as RealtimeChannelEvent
        this.rememberRealtimeChannelEvent(channelEvent, "realtime")
      } else if (event.name === "realtime.channel.signal") {
        const signal = event.payload as RealtimeSignal
        this.rememberRealtimeChannelSignal(signal, "realtime")
      }
      for (const listener of this.userListeners) {
        listener(event)
      }
      return
    }

    if (event.type === "objectCommitted") {
      if (this.hasSeenObjectLsn(event.lsn)) {
        return
      }
      await this.putObjectCached(event.object)
      this.advanceObjectEvent(event)
      this.emitCacheChange({
        type: "objectUpserted",
        source: "realtime",
        objectId: event.object.id,
        metadata: event.object,
      })
      for (const listener of this.objectListeners) {
        listener(event)
      }
      return
    }

    if (event.type === "objectDeleted") {
      if (this.hasSeenObjectLsn(event.lsn)) {
        return
      }
      await this.cache.deleteObject(event.objectId)
      this.advanceObjectEvent(event)
      this.emitCacheChange({
        type: "objectDeleted",
        source: "realtime",
        objectId: event.objectId,
      })
      for (const listener of this.objectListeners) {
        listener(event)
      }
      return
    }

    this.dispatchEvent(event)
  }

  private async handleSubscriptionCatchUp(
    frame: Extract<ServerFrame, { type: "subscriptionCatchUp" }>,
  ): Promise<void> {
    const caughtUpLsn = frame.hasMore ? frame.nextAfterLsn : frame.currentLsn
    for (const roomId of frame.rooms) {
      await this.markRoomCaughtUp(roomId, caughtUpLsn)
    }
    for (const userId of frame.users) {
      await this.markUserCaughtUp(userId, caughtUpLsn)
    }
    const nestedLogicalTables = new Set((frame.nestedTables ?? []).map((target) => nestedRecordTable(target.table, target.nested)))
    for (const table of frame.tables) {
      if (nestedLogicalTables.has(table)) {
        continue
      }
      await this.markTableCaughtUp(table, caughtUpLsn)
    }
    for (const nestedTable of frame.nestedTables ?? []) {
      await this.markNestedTableCaughtUp(nestedTable, caughtUpLsn)
    }
    if (frame.objects) {
      await this.markObjectsCaughtUp(caughtUpLsn)
    }
    this.advanceLsn(caughtUpLsn)

    if (frame.hasMore) {
      await this.syncUntilCaughtUp({
        afterLsn: frame.nextAfterLsn,
        rooms: frame.rooms,
        users: frame.users,
        tables: frame.tables,
        nestedTables: frame.nestedTables,
        objects: frame.objects,
        limit: 500,
      })
    }
  }

  private async recoverSubscribedTargets(): Promise<void> {
    await this.reconcileCacheControl()
    const rooms = this.activeRoomSubscriptionIds()
    const users = this.userId && (this.userListeners.size > 0 || this.persistentUserSubscription) ? [this.userId] : []
    const tables = this.activeTableNames()
    const nestedTables = this.activeNestedTableSubscriptionTargets()
    const objects = this.objectListeners.size > 0 || this.persistentObjectSubscription !== undefined
    await this.hydrateCursorsFor({ rooms, users, tables, nestedTables, objects })
    const recoverableRooms = rooms.filter((roomId) => (this.roomSeenLsn.get(roomId) ?? 0) > 0)
    const recoverableUsers = users.filter((userId) => (this.userSeenLsn.get(userId) ?? 0) > 0)
    const recoverableTables = tables.filter((table) => (this.tableSeenLsn.get(table) ?? 0) > 0)
    const recoverableNestedTables = nestedTables.filter((subscription) => (this.nestedTableSeenLsn.get(nestedTableCursorId(subscription.table, subscription.parentKey, subscription.nested)) ?? 0) > 0)
    const recoverableObjects = objects && this.objectSeenLsn > 0
    if (recoverableRooms.length === 0 && recoverableUsers.length === 0 && recoverableTables.length === 0 && recoverableNestedTables.length === 0 && !recoverableObjects) {
      return
    }

    const seenLsns = [
      await this.minimumSeenRoomLsn(recoverableRooms),
      await this.minimumSeenUserLsn(recoverableUsers),
      await this.minimumSeenTableLsn(recoverableTables),
      await this.minimumSeenNestedTableLsn(recoverableNestedTables),
    ]
    if (recoverableObjects) {
      seenLsns.push(await this.minimumSeenObjectLsn())
    }
    this.recoverPromise ??= this.syncUntilCaughtUp({
      afterLsn: Math.max(0, Math.min(...seenLsns) - 1),
      rooms: recoverableRooms,
      users: recoverableUsers,
      tables: recoverableTables,
      nestedTables: recoverableNestedTables,
      objects: recoverableObjects,
      limit: 500,
    })
      .then(() => undefined)
      .finally(() => {
        this.recoverPromise = undefined
      })
    return this.recoverPromise
  }

  private async ensureRealtimeTransportOpen(timeoutMs = 2_000): Promise<void> {
    this.ensureSocket()
    const deadline = Date.now() + timeoutMs
    while (Date.now() <= deadline) {
      if (this.transport?.state === "open") {
        return
      }
      await delay(Math.min(25, Math.max(1, deadline - Date.now())))
    }
    throw new Error("timed out waiting for realtime transport to open")
  }

  private async rejoinRealtimeChannels(): Promise<void> {
    const userId = this.userId
    if (!userId || this.joinedRealtimeChannels.size === 0) {
      return
    }
    const memberships = [...this.joinedRealtimeChannels.entries()]
    await Promise.all(memberships.map(([channelId, membership]) =>
      this.postRealtimeChannelJoin(channelId, membership.metadata, true)
        .then((response) => {
          this.rememberRealtimeChannelMembers(response.channelId, response.members, "sync")
          return this.refreshRealtimeChannelState(channelId)
        })
        .catch((error) => {
          console.error(`nextdb realtime channel ${channelId} rejoin failed`, error)
        })
    ))
  }

  private async postRealtimeChannelJoin(
    channelId: string,
    metadata: unknown,
    retryActiveSessionRace: boolean,
  ): Promise<RealtimeJoinResponse> {
    const userId = this.requireUserId("joinRealtimeChannel")
    const deadline = Date.now() + 2_000
    while (true) {
      try {
        return await this.post<RealtimeJoinResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/join`, {
          userId,
          sessionId: this.sessionId,
          metadata,
        })
      } catch (error) {
        if (
          !retryActiveSessionRace ||
          !isRealtimeJoinActiveSessionRace(error) ||
          Date.now() >= deadline
        ) {
          throw error
        }
        await delay(Math.min(50, Math.max(1, deadline - Date.now())))
      }
    }
  }

  private async refreshRealtimeChannelState(channelId: string): Promise<void> {
    const response = await this.get<RealtimeChannelStateResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`)
    this.rememberRealtimeChannelState(response.state, "sync", true)
  }

  private async refreshRealtimeChannelMembers(channelId: string): Promise<void> {
    const response = await this.get<RealtimeMembersResponse>(`/v1/realtime/channels/${encodeURIComponent(channelId)}/members`)
    this.rememberRealtimeChannelMembers(response.channelId, response.members, "sync")
  }

  private rememberRealtimeChannelState(
    snapshot: RealtimeChannelStateSnapshot,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
    allowOlder: boolean,
  ): void {
    const current = this.realtimeChannelStates.get(snapshot.channelId)
    if (!allowOlder && current !== undefined && current.version >= snapshot.version) {
      return
    }
    if (
      current !== undefined &&
      current.version === snapshot.version &&
      current.updatedAtMs === snapshot.updatedAtMs &&
      JSON.stringify(current.state) === JSON.stringify(snapshot.state)
    ) {
      return
    }
    this.realtimeChannelStates.set(snapshot.channelId, snapshot)
    this.emitCacheChange({
      type: "realtimeChannelStateUpdated",
      source,
      channelId: snapshot.channelId,
      state: snapshot,
    })
  }

  private forgetRealtimeChannelState(channelId: string, source: Extract<CacheChangeSource, "manual">): void {
    if (!this.realtimeChannelStates.delete(channelId)) {
      return
    }
    this.emitCacheChange({
      type: "realtimeChannelStateCleared",
      source,
      channelId,
    })
  }

  private rememberRealtimeChannelMembers(
    channelId: string,
    members: RealtimeMember[],
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    const normalized = [...members]
    const current = this.realtimeChannelMemberSnapshots.get(channelId)
    if (current !== undefined && sameRealtimeMembers(current, normalized)) {
      return
    }
    this.realtimeChannelMemberSnapshots.set(channelId, normalized)
    this.emitCacheChange({
      type: "realtimeChannelMembersUpdated",
      source,
      channelId,
      members: normalized,
    })
  }

  private upsertRealtimeChannelMember(
    channelId: string,
    member: RealtimeMember,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    const current = this.realtimeChannelMemberSnapshots.get(channelId) ?? []
    const key = realtimeMemberKey(member)
    const next = current.filter((candidate) => realtimeMemberKey(candidate) !== key)
    next.push(member)
    this.rememberRealtimeChannelMembers(channelId, next, source)
  }

  private forgetRealtimeChannelMembers(channelId: string, source: Extract<CacheChangeSource, "manual">): void {
    if (!this.realtimeChannelMemberSnapshots.delete(channelId)) {
      return
    }
    this.emitCacheChange({
      type: "realtimeChannelMembersCleared",
      source,
      channelId,
    })
  }

  private rememberRealtimeChannelEvent(
    event: RealtimeChannelEvent,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    const current = this.realtimeChannelEventSnapshots.get(event.channelId) ?? []
    if (
      current.some((candidate) =>
        candidate.sequence === event.sequence &&
        candidate.timestampMs === event.timestampMs &&
        candidate.kind === event.kind
      )
    ) {
      return
    }
    const next = [...current, event].slice(-DEFAULT_REALTIME_CHANNEL_EVENT_LIMIT)
    this.realtimeChannelEventSnapshots.set(event.channelId, next)
    this.emitCacheChange({
      type: "realtimeChannelEventReceived",
      source,
      channelId: event.channelId,
      event,
    })
  }

  private forgetRealtimeChannelEvents(channelId: string, source: Extract<CacheChangeSource, "manual">): void {
    if (!this.realtimeChannelEventSnapshots.delete(channelId)) {
      return
    }
    this.emitCacheChange({
      type: "realtimeChannelEventsCleared",
      source,
      channelId,
    })
  }

  private rememberRealtimeChannelSignal(
    signal: RealtimeSignal,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    const current = this.realtimeChannelSignalSnapshots.get(signal.channelId) ?? []
    if (
      current.some((candidate) =>
        candidate.sequence === signal.sequence &&
        candidate.timestampMs === signal.timestampMs &&
        candidate.kind === signal.kind &&
        candidate.fromUserId === signal.fromUserId &&
        candidate.toUserId === signal.toUserId
      )
    ) {
      return
    }
    const next = [...current, signal].slice(-DEFAULT_REALTIME_CHANNEL_EVENT_LIMIT)
    this.realtimeChannelSignalSnapshots.set(signal.channelId, next)
    this.emitCacheChange({
      type: "realtimeChannelSignalReceived",
      source,
      channelId: signal.channelId,
      signal,
    })
  }

  private forgetRealtimeChannelSignals(channelId: string, source: Extract<CacheChangeSource, "manual">): void {
    if (!this.realtimeChannelSignalSnapshots.delete(channelId)) {
      return
    }
    this.emitCacheChange({
      type: "realtimeChannelSignalsCleared",
      source,
      channelId,
    })
  }

  private rememberConnectionList(
    response: ConnectionListResponse,
    options: ListConnectionsOptions,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    const returnedIds = new Set(response.sessions.map((session) => session.sessionId))
    if (options.userId === undefined && options.transport === undefined) {
      this.connectionSessionSnapshots.clear()
      this.connectionSessionsLoaded = true
    } else {
      for (const [sessionId, session] of [...this.connectionSessionSnapshots]) {
        if (
          connectionSessionMatches(session, options) &&
          !returnedIds.has(sessionId)
        ) {
          this.connectionSessionSnapshots.delete(sessionId)
        }
      }
    }
    for (const session of response.sessions) {
      this.connectionSessionSnapshots.set(session.sessionId, session)
    }
    this.emitConnectionSessionsUpdated(source)
  }

  private rememberConnectionSession(
    session: ConnectionSession,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    const current = this.connectionSessionSnapshots.get(session.sessionId)
    if (current !== undefined && sameConnectionSession(current, session)) {
      return
    }
    this.connectionSessionSnapshots.set(session.sessionId, session)
    this.emitConnectionSessionsUpdated(source)
  }

  private forgetConnectionSession(
    sessionId: string | undefined,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    if (sessionId === undefined || !this.connectionSessionSnapshots.delete(sessionId)) {
      return
    }
    this.emitConnectionSessionsUpdated(source)
  }

  private applyConnectionEvent(
    event: ConnectionEvent,
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    if (event.eventType === "disconnected") {
      this.forgetConnectionSession(event.session?.sessionId ?? event.sessionId, source)
      return
    }
    if (event.session) {
      this.rememberConnectionSession(event.session, source)
    }
  }

  private emitConnectionSessionsUpdated(
    source: Exclude<CacheChangeSource, "offline" | "cacheInvalidation">,
  ): void {
    this.emitCacheChange({
      type: "connectionSessionsUpdated",
      source,
      connections: buildConnectionListResponse([...this.connectionSessionSnapshots.values()]),
    })
  }

  private async reconcileCacheControl(force = false): Promise<ClientCacheProfileResponse> {
    const metadata = await this.cache.getMetadata()
    const now = Date.now()
    if (!force && this.volatileRecordOverlays.size === 0 && metadata && metadata.leaseExpiresAtMs > now) {
      const profile = {
        version: metadata.profileVersion,
        leaseTtlMs: Math.max(0, metadata.leaseExpiresAtMs - now),
        maxObjects: metadata.maxObjects ?? 0,
        maxObjectBytes: metadata.maxObjectBytes ?? 0,
        maxRoomMessages: metadata.maxRoomMessages ?? 0,
        maxUserEvents: metadata.maxUserEvents ?? 0,
        maxRecordsPerTable: metadata.maxRecordsPerTable ?? 0,
        maxNestedPartitions: metadata.maxNestedPartitions ?? 0,
        maxPendingWrites: metadata.maxPendingWrites ?? 0,
        maxPendingWriteBytes: metadata.maxPendingWriteBytes ?? 0,
        offlineWrites: metadata.offlineWrites ?? this.offlineWrites,
      }
      this.clientCacheProfile = profile
      return {
        runtimeId: metadata.runtimeId ?? "",
        profile,
        lease: {
          clientId: metadata.clientId,
          sessionId: metadata.sessionId,
          issuedAtMs: metadata.lastValidatedAtMs,
          expiresAtMs: metadata.leaseExpiresAtMs,
          profileVersion: metadata.profileVersion,
        },
        invalidations: [],
        currentLsn: this.lastSeenLsn,
        schemaVersion: metadata.schemaVersion,
        resetRequired: false,
      }
    }

    this.cacheControlPromise ??= this.fetchAndApplyCacheControl(metadata)
      .finally(() => {
        this.cacheControlPromise = undefined
      })
    return this.cacheControlPromise
  }

  private async fetchAndApplyCacheControl(metadata?: ClientCacheMetadata): Promise<ClientCacheProfileResponse> {
    const cursorLsn = await this.cache.getGlobalCursor()
    const params = new URLSearchParams({
      clientId: metadata?.clientId ?? this.clientId,
      afterInvalidationGeneration: String(metadata?.invalidationGeneration ?? 0),
      cursorLsn: String(cursorLsn),
    })
    if (this.sessionId ?? metadata?.sessionId) {
      params.set("sessionId", this.sessionId ?? metadata?.sessionId ?? "")
    }
    if (metadata?.schemaVersion !== undefined) {
      params.set("schemaVersion", String(metadata.schemaVersion))
    }

    const response = await this.get<ClientCacheProfileResponse>(`/v1/cache/profile?${params}`)
    await this.applyCacheControlResponse(response, metadata)
    return response
  }

  private async applyCacheControlResponse(
    response: ClientCacheProfileResponse,
    metadata?: ClientCacheMetadata,
  ): Promise<void> {
    if (metadata?.runtimeId !== undefined && metadata.runtimeId !== response.runtimeId) {
      await this.clearVolatileRuntimeState()
    }
    if (
      response.resetRequired ||
      (metadata && metadata.profileVersion !== response.profile.version) ||
      (metadata && metadata.schemaVersion !== response.schemaVersion)
    ) {
      await this.clearCachedDataPreservingPendingWrites()
      this.lastSeenLsn = 0
      this.roomSeenLsn.clear()
      this.userSeenLsn.clear()
      this.tableSeenLsn.clear()
      this.tableSeenEventIds.clear()
      this.tableCaughtUpLsn.clear()
      this.tableAppliedEventIds.clear()
      this.nestedTableSeenLsn.clear()
      this.nestedTableSeenEventIds.clear()
      this.nestedTableCaughtUpLsn.clear()
      this.nestedTableAppliedEventIds.clear()
      this.emitCacheChange({
        type: "allInvalidated",
        source: "cacheInvalidation",
        minValidLsn: 0,
      })
    }
    const profile: ClientCacheProfile = {
      ...response.profile,
      maxNestedPartitions: response.profile.maxNestedPartitions ?? 0,
      maxPendingWrites: response.profile.maxPendingWrites ?? 0,
      maxPendingWriteBytes: response.profile.maxPendingWriteBytes ?? 0,
    }
    this.clientCacheProfile = profile

    let generation = metadata?.invalidationGeneration ?? 0
    for (const invalidation of response.invalidations) {
      if (invalidation.generation <= generation) {
        continue
      }
      await this.applyCacheInvalidation(invalidation)
      generation = Math.max(generation, invalidation.generation)
    }

    await this.cache.setMetadata({
      clientId: response.lease.clientId,
      sessionId: response.lease.sessionId,
      runtimeId: response.runtimeId,
      profileVersion: profile.version,
      schemaVersion: response.schemaVersion,
      maxObjects: profile.maxObjects,
      maxObjectBytes: profile.maxObjectBytes,
      maxRoomMessages: profile.maxRoomMessages,
      maxUserEvents: profile.maxUserEvents,
      maxRecordsPerTable: profile.maxRecordsPerTable,
      maxNestedPartitions: profile.maxNestedPartitions,
      maxPendingWrites: profile.maxPendingWrites,
      maxPendingWriteBytes: profile.maxPendingWriteBytes,
      offlineWrites: profile.offlineWrites,
      invalidationGeneration: generation,
      leaseExpiresAtMs: response.lease.expiresAtMs,
      lastValidatedAtMs: Date.now(),
    })
    const result = await this.enforceCacheProfile(profile)
    if (result.removed.total > 0) {
      this.emitCacheChange({
        type: "cacheProfileEnforced",
        source: "cacheInvalidation",
        result,
      })
    }
  }

  private async putObjectCached(metadata: NextDbObjectMetadata, body?: Blob): Promise<void> {
    await this.cache.putObject(metadata, body)
    const maxObjects = this.clientCacheProfile?.maxObjects ?? 0
    const maxBytes = this.clientCacheProfile?.maxObjectBytes ?? 0
    if (maxObjects > 0 || maxBytes > 0) {
      await this.cache.trimObjects(maxObjects, maxBytes)
    }
  }

  private async putRoomMessagesCached(roomId: string, messages: NextDbMessage[]): Promise<void> {
    const cacheable = messages.filter(isCacheableMessage)
    if (cacheable.length === 0) {
      return
    }
    await this.cache.putRoomMessages(roomId, cacheable)
    const keepLatest = this.clientCacheProfile?.maxRoomMessages ?? 0
    if (keepLatest > 0) {
      await this.cache.trimRoom(roomId, keepLatest)
    }
  }

  private async putUserEventsCached(userId: string, events: NextDbUserEvent[]): Promise<void> {
    await this.cache.putUserEvents(userId, events)
    const keepLatest = this.clientCacheProfile?.maxUserEvents ?? 0
    if (keepLatest > 0) {
      await this.cache.trimUserEvents(userId, keepLatest)
    }
  }

  private async putUserProfileCached(
    profile: NextDbUserProfile,
    source: Exclude<CacheChangeSource, "cacheInvalidation" | "manual">,
  ): Promise<void> {
    await this.cache.putUserProfile(profile)
    this.advanceUserProfile(profile)
    this.emitCacheChange({
      type: "userProfileUpserted",
      source,
      userId: profile.userId,
      lsn: profile.lsn,
      user: profile,
    })
  }

  private async putRecordsCached(records: NextDbRecord[]): Promise<void> {
    for (const record of records) {
      if (!isCacheableRecord(record)) {
        await this.rememberVolatileRecordOverlay(record)
      }
    }
    const cacheable = records.filter((record) =>
      isCacheableRecord(record) && !this.volatileRecordOverlays.has(recordOverlayKey(record.table, record.key)),
    )
    if (cacheable.length === 0) {
      return
    }
    await this.cache.putRecords(cacheable)
    const keepLatest = this.clientCacheProfile?.maxRecordsPerTable ?? 0
    if (keepLatest <= 0) {
      return
    }
    const wholeTables = new Set<string>()
    const nestedPartitions = new Map<string, Set<string>>()
    for (const record of cacheable) {
      const keyPrefix = nestedRecordKeyPrefix(record)
      if (keyPrefix === undefined) {
        wholeTables.add(record.table)
      } else {
        const prefixes = nestedPartitions.get(record.table) ?? new Set<string>()
        prefixes.add(keyPrefix)
        nestedPartitions.set(record.table, prefixes)
      }
    }
    for (const [table, prefixes] of nestedPartitions) {
      for (const keyPrefix of prefixes) {
        await this.cache.trimRecordsByKeyPrefix(table, keyPrefix, keepLatest)
      }
      const keepPartitions = this.clientCacheProfile?.maxNestedPartitions ?? 0
      if (keepPartitions > 0) {
        await this.cache.trimNestedTablePartitions(table, keepPartitions, keepLatest)
      }
    }
    for (const table of wholeTables) {
      await this.cache.trimTable(table, keepLatest)
    }
  }

  private async putAuthoritativeRecordsCached(records: NextDbRecord[]): Promise<void> {
    for (const record of records) {
      if (isCacheableRecord(record)) {
        this.volatileRecordOverlays.delete(recordOverlayKey(record.table, record.key))
      }
    }
    await this.putRecordsCached(records)
  }

  private hasVolatileRecordOverlayForTable(table: string): boolean {
    const prefix = `${table}\0`
    for (const overlay of this.volatileRecordOverlays) {
      if (overlay.startsWith(prefix)) {
        return true
      }
    }
    return false
  }

  private async rememberVolatileRecordOverlay(record: NextDbRecord): Promise<void> {
    this.volatileRecordOverlays.add(recordOverlayKey(record.table, record.key))
    await this.cache.deleteRecord(record.table, record.key)
  }

  private async forgetVolatileRecordOverlay(event: { table: string; key: string }): Promise<void> {
    this.volatileRecordOverlays.delete(recordOverlayKey(event.table, event.key))
    await this.cache.deleteRecord(event.table, event.key)
  }

  private async clearVolatileRuntimeState(): Promise<void> {
    this.volatileRecordOverlays.clear()
    this.queryResults.clear()
    for (const subscription of this.querySubscriptions.values()) {
      subscription.resultId = undefined
    }
    for (const subscription of this.pendingQuerySubscriptions.values()) {
      subscription.resultId = undefined
    }
    for (const [queryId, subscription] of this.persistentQuerySubscriptions) {
      const reset = { ...subscription, resultId: undefined }
      this.persistentQuerySubscriptions.set(queryId, reset)
      await this.storeSubscription({
        id: storedQuerySubscriptionId(queryId),
        kind: "query",
        query: reset,
      })
    }
  }

  private async enforceCacheProfile(profile: ClientCacheProfile): Promise<LocalCacheProfileEnforcementResult> {
    const stats = await this.cache.stats()
    const removed: LocalCacheProfileTrimReport = {
      objects: 0,
      roomMessages: {},
      userEvents: {},
      records: {},
      nestedRecords: {},
      nestedPartitions: {},
      total: 0,
    }
    if (profile.maxObjects > 0 || profile.maxObjectBytes > 0) {
      removed.objects = await this.cache.trimObjects(profile.maxObjects, profile.maxObjectBytes)
    }
    if (profile.maxRoomMessages > 0) {
      for (const roomId of Object.keys(stats.rooms)) {
        const count = await this.cache.trimRoom(roomId, profile.maxRoomMessages)
        if (count > 0) {
          removed.roomMessages[roomId] = count
        }
      }
    }
    if (profile.maxUserEvents > 0) {
      for (const userId of Object.keys(stats.users)) {
        const count = await this.cache.trimUserEvents(userId, profile.maxUserEvents)
        if (count > 0) {
          removed.userEvents[userId] = count
        }
      }
    }
    if (profile.maxRecordsPerTable > 0) {
      for (const table of Object.keys(stats.tables)) {
        const nestedPartitions = stats.nestedTables[table]
        if (nestedPartitions !== undefined && Object.keys(nestedPartitions).length > 0) {
          for (const keyPrefix of Object.keys(nestedPartitions)) {
            const count = await this.cache.trimRecordsByKeyPrefix(table, keyPrefix, profile.maxRecordsPerTable)
            if (count > 0) {
              removed.nestedRecords[table] ??= {}
              removed.nestedRecords[table][keyPrefix] = count
            }
          }
        } else {
          const count = await this.cache.trimTable(table, profile.maxRecordsPerTable)
          if (count > 0) {
            removed.records[table] = count
          }
        }
      }
    }
    if ((profile.maxNestedPartitions ?? 0) > 0) {
      for (const table of Object.keys(stats.nestedTables)) {
        const count = await this.cache.trimNestedTablePartitions(table, profile.maxNestedPartitions, profile.maxRecordsPerTable)
        if (count > 0) {
          removed.nestedPartitions[table] = count
        }
      }
    }
    removed.total = removed.objects +
      sumRecordValues(removed.roomMessages) +
      sumRecordValues(removed.userEvents) +
      sumRecordValues(removed.records) +
      sumNestedRecordValues(removed.nestedRecords) +
      sumRecordValues(removed.nestedPartitions)
    return {
      profile,
      before: stats,
      after: await this.cache.stats(),
      removed,
    }
  }

  private async applyCacheInvalidation(invalidation: ClientCacheInvalidationEntry): Promise<void> {
    const minValidLsn = Math.max(0, invalidation.minValidLsn)
    if (invalidation.scope === "all") {
      await this.clearCachedDataPreservingPendingWrites()
      this.lastSeenLsn = minValidLsn
      this.objectSeenLsn = minValidLsn
      this.roomSeenLsn.clear()
      this.userSeenLsn.clear()
      this.tableSeenLsn.clear()
      this.tableSeenEventIds.clear()
      this.tableCaughtUpLsn.clear()
      this.tableAppliedEventIds.clear()
      this.nestedTableSeenLsn.clear()
      this.nestedTableSeenEventIds.clear()
      this.nestedTableCaughtUpLsn.clear()
      this.nestedTableAppliedEventIds.clear()
      await this.cache.setGlobalCursor(minValidLsn)
      this.emitCacheChange({
        type: "allInvalidated",
        source: "cacheInvalidation",
        minValidLsn,
      })
      return
    }
    if (invalidation.scope === "profile") {
      this.emitCacheChange({
        type: "cacheProfileUpdated",
        source: "cacheInvalidation",
      })
      return
    }
    if (invalidation.scope === "room" && invalidation.key) {
      await this.cache.clearRoom(invalidation.key)
      this.roomSeenLsn.set(invalidation.key, minValidLsn)
      await this.cache.setRoomCursor(invalidation.key, minValidLsn)
      this.emitCacheChange({
        type: "roomInvalidated",
        source: "cacheInvalidation",
        roomId: invalidation.key,
        minValidLsn,
      })
      return
    }
    if (invalidation.scope === "object" && invalidation.key) {
      await this.cache.deleteObject(invalidation.key)
      this.emitCacheChange({
        type: "objectDeleted",
        source: "cacheInvalidation",
        objectId: invalidation.key,
      })
      return
    }
    if (invalidation.scope === "user" && invalidation.key) {
      await this.cache.clearUserEvents(invalidation.key)
      await this.cache.deleteUserProfile(invalidation.key)
      this.userSeenLsn.set(invalidation.key, minValidLsn)
      await this.cache.setUserCursor(invalidation.key, minValidLsn)
      this.emitCacheChange({
        type: "userProfileDeleted",
        source: "cacheInvalidation",
        userId: invalidation.key,
      })
      this.emitCacheChange({
        type: "userInvalidated",
        source: "cacheInvalidation",
        userId: invalidation.key,
        minValidLsn,
      })
      return
    }
    if (invalidation.scope === "table" && invalidation.key) {
      await this.cache.clearTable(invalidation.key)
      this.tableSeenLsn.set(invalidation.key, minValidLsn)
      this.tableSeenEventIds.delete(invalidation.key)
      this.tableCaughtUpLsn.set(invalidation.key, minValidLsn)
      this.tableAppliedEventIds.delete(invalidation.key)
      this.clearNestedCursorStateForLogicalTable(invalidation.key)
      await this.cache.setTableCursor(invalidation.key, minValidLsn)
      this.emitCacheChange({
        type: "tableInvalidated",
        source: "cacheInvalidation",
        table: invalidation.key,
        minValidLsn,
      })
      return
    }
    if (invalidation.scope === "nestedTable" && invalidation.table && invalidation.parentKey && invalidation.nested) {
      const logicalTable = nestedRecordTable(invalidation.table, invalidation.nested)
      const cursorId = nestedTableCursorId(invalidation.table, invalidation.parentKey, invalidation.nested)
      await this.cache.clearRecordsByKeyPrefix(logicalTable, nestedRecordPrefix(invalidation.parentKey))
      this.nestedTableSeenLsn.set(cursorId, minValidLsn)
      this.nestedTableSeenEventIds.delete(cursorId)
      this.nestedTableCaughtUpLsn.set(cursorId, minValidLsn)
      this.nestedTableAppliedEventIds.delete(cursorId)
      await this.cache.setNestedTableCursor(invalidation.table, invalidation.parentKey, invalidation.nested, minValidLsn)
      this.emitCacheChange({
        type: "tableInvalidated",
        source: "cacheInvalidation",
        table: logicalTable,
        minValidLsn,
      })
    }
  }

  private async applyRealtimeCacheInvalidation(invalidation: ClientCacheInvalidationEntry): Promise<void> {
    const metadata = await this.cache.getMetadata()
    await this.applyCacheInvalidation(invalidation)
    if (metadata && invalidation.generation > metadata.invalidationGeneration) {
      await this.cache.setMetadata({
        ...metadata,
        invalidationGeneration: invalidation.generation,
        lastValidatedAtMs: Date.now(),
      })
    }
    if (invalidation.scope === "all" || invalidation.scope === "profile") {
      await this.reconcileCacheControl(true)
    }
  }

  private async clearCachedDataPreservingPendingWrites(): Promise<void> {
    const pendingWrites = await this.cache.listPendingWrites()
    const subscriptions = await this.cache.listSubscriptions()
    await this.cache.clearAll()
    this.objectSeenLsn = 0
    for (const pendingWrite of pendingWrites) {
      await this.cache.putPendingWrite(pendingWrite)
    }
    for (const subscription of subscriptions) {
      await this.cache.putSubscription(subscription)
    }
  }

  private async applySyncEvents(events: DeliveryEvent[], nestedTargets: SyncNestedTableTarget[] = []): Promise<void> {
    const syncMessages = new Set<Extract<DeliveryEvent, { type: "messageCreated" }>>()
    const roomMessages = new Map<string, NextDbMessage[]>()
    const roomSeenLsn = new Map(this.roomSeenLsn)
    for (const event of events) {
      if (event.type !== "messageCreated" || !isCacheableMessage(event.message)) {
        continue
      }
      const previous = roomSeenLsn.get(event.roomId) ?? 0
      if (event.message.lsn <= previous) {
        continue
      }
      roomSeenLsn.set(event.roomId, event.message.lsn)
      syncMessages.add(event)
      const messages = roomMessages.get(event.roomId) ?? []
      messages.push(event.message)
      roomMessages.set(event.roomId, messages)
    }
    for (const [roomId, messages] of roomMessages) {
      await this.putRoomMessagesCached(roomId, messages)
    }
    this.advanceSyncMessageCursors(roomMessages)

    const syncRecordUpserts: Array<{
      event: Extract<DeliveryEvent, { type: "recordUpserted" }>
      nestedTargets: SyncNestedTableTarget[]
    }> = []
    const syncUserEvents: Array<Extract<DeliveryEvent, { type: "userEvent" }>> = []
    const userSeenLsn = new Map(this.userSeenLsn)
    const flushSyncRecordUpserts = async () => {
      await this.putAuthoritativeRecordsCached(syncRecordUpserts.map(({ event }) => event.record))
      this.advanceSyncRecordUpsertCursors(syncRecordUpserts)
      for (const { event } of syncRecordUpserts) {
        this.emitRecordCached("sync", event.record)
        this.dispatchEvent(event)
      }
      syncRecordUpserts.length = 0
    }
    const flushSyncUserEvents = async () => {
      const eventsByUser = new Map<string, NextDbUserEvent[]>()
      for (const event of syncUserEvents) {
        const events = eventsByUser.get(event.userId) ?? []
        events.push(event.event)
        eventsByUser.set(event.userId, events)
      }
      for (const [userId, events] of eventsByUser) {
        await this.putUserEventsCached(userId, events)
      }
      this.advanceSyncUserEventCursors(eventsByUser)
      for (const event of syncUserEvents) {
        this.emitUserEventCached("sync", event.event)
        this.dispatchEvent(event)
      }
      syncUserEvents.length = 0
    }

    for (const event of events) {
      if (event.type === "messageCreated") {
        if (syncRecordUpserts.length > 0) {
          await flushSyncRecordUpserts()
        }
        if (syncUserEvents.length > 0) {
          await flushSyncUserEvents()
        }
        if (!syncMessages.has(event)) {
          continue
        }
        this.emitMessageCached("sync", event.message)
      }
      if (event.type === "recordUpserted") {
        if (syncUserEvents.length > 0) {
          await flushSyncUserEvents()
        }
        if (!isCacheableRecord(event.record)) {
          if (syncRecordUpserts.length > 0) {
            await flushSyncRecordUpserts()
          }
          continue
        }
        const eventNestedTargets = this.nestedTableSubscriptionTargetsForEvent(event, nestedTargets)
        if (this.hasSeenTableEvent(event, eventNestedTargets)) {
          continue
        }
        syncRecordUpserts.push({
          event,
          nestedTargets: eventNestedTargets,
        })
        continue
      }
      if (event.type === "recordDeleted") {
        if (syncRecordUpserts.length > 0) {
          await flushSyncRecordUpserts()
        }
        if (syncUserEvents.length > 0) {
          await flushSyncUserEvents()
        }
        if (!isCacheableRecordDelete(event)) {
          continue
        }
        const eventNestedTargets = this.nestedTableSubscriptionTargetsForEvent(event, nestedTargets)
        if (this.hasSeenTableEvent(event, eventNestedTargets)) {
          continue
        }
        await this.cache.deleteRecord(event.table, event.key)
        this.advanceRecordDelete(event, eventNestedTargets)
        this.emitRecordDeleted("sync", event)
      }
      if (event.type === "userEvent") {
        if (syncRecordUpserts.length > 0) {
          await flushSyncRecordUpserts()
        }
        const previous = userSeenLsn.get(event.userId) ?? 0
        if (event.event.lsn <= previous) {
          continue
        }
        userSeenLsn.set(event.userId, event.event.lsn)
        syncUserEvents.push(event)
        continue
      }
      if (event.type === "userUpserted") {
        if (syncRecordUpserts.length > 0) {
          await flushSyncRecordUpserts()
        }
        if (syncUserEvents.length > 0) {
          await flushSyncUserEvents()
        }
        if (this.hasSeenUserLsn(event.userId, event.user.lsn)) {
          continue
        }
        await this.putUserProfileCached(event.user, "sync")
      }
      if (event.type === "objectCommitted") {
        if (syncRecordUpserts.length > 0) {
          await flushSyncRecordUpserts()
        }
        if (syncUserEvents.length > 0) {
          await flushSyncUserEvents()
        }
        if (this.hasSeenObjectLsn(event.lsn)) {
          continue
        }
        await this.putObjectCached(event.object)
        this.advanceObjectEvent(event)
        this.emitCacheChange({
          type: "objectUpserted",
          source: "sync",
          objectId: event.object.id,
          metadata: event.object,
        })
      }
      if (event.type === "objectDeleted") {
        if (syncRecordUpserts.length > 0) {
          await flushSyncRecordUpserts()
        }
        if (syncUserEvents.length > 0) {
          await flushSyncUserEvents()
        }
        if (this.hasSeenObjectLsn(event.lsn)) {
          continue
        }
        await this.cache.deleteObject(event.objectId)
        this.advanceObjectEvent(event)
        this.emitCacheChange({
          type: "objectDeleted",
          source: "sync",
          objectId: event.objectId,
        })
      }
      this.dispatchEvent(event)
    }
    if (syncRecordUpserts.length > 0) {
      await flushSyncRecordUpserts()
    }
    if (syncUserEvents.length > 0) {
      await flushSyncUserEvents()
    }
  }

  private dispatchEvent(event: DeliveryEvent): void {
    if (event.type === "volatileUserEvent" || event.type === "userEvent" || event.type === "userUpserted") {
      for (const listener of this.userListeners) {
        listener(event)
      }
      return
    }

    if (event.type === "messageCreated" || event.type === "volatileRoomEvent") {
      const listeners = this.roomListeners.get(event.roomId)
      if (!listeners) {
        return
      }
      for (const listener of listeners) {
        listener(event)
      }
      return
    }

    if (event.type === "recordUpserted" || event.type === "recordDeleted") {
      const listeners = this.tableListeners.get(event.table)
      if (!listeners) {
        return
      }
      for (const entry of listeners) {
        if (tableEventMatchesSubscription(event, entry.options)) {
          entry.listener(event)
        }
      }
      return
    }

    if (event.type === "objectCommitted" || event.type === "objectDeleted") {
      for (const listener of this.objectListeners) {
        listener(event)
      }
    }
  }

  private dispatchQueryResult(event: RecordLiveQueryResult): void {
    const listeners = this.queryListeners.get(event.queryId)
    if (!listeners) {
      return
    }
    for (const listener of listeners) {
      listener(event)
    }
  }

  private dispatchAggregateCount(event: AggregateCountEvent): void {
    const listeners = this.aggregateCountListeners.get(event.table)
    if (!listeners) {
      return
    }
    for (const listener of listeners) {
      listener(event)
    }
  }

  private dispatchAggregateSum(event: AggregateSumEvent): void {
    const listeners = this.aggregateSumListeners.get(aggregateSumSubscriptionId(event.table, event.field))
    if (!listeners) {
      return
    }
    for (const listener of listeners) {
      listener(event)
    }
  }

  private dispatchAggregatePresence(event: AggregatePresenceEvent): void {
    const listeners = this.aggregatePresenceListeners.get(event.channelId)
    if (!listeners) {
      return
    }
    for (const listener of listeners) {
      listener(event)
    }
  }

  private rememberQueryResultId(queryId: string, resultId: string): void {
    const current = this.querySubscriptions.get(queryId)
    if (current) {
      current.resultId = resultId
    }
    const pending = this.pendingQuerySubscriptions.get(queryId)
    if (pending) {
      pending.resultId = resultId
    }
    const persistent = this.persistentQuerySubscriptions.get(queryId)
    if (persistent) {
      const response = this.queryResults.get(queryId)
      const storedQuery = response?.hasMore === false
        ? { ...persistent, resultId }
        : { ...persistent, resultId: undefined }
      void this.storeSubscription({
        id: storedQuerySubscriptionId(queryId),
        kind: "query",
        query: storedQuery,
      })
    }
  }

  private emitCacheChange(event: NextDbCacheChange): void {
    for (const listener of this.cacheChangeListeners) {
      listener(event)
    }
  }

  private async emitPendingWriteChange(
    event: Omit<Extract<NextDbCacheChange, { type: "pendingWriteQueued" }>, "stats">
      | Omit<Extract<NextDbCacheChange, { type: "pendingWriteRejected" }>, "stats">
      | Omit<Extract<NextDbCacheChange, { type: "pendingWriteReset" }>, "stats">
      | Omit<Extract<NextDbCacheChange, { type: "pendingWriteDiscarded" }>, "stats">
      | Omit<Extract<NextDbCacheChange, { type: "pendingWritesCleared" }>, "stats">
      | Omit<Extract<NextDbCacheChange, { type: "pendingWriteCommitted" }>, "stats">
      | Omit<Extract<NextDbCacheChange, { type: "pendingWriteFailed" }>, "stats">,
  ): Promise<void> {
    const stats = await this.pendingWriteStats()
    this.emitCacheChange({
      ...event,
      stats,
    } as NextDbCacheChange)
  }

  private emitMessageCached(source: CacheChangeSource, message: NextDbMessage): void {
    this.emitCacheChange({
      type: "messageUpserted",
      source,
      roomId: message.roomId,
      key: message.id,
      lsn: message.lsn,
      message,
    })
  }

  private emitUserEventCached(source: Exclude<CacheChangeSource, "offline">, event: NextDbUserEvent): void {
    this.emitCacheChange({
      type: "userEventUpserted",
      source,
      userId: event.userId,
      key: event.id,
      lsn: event.lsn,
      event,
    })
  }

  private emitRecordCached(source: CacheChangeSource, record: NextDbRecord): void {
    this.emitCacheChange({
      type: "recordUpserted",
      source,
      table: record.table,
      key: record.key,
      lsn: record.lsn,
      record,
    })
  }

  private emitRecordDeleted(
    source: CacheChangeSource,
    event: { table: string; key: string; lsn: number; deletedAtMs?: number; path?: string },
  ): void {
    this.emitCacheChange({
      type: "recordDeleted",
      source,
      table: event.table,
      key: event.key,
      lsn: event.lsn,
      deletedAtMs: event.deletedAtMs,
      path: event.path,
    })
  }

  private advanceMessages(messages: NextDbMessage[]): void {
    for (const message of messages) {
      this.advanceMessage(message)
    }
  }

  private advanceSyncMessageCursors(roomMessages: Map<string, NextDbMessage[]>): void {
    let highestLsn = 0
    for (const [roomId, messages] of roomMessages) {
      const lsn = maxLsn(messages)
      if (lsn === undefined) {
        continue
      }
      highestLsn = Math.max(highestLsn, lsn)
      const previous = this.roomSeenLsn.get(roomId) ?? 0
      if (lsn > previous) {
        this.roomSeenLsn.set(roomId, lsn)
        void this.cache.setRoomCursor(roomId, lsn)
      }
    }
    this.advanceLsn(highestLsn)
  }

  private advanceMessage(message: NextDbMessage): void {
    this.advanceLsn(message.lsn)
    const previous = this.roomSeenLsn.get(message.roomId) ?? 0
    if (message.lsn > previous) {
      this.roomSeenLsn.set(message.roomId, message.lsn)
      void this.cache.setRoomCursor(message.roomId, message.lsn)
    }
  }

  private async markRoomCaughtUp(roomId: string, lsn: number): Promise<void> {
    const previous = this.roomSeenLsn.get(roomId) ?? 0
    if (lsn > previous) {
      this.roomSeenLsn.set(roomId, lsn)
      await this.cache.setRoomCursor(roomId, lsn)
    }
  }

  private advanceUserEvent(event: NextDbUserEvent): void {
    this.advanceLsn(event.lsn)
    const previous = this.userSeenLsn.get(event.userId) ?? 0
    if (event.lsn > previous) {
      this.userSeenLsn.set(event.userId, event.lsn)
      void this.cache.setUserCursor(event.userId, event.lsn)
    }
  }

  private advanceSyncUserEventCursors(userEvents: Map<string, NextDbUserEvent[]>): void {
    let highestLsn = 0
    for (const [userId, events] of userEvents) {
      const lsn = maxLsn(events)
      if (lsn === undefined) {
        continue
      }
      highestLsn = Math.max(highestLsn, lsn)
      const previous = this.userSeenLsn.get(userId) ?? 0
      if (lsn > previous) {
        this.userSeenLsn.set(userId, lsn)
        void this.cache.setUserCursor(userId, lsn)
      }
    }
    this.advanceLsn(highestLsn)
  }

  private advanceUserProfile(profile: NextDbUserProfile): void {
    this.advanceLsn(profile.lsn)
    const previous = this.userSeenLsn.get(profile.userId) ?? 0
    if (profile.lsn > previous) {
      this.userSeenLsn.set(profile.userId, profile.lsn)
      void this.cache.setUserCursor(profile.userId, profile.lsn)
    }
  }

  private async markUserCaughtUp(userId: string, lsn: number): Promise<void> {
    const previous = this.userSeenLsn.get(userId) ?? 0
    if (lsn > previous) {
      this.userSeenLsn.set(userId, lsn)
      await this.cache.setUserCursor(userId, lsn)
    }
  }

  private advanceRecords(records: NextDbRecord[]): void {
    for (const record of records) {
      this.advanceRecord(record)
    }
  }

  private advanceRecord(record: NextDbRecord, nestedTargets: SyncNestedTableTarget[] = []): void {
    if (!isCacheableRecord(record)) {
      return
    }
    const event = {
      table: record.table,
      key: record.key,
      lsn: record.lsn,
      eventId: `upsert:${record.key}`,
    }
    if (this.markNestedTableEventApplied(event, nestedTargets)) {
      return
    }
    this.markTableEventApplied(event)
  }

  private advanceSyncRecordUpsertCursors(
    upserts: Array<{
      event: Extract<DeliveryEvent, { type: "recordUpserted" }>
      nestedTargets: SyncNestedTableTarget[]
    }>,
  ): void {
    let highestLsn = 0
    const tableCursorWrites = new Map<string, number>()
    const nestedCursorWrites = new Map<string, { target: SyncNestedTableTarget; lsn: number }>()
    for (const { event, nestedTargets } of upserts) {
      if (!isCacheableRecord(event.record)) {
        continue
      }
      const lsn = event.record.lsn
      highestLsn = Math.max(highestLsn, lsn)
      const eventId = tableEventId(event)
      const targets = this.nestedTableSubscriptionTargetsForEvent(event, nestedTargets)
      if (targets.length > 0) {
        for (const target of targets) {
          const cursorId = nestedTableCursorId(target.table, target.parentKey, target.nested)
          this.rememberNestedTableEventApplied(cursorId, lsn, eventId)
          const previous = this.nestedTableSeenLsn.get(cursorId) ?? 0
          if (lsn > previous) {
            this.nestedTableSeenLsn.set(cursorId, lsn)
            this.nestedTableSeenEventIds.set(cursorId, {
              lsn,
              ids: new Set([eventId]),
            })
            nestedCursorWrites.set(cursorId, { target, lsn })
          } else if (lsn === previous) {
            const seen = this.nestedTableSeenEventIds.get(cursorId)
            if (seen?.lsn === lsn) {
              seen.ids.add(eventId)
            } else {
              this.nestedTableSeenEventIds.set(cursorId, {
                lsn,
                ids: new Set([eventId]),
              })
            }
          }
        }
        continue
      }
      const previous = this.tableSeenLsn.get(event.table) ?? 0
      this.rememberTableEventApplied(event.table, lsn, eventId)
      if (lsn > previous) {
        this.tableSeenLsn.set(event.table, lsn)
        this.tableSeenEventIds.set(event.table, {
          lsn,
          ids: new Set([eventId]),
        })
        tableCursorWrites.set(event.table, lsn)
      } else if (lsn === previous) {
        const seen = this.tableSeenEventIds.get(event.table)
        if (seen?.lsn === lsn) {
          seen.ids.add(eventId)
        } else {
          this.tableSeenEventIds.set(event.table, {
            lsn,
            ids: new Set([eventId]),
          })
        }
      }
    }
    this.advanceLsn(highestLsn)
    for (const [table, lsn] of tableCursorWrites) {
      void this.cache.setTableCursor(table, lsn)
    }
    for (const { target, lsn } of nestedCursorWrites.values()) {
      void this.cache.setNestedTableCursor(target.table, target.parentKey, target.nested, lsn)
    }
  }

  private advanceRecordDelete(event: { table: string; key: string; lsn: number; path?: string }, nestedTargets: SyncNestedTableTarget[] = []): void {
    if (!isCacheableRecordDelete(event)) {
      return
    }
    const tableEvent = {
      table: event.table,
      key: event.key,
      lsn: event.lsn,
      eventId: `delete:${event.key}`,
    }
    if (this.markNestedTableEventApplied(tableEvent, nestedTargets)) {
      return
    }
    this.markTableEventApplied(tableEvent)
  }

  private rememberTableEventApplied(table: string, lsn: number, eventId: string): void {
    rememberRecentDeliveryEvent(this.tableAppliedEventIds, table, lsn, eventId)
  }

  private rememberNestedTableEventApplied(cursorId: string, lsn: number, eventId: string): void {
    rememberRecentDeliveryEvent(this.nestedTableAppliedEventIds, cursorId, lsn, eventId)
  }

  private hasRecentlyAppliedTableEvent(table: string, lsn: number, eventId: string): boolean {
    return hasRecentDeliveryEvent(this.tableAppliedEventIds, table, lsn, eventId)
  }

  private hasRecentlyAppliedNestedTableEvent(cursorId: string, lsn: number, eventId: string): boolean {
    return hasRecentDeliveryEvent(this.nestedTableAppliedEventIds, cursorId, lsn, eventId)
  }

  private markTableEventApplied(event: { table: string; lsn: number; eventId: string }): void {
    this.advanceLsn(event.lsn)
    this.rememberTableEventApplied(event.table, event.lsn, event.eventId)
    const previous = this.tableSeenLsn.get(event.table) ?? 0
    if (event.lsn > previous) {
      this.tableSeenLsn.set(event.table, event.lsn)
      this.tableSeenEventIds.set(event.table, {
        lsn: event.lsn,
        ids: new Set([event.eventId]),
      })
      void this.cache.setTableCursor(event.table, event.lsn)
      return
    }
    if (event.lsn === previous) {
      const seen = this.tableSeenEventIds.get(event.table)
      if (seen?.lsn === event.lsn) {
        seen.ids.add(event.eventId)
      } else {
        this.tableSeenEventIds.set(event.table, {
          lsn: event.lsn,
          ids: new Set([event.eventId]),
        })
      }
    }
  }

  private markNestedTableEventApplied(
    event: { table: string; key: string; lsn: number; eventId: string },
    nestedTargets: SyncNestedTableTarget[],
  ): boolean {
    const targets = this.nestedTableSubscriptionTargetsForEvent(event, nestedTargets)
    if (targets.length === 0) {
      return false
    }
    this.advanceLsn(event.lsn)
    for (const target of targets) {
      const cursorId = nestedTableCursorId(target.table, target.parentKey, target.nested)
      this.rememberNestedTableEventApplied(cursorId, event.lsn, event.eventId)
      const previous = this.nestedTableSeenLsn.get(cursorId) ?? 0
      if (event.lsn > previous) {
        this.nestedTableSeenLsn.set(cursorId, event.lsn)
        this.nestedTableSeenEventIds.set(cursorId, {
          lsn: event.lsn,
          ids: new Set([event.eventId]),
        })
        void this.cache.setNestedTableCursor(target.table, target.parentKey, target.nested, event.lsn)
      } else if (event.lsn === previous) {
        const seen = this.nestedTableSeenEventIds.get(cursorId)
        if (seen?.lsn === event.lsn) {
          seen.ids.add(event.eventId)
        } else {
          this.nestedTableSeenEventIds.set(cursorId, {
            lsn: event.lsn,
            ids: new Set([event.eventId]),
          })
        }
      }
    }
    return true
  }

  private async markTableCaughtUp(table: string, lsn: number): Promise<void> {
    const previous = this.tableSeenLsn.get(table) ?? 0
    if (lsn > previous) {
      this.tableSeenLsn.set(table, lsn)
      this.tableSeenEventIds.set(table, {
        lsn,
        ids: new Set(),
      })
      this.tableCaughtUpLsn.set(table, lsn)
      this.tableAppliedEventIds.delete(table)
      await this.cache.setTableCursor(table, lsn)
    }
  }

  private async markNestedTableCaughtUp(target: SyncNestedTableTarget, lsn: number): Promise<void> {
    const cursorId = nestedTableCursorId(target.table, target.parentKey, target.nested)
    const previous = this.nestedTableSeenLsn.get(cursorId) ?? 0
    if (lsn > previous) {
      this.nestedTableSeenLsn.set(cursorId, lsn)
      this.nestedTableSeenEventIds.set(cursorId, {
        lsn,
        ids: new Set(),
      })
      this.nestedTableCaughtUpLsn.set(cursorId, lsn)
      this.nestedTableAppliedEventIds.delete(cursorId)
      await this.cache.setNestedTableCursor(target.table, target.parentKey, target.nested, lsn)
    }
  }

  private async markSyncTargetsCaughtUp(options: SyncPullOptions, lsn: number): Promise<void> {
    await Promise.all([
      ...(options.rooms ?? []).map((roomId) => this.markRoomCaughtUp(roomId, lsn)),
      ...(options.users ?? []).map((userId) => this.markUserCaughtUp(userId, lsn)),
      ...(options.tables ?? []).map((table) => this.markTableCaughtUp(table, lsn)),
      ...(options.nestedTables ?? []).map((target) => this.markNestedTableCaughtUp(target, lsn)),
      options.objects ? this.markObjectsCaughtUp(lsn) : Promise.resolve(),
    ])
  }

  private advanceObjectEvent(event: ObjectDeliveryEvent): void {
    this.advanceLsn(event.lsn)
    if (event.lsn > this.objectSeenLsn) {
      this.objectSeenLsn = event.lsn
      void this.cache.setObjectCursor(event.lsn)
    }
  }

  private async markObjectsCaughtUp(lsn: number): Promise<void> {
    if (lsn > this.objectSeenLsn) {
      this.objectSeenLsn = lsn
      await this.cache.setObjectCursor(lsn)
    }
  }

  private advanceLsn(lsn: number): void {
    const next = Math.max(this.lastSeenLsn, lsn)
    if (next > this.lastSeenLsn) {
      this.lastSeenLsn = next
      void this.cache.setGlobalCursor(next)
    }
  }

  private hasSeenRoomLsn(roomId: string, lsn: number): boolean {
    return lsn <= (this.roomSeenLsn.get(roomId) ?? 0)
  }

  private hasSeenUserLsn(userId: string, lsn: number): boolean {
    return lsn <= (this.userSeenLsn.get(userId) ?? 0)
  }

  private hasSeenTableEvent(event: TableDeliveryEvent, nestedTargets: SyncNestedTableTarget[] = []): boolean {
    const targets = this.nestedTableSubscriptionTargetsForEvent(event, nestedTargets)
    if (targets.length > 0) {
      return targets.every((target) => this.hasSeenNestedTableEvent(event, target))
    }
    const lsn = event.type === "recordUpserted" ? event.record.lsn : event.lsn
    const seenLsn = this.tableSeenLsn.get(event.table) ?? 0
    if (lsn < seenLsn) {
      if (lsn <= (this.tableCaughtUpLsn.get(event.table) ?? 0)) {
        return true
      }
      return this.hasRecentlyAppliedTableEvent(event.table, lsn, tableEventId(event))
    }
    if (lsn > seenLsn) {
      return false
    }
    const seen = this.tableSeenEventIds.get(event.table)
    if (seen?.lsn !== lsn) {
      return false
    }
    return seen.ids.has(tableEventId(event))
  }

  private hasSeenNestedTableEvent(event: TableDeliveryEvent, target: SyncNestedTableTarget): boolean {
    const lsn = event.type === "recordUpserted" ? event.record.lsn : event.lsn
    const cursorId = nestedTableCursorId(target.table, target.parentKey, target.nested)
    const seenLsn = this.nestedTableSeenLsn.get(cursorId) ?? 0
    if (lsn < seenLsn) {
      if (lsn <= (this.nestedTableCaughtUpLsn.get(cursorId) ?? 0)) {
        return true
      }
      return this.hasRecentlyAppliedNestedTableEvent(cursorId, lsn, tableEventId(event))
    }
    if (lsn > seenLsn) {
      return false
    }
    const seen = this.nestedTableSeenEventIds.get(cursorId)
    if (seen?.lsn !== lsn) {
      return false
    }
    return seen.ids.has(tableEventId(event))
  }

  private hasSeenObjectLsn(lsn: number): boolean {
    return lsn <= this.objectSeenLsn
  }

  private async minimumSeenRoomLsn(rooms: string[]): Promise<number> {
    if (rooms.length === 0) {
      return this.lastSeenLsn
    }
    await this.hydrateCursorsFor({ rooms })
    return Math.min(...rooms.map((roomId) => this.roomSeenLsn.get(roomId) ?? 0))
  }

  private async minimumSeenUserLsn(users: string[]): Promise<number> {
    if (users.length === 0) {
      return this.lastSeenLsn
    }
    await this.hydrateCursorsFor({ users })
    return Math.min(...users.map((userId) => this.userSeenLsn.get(userId) ?? 0))
  }

  private async minimumSeenTableLsn(tables: string[]): Promise<number> {
    if (tables.length === 0) {
      return this.lastSeenLsn
    }
    await this.hydrateCursorsFor({ tables })
    return Math.min(...tables.map((table) => this.tableSeenLsn.get(table) ?? 0))
  }

  private async minimumSeenNestedTableLsn(nestedTables: SyncNestedTableTarget[]): Promise<number> {
    if (nestedTables.length === 0) {
      return this.lastSeenLsn
    }
    await this.hydrateCursorsFor({ nestedTables })
    return Math.min(...nestedTables.map((target) => this.nestedTableSeenLsn.get(nestedTableCursorId(target.table, target.parentKey, target.nested)) ?? 0))
  }

  private async minimumSeenObjectLsn(): Promise<number> {
    await this.hydrateCursorsFor({ objects: true })
    return this.objectSeenLsn
  }

  private async hydrateCursorsFor(targets: { rooms?: string[]; users?: string[]; tables?: string[]; nestedTables?: SyncNestedTableTarget[]; objects?: boolean } = {}): Promise<void> {
    this.cursorHydratePromise ??= this.cache.getGlobalCursor()
      .then((lsn) => {
        this.lastSeenLsn = Math.max(this.lastSeenLsn, lsn)
      })
      .finally(() => {
        this.cursorHydratePromise = undefined
      })
    await this.cursorHydratePromise

    if (targets.objects && this.objectSeenLsn <= 0) {
      const lsn = await this.cache.getObjectCursor()
      if (lsn > 0) {
        this.objectSeenLsn = lsn
      }
    }

    await Promise.all([
      ...(targets.rooms ?? []).map(async (roomId) => {
        if ((this.roomSeenLsn.get(roomId) ?? 0) > 0) {
          return
        }
        const lsn = await this.cache.getRoomCursor(roomId)
        if (lsn > 0) {
          this.roomSeenLsn.set(roomId, lsn)
        }
      }),
      ...(targets.users ?? []).map(async (userId) => {
        if ((this.userSeenLsn.get(userId) ?? 0) > 0) {
          return
        }
        const lsn = await this.cache.getUserCursor(userId)
        if (lsn > 0) {
          this.userSeenLsn.set(userId, lsn)
        }
      }),
      ...(targets.tables ?? []).map(async (table) => {
        if ((this.tableSeenLsn.get(table) ?? 0) > 0) {
          return
        }
        const lsn = await this.cache.getTableCursor(table)
        if (lsn > 0) {
          this.tableSeenLsn.set(table, lsn)
        }
      }),
      ...(targets.nestedTables ?? []).map(async (target) => {
        const cursorId = nestedTableCursorId(target.table, target.parentKey, target.nested)
        if ((this.nestedTableSeenLsn.get(cursorId) ?? 0) > 0) {
          return
        }
        const lsn = await this.cache.getNestedTableCursor(target.table, target.parentKey, target.nested)
        if (lsn > 0) {
          this.nestedTableSeenLsn.set(cursorId, lsn)
        }
      }),
    ])
  }

  private sendWhenReady(frame: ClientFrame): void {
    const frameToSend = frame.type === "subscribeQuery"
      ? this.prepareQuerySubscriptionForSend(frame)
      : frame
    const subscriptionOptions = subscriptionOptionsFromFrame(frame)
    if (!this.transport || this.transport.state === "closed") {
      if (frame.type === "unsubscribeUserEvents") {
        this.pendingUserSubscription = undefined
        this.userSubscriptionActive = false
        return
      }
      if (frame.type === "unsubscribeObjects") {
        this.pendingObjectSubscription = undefined
        this.objectSubscriptionActive = false
        return
      }
      if (frame.type === "unsubscribeConnectionEvents") {
        this.connectionEventsSubscriptionActive = false
        return
      }
      if (frame.type === "unsubscribeAggregateCount") {
        this.pendingAggregateCountSubscriptions.delete(frame.table)
        return
      }
      if (frame.type === "unsubscribeAggregateSum") {
        this.pendingAggregateSumSubscriptions.delete(aggregateSumSubscriptionId(frame.table, frame.field))
        return
      }
      if (frame.type === "unsubscribeAggregatePresence") {
        this.pendingAggregatePresenceSubscriptions.delete(frame.channelId)
        return
      }
      if (frame.type === "unsubscribeNestedTable") {
        this.pendingNestedTableSubscriptions.delete(storedNestedTableSubscriptionId(frame.table, frame.parentKey, frame.nested))
        return
      }
      if (frame.type === "unsubscribeTable") {
        this.pendingTableSubscriptions.delete(tableSubscriptionFrameId(frame))
        return
      }
      if (frame.type === "subscribeRoom") {
        this.pendingRoomSubscriptions.set(frame.roomId, subscriptionOptions)
      }
      if (frame.type === "subscribeTable") {
        this.pendingTableSubscriptions.set(tableSubscriptionFrameId(frame), frame)
      }
      if (frame.type === "subscribeNestedTable") {
        this.pendingNestedTableSubscriptions.set(
          storedNestedTableSubscriptionId(frame.table, frame.parentKey, frame.nested),
          frame,
        )
      }
      if (frameToSend.type === "subscribeQuery") {
        this.pendingQuerySubscriptions.set(frameToSend.queryId, frameToSend)
      }
      if (frame.type === "subscribeUserEvents") {
        this.pendingUserSubscription = subscriptionOptions
      }
      if (frame.type === "subscribeObjects") {
        this.pendingObjectSubscription = subscriptionOptions
      }
      if (frame.type === "subscribeAggregateCount") {
        this.pendingAggregateCountSubscriptions.add(frame.table)
      }
      if (frame.type === "subscribeAggregateSum") {
        this.pendingAggregateSumSubscriptions.add(aggregateSumSubscriptionId(frame.table, frame.field))
      }
      if (frame.type === "subscribeAggregatePresence") {
        this.pendingAggregatePresenceSubscriptions.add(frame.channelId)
      }
      this.ensureSocket()
      return
    }

    if (this.transport.state === "connecting") {
      if (frame.type === "unsubscribeUserEvents") {
        this.pendingUserSubscription = undefined
        this.userSubscriptionActive = false
        return
      }
      if (frame.type === "unsubscribeObjects") {
        this.pendingObjectSubscription = undefined
        this.objectSubscriptionActive = false
        return
      }
      if (frame.type === "unsubscribeConnectionEvents") {
        this.connectionEventsSubscriptionActive = false
        return
      }
      if (frame.type === "unsubscribeAggregateCount") {
        this.pendingAggregateCountSubscriptions.delete(frame.table)
        return
      }
      if (frame.type === "unsubscribeAggregateSum") {
        this.pendingAggregateSumSubscriptions.delete(aggregateSumSubscriptionId(frame.table, frame.field))
        return
      }
      if (frame.type === "unsubscribeAggregatePresence") {
        this.pendingAggregatePresenceSubscriptions.delete(frame.channelId)
        return
      }
      if (frame.type === "unsubscribeNestedTable") {
        this.pendingNestedTableSubscriptions.delete(storedNestedTableSubscriptionId(frame.table, frame.parentKey, frame.nested))
        return
      }
      if (frame.type === "unsubscribeTable") {
        this.pendingTableSubscriptions.delete(tableSubscriptionFrameId(frame))
        return
      }
      if (frame.type === "subscribeRoom") {
        this.pendingRoomSubscriptions.set(frame.roomId, subscriptionOptions)
      }
      if (frame.type === "subscribeTable") {
        this.pendingTableSubscriptions.set(tableSubscriptionFrameId(frame), frame)
      }
      if (frame.type === "subscribeNestedTable") {
        this.pendingNestedTableSubscriptions.set(
          storedNestedTableSubscriptionId(frame.table, frame.parentKey, frame.nested),
          frame,
        )
      }
      if (frameToSend.type === "subscribeQuery") {
        this.pendingQuerySubscriptions.set(frameToSend.queryId, frameToSend)
      }
      if (frame.type === "subscribeUserEvents") {
        this.pendingUserSubscription = subscriptionOptions
      }
      if (frame.type === "subscribeObjects") {
        this.pendingObjectSubscription = subscriptionOptions
      }
      if (frame.type === "subscribeAggregateCount") {
        this.pendingAggregateCountSubscriptions.add(frame.table)
      }
      if (frame.type === "subscribeAggregateSum") {
        this.pendingAggregateSumSubscriptions.add(aggregateSumSubscriptionId(frame.table, frame.field))
      }
      if (frame.type === "subscribeAggregatePresence") {
        this.pendingAggregatePresenceSubscriptions.add(frame.channelId)
      }
      return
    }

    if (frame.type === "subscribeUserEvents") {
      if (this.userSubscriptionActive) {
        return
      }
      this.userSubscriptionActive = true
    }
    if (frame.type === "unsubscribeUserEvents") {
      this.userSubscriptionActive = false
      this.pendingUserSubscription = undefined
    }
    if (frame.type === "subscribeObjects") {
      if (this.objectSubscriptionActive) {
        return
      }
      this.objectSubscriptionActive = true
    }
    if (frame.type === "unsubscribeObjects") {
      this.objectSubscriptionActive = false
      this.pendingObjectSubscription = undefined
    }
    if (frame.type === "subscribeConnectionEvents") {
      if (this.connectionEventsSubscriptionActive) {
        return
      }
      this.connectionEventsSubscriptionActive = true
    }
    if (frame.type === "unsubscribeConnectionEvents") {
      this.connectionEventsSubscriptionActive = false
    }
    this.transport.send(frameToSend)
  }

  private unsubscribeWhenConnected(frame: Extract<ClientFrame, {
    type:
      | "unsubscribeRoom"
      | "unsubscribeTable"
      | "unsubscribeNestedTable"
      | "unsubscribeQuery"
      | "unsubscribeUserEvents"
      | "unsubscribeObjects"
  }>): void {
    if (frame.type === "unsubscribeRoom") {
      this.pendingRoomSubscriptions.delete(frame.roomId)
    }
    if (frame.type === "unsubscribeTable") {
      this.pendingTableSubscriptions.delete(tableSubscriptionFrameId(frame))
    }
    if (frame.type === "unsubscribeNestedTable") {
      this.pendingNestedTableSubscriptions.delete(storedNestedTableSubscriptionId(frame.table, frame.parentKey, frame.nested))
    }
    if (frame.type === "unsubscribeQuery") {
      this.pendingQuerySubscriptions.delete(frame.queryId)
    }
    if (frame.type === "unsubscribeUserEvents") {
      this.pendingUserSubscription = undefined
      this.userSubscriptionActive = false
    }
    if (frame.type === "unsubscribeObjects") {
      this.pendingObjectSubscription = undefined
      this.objectSubscriptionActive = false
    }
    if (this.transport?.state === "open") {
      this.sendWhenReady(frame)
    }
  }

  private prepareQuerySubscriptionForSend(
    frame: Extract<ClientFrame, { type: "subscribeQuery" }>,
  ): Extract<ClientFrame, { type: "subscribeQuery" }> {
    if (frame.resultId === undefined || frame.diff === false || this.queryResults.has(frame.queryId)) {
      return frame
    }
    return { ...frame, resultId: undefined }
  }

  private async get<T>(path: string): Promise<T> {
    const response = await fetch(`${this.activeEndpoint}${path}`, {
      headers: this.authHeaders(),
    })
    return parseResponse<T>(response)
  }

  private async getFrom<T>(endpoint: string, path: string): Promise<T> {
    const response = await fetch(`${endpoint}${path}`, {
      headers: this.authHeaders(),
    })
    return parseResponse<T>(response)
  }

  private async readRecordWithQuorum<T>(
    routeOptions: ClusterRouteOptions,
    path: string,
    options: FreshnessOptions,
    label: string,
  ): Promise<NextDbRecord<T>> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute(routeOptions)
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    let lastFailures: string[] = []
    let lastFreshRecords: Array<NextDbRecord<T>> = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          const response = await this.getFrom<RecordResponse<T>>(endpoint, path)
          return { endpoint, record: response.record, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, record: undefined, error: errorMessage(error) }
        }
      }))
      const freshRecords = results
        .map((result) => result.record)
        .filter((record): record is NextDbRecord<T> =>
          record !== undefined && freshnessSatisfied(options, record.lsn))
      lastFreshRecords = freshRecords
      lastFailures = results
        .filter((result) => result.record === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing record"}`)

      if (freshRecords.length >= required) {
        return highestLsnRecord(freshRecords)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const highestFreshLsn = maxLsn(lastFreshRecords) ?? 0
    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} read quorum failed for ${label}: required ${required}/${endpoints.length} fresh record response(s)${minimum}, got ${lastFreshRecords.length}; highest fresh LSN ${highestFreshLsn}${failures}`,
    )
  }

  private async readRoomMessagesWithQuorum(
    roomId: string,
    limit: number,
    beforeLsn: number | undefined,
    options: FreshnessOptions,
  ): Promise<MessagesResponse> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute({ roomId })
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    const params = new URLSearchParams({ limit: String(limit) })
    if (beforeLsn !== undefined) {
      params.set("beforeLsn", String(beforeLsn))
    }
    const path = `/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?${params}`
    let lastFailures: string[] = []
    let lastPages: MessagesResponse[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const response = await this.getFrom<MessagesResponse>(endpoint, path)
          return { endpoint, response, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, response: undefined, error: errorMessage(error) }
        }
      }))
      const pages = results
        .map((result) => result.response)
        .filter((response): response is MessagesResponse => response !== undefined)
      lastPages = pages
      lastFailures = results
        .filter((result) => result.response === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing page"}`)

      if (pages.length >= required) {
        return mergeRoomMessagesQuorum(roomId, pages, limit, beforeLsn)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} room message read quorum failed for ${roomId}: required ${required}/${endpoints.length} fresh page response(s)${minimum}, got ${lastPages.length}${failures}`,
    )
  }

  private async fetchUserEvents(
    userId: string,
    limit: number,
    beforeLsn: number | undefined,
  ): Promise<UserEventsResponse> {
    const params = new URLSearchParams({ limit: String(limit) })
    if (beforeLsn !== undefined) {
      params.set("beforeLsn", String(beforeLsn))
    }
    return this.get<UserEventsResponse>(`/v1/users/${encodeURIComponent(userId)}/events?${params}`)
  }

  private async readUserEventsWithQuorum(
    userId: string,
    limit: number,
    beforeLsn: number | undefined,
    options: FreshnessOptions,
  ): Promise<UserEventsResponse> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute({ key: userId })
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    const params = new URLSearchParams({ limit: String(limit) })
    if (beforeLsn !== undefined) {
      params.set("beforeLsn", String(beforeLsn))
    }
    const path = `/v1/users/${encodeURIComponent(userId)}/events?${params}`
    let lastFailures: string[] = []
    let lastPages: UserEventsResponse[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const response = await this.getFrom<UserEventsResponse>(endpoint, path)
          return { endpoint, response, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, response: undefined, error: errorMessage(error) }
        }
      }))
      const pages = results
        .map((result) => result.response)
        .filter((response): response is UserEventsResponse => response !== undefined)
      lastPages = pages
      lastFailures = results
        .filter((result) => result.response === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing page"}`)

      if (pages.length >= required) {
        return mergeUserEventsQuorum(userId, pages, limit, beforeLsn)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} user event read quorum failed for ${userId}: required ${required}/${endpoints.length} fresh page response(s)${minimum}, got ${lastPages.length}${failures}`,
    )
  }

  private async readUserWithQuorum(
    userId: string,
    options: FreshnessOptions,
  ): Promise<NextDbUserProfile> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute({ key: userId })
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    const path = `/v1/users/${encodeURIComponent(userId)}`
    let lastFailures: string[] = []
    let lastFreshUsers: NextDbUserProfile[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const response = await this.getFrom<UserResponse>(endpoint, path)
          return { endpoint, user: response.user, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, user: undefined, error: errorMessage(error) }
        }
      }))
      const freshUsers = results
        .map((result) => result.user)
        .filter((user): user is NextDbUserProfile =>
          user !== undefined && freshnessSatisfied(options, user.lsn))
      lastFreshUsers = freshUsers
      lastFailures = results
        .filter((result) => result.user === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing user"}`)

      if (freshUsers.length >= required) {
        return highestLsnUserProfile(freshUsers)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const highestFreshLsn = maxLsn(lastFreshUsers) ?? 0
    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} user read quorum failed for ${userId}: required ${required}/${endpoints.length} fresh user response(s)${minimum}, got ${lastFreshUsers.length}; highest fresh LSN ${highestFreshLsn}${failures}`,
    )
  }

  private async readUserListWithQuorum(
    params: URLSearchParams,
    limit: number,
    options: FreshnessOptions,
  ): Promise<ListUsersResponse> {
    const topology = await this.clusterTopology()
    const pages = await Promise.all(topology.shards.map((shard) =>
      this.readUserListShardWithQuorum(
        shard,
        `/v1/admin/users?${recordListShardParams(params, shard.shard)}`,
        limit,
        options,
      )
    ))
    return mergeUserListQuorum(pages, limit)
  }

  private async readUserListShardWithQuorum(
    shard: ClusterShard,
    path: string,
    limit: number,
    options: FreshnessOptions,
  ): Promise<ListUsersResponse> {
    const consistency = options.consistency ?? "local"
    const endpoints = readQuorumEndpointsForShard(shard, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    let lastFailures: string[] = []
    let lastFreshPages: ListUsersResponse[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const response = await this.getFrom<ListUsersResponse>(endpoint, path)
          return { endpoint, response, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, response: undefined, error: errorMessage(error) }
        }
      }))
      const freshPages = results
        .map((result) => result.response)
        .filter((response): response is ListUsersResponse => response !== undefined)
      lastFreshPages = freshPages
      lastFailures = results
        .filter((result) => result.response === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing page"}`)

      if (freshPages.length >= required) {
        return mergeUserListQuorum(freshPages, limit)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const highestFreshLsn = maxLsn(lastFreshPages.flatMap((page) => page.users)) ?? 0
    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} user list read quorum failed for shard ${shard.shard}: required ${required}/${endpoints.length} fresh page response(s)${minimum}, got ${lastFreshPages.length}; highest fresh LSN ${highestFreshLsn}${failures}`,
    )
  }

  private async readRecordListWithQuorum<T>(
    path: string,
    params: URLSearchParams,
    table: string,
    limit: number,
    options: FreshnessOptions,
    merge: RecordListQuorumMerge = { kind: "key" },
  ): Promise<ListRecordsResponse<T>> {
    const topology = await this.clusterTopology()
    const pages = await Promise.all(topology.shards.map((shard) =>
      this.readRecordListShardWithQuorum<T>(
        shard,
        `${path}?${recordListShardParams(params, shard.shard)}`,
        table,
        limit,
        options,
        merge,
      )
    ))
    return mergeRecordListQuorum(table, pages, limit, merge)
  }

  private async readRecordListShardWithQuorum<T>(
    shard: ClusterShard,
    path: string,
    table: string,
    limit: number,
    options: FreshnessOptions,
    merge: RecordListQuorumMerge,
  ): Promise<ListRecordsResponse<T>> {
    const consistency = options.consistency ?? "local"
    const endpoints = readQuorumEndpointsForShard(shard, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    let lastFailures: string[] = []
    let lastFreshPages: Array<ListRecordsResponse<T>> = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          if (options.minLsn !== undefined) {
            const waitParams = new URLSearchParams({
              minLsn: String(Math.max(0, Math.floor(options.minLsn))),
              timeoutMs: String(Math.max(0, deadline - Date.now())),
              consistency: "local",
            })
            const wait = await this.getFrom<SyncWaitResponse>(endpoint, `/v1/sync/wait?${waitParams}`)
            if (!wait.caughtUp) {
              return { endpoint, response: undefined, error: `node is at LSN ${wait.currentLsn}` }
            }
          }
          const response = await this.getFrom<ListRecordsResponse<T>>(endpoint, path)
          return { endpoint, response, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, response: undefined, error: errorMessage(error) }
        }
      }))
      const freshPages = results
        .map((result) => result.response)
        .filter((response): response is ListRecordsResponse<T> => response !== undefined)
      lastFreshPages = freshPages
      lastFailures = results
        .filter((result) => result.response === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing page"}`)

      if (freshPages.length >= required) {
        return mergeRecordListQuorum(table, freshPages, limit, merge)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const highestFreshLsn = maxLsn(lastFreshPages.flatMap((page) => page.records)) ?? 0
    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} list read quorum failed for ${table} shard ${shard.shard}: required ${required}/${endpoints.length} fresh page response(s)${minimum}, got ${lastFreshPages.length}; highest fresh LSN ${highestFreshLsn}${failures}`,
    )
  }

  private async readObjectListWithQuorum(
    params: URLSearchParams,
    limit: number,
    options: FreshnessOptions,
  ): Promise<ListObjectsResponse> {
    const topology = await this.clusterTopology()
    const pages = await Promise.all(topology.shards.map((shard) =>
      this.readObjectListShardWithQuorum(
        shard,
        `/v1/objects?${recordListShardParams(params, shard.shard)}`,
        limit,
        options,
      )
    ))
    return mergeObjectListQuorum(pages, limit)
  }

  private async readObjectMetadataWithQuorum(
    objectId: string,
    options: FreshnessOptions,
  ): Promise<NextDbObjectMetadata> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute({ objectId })
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    const path = `/v1/objects/${encodeURIComponent(objectId)}/metadata`
    let lastFailures: string[] = []
    let lastMetadata: NextDbObjectMetadata[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const metadata = await this.getFrom<NextDbObjectMetadata>(endpoint, path)
          return { endpoint, metadata, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, metadata: undefined, error: errorMessage(error) }
        }
      }))
      const metadata = results
        .map((result) => result.metadata)
        .filter((item): item is NextDbObjectMetadata => item !== undefined)
      lastMetadata = metadata
      lastFailures = results
        .filter((result) => result.metadata === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing metadata"}`)

      if (metadata.length >= required) {
        return metadata[0]
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} object metadata read quorum failed for ${objectId}: required ${required}/${endpoints.length} fresh metadata response(s)${minimum}, got ${lastMetadata.length}${failures}`,
    )
  }

  private async readObjectBodyWithQuorum(
    objectId: string,
    metadata: NextDbObjectMetadata,
    options: FreshnessOptions,
  ): Promise<Blob> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute({ objectId })
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    const path = `/v1/objects/${encodeURIComponent(objectId)}/body`
    let lastFailures: string[] = []
    let lastBodies: Blob[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const body = await this.fetchObjectBodyFrom(endpoint, path)
          if (!await objectBodyMatchesMetadata(body, metadata)) {
            return { endpoint, body: undefined, error: "body does not match metadata" }
          }
          return { endpoint, body, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, body: undefined, error: errorMessage(error) }
        }
      }))
      const bodies = results
        .map((result) => result.body)
        .filter((body): body is Blob => body !== undefined)
      lastBodies = bodies
      lastFailures = results
        .filter((result) => result.body === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing body"}`)

      if (bodies.length >= required) {
        return bodies[0]
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} object body read quorum failed for ${objectId}: required ${required}/${endpoints.length} fresh body response(s)${minimum}, got ${lastBodies.length}${failures}`,
    )
  }

  private async readObjectBodyRangeWithQuorum(
    objectId: string,
    metadata: NextDbObjectMetadata,
    options: ObjectBodyRangeOptions,
  ): Promise<ObjectBodyRangeResponse> {
    const consistency = options.consistency ?? "local"
    const route = await this.clusterRoute({ objectId })
    const endpoints = readQuorumEndpoints(route, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    const path = `/v1/objects/${encodeURIComponent(objectId)}/body`
    const range = objectRangeHeader(options)
    let lastFailures: string[] = []
    let lastRanges: ObjectBodyRangeResponse[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          await this.waitForEndpointFreshness(endpoint, options, deadline)
          const response = await this.fetchObjectBodyRangeFrom(endpoint, path, range)
          if (!objectBodyRangeMatchesMetadata(response, metadata)) {
            return { endpoint, response: undefined, error: "range response does not match metadata" }
          }
          return { endpoint, response, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, response: undefined, error: errorMessage(error) }
        }
      }))
      const ranges = results
        .map((result) => result.response)
        .filter((response): response is ObjectBodyRangeResponse => response !== undefined)
      lastRanges = ranges
      lastFailures = results
        .filter((result) => result.response === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing range"}`)

      if (ranges.length >= required) {
        return ranges[0]
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} object range read quorum failed for ${objectId}: required ${required}/${endpoints.length} fresh range response(s)${minimum}, got ${lastRanges.length}${failures}`,
    )
  }

  private async cachedObjectBodyRange(
    objectId: string,
    options: ObjectBodyRangeOptions,
  ): Promise<ObjectBodyRangeResponse | undefined> {
    const [metadata, body] = await Promise.all([
      this.cache.getObjectMetadata(objectId),
      this.cache.getObjectBody(objectId),
    ])
    if (metadata === undefined) {
      return undefined
    }
    const range = cachedObjectByteRange(options, metadata.byteSize)
    if (range === undefined) {
      return undefined
    }
    if (body !== undefined && body.size === metadata.byteSize) {
      return {
        body: body.slice(range.start, range.end + 1, metadata.contentType),
        contentRange: objectContentRange(range.start, range.end, metadata.byteSize),
        start: range.start,
        end: range.end,
        byteSize: metadata.byteSize,
        contentType: metadata.contentType,
      }
    }
    return this.cache.getObjectBodyRange(metadata, range.start, range.end)
  }

  private async putObjectBodyRangeCached(
    objectId: string,
    response: ObjectBodyRangeResponse,
    options: ObjectBodyRangeOptions,
  ): Promise<void> {
    if (!freshnessSatisfied(options, undefined)) {
      return
    }
    const metadata = await this.cache.getObjectMetadata(objectId)
    if (metadata === undefined || !objectBodyRangeMatchesMetadata(response, metadata)) {
      return
    }
    await this.cache.putObjectBodyRange(metadata, response)
  }

  private async waitForEndpointFreshness(
    endpoint: string,
    options: FreshnessOptions,
    deadline: number,
  ): Promise<void> {
    if (options.minLsn === undefined) {
      return
    }
    const waitParams = new URLSearchParams({
      minLsn: String(Math.max(0, Math.floor(options.minLsn))),
      timeoutMs: String(Math.max(0, deadline - Date.now())),
      consistency: "local",
    })
    const wait = await this.getFrom<SyncWaitResponse>(endpoint, `/v1/sync/wait?${waitParams}`)
    if (!wait.caughtUp) {
      throw new Error(`node is at LSN ${wait.currentLsn}`)
    }
  }

  private async fetchObjectBodyFrom(endpoint: string, path: string): Promise<Blob> {
    const response = await fetch(`${endpoint}${path}`, {
      headers: this.authHeaders(),
    })
    if (!response.ok) {
      const payload = await response.json().catch(() => undefined)
      throw new NextDbHttpError(
        payload?.error ?? `NextDB object request failed with ${response.status}`,
        response.status,
        payload,
      )
    }
    return response.blob()
  }

  private async fetchObjectBodyRangeFrom(
    endpoint: string,
    path: string,
    range: string,
  ): Promise<ObjectBodyRangeResponse> {
    const response = await fetch(`${endpoint}${path}`, {
      headers: {
        ...this.authHeaders(),
        range,
      },
    })
    if (!response.ok) {
      const payload = await response.json().catch(() => undefined)
      throw new NextDbHttpError(
        payload?.error ?? `NextDB object range request failed with ${response.status}`,
        response.status,
        payload,
      )
    }
    const contentRange = response.headers.get("content-range")
    if (!contentRange) {
      throw new Error("NextDB object range response is missing content-range")
    }
    const parsed = parseObjectContentRange(contentRange)
    return {
      body: await response.blob(),
      contentRange,
      start: parsed.start,
      end: parsed.end,
      byteSize: parsed.byteSize,
      contentType: response.headers.get("content-type") ?? "application/octet-stream",
    }
  }

  private async readObjectListShardWithQuorum(
    shard: ClusterShard,
    path: string,
    limit: number,
    options: FreshnessOptions,
  ): Promise<ListObjectsResponse> {
    const consistency = options.consistency ?? "local"
    const endpoints = readQuorumEndpointsForShard(shard, this.activeEndpoint)
    const required = readQuorumRequiredAcks(consistency, endpoints.length)
    const deadline = Date.now() + Math.max(0, Math.floor(options.timeoutMs ?? 0))
    let lastFailures: string[] = []
    let lastFreshPages: ListObjectsResponse[] = []

    for (;;) {
      const results = await Promise.all(endpoints.map(async (endpoint) => {
        try {
          if (options.minLsn !== undefined) {
            const waitParams = new URLSearchParams({
              minLsn: String(Math.max(0, Math.floor(options.minLsn))),
              timeoutMs: String(Math.max(0, deadline - Date.now())),
              consistency: "local",
            })
            const wait = await this.getFrom<SyncWaitResponse>(endpoint, `/v1/sync/wait?${waitParams}`)
            if (!wait.caughtUp) {
              return { endpoint, response: undefined, error: `node is at LSN ${wait.currentLsn}` }
            }
          }
          const response = await this.getFrom<ListObjectsResponse>(endpoint, path)
          return { endpoint, response, error: undefined as string | undefined }
        } catch (error) {
          return { endpoint, response: undefined, error: errorMessage(error) }
        }
      }))
      const freshPages = results
        .map((result) => result.response)
        .filter((response): response is ListObjectsResponse => response !== undefined)
      lastFreshPages = freshPages
      lastFailures = results
        .filter((result) => result.response === undefined)
        .map((result) => `${result.endpoint}: ${result.error ?? "missing page"}`)

      if (freshPages.length >= required) {
        return mergeObjectListQuorum(freshPages, limit)
      }
      if (Date.now() >= deadline) {
        break
      }
      await delay(Math.min(50, Math.max(1, deadline - Date.now())))
    }

    const minimum = options.minLsn === undefined ? "" : ` at or above LSN ${options.minLsn}`
    const failures = lastFailures.length > 0 ? `; failures: ${lastFailures.join("; ")}` : ""
    throw new Error(
      `NextDB ${consistency} object list read quorum failed for shard ${shard.shard}: required ${required}/${endpoints.length} fresh page response(s)${minimum}, got ${lastFreshPages.length}${failures}`,
    )
  }

  private async getText(path: string): Promise<string> {
    const response = await fetch(`${this.activeEndpoint}${path}`, {
      headers: this.authHeaders(),
    })
    if (!response.ok) {
      const message = await response.text().catch(() => "")
      throw new Error(message || `Request failed with ${response.status}`)
    }
    return response.text()
  }

  private async post<T = unknown>(path: string, body: unknown): Promise<T> {
    return this.postOwnerAware<T>(path, body)
  }

  private async postOwnerAware<T = unknown>(path: string, body: unknown): Promise<T> {
    try {
      return await this.postTo<T>(this.activeEndpoint, path, body)
    } catch (error) {
      const ownerEndpoint = ownerRetryEndpoint(error)
      if (ownerEndpoint && !sameEndpoint(ownerEndpoint, this.activeEndpoint)) {
        return this.postTo<T>(ownerEndpoint, path, body)
      }
      const drainEndpoint = await this.drainingRetryEndpoint(error)
      if (!drainEndpoint) {
        throw error
      }
      return this.postTo<T>(drainEndpoint, path, body)
    }
  }

  private async deleteOwnerAware<T = unknown>(path: string): Promise<T> {
    try {
      return await this.deleteFrom<T>(this.activeEndpoint, path)
    } catch (error) {
      const ownerEndpoint = ownerRetryEndpoint(error)
      if (ownerEndpoint && !sameEndpoint(ownerEndpoint, this.activeEndpoint)) {
        return this.deleteFrom<T>(ownerEndpoint, path)
      }
      const drainEndpoint = await this.drainingRetryEndpoint(error)
      if (!drainEndpoint) {
        throw error
      }
      return this.deleteFrom<T>(drainEndpoint, path)
    }
  }

  private async postTo<T = unknown>(endpoint: string, path: string, body: unknown): Promise<T> {
    const response = await fetch(`${endpoint}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.authHeaders(),
      },
      body: JSON.stringify(body),
    })
    return parseResponse<T>(response)
  }

  private async deleteFrom<T = unknown>(endpoint: string, path: string): Promise<T> {
    const response = await fetch(`${endpoint}${path}`, {
      method: "DELETE",
      headers: this.authHeaders(),
    })
    return parseResponse<T>(response)
  }

  private currentWsEndpoint(): string {
    if (sameEndpoint(this.activeEndpoint, this.initialEndpoint) && this.explicitWsEndpoint) {
      return this.explicitWsEndpoint
    }
    return this.activeEndpoint.replace(/^http/, "ws")
  }

  private async drainingRetryEndpoint(error: unknown): Promise<string | undefined> {
    if (!isDrainingError(error)) {
      return undefined
    }
    const endpoint = await this.findAcceptingWriteEndpoint()
    if (endpoint !== undefined) {
      this.switchActiveEndpoint(endpoint)
    }
    return endpoint
  }

  private async findAcceptingWriteEndpoint(): Promise<string | undefined> {
    const candidates = uniqueEndpoints(this.knownEndpoints)
    await this.addHealthEndpoints(this.activeEndpoint, candidates)

    for (let index = 0; index < candidates.length && index < 32; index += 1) {
      const endpoint = candidates[index]
      if (sameEndpoint(endpoint, this.activeEndpoint)) {
        continue
      }
      const health = await this.fetchHealthFrom(endpoint)
      if (!health) {
        continue
      }
      this.rememberEndpoints(healthEndpoints(health))
      for (const discovered of healthEndpoints(health)) {
        if (!candidates.some((candidate) => sameEndpoint(candidate, discovered))) {
          candidates.push(discovered)
        }
      }
      if (health.acceptingWrites) {
        return endpoint
      }
    }

    return undefined
  }

  private async recoverEndpointForRealtime(): Promise<void> {
    const health = await this.fetchHealthFrom(this.activeEndpoint)
    if (health?.acceptingWrites) {
      this.rememberEndpoints(healthEndpoints(health))
      return
    }
    const endpoint = await this.findAcceptingWriteEndpoint()
    if (endpoint) {
      this.switchActiveEndpoint(endpoint)
    }
  }

  private async addHealthEndpoints(endpoint: string, candidates: string[]): Promise<void> {
    const health = await this.fetchHealthFrom(endpoint)
    if (!health) {
      return
    }
    const endpoints = healthEndpoints(health)
    this.rememberEndpoints(endpoints)
    for (const discovered of endpoints) {
      if (!candidates.some((candidate) => sameEndpoint(candidate, discovered))) {
        candidates.push(discovered)
      }
    }
  }

  private async fetchHealthFrom(endpoint: string): Promise<NextDbHealth | undefined> {
    try {
      const response = await fetch(`${endpoint}/v1/health`, {
        headers: this.authHeaders(),
      })
      return await parseResponse<NextDbHealth>(response)
    } catch {
      return undefined
    }
  }

  private rememberEndpoints(endpoints: string[]): void {
    for (const endpoint of endpoints) {
      if (!this.knownEndpoints.some((known) => sameEndpoint(known, endpoint))) {
        this.knownEndpoints.push(endpoint)
      }
    }
  }

  private switchActiveEndpoint(endpoint: string): void {
    if (sameEndpoint(endpoint, this.activeEndpoint)) {
      return
    }
    this.activeEndpoint = endpoint
    for (const roomId of this.activeRoomSubscriptionIds()) {
      this.pendingRoomSubscriptions.set(roomId, this.persistentRoomSubscriptions.get(roomId) ?? {})
    }
    for (const [id, subscription] of this.activeTableSubscriptionsForReconnect()) {
      this.pendingTableSubscriptions.set(id, {
        ...tableSubscriptionFrame(subscription.table, subscription.options),
        afterLsn: this.tableSeenLsn.get(subscription.table) ?? 0,
        catchUpLimit: subscription.options.catchUpLimit,
      })
    }
    for (const [id, subscription] of this.activeNestedTableSubscriptionsForReconnect()) {
      this.pendingNestedTableSubscriptions.set(id, this.nestedTableSubscriptionFrame(subscription))
    }
    for (const [queryId, subscription] of this.activeQuerySubscriptions()) {
      this.pendingQuerySubscriptions.set(queryId, subscription)
    }
    const transport = this.transport
    this.transport = undefined
    transport?.close()
    if (this.userId && (this.userListeners.size > 0 || this.persistentUserSubscription)) {
      this.pendingUserSubscription = this.persistentUserSubscription?.options ?? {}
    }
    if (this.objectListeners.size > 0 || this.persistentObjectSubscription) {
      this.pendingObjectSubscription = this.persistentObjectSubscription ?? {}
    }
    if (
      this.activeRoomSubscriptionIds().length > 0 ||
      this.activeTableSubscriptionIds().length > 0 ||
      this.activeNestedTableSubscriptionsForReconnect().length > 0 ||
      this.activeQuerySubscriptions().length > 0 ||
      this.userListeners.size > 0 ||
      this.persistentUserSubscription ||
      this.objectListeners.size > 0 ||
      this.persistentObjectSubscription ||
      this.joinedRealtimeChannels.size > 0
    ) {
      setTimeout(() => this.ensureSocket(), 0)
    }
  }

  private activeRoomSubscriptionIds(): string[] {
    return [...new Set([
      ...this.roomListeners.keys(),
      ...this.persistentRoomSubscriptions.keys(),
    ])]
  }

  private activeTableSubscriptionIds(): string[] {
    return [...new Set([
      ...this.activeTableSubscriptions.keys(),
      ...this.persistentTableSubscriptions.keys(),
    ])]
  }

  private activeTableNames(): string[] {
    return [...new Set([
      ...[...this.activeTableSubscriptions.values()].map((subscription) => subscription.table),
      ...[...this.persistentTableSubscriptions.values()].map((subscription) => subscription.table),
    ])]
  }

  private activeTableSubscriptionsForReconnect(): Array<[string, {
    table: string
    options: SubscriptionOptions
  }]> {
    const subscriptions = new Map<string, { table: string; options: SubscriptionOptions }>()
    for (const [id, subscription] of this.activeTableSubscriptions) {
      subscriptions.set(id, {
        table: subscription.table,
        options: subscription.options,
      })
    }
    for (const [id, subscription] of this.persistentTableSubscriptions) {
      subscriptions.set(id, subscription)
    }
    return [...subscriptions.entries()]
  }

  private activeNestedTableSubscriptionIds(): string[] {
    return [...new Set([
      ...[...this.activeNestedTableSubscriptions.values()].map(nestedTableSubscriptionLabel),
      ...[...this.persistentNestedTableSubscriptions.values()].map(nestedTableSubscriptionLabel),
    ])]
  }

  private activeNestedTableSubscriptionTargets(): SyncNestedTableTarget[] {
    const targets = new Map<string, SyncNestedTableTarget>()
    for (const [id, subscription] of this.activeNestedTableSubscriptionsForReconnect()) {
      targets.set(id, {
        table: subscription.table,
        parentKey: subscription.parentKey,
        nested: subscription.nested,
      })
    }
    return [...targets.values()]
  }

  private activeNestedTableSubscriptionTargetsForEvent(event: TableDeliveryEvent): SyncNestedTableTarget[] {
    return this.nestedTableSubscriptionTargetsForEvent(event, this.activeNestedTableSubscriptionTargets())
  }

  private nestedTableSubscriptionTargetsForEvent(
    event: { table: string; key: string },
    targets: SyncNestedTableTarget[],
  ): SyncNestedTableTarget[] {
    return targets.filter((target) =>
      event.table === nestedRecordTable(target.table, target.nested) &&
      event.key.startsWith(nestedRecordPrefix(target.parentKey)),
    )
  }

  private clearNestedCursorState(table: string, parentKey: string, nested: string): void {
    const cursorId = nestedTableCursorId(table, parentKey, nested)
    this.nestedTableSeenLsn.delete(cursorId)
    this.nestedTableSeenEventIds.delete(cursorId)
    this.nestedTableCaughtUpLsn.delete(cursorId)
    this.nestedTableAppliedEventIds.delete(cursorId)
  }

  private clearNestedCursorStateForLogicalTable(logicalTable: string): void {
    const prefix = `${nestedTableCursorIdPrefix(logicalTable)}:`
    for (const cursorId of [...this.nestedTableSeenLsn.keys()]) {
      if (cursorId.startsWith(prefix)) {
        this.nestedTableSeenLsn.delete(cursorId)
      }
    }
    for (const cursorId of [...this.nestedTableSeenEventIds.keys()]) {
      if (cursorId.startsWith(prefix)) {
        this.nestedTableSeenEventIds.delete(cursorId)
      }
    }
    for (const cursorId of [...this.nestedTableCaughtUpLsn.keys()]) {
      if (cursorId.startsWith(prefix)) {
        this.nestedTableCaughtUpLsn.delete(cursorId)
      }
    }
    for (const cursorId of [...this.nestedTableAppliedEventIds.keys()]) {
      if (cursorId.startsWith(prefix)) {
        this.nestedTableAppliedEventIds.delete(cursorId)
      }
    }
  }

  private hasPersistentTableTarget(table: string): boolean {
    return [...this.persistentTableSubscriptions.values()].some((subscription) => subscription.table === table) ||
      [...this.persistentNestedTableSubscriptions.values()].some((subscription) => subscription.logicalTable === table)
  }

  private optionsForActiveTableSubscription(table: string): SubscriptionOptions {
    return this.activeTableSubscriptionsForReconnect()
      .find(([, subscription]) => subscription.table === table)?.[1].options ?? {}
  }

  private activeNestedTableSubscriptionsForReconnect(): Array<[string, {
    table: string
    parentKey: string
    nested: string
    logicalTable: string
    options: SubscriptionOptions
  }]> {
    return [
      ...new Map([
        ...[...this.activeNestedTableSubscriptions.entries()].map(([id, subscription]) => [id, subscription] as const),
        ...this.persistentNestedTableSubscriptions,
      ]),
    ]
  }

  private nestedTableSubscriptionFrame(
    subscription: {
      table: string
      parentKey: string
      nested: string
      logicalTable: string
      options: SubscriptionOptions
    },
  ): Extract<ClientFrame, { type: "subscribeNestedTable" }> {
    const frame: Extract<ClientFrame, { type: "subscribeNestedTable" }> = {
      type: "subscribeNestedTable",
      table: subscription.table,
      parentKey: subscription.parentKey,
      nested: subscription.nested,
    }
    if (subscription.options.serverSnapshot === true) {
      frame.snapshotLimit = normalizePageLimit(subscription.options.snapshotLimit)
      return frame
    }
    if (subscription.options.catchUp !== false) {
      frame.afterLsn = this.nestedTableSeenLsn.get(nestedTableCursorId(subscription.table, subscription.parentKey, subscription.nested)) ?? 0
      frame.catchUpLimit = subscription.options.catchUpLimit
    }
    return frame
  }

  private activeQuerySubscriptions(): Array<[string, Extract<ClientFrame, { type: "subscribeQuery" }>] > {
    return [
      ...new Map([
        ...this.querySubscriptions,
        ...this.persistentQuerySubscriptions,
      ]),
    ]
  }

  private authHeaders(): Record<string, string> {
    const headers: Record<string, string> = {}
    if (this.schemaVersion !== undefined) {
      headers["x-nextdb-schema-version"] = String(this.schemaVersion)
    }
    const token = this.adminToken ?? this.authToken
    if (!token) {
      return headers
    }
    headers.authorization = `Bearer ${token}`
    headers["x-nextdb-client-token"] = this.authToken ?? token
    headers["x-nextdb-admin-token"] = this.adminToken ?? token
    return headers
  }

  private async commitSendMessage(request: {
    roomId: string
    userId: string
    body: string
    attachments: string[]
    durability: Durability
    clientMutationId: string
  }): Promise<NextDbMessage> {
    const response = await this.postOwnerAware<{ type: "messageCreated"; message: NextDbMessage }>("/v1/mutate", {
      type: "sendMessage",
      roomId: request.roomId,
      userId: request.userId,
      body: request.body,
      attachments: request.attachments,
      durability: request.durability,
      clientMutationId: request.clientMutationId,
    })
    if (isCacheableMessage(response.message)) {
      await this.putRoomMessagesCached(request.roomId, [response.message])
      this.advanceMessage(response.message)
      this.emitMessageCached("mutation", response.message)
    }
    return response.message
  }

  private commitSendMessageBatched(request: {
    roomId: string
    userId: string
    body: string
    attachments: string[]
    durability: Durability
    clientMutationId: string
  }): Promise<NextDbMessage> {
    const key = sendMessageBatchKey(request.roomId, request.userId, request.durability)
    let batch = this.pendingSendMessageBatches.get(key)
    if (batch?.items.some((item) => item.clientMutationId === request.clientMutationId)) {
      return this.commitSendMessage(request)
    }
    if (batch === undefined) {
      batch = {
        roomId: request.roomId,
        userId: request.userId,
        durability: request.durability,
        items: [],
        scheduled: false,
      }
      this.pendingSendMessageBatches.set(key, batch)
    }
    const promise = new Promise<NextDbMessage>((resolve, reject) => {
      batch.items.push({
        body: request.body,
        attachments: request.attachments,
        clientMutationId: request.clientMutationId,
        resolve,
        reject,
      })
    })
    if (batch.items.length >= DEFAULT_SEND_MESSAGE_BATCH_MAX) {
      this.pendingSendMessageBatches.delete(key)
      batch.scheduled = true
      void this.flushSendMessageBatch(batch)
    } else if (!batch.scheduled) {
      batch.scheduled = true
      queueMicrotask(() => {
        const current = this.pendingSendMessageBatches.get(key)
        if (current === batch) {
          this.pendingSendMessageBatches.delete(key)
        }
        void this.flushSendMessageBatch(batch)
      })
    }
    return promise
  }

  private async flushSendMessageBatch(batch: PendingSendMessageBatch): Promise<void> {
    const items = batch.items.splice(0)
    if (items.length === 0) {
      return
    }
    try {
      if (items.length === 1) {
        const item = items[0]
        const message = await this.commitSendMessage({
          roomId: batch.roomId,
          userId: batch.userId,
          body: item.body,
          attachments: item.attachments,
          durability: batch.durability,
          clientMutationId: item.clientMutationId,
        })
        item.resolve(message)
        return
      }
      const messages = await this.commitSendMessages({
        roomId: batch.roomId,
        userId: batch.userId,
        messages: items.map((item) => ({
          body: item.body,
          attachments: item.attachments,
          clientMutationId: item.clientMutationId,
        })),
        durability: batch.durability,
      })
      if (messages.length !== items.length) {
        throw new Error(`sendMessages returned ${messages.length} messages for ${items.length} requests`)
      }
      for (let index = 0; index < items.length; index += 1) {
        items[index].resolve(messages[index])
      }
    } catch (error) {
      for (const item of items) {
        item.reject(error)
      }
    }
  }

  private async commitSendMessages(request: {
    roomId: string
    userId: string
    messages: Array<SendMessagesItem & { attachments: string[] }>
    durability: Durability
  }): Promise<NextDbMessage[]> {
    const response = await this.postOwnerAware<MessagesCreatedResponse>("/v1/mutate", {
      type: "sendMessages",
      roomId: request.roomId,
      userId: request.userId,
      messages: request.messages,
      durability: request.durability,
    })
    const cacheable = response.messages.filter(isCacheableMessage)
    if (cacheable.length > 0) {
      await this.putRoomMessagesCached(request.roomId, cacheable)
      for (const message of cacheable) {
        this.advanceMessage(message)
        this.emitMessageCached("mutation", message)
      }
    }
    return response.messages
  }

  private async commitUserEvent(request: {
    userId: string
    name: string
    payload: unknown
    durability: Exclude<Durability, "volatile">
    clientMutationId: string
  }): Promise<NextDbUserEvent> {
    const response = await this.postOwnerAware<UserEventPublishResponse>("/v1/mutate", {
      type: "publishUserEvent",
      userId: request.userId,
      name: request.name,
      payload: request.payload,
      durability: request.durability,
      clientMutationId: request.clientMutationId,
    })
    await this.putUserEventsCached(request.userId, [response.event])
    this.advanceUserEvent(response.event)
    this.emitUserEventCached("mutation", response.event)
    return response.event
  }

  private async commitUserProfileUpsert(request: {
    userId: string
    displayName?: string
    metadata: unknown
    clientMutationId: string
  }): Promise<NextDbUserProfile> {
    const response = await this.postOwnerAware<UserResponse>(
      `/v1/users/${encodeURIComponent(request.userId)}`,
      {
        displayName: request.displayName,
        metadata: request.metadata,
        clientMutationId: request.clientMutationId,
      },
    )
    await this.putUserProfileCached(response.user, "mutation")
    return response.user
  }

  private async commitRecordTransaction<T = unknown>(
    operations: Array<RecordTransactionOperation<T>>,
    options: {
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
  ): Promise<RecordTransactionResponse<T>> {
    const response = await this.postOwnerAware<RecordTransactionResponse<T>>("/v1/records/transaction", {
      durability: options.durability,
      clientMutationId: options.clientMutationId,
      operations,
    })
    await this.applyRecordTransactionResult(response)
    return response
  }

  private async commitRecordBatch<T = unknown>(
    operations: Array<RecordTransactionOperation<T>>,
    options: {
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
  ): Promise<RecordBatchResponse<T>> {
    const topology = await this.clusterTopologyForRouting()
    if (topology.enforceOwnership) {
      const ownerGroups = await this.groupRecordTransactionOperationsByOwnerEndpoint(operations, topology)
      if (ownerGroups.size > 1) {
        const responses: Array<RecordBatchResponse<T>> = []
        let ownerIndex = 0
        for (const [endpoint, group] of ownerGroups.entries()) {
          responses.push(await this.commitRecordBatchTo<T>(endpoint, group, {
            ...options,
            clientMutationId: await recordBatchChildClientMutationId(
              options.clientMutationId,
              ownerIndex,
              0,
              true,
            ),
          }))
          ownerIndex += 1
        }
        return combineRecordBatchResponses(responses)
      }
    }
    const response = await this.postOwnerAware<RecordBatchResponse<T>>("/v1/records/batch", {
      durability: options.durability,
      clientMutationId: options.clientMutationId,
      operations,
    })
    await this.applyRecordTransactionResult(response)
    return response
  }

  private async commitRecordBatchTo<T = unknown>(
    endpoint: string,
    operations: Array<RecordTransactionOperation<T>>,
    options: {
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
  ): Promise<RecordBatchResponse<T>> {
    const response = await this.postTo<RecordBatchResponse<T>>(endpoint, "/v1/records/batch", {
      durability: options.durability,
      clientMutationId: options.clientMutationId,
      operations,
    })
    await this.applyRecordTransactionResult(response)
    return response
  }

  private async commitRecordBatchViaTransactions<T = unknown>(
    operations: Array<RecordTransactionOperation<T>>,
    options: {
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
  ): Promise<RecordBatchResponse<T>> {
    const groups = await this.groupRecordTransactionOperationsByShard(operations)
    const splitTransaction = groups.size > 1 || [...groups.values()].some((group) => group.length > 500)
    const responses: Array<RecordTransactionResponse<T>> = []
    for (const [shard, group] of groups.entries()) {
      for (let offset = 0; offset < group.length; offset += 500) {
        const chunk = group.slice(offset, offset + 500)
        responses.push(await this.recordTransaction<T>(chunk, {
          durability: options.durability,
          clientMutationId: await recordBatchChildClientMutationId(
            options.clientMutationId,
            shard,
            Math.floor(offset / 500),
            splitTransaction,
          ),
        }))
      }
    }
    return {
      lsn: Math.max(0, ...responses.map((response) => response.lsn)),
      transactionCount: responses.length,
      operations: responses.flatMap((response) => response.operations),
    }
  }

  private async groupRecordTransactionOperationsByShard<T>(
    operations: Array<RecordTransactionOperation<T>>,
  ): Promise<Map<number, Array<RecordTransactionOperation<T>>>> {
    const groups = new Map<number, Array<RecordTransactionOperation<T>>>()
    const shards = await Promise.all(operations.map((operation) =>
      this.clusterShardForKey(recordTransactionOperationShardKey(operation))))
    for (let index = 0; index < operations.length; index += 1) {
      const shard = shards[index]
      const group = groups.get(shard)
      if (group === undefined) {
        groups.set(shard, [operations[index]])
      } else {
        group.push(operations[index])
      }
    }
    return groups
  }

  private async groupRecordTransactionOperationsByOwnerEndpoint<T>(
    operations: Array<RecordTransactionOperation<T>>,
    topology: ClusterTopology,
  ): Promise<Map<string, Array<RecordTransactionOperation<T>>>> {
    const groups = new Map<string, Array<RecordTransactionOperation<T>>>()
    const shards = await Promise.all(operations.map((operation) =>
      this.clusterShardForKey(recordTransactionOperationShardKey(operation))))
    for (let index = 0; index < operations.length; index += 1) {
      const shard = topology.shards.find((entry) => entry.shard === shards[index])
      const endpoint = shard?.ownerUrl ?? (shard?.role === "owner" ? this.activeEndpoint : this.activeEndpoint)
      const group = groups.get(endpoint)
      if (group === undefined) {
        groups.set(endpoint, [operations[index]])
      } else {
        group.push(operations[index])
      }
    }
    return groups
  }

  private async commitRecordUpsert<T = unknown>(
    table: string,
    key: string,
    value: T,
    durability: Durability,
    expectedLsn?: number,
    clientMutationId?: string,
  ): Promise<NextDbRecord<T>> {
    const response = await this.postOwnerAware<RecordResponse<T>>(
      `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(key)}`,
      { value, durability, expectedLsn, clientMutationId },
    )
    if (isCacheableRecord(response.record)) {
      await this.putAuthoritativeRecordsCached([response.record])
      this.advanceRecord(response.record)
      this.emitRecordCached("mutation", response.record)
    } else {
      await this.rememberVolatileRecordOverlay(response.record)
    }
    return response.record
  }

  private async commitRecordDelete(
    table: string,
    key: string,
    durability: Durability,
    expectedLsn?: number,
    clientMutationId?: string,
  ): Promise<DeleteRecordResponse> {
    const params = new URLSearchParams({ durability })
    if (expectedLsn !== undefined) {
      params.set("expectedLsn", String(expectedLsn))
    }
    if (clientMutationId !== undefined) {
      params.set("clientMutationId", clientMutationId)
    }
    const response = await this.deleteOwnerAware<DeleteRecordResponse>(
      `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(key)}?${params}`,
    )
    if (response.deleted && isCacheableRecordDelete(response)) {
      await this.cache.deleteRecord(table, key)
      this.advanceRecordDelete(response)
      this.emitRecordDeleted("mutation", response)
    } else if (response.path.startsWith("volatile/")) {
      await this.forgetVolatileRecordOverlay(response)
    }
    return response
  }

  private async commitObjectPut(
    objectId: string,
    body: Blob,
    contentType: string,
    clientMutationId: string,
  ): Promise<NextDbObjectMetadata> {
    const params = new URLSearchParams({
      contentType,
      objectId,
      clientMutationId,
    })
    const metadata = await this.putObjectWithRetry(params, body, contentType)
    const cacheableBody = await objectBodyMatchesMetadata(body, metadata)
    await this.putObjectCached(metadata, cacheableBody ? body : undefined)
    this.emitCacheChange({
      type: "objectUpserted",
      source: "mutation",
      objectId: metadata.id,
      metadata,
    })
    return metadata
  }

  private async commitObjectDelete(
    objectId: string,
    force: boolean | undefined,
    clientMutationId: string,
  ): Promise<DeleteObjectResponse> {
    const params = new URLSearchParams({ clientMutationId })
    if (force !== undefined) {
      params.set("force", String(force))
    }
    const response = await this.deleteOwnerAware<DeleteObjectResponse>(
      `/v1/objects/${encodeURIComponent(objectId)}?${params}`,
    )
    await this.cache.deleteObject(objectId)
    this.emitCacheChange({
      type: "objectDeleted",
      source: "mutation",
      objectId,
    })
    return response
  }

  private async commitNestedRecordUpsert<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    value: T,
    durability: Durability,
    expectedLsn?: number,
    clientMutationId?: string,
  ): Promise<NextDbRecord<T>> {
    const response = await this.postOwnerAware<RecordResponse<T>>(
      `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(parentKey)}/${encodeURIComponent(nested)}/${encodeURIComponent(nestedKey)}`,
      { value, durability, expectedLsn, clientMutationId },
    )
    if (isCacheableRecord(response.record)) {
      await this.putAuthoritativeRecordsCached([response.record])
      this.advanceRecord(response.record)
      this.emitRecordCached("mutation", response.record)
    } else {
      await this.rememberVolatileRecordOverlay(response.record)
    }
    return response.record
  }

  private async commitNestedRecordDelete(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    durability: Durability,
    expectedLsn?: number,
    clientMutationId?: string,
  ): Promise<DeleteRecordResponse> {
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    const params = new URLSearchParams({ durability })
    if (expectedLsn !== undefined) {
      params.set("expectedLsn", String(expectedLsn))
    }
    if (clientMutationId !== undefined) {
      params.set("clientMutationId", clientMutationId)
    }
    const response = await this.deleteOwnerAware<DeleteRecordResponse>(
      `/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(parentKey)}/${encodeURIComponent(nested)}/${encodeURIComponent(nestedKey)}?${params}`,
    )
    if (response.deleted && isCacheableRecordDelete(response)) {
      await this.cache.deleteRecord(logicalTable, logicalKey)
      this.advanceRecordDelete(response)
      this.emitRecordDeleted("mutation", response)
    } else if (response.path.startsWith("volatile/")) {
      await this.forgetVolatileRecordOverlay(response)
    }
    return response
  }

  private async applyRecordTransactionResult(response: RecordTransactionResponse): Promise<void> {
    const records: NextDbRecord[] = []
    for (const operation of response.operations) {
      if (operation.type === "recordUpserted") {
        records.push(operation.record)
        this.advanceRecord(operation.record)
        this.emitRecordCached("mutation", operation.record)
      } else {
        await this.cache.deleteRecord(operation.table, operation.key)
        this.advanceRecordDelete(operation)
        this.emitRecordDeleted("mutation", operation)
      }
    }
    if (records.length > 0) {
      await this.putAuthoritativeRecordsCached(records)
    }
    this.advanceLsn(response.lsn)
  }

  private async enqueuePendingMessage(
    request: {
      roomId: string
      userId: string
      body: string
      attachments: string[]
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
    error: unknown,
  ): Promise<NextDbMessage> {
    const id = nextClientId("pending-message")
    const createdAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingSendMessageWrite = {
      id,
      type: "sendMessage",
      createdAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      ...request,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    const message: NextDbMessage = {
      id,
      roomId: request.roomId,
      senderId: request.userId,
      body: request.body,
      attachments: [],
      createdAtMs,
      lsn: 0,
      path: `pending/rooms/${request.roomId}/messages/${id}`,
    }
    await this.putRoomMessagesCached(request.roomId, [message])
    this.emitMessageCached("offline", message)
    return message
  }

  private async enqueuePendingUserEvent(
    request: {
      userId: string
      name: string
      payload: unknown
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
    error: unknown,
  ): Promise<NextDbUserEvent> {
    const id = nextClientId("pending-user-event")
    const createdAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingUserEventWrite = {
      id,
      type: "userEvent",
      createdAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      ...request,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    return {
      id,
      userId: request.userId,
      name: request.name,
      payload: request.payload,
      createdAtMs,
      lsn: 0,
      path: `pending/users/${request.userId}/events/${id}`,
    }
  }

  private async enqueuePendingUserProfileUpsert(
    request: {
      userId: string
      displayName?: string
      metadata: unknown
      clientMutationId: string
    },
    error: unknown,
  ): Promise<NextDbUserProfile> {
    const createdAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingUserProfileUpsertWrite = {
      id: nextClientId("pending-user-profile"),
      type: "userProfileUpsert",
      createdAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      ...request,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    const existing = await this.cache.getUserProfile(request.userId)
    const profile: NextDbUserProfile = {
      userId: request.userId,
      displayName: request.displayName,
      metadata: request.metadata,
      createdAtMs: existing?.createdAtMs ?? createdAtMs,
      updatedAtMs: createdAtMs,
      lsn: 0,
      path: `users/${request.userId}`,
    }
    await this.putUserProfileCached(profile, "offline")
    return profile
  }

  private async enqueuePendingRecordTransaction<T = unknown>(
    request: {
      operations: Array<RecordTransactionOperation<T>>
      durability: Exclude<Durability, "volatile">
      clientMutationId: string
    },
    error: unknown,
  ): Promise<RecordTransactionResponse<T>> {
    const write: PendingRecordTransactionWrite<T> = {
      id: nextClientId("pending-record-transaction"),
      type: "recordTransaction",
      createdAtMs: this.nextPendingWriteCreatedAtMs(),
      attempts: 0,
      lastError: errorMessage(error),
      operations: request.operations,
      durability: request.durability,
      clientMutationId: request.clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    return {
      lsn: 0,
      operations: [],
    }
  }

  private async enqueuePendingRecord<T = unknown>(
    table: string,
    key: string,
    value: T,
    durability: Exclude<Durability, "volatile">,
    expectedLsn: number | undefined,
    clientMutationId: string,
    error: unknown,
  ): Promise<NextDbRecord<T>> {
    const id = nextClientId("pending-record")
    const updatedAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingRecordUpsertWrite<T> = {
      id,
      type: "recordUpsert",
      createdAtMs: updatedAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      table,
      key,
      value,
      durability,
      expectedLsn,
      clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    const record: NextDbRecord<T> = {
      table,
      key,
      value,
      updatedAtMs,
      lsn: 0,
      path: `tables/${table}/${key}`,
    }
    await this.putRecordsCached([record])
    this.emitRecordCached("offline", record)
    return record
  }

  private async enqueuePendingRecordDelete(
    table: string,
    key: string,
    durability: Exclude<Durability, "volatile">,
    expectedLsn: number | undefined,
    clientMutationId: string,
    error: unknown,
  ): Promise<DeleteRecordResponse> {
    const id = nextClientId("pending-record-delete")
    const deletedAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingRecordDeleteWrite = {
      id,
      type: "recordDelete",
      createdAtMs: deletedAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      table,
      key,
      durability,
      expectedLsn,
      clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    await this.cache.deleteRecord(table, key)
    const response = {
      table,
      key,
      deleted: true,
      lsn: 0,
      deletedAtMs,
      path: `tables/${table}/${key}`,
    }
    this.emitRecordDeleted("offline", response)
    return response
  }

  private async enqueuePendingNestedRecord<T = unknown>(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    value: T,
    durability: Exclude<Durability, "volatile">,
    expectedLsn: number | undefined,
    clientMutationId: string,
    error: unknown,
  ): Promise<NextDbRecord<T>> {
    const id = nextClientId("pending-nested-record")
    const updatedAtMs = this.nextPendingWriteCreatedAtMs()
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    const write: PendingNestedRecordUpsertWrite<T> = {
      id,
      type: "nestedRecordUpsert",
      createdAtMs: updatedAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      table,
      parentKey,
      nested,
      nestedKey,
      value,
      durability,
      expectedLsn,
      clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    const record: NextDbRecord<T> = {
      table: logicalTable,
      key: logicalKey,
      value,
      updatedAtMs,
      lsn: 0,
      path: nestedRecordPath(table, parentKey, nested, nestedKey),
    }
    await this.putRecordsCached([record])
    this.emitRecordCached("offline", record)
    return record
  }

  private async enqueuePendingNestedRecordDelete(
    table: string,
    parentKey: string,
    nested: string,
    nestedKey: string,
    durability: Exclude<Durability, "volatile">,
    expectedLsn: number | undefined,
    clientMutationId: string,
    error: unknown,
  ): Promise<DeleteRecordResponse> {
    const id = nextClientId("pending-nested-record-delete")
    const deletedAtMs = this.nextPendingWriteCreatedAtMs()
    const logicalTable = nestedRecordTable(table, nested)
    const logicalKey = nestedRecordKey(parentKey, nestedKey)
    const write: PendingNestedRecordDeleteWrite = {
      id,
      type: "nestedRecordDelete",
      createdAtMs: deletedAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      table,
      parentKey,
      nested,
      nestedKey,
      durability,
      expectedLsn,
      clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    await this.cache.deleteRecord(logicalTable, logicalKey)
    const response = {
      table: logicalTable,
      key: logicalKey,
      deleted: true,
      lsn: 0,
      deletedAtMs,
      path: nestedRecordPath(table, parentKey, nested, nestedKey),
    }
    this.emitRecordDeleted("offline", response)
    return response
  }

  private async enqueuePendingObjectPut(
    objectId: string,
    body: Blob,
    contentType: string,
    clientMutationId: string,
    error: unknown,
  ): Promise<NextDbObjectMetadata> {
    const createdAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingObjectPutWrite = {
      id: nextClientId("pending-object"),
      type: "objectPut",
      createdAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      objectId,
      contentType,
      body,
      clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    const metadata: NextDbObjectMetadata = {
      id: objectId,
      path: `objects/${objectId}`,
      contentType,
      byteSize: body.size,
      sha256: await objectBodySha256(body) ?? `pending:${objectId}`,
      createdAtMs,
    }
    await this.putObjectCached(metadata, body)
    this.emitCacheChange({
      type: "objectUpserted",
      source: "offline",
      objectId,
      metadata,
    })
    return metadata
  }

  private async enqueuePendingObjectDelete(
    objectId: string,
    force: boolean | undefined,
    clientMutationId: string,
    error: unknown,
  ): Promise<DeleteObjectResponse> {
    const deletedAtMs = this.nextPendingWriteCreatedAtMs()
    const write: PendingObjectDeleteWrite = {
      id: nextClientId("pending-object-delete"),
      type: "objectDelete",
      createdAtMs: deletedAtMs,
      attempts: 0,
      lastError: errorMessage(error),
      objectId,
      force,
      clientMutationId,
    }
    await this.putPendingWriteQueued(write)
    this.schedulePendingWriteFlush(0)
    await this.cache.deleteObject(objectId)
    const response: DeleteObjectResponse = {
      objectId,
      deleted: true,
      lsn: 0,
      deletedAtMs,
      path: `objects/${objectId}`,
    }
    this.emitCacheChange({
      type: "objectDeleted",
      source: "offline",
      objectId,
    })
    return response
  }

  private nextPendingWriteCreatedAtMs(): number {
    const now = Date.now()
    this.pendingWriteClock = Math.max(now, this.pendingWriteClock + 1)
    return this.pendingWriteClock
  }

  private async putPendingWriteQueued(write: NextDbPendingWrite): Promise<void> {
    try {
      await this.assertPendingWriteWithinProfile(write)
    } catch (error) {
      if (error instanceof NextDbPendingWriteLimitError) {
        await this.emitPendingWriteChange({
          type: "pendingWriteRejected",
          source: "offline",
          write,
          limit: error.details,
        })
      }
      throw error
    }
    await this.cache.putPendingWrite(write)
    await this.emitPendingWriteChange({
      type: "pendingWriteQueued",
      source: "offline",
      write,
    })
  }

  private async assertPendingWriteWithinProfile(write: NextDbPendingWrite): Promise<void> {
    const [writes, metadata] = await Promise.all([
      this.cache.listPendingWrites(),
      this.cache.getMetadata(),
    ])
    const maxWrites = this.clientCacheProfile?.maxPendingWrites ?? metadata?.maxPendingWrites ?? 0
    const maxBytes = this.clientCacheProfile?.maxPendingWriteBytes ?? metadata?.maxPendingWriteBytes ?? 0
    const isNewWrite = writes.every((existing) => existing.id !== write.id)
    const nextTotal = writes.length + (isNewWrite ? 1 : 0)
    const currentBytes = writes.reduce(
      (sum, existing) => sum + (existing.id === write.id ? 0 : estimatePendingWriteBytes(existing)),
      0,
    )
    const writeBytes = estimatePendingWriteBytes(write)
    const nextBytes = currentBytes + writeBytes
    if (maxWrites > 0 && nextTotal > maxWrites) {
      throw new NextDbPendingWriteLimitError({
        limitKind: "maxPendingWrites",
        writeId: write.id,
        writeType: write.type,
        currentWrites: writes.length,
        nextWrites: nextTotal,
        maxWrites,
        currentBytes,
        writeBytes,
        nextBytes,
        maxBytes,
      })
    }
    if (maxBytes > 0 && nextBytes > maxBytes) {
      throw new NextDbPendingWriteLimitError({
        limitKind: "maxPendingWriteBytes",
        writeId: write.id,
        writeType: write.type,
        currentWrites: writes.length,
        nextWrites: nextTotal,
        maxWrites,
        currentBytes,
        writeBytes,
        nextBytes,
        maxBytes,
      })
    }
  }

  private requireUserId(operation: string): string {
    if (!this.userId) {
      throw new Error(`${operation} requires userId in NextDbClientOptions`)
    }
    return this.userId
  }
}

export class RoomHandle {
  constructor(
    private readonly client: NextDbClient,
    readonly roomId: string,
  ) {}

  messages = {
    latest: (limitOrOptions?: number | (FreshnessOptions & { limit?: number })) =>
      this.client.latestMessages(this.roomId, limitOrOptions),
    before: (beforeLsn: number, limitOrOptions?: number | (FreshnessOptions & { limit?: number })) =>
      this.client.messagesBefore(this.roomId, beforeLsn, limitOrOptions),
    cached: (options?: ListCachedRoomMessagesOptions) =>
      this.client.listCachedRoomMessages(this.roomId, options),
    subscribe: (listener: (event: RoomDeliveryEvent) => void, options?: SubscriptionOptions) =>
      this.client.subscribeRoom(this.roomId, listener, options),
    watchLatest: (listener: (snapshot: RoomMessagesSnapshot) => void, options?: WatchOptions) =>
      this.client.watchRoomMessages(this.roomId, listener, options),
    sync: (options?: { limit?: number; maxPages?: number }) => this.client.syncRoom(this.roomId, options),
    activateRuntime: (options: RuntimeRecordHandleActivationOptions = {}) =>
      this.client.activateRuntimeRecords({
        ...options,
        table: "rooms",
        parentKey: this.roomId,
        nested: "messages",
      }),
    send: (body: string, optionsOrDurability?: SendMessageOptions | Durability) =>
      this.client.sendMessage(this.roomId, body, optionsOrDurability ?? "strict"),
    sendMany: (messages: Array<string | SendMessagesItem>, optionsOrDurability?: SendMessagesOptions | Durability) =>
      this.client.sendMessages(this.roomId, messages, optionsOrDurability ?? "strict"),
  }

  cache = {
    clear: () => this.client.clearRoomCache(this.roomId),
    trim: (keepLatest: number) => this.client.trimRoomCache(this.roomId, keepLatest),
  }

  publishVolatile(name: string, payload: unknown): Promise<VolatilePublishResponse> {
    return this.client.publishVolatile(this.roomId, name, payload)
  }
}

export class TableHandle {
  constructor(
    private readonly client: NextDbClient,
    readonly table: string,
  ) {}

  upsert<T = unknown>(
    key: string,
    value: T,
    optionsOrDurability: UpsertRecordOptions | Durability = "strict",
  ): Promise<NextDbRecord<T>> {
    return this.client.upsertRecord(this.table, key, value, optionsOrDurability)
  }

  async upsertMany<T = unknown>(
    records: Array<UpsertManyRecordItem<T>>,
    options: RecordTransactionOptions = {},
  ): Promise<Array<NextDbRecord<T>>> {
    if (records.length === 0) {
      return []
    }
    const response = await this.client.recordBatch<T>(
      records.map((record) => ({
        type: "upsert",
        table: this.table,
        key: record.key,
        value: record.value,
        expectedLsn: record.expectedLsn,
      })),
      options,
    )
    const byKey = new Map<string, NextDbRecord<T>>()
    for (const operation of response.operations) {
      if (operation.type === "recordUpserted") {
        byKey.set(operation.record.key, operation.record)
      }
    }
    return records.map((record) => {
      const upserted = byKey.get(record.key)
      if (upserted === undefined) {
        throw new Error(`upsertMany response missing record ${record.key}`)
      }
      return upserted
    })
  }

  delete(
    key: string,
    optionsOrDurability: DeleteRecordOptions | Durability = "strict",
  ): Promise<DeleteRecordResponse> {
    return this.client.deleteRecord(this.table, key, optionsOrDurability)
  }

  get<T = unknown>(key: string, options?: FreshnessOptions): Promise<NextDbRecord<T>> {
    return this.client.getRecord<T>(this.table, key, options)
  }

  list<T = unknown>(limitOrOptions?: number | PageReadOptions, afterKey?: string): Promise<ListRecordsResponse<T>> {
    return this.client.listRecords<T>(this.table, limitOrOptions, afterKey)
  }

  index<T = unknown>(indexName: string, options: QueryRecordsByIndexOptions): Promise<ListRecordsResponse<T>> {
    return this.client.queryRecordsByIndex<T>(this.table, indexName, options)
  }

  activateRuntime(options: RuntimeRecordHandleActivationOptions = {}): Promise<RuntimeRecordActivationResponse> {
    return this.client.activateRuntimeRecords({ ...options, table: this.table })
  }

  evictRuntime(options: RuntimeRecordHandleActivationOptions = {}): Promise<RuntimeRecordActivationResponse> {
    return this.client.evictRuntimeRecords({ ...options, table: this.table })
  }

  transaction<T = unknown>(
    operations: Array<
      | {
          type: "upsert"
          key: string
          value: T
          expectedLsn?: number
        }
      | {
          type: "delete"
          key: string
          expectedLsn?: number
        }
    >,
    options: RecordTransactionOptions = {},
  ): Promise<RecordTransactionResponse<T>> {
    return this.client.recordTransaction<T>(
      operations.map((operation) => {
        if (operation.type === "upsert") {
          return {
            type: "upsert",
            table: this.table,
            key: operation.key,
            value: operation.value,
            expectedLsn: operation.expectedLsn,
          }
        }
        return {
          type: "delete",
          table: this.table,
          key: operation.key,
          expectedLsn: operation.expectedLsn,
        }
      }),
      options,
    )
  }

  sync(options?: { limit?: number; maxPages?: number }): Promise<SyncUntilCaughtUpResponse> {
    return this.client.syncTable(this.table, options)
  }

  subscribe(listener: (event: TableDeliveryEvent) => void, options?: SubscriptionOptions): () => void {
    return this.client.subscribeTable(this.table, listener, options)
  }

  subscribeQuery<T = unknown>(
    listener: (event: RecordLiveQueryResult<T>) => void,
    options: Omit<RecordLiveQueryOptions, "table" | "parentKey" | "nested"> = {},
  ): () => void {
    return this.client.subscribeQuery<T>({ ...options, table: this.table }, listener)
  }

  watchList<T = unknown>(listener: (snapshot: TableRecordsSnapshot<T>) => void, options?: WatchOptions): () => void {
    return this.client.watchTableRecords<T>(this.table, listener, options)
  }

  watch<T = unknown>(key: string, listener: (snapshot: RecordSnapshot<T>) => void, options?: WatchOptions): () => void {
    return this.client.watchRecord<T>(this.table, key, listener, options)
  }

  cache = {
    get: <T = unknown>(key: string) => this.client.getCachedRecord<T>(this.table, key),
    list: <T = unknown>(options?: ListCachedRecordsOptions) => this.client.listCachedRecords<T>(this.table, options),
    clear: () => this.client.clearTableCache(this.table),
  }
}

export class NestedTableHandle {
  constructor(
    private readonly client: NextDbClient,
    readonly table: string,
    readonly parentKey: string,
    readonly nested: string,
  ) {}

  upsert<T = unknown>(
    key: string,
    value: T,
    optionsOrDurability: UpsertRecordOptions | Durability = "strict",
  ): Promise<NextDbRecord<T>> {
    return this.client.upsertNestedRecord(this.table, this.parentKey, this.nested, key, value, optionsOrDurability)
  }

  async upsertMany<T = unknown>(
    records: Array<UpsertManyRecordItem<T>>,
    options: RecordTransactionOptions = {},
  ): Promise<Array<NextDbRecord<T>>> {
    const response = await this.transaction<T>(
      records.map((record) => ({
        type: "upsert",
        key: record.key,
        value: record.value,
        expectedLsn: record.expectedLsn,
      })),
      options,
    )
    return response.operations.flatMap((operation) =>
      operation.type === "recordUpserted" ? [operation.record] : [])
  }

  delete(
    key: string,
    optionsOrDurability: DeleteRecordOptions | Durability = "strict",
  ): Promise<DeleteRecordResponse> {
    return this.client.deleteNestedRecord(this.table, this.parentKey, this.nested, key, optionsOrDurability)
  }

  get<T = unknown>(key: string, options?: FreshnessOptions): Promise<NextDbRecord<T>> {
    return this.client.getNestedRecord<T>(this.table, this.parentKey, this.nested, key, options)
  }

  list<T = unknown>(limitOrOptions?: number | PageReadOptions, afterKey?: string): Promise<ListRecordsResponse<T>> {
    return this.client.listNestedRecords<T>(this.table, this.parentKey, this.nested, limitOrOptions, afterKey)
  }

  listBySchemaOrder<T = unknown>(
    limitOrOptions?: number | NestedSchemaOrderListOptions,
    afterKey?: string,
  ): Promise<ListRecordsResponse<T>> {
    const options = typeof limitOrOptions === "object" ? limitOrOptions : { limit: limitOrOptions, afterKey }
    return this.client.listNestedRecords<T>(
      this.table,
      this.parentKey,
      this.nested,
      options,
      options.afterKey,
      "schema",
      options.afterCursor,
    )
  }

  index<T = unknown>(indexName: string, options: QueryRecordsByIndexOptions): Promise<ListRecordsResponse<T>> {
    return this.client.queryNestedRecordsByIndex<T>(this.table, this.parentKey, this.nested, indexName, options)
  }

  activateRuntime(options: RuntimeRecordHandleActivationOptions = {}): Promise<RuntimeRecordActivationResponse> {
    return this.client.activateRuntimeRecords({
      ...options,
      table: this.table,
      parentKey: this.parentKey,
      nested: this.nested,
    })
  }

  evictRuntime(options: RuntimeRecordHandleActivationOptions = {}): Promise<RuntimeRecordActivationResponse> {
    return this.client.evictRuntimeRecords({
      ...options,
      table: this.table,
      parentKey: this.parentKey,
      nested: this.nested,
    })
  }

  transaction<T = unknown>(
    operations: Array<
      | {
          type: "upsert"
          key: string
          value: T
          expectedLsn?: number
        }
      | {
          type: "delete"
          key: string
          expectedLsn?: number
        }
    >,
    options: RecordTransactionOptions = {},
  ): Promise<RecordTransactionResponse<T>> {
    return this.client.recordTransaction<T>(
      operations.map((operation) => {
        if (operation.type === "upsert") {
          return {
            type: "nestedUpsert",
            table: this.table,
            parentKey: this.parentKey,
            nested: this.nested,
            nestedKey: operation.key,
            value: operation.value,
            expectedLsn: operation.expectedLsn,
          }
        }
        return {
          type: "nestedDelete",
          table: this.table,
          parentKey: this.parentKey,
          nested: this.nested,
          nestedKey: operation.key,
          expectedLsn: operation.expectedLsn,
        }
      }),
      options,
    )
  }

  sync(options?: { limit?: number; maxPages?: number }): Promise<SyncUntilCaughtUpResponse> {
    return this.client.syncUntilCaughtUp({
      nestedTables: [{
        table: this.table,
        parentKey: this.parentKey,
        nested: this.nested,
      }],
      limit: options?.limit,
      maxPages: options?.maxPages,
    })
  }

  subscribe(listener: (event: TableDeliveryEvent) => void, options?: SubscriptionOptions): () => void {
    return this.client.subscribeNestedTable(this.table, this.parentKey, this.nested, listener, options)
  }

  watchList<T = unknown>(listener: (snapshot: TableRecordsSnapshot<T>) => void, options?: WatchOptions): () => void {
    return this.client.watchNestedRecords<T>(this.table, this.parentKey, this.nested, listener, options)
  }

  watch<T = unknown>(key: string, listener: (snapshot: RecordSnapshot<T>) => void, options?: WatchOptions): () => void {
    return this.client.watchNestedRecord<T>(this.table, this.parentKey, this.nested, key, listener, options)
  }

  subscribeQuery<T = unknown>(
    listener: (event: RecordLiveQueryResult<T>) => void,
    options: Omit<RecordLiveQueryOptions, "table" | "parentKey" | "nested"> = {},
  ): () => void {
    return this.client.subscribeQuery<T>({
      ...options,
      table: this.table,
      parentKey: this.parentKey,
      nested: this.nested,
    }, listener)
  }

  cache = {
    get: <T = unknown>(key: string) =>
      this.client.getCachedNestedRecord<T>(this.table, this.parentKey, this.nested, key),
    list: <T = unknown>(options?: ListCachedNestedRecordsOptions) =>
      this.client.listCachedNestedRecords<T>(this.table, this.parentKey, this.nested, options),
    listBySchemaOrder: <T = unknown>(options?: Omit<ListCachedNestedRecordsOptions, "order">) =>
      this.client.listCachedNestedRecords<T>(this.table, this.parentKey, this.nested, {
        ...options,
        order: "schema",
      }),
    clear: () => this.client.clearNestedTableCache(this.table, this.parentKey, this.nested),
  }
}

export class ObjectStoreHandle {
  constructor(
    private readonly client: NextDbClient,
    readonly objectName: string,
  ) {}

  put(
    body: Blob | ArrayBuffer | Uint8Array | string,
    contentTypeOrOptions?: string | PutObjectOptions,
  ): Promise<NextDbObjectMetadata> {
    return this.client.putObject(body, contentTypeOrOptions)
  }

  getMetadata(objectId: string, options?: FreshnessOptions): Promise<NextDbObjectMetadata> {
    return this.client.getObjectMetadata(objectId, options)
  }

  getCachedMetadata(objectId: string): Promise<NextDbObjectMetadata | undefined> {
    return this.client.getCachedObjectMetadata(objectId)
  }

  getBody(objectId: string, options?: FreshnessOptions): Promise<Blob> {
    return this.client.getObjectBody(objectId, options)
  }

  getCachedBody(objectId: string): Promise<Blob | undefined> {
    return this.client.getCachedObjectBody(objectId)
  }

  getBodyRange(objectId: string, options: ObjectBodyRangeOptions): Promise<ObjectBodyRangeResponse> {
    return this.client.getObjectBodyRange(objectId, options)
  }

  getReferences(objectId: string): Promise<ObjectReferences> {
    return this.client.getObjectReferences(objectId)
  }

  delete(objectId: string, options?: DeleteObjectOptions): Promise<DeleteObjectResponse> {
    return this.client.deleteObject(objectId, options)
  }

  list(options?: ListObjectsOptions): Promise<ListObjectsResponse> {
    return this.client.listObjects(options)
  }

  listCached(options?: ListCachedObjectsOptions): Promise<ListObjectsResponse> {
    return this.client.listCachedObjects(options)
  }

  sync(options?: { limit?: number; maxPages?: number }): Promise<SyncUntilCaughtUpResponse> {
    return this.client.syncObjects(options)
  }

  subscribe(listener: (event: ObjectDeliveryEvent) => void, options?: SubscriptionOptions): () => void {
    return this.client.subscribeObjects(listener, options)
  }

  watchList(listener: (snapshot: ObjectListSnapshot) => void, options?: WatchOptions): () => void {
    return this.client.watchObjects(listener, options)
  }

  watch(objectId: string, listener: (snapshot: ObjectSnapshot) => void, options?: ObjectWatchOptions): () => void {
    return this.client.watchObject(objectId, listener, options)
  }
}

export class RealtimeChannelHandle {
  constructor(
    private readonly client: NextDbClient,
    readonly channelId: string,
  ) {}

  join(metadata: unknown = {}): Promise<RealtimeJoinResponse> {
    return this.client.joinRealtimeChannel(this.channelId, metadata)
  }

  leave(): Promise<RealtimeLeaveResponse> {
    return this.client.leaveRealtimeChannel(this.channelId)
  }

  updatePresence(metadata: unknown): Promise<RealtimePresenceUpdateResponse> {
    return this.client.updateRealtimePresence(this.channelId, metadata)
  }

  members(): Promise<RealtimeMembersResponse> {
    return this.client.realtimeChannelMembers(this.channelId)
  }

  cachedMembers<TMetadata = unknown>(): (RealtimeMembersResponse & { members: Array<RealtimeMember & { metadata: TMetadata }> }) | undefined {
    return this.client.cachedRealtimeChannelMembers<TMetadata>(this.channelId)
  }

  watchMembers<TMetadata = unknown>(
    listener: (snapshot: RealtimeMembersSnapshotView<TMetadata>) => void,
    options?: WatchOptions,
  ): () => void {
    return this.client.watchRealtimeChannelMembers<TMetadata>(this.channelId, listener, options)
  }

  state<T = unknown>(): Promise<RealtimeChannelStateResponse<T>> {
    return this.client.realtimeChannelState<T>(this.channelId)
  }

  cachedState<T = unknown>(): RealtimeChannelStateSnapshot<T> | undefined {
    return this.client.cachedRealtimeChannelState<T>(this.channelId)
  }

  watchState<T = unknown>(
    listener: (snapshot: RealtimeChannelStateSnapshotView<T>) => void,
    options?: WatchOptions,
  ): () => void {
    return this.client.watchRealtimeChannelState<T>(this.channelId, listener, options)
  }

  cachedRecentEvents(options?: RealtimeChannelEventsOptions): RealtimeChannelEvent[] {
    return this.client.cachedRealtimeChannelEvents(this.channelId, options)
  }

  watchRecentEvents(
    listener: (snapshot: RealtimeChannelEventsSnapshotView) => void,
    options?: WatchOptions & RealtimeChannelEventsOptions,
  ): () => void {
    return this.client.watchRealtimeChannelEvents(this.channelId, listener, options)
  }

  cachedRecentSignals(options?: RealtimeChannelSignalsOptions): RealtimeSignal[] {
    return this.client.cachedRealtimeChannelSignals(this.channelId, options)
  }

  watchRecentSignals(
    listener: (snapshot: RealtimeChannelSignalsSnapshotView) => void,
    options?: WatchOptions & RealtimeChannelSignalsOptions,
  ): () => void {
    return this.client.watchRealtimeChannelSignals(this.channelId, listener, options)
  }

  updateState<T = unknown>(
    state: T,
    options?: { expectedVersion?: number },
  ): Promise<RealtimeChannelStateUpdateResponse<T>> {
    return this.client.updateRealtimeChannelState<T>(this.channelId, state, options)
  }

  signal(toUserId: string, kind: RealtimeSignal["kind"], payload: unknown): Promise<RealtimeSignalResponse> {
    return this.client.sendRealtimeSignal(this.channelId, toUserId, kind, payload)
  }

  sendOffer(toUserId: string, payload: unknown): Promise<RealtimeSignalResponse> {
    return this.signal(toUserId, "offer", payload)
  }

  sendAnswer(toUserId: string, payload: unknown): Promise<RealtimeSignalResponse> {
    return this.signal(toUserId, "answer", payload)
  }

  sendIce(toUserId: string, payload: unknown): Promise<RealtimeSignalResponse> {
    return this.signal(toUserId, "ice", payload)
  }

  broadcast(kind: RealtimeChannelEvent["kind"], payload: unknown, options?: { includeSelf?: boolean }): Promise<RealtimeBroadcastResponse> {
    return this.client.broadcastRealtimeEvent(this.channelId, kind, payload, options)
  }

  sendGameInput(payload: unknown, options?: { includeSelf?: boolean }): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("gameInput", payload, options)
  }

  async sendGameInputFrame(body: RealtimeBinaryBody, options: RealtimeBinaryFrameOptions = {}): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("gameInput", await createRealtimeBinaryFrame(body, options), { includeSelf: options.includeSelf })
  }

  sendStatePatch(payload: unknown, options?: { includeSelf?: boolean }): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("statePatch", payload, options)
  }

  sendVoice(payload: unknown, options?: { includeSelf?: boolean }): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("voice", payload, options)
  }

  async sendVoiceFrame(body: RealtimeBinaryBody, options: RealtimeBinaryFrameOptions = {}): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("voice", await createRealtimeBinaryFrame(body, options), { includeSelf: options.includeSelf })
  }

  sendVideo(payload: unknown, options?: { includeSelf?: boolean }): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("video", payload, options)
  }

  async sendVideoFrame(body: RealtimeBinaryBody, options: RealtimeBinaryFrameOptions = {}): Promise<RealtimeBroadcastResponse> {
    return this.broadcast("video", await createRealtimeBinaryFrame(body, options), { includeSelf: options.includeSelf })
  }

  onEvent(listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.client.onUserEvent((event) => {
      if (event.type !== "volatileUserEvent") {
        return
      }
      if (event.name !== "realtime.channel.event") {
        return
      }
      const channelEvent = event.payload as RealtimeChannelEvent
      if (channelEvent.channelId === this.channelId) {
        listener(channelEvent)
      }
    })
  }

  onEventKind(kind: RealtimeChannelEvent["kind"], listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.onTypedEvent(kind, listener)
  }

  onGameInput(listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.onTypedEvent("gameInput", listener)
  }

  onStatePatch(listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.onTypedEvent("statePatch", listener)
  }

  onVoice(listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.onTypedEvent("voice", listener)
  }

  onVideo(listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.onTypedEvent("video", listener)
  }

  onState<T = unknown>(listener: (event: RealtimeChannelStateEvent<T>) => void): () => void {
    return this.client.onUserEvent((event) => {
      if (event.type !== "volatileUserEvent") {
        return
      }
      if (event.name !== "realtime.channel.state") {
        return
      }
      const stateEvent = event.payload as RealtimeChannelStateEvent<T>
      if (stateEvent.channelId === this.channelId) {
        listener(stateEvent)
      }
    })
  }

  onSignal(listener: (signal: RealtimeSignal) => void): () => void {
    return this.client.onUserEvent((event) => {
      if (event.type !== "volatileUserEvent") {
        return
      }
      if (event.name !== "realtime.channel.signal") {
        return
      }
      const signal = event.payload as RealtimeSignal
      if (signal.channelId === this.channelId) {
        listener(signal)
      }
    })
  }

  onSignalKind(kind: RealtimeSignal["kind"], listener: (signal: RealtimeSignal) => void): () => void {
    return this.onTypedSignal(kind, listener)
  }

  onOffer(listener: (signal: RealtimeSignal) => void): () => void {
    return this.onTypedSignal("offer", listener)
  }

  onAnswer(listener: (signal: RealtimeSignal) => void): () => void {
    return this.onTypedSignal("answer", listener)
  }

  onIce(listener: (signal: RealtimeSignal) => void): () => void {
    return this.onTypedSignal("ice", listener)
  }

  onMemberJoined(listener: (event: RealtimeMemberJoinedEvent) => void): () => void {
    return this.onChannelUserEvent("realtime.channel.memberJoined", listener)
  }

  onMemberLeft(listener: (event: RealtimeMemberLeftEvent) => void): () => void {
    return this.onChannelUserEvent("realtime.channel.memberLeft", listener)
  }

  onMemberUpdated(listener: (event: RealtimeMemberUpdatedEvent) => void): () => void {
    return this.onChannelUserEvent("realtime.channel.memberUpdated", listener)
  }

  private onChannelUserEvent<T extends { channelId: string }>(name: string, listener: (event: T) => void): () => void {
    return this.client.onUserEvent((event) => {
      if (event.type !== "volatileUserEvent") {
        return
      }
      if (event.name !== name) {
        return
      }
      const channelEvent = event.payload as T
      if (channelEvent.channelId === this.channelId) {
        listener(channelEvent)
      }
    })
  }

  private onTypedEvent(kind: string, listener: (event: RealtimeChannelEvent) => void): () => void {
    return this.onEvent((event) => {
      if (event.kind === kind) {
        listener(event)
      }
    })
  }

  private onTypedSignal(kind: string, listener: (signal: RealtimeSignal) => void): () => void {
    return this.onSignal((signal) => {
      if (signal.kind === kind) {
        listener(signal)
      }
    })
  }
}

async function parseResponse<T>(response: Response): Promise<T> {
  const payload = await response.json().catch(() => undefined)
  if (!response.ok) {
    throw new NextDbHttpError(
      payload?.error ?? `NextDB request failed with ${response.status}`,
      response.status,
      payload,
    )
  }
  return payload as T
}

export class NextDbHttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly payload: unknown,
  ) {
    super(message)
    this.name = "NextDbHttpError"
  }
}

export class NextDbPendingWriteLimitError extends Error {
  readonly details: PendingWriteLimitDetails
  readonly limitKind: PendingWriteLimitKind
  readonly writeId: string
  readonly writeType: PendingWriteType
  readonly currentWrites: number
  readonly nextWrites: number
  readonly maxWrites: number
  readonly currentBytes: number
  readonly writeBytes: number
  readonly nextBytes: number
  readonly maxBytes: number

  constructor(details: PendingWriteLimitDetails) {
    super(`pending write queue ${details.limitKind} exceeded`)
    this.name = "NextDbPendingWriteLimitError"
    Object.setPrototypeOf(this, NextDbPendingWriteLimitError.prototype)
    this.details = details
    this.limitKind = details.limitKind
    this.writeId = details.writeId
    this.writeType = details.writeType
    this.currentWrites = details.currentWrites
    this.nextWrites = details.nextWrites
    this.maxWrites = details.maxWrites
    this.currentBytes = details.currentBytes
    this.writeBytes = details.writeBytes
    this.nextBytes = details.nextBytes
    this.maxBytes = details.maxBytes
  }
}

function ownerRetryEndpoint(error: unknown): string | undefined {
  if (!(error instanceof NextDbHttpError) || error.status !== 409) {
    return undefined
  }
  if (isRecord(error.payload) && typeof error.payload.ownerUrl === "string") {
    return normalizeEndpoint(error.payload.ownerUrl)
  }
  const match = error.message.match(/retry against\s+(https?:\/\/\S+)/i)
  return match?.[1] ? normalizeEndpoint(match[1]) : undefined
}

function isDrainingError(error: unknown): boolean {
  return error instanceof NextDbHttpError
    && error.status === 503
    && isRecord(error.payload)
    && error.payload.draining === true
}

function isRetryablePendingWriteError(error: unknown): boolean {
  return isNetworkFailure(error) || isDrainingError(error) || ownerRetryEndpoint(error) !== undefined
}

function isPendingWriteChange(change: NextDbCacheChange): change is Extract<
  NextDbCacheChange,
  | { type: "pendingWriteQueued" }
  | { type: "pendingWriteRejected" }
  | { type: "pendingWriteReset" }
  | { type: "pendingWriteDiscarded" }
  | { type: "pendingWritesCleared" }
  | { type: "pendingWriteCommitted" }
  | { type: "pendingWriteFailed" }
> {
  return change.type === "pendingWriteQueued"
    || change.type === "pendingWriteRejected"
    || change.type === "pendingWriteReset"
    || change.type === "pendingWriteDiscarded"
    || change.type === "pendingWritesCleared"
    || change.type === "pendingWriteCommitted"
    || change.type === "pendingWriteFailed"
}

function estimatePendingWriteBytes(write: NextDbPendingWrite): number {
  if (write.type === "objectPut") {
    return write.body.size + utf8JsonByteLength({
      ...write,
      body: undefined,
      bodyBytes: write.body.size,
    })
  }
  return utf8JsonByteLength(write)
}

function utf8JsonByteLength(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength
}

function isCacheableMessage(message: NextDbMessage): boolean {
  return message.lsn > 0 && !message.path.startsWith("volatile/")
}

function isCacheableRecord(record: NextDbRecord): boolean {
  return !record.path.startsWith("volatile/")
}

function isCacheableRecordDelete(event: { lsn: number; path?: string }): boolean {
  return !event.path?.startsWith("volatile/")
}

const RECENT_DELIVERY_EVENT_LSN_WINDOW = 2_048
const RECENT_DELIVERY_EVENT_MAX_IDS = 4_096

function recentDeliveryEventKey(lsn: number, eventId: string): string {
  return `${lsn}:${eventId}`
}

function rememberRecentDeliveryEvent(
  eventsByScope: Map<string, Map<string, number>>,
  scope: string,
  lsn: number,
  eventId: string,
): void {
  let events = eventsByScope.get(scope)
  if (events === undefined) {
    events = new Map()
    eventsByScope.set(scope, events)
  }
  events.set(recentDeliveryEventKey(lsn, eventId), lsn)
  if (events.size <= RECENT_DELIVERY_EVENT_MAX_IDS) {
    return
  }
  const floor = Math.max(0, lsn - RECENT_DELIVERY_EVENT_LSN_WINDOW)
  for (const [key, eventLsn] of events) {
    if (eventLsn < floor || events.size > RECENT_DELIVERY_EVENT_MAX_IDS) {
      events.delete(key)
    }
  }
}

function hasRecentDeliveryEvent(
  eventsByScope: Map<string, Map<string, number>>,
  scope: string,
  lsn: number,
  eventId: string,
): boolean {
  return eventsByScope.get(scope)?.has(recentDeliveryEventKey(lsn, eventId)) ?? false
}

function deliveryEventLsn(event: DeliveryEvent): number | undefined {
  if (event.type === "messageCreated") {
    return event.message.lsn
  }
  if (event.type === "userEvent") {
    return event.event.lsn
  }
  if (event.type === "userUpserted") {
    return event.user.lsn
  }
  if (event.type === "recordUpserted") {
    return event.record.lsn
  }
  if (event.type === "recordDeleted" || event.type === "objectCommitted" || event.type === "objectDeleted") {
    return event.lsn
  }
  return undefined
}

function orderedRealtimeDeliveryEvents(events: DeliveryEvent[]): DeliveryEvent[] {
  if (events.length < 2) {
    return events
  }
  const orderedDurableEvents = events
    .map((event, index) => ({
      event,
      index,
      lsn: deliveryEventLsn(event),
    }))
    .filter((entry): entry is { event: DeliveryEvent; index: number; lsn: number } => entry.lsn !== undefined)
    .sort((left, right) => left.lsn - right.lsn || left.index - right.index)
  if (orderedDurableEvents.length < 2) {
    return events
  }
  let durableIndex = 0
  return events.map((event) => {
    if (deliveryEventLsn(event) === undefined) {
      return event
    }
    const ordered = orderedDurableEvents[durableIndex]?.event ?? event
    durableIndex += 1
    return ordered
  })
}

function recordOverlayKey(table: string, key: string): string {
  return `${table}\0${key}`
}

function sendMessageBatchKey(roomId: string, userId: string, durability: Durability): string {
  return `${roomId}\0${userId}\0${durability}`
}

function isRealtimeJoinActiveSessionRace(error: unknown): boolean {
  return error instanceof NextDbHttpError
    && error.status === 400
    && error.message.includes("sessionId must reference an active connection")
}

function defaultPendingWriteAutoFlush(): Required<PendingWriteAutoFlushOptions> {
  return {
    enabled: false,
    intervalMs: 5000,
    limit: 100,
    retryOnStart: true,
  }
}

function normalizePendingWriteAutoFlush(
  options: boolean | PendingWriteAutoFlushOptions | undefined,
): Required<PendingWriteAutoFlushOptions> {
  const defaults = defaultPendingWriteAutoFlush()
  if (options === undefined) {
    return defaults
  }
  if (typeof options === "boolean") {
    return {
      ...defaults,
      enabled: options,
    }
  }
  return {
    enabled: options.enabled ?? defaults.enabled,
    intervalMs: Math.max(1, Math.floor(options.intervalMs ?? defaults.intervalMs)),
    limit: Math.max(1, Math.floor(options.limit ?? defaults.limit)),
    retryOnStart: options.retryOnStart ?? defaults.retryOnStart,
  }
}

function healthEndpoints(health: NextDbHealth): string[] {
  const endpoints = [
    health.clusterTopology.nodeUrl,
    ...health.clusterTopology.nodes.map((node) => node.url),
    ...health.clusterTopology.shards.map((shard) => shard.ownerUrl),
    ...health.clusterTopology.shards.flatMap((shard) => shard.replicaUrls),
    ...health.walReplicas.flatMap((replica) => replica.remoteReplicas),
    ...health.objectRemoteReplicas,
  ]
  return uniqueEndpoints(endpoints.filter((endpoint): endpoint is string => typeof endpoint === "string"))
}

function uniqueEndpoints(endpoints: string[]): string[] {
  const normalized: string[] = []
  for (const endpoint of endpoints) {
    const value = normalizeEndpoint(endpoint)
    if (value && !normalized.some((existing) => sameEndpoint(existing, value))) {
      normalized.push(value)
    }
  }
  return normalized
}

function normalizeEndpoint(endpoint: string): string | undefined {
  const normalized = endpoint.trim().replace(/\/$/, "")
  if (!/^https?:\/\//i.test(normalized)) {
    return undefined
  }
  return normalized
}

function normalizeCacheNamespace(namespace: string | undefined): string {
  const value = namespace?.trim()
  return value && value.length > 0 ? value : "default"
}

function serializeConnectionMetadata(metadata: unknown): string {
  const serialized = JSON.stringify(metadata === undefined ? {} : metadata)
  if (serialized === undefined) {
    return "{}"
  }
  return serialized
}

function defaultIndexedDbCacheName(endpoint: string, userId: string | undefined, namespace: string): string {
  const normalizedEndpoint = normalizeEndpoint(endpoint) ?? endpoint.trim().replace(/\/$/, "")
  const scope = JSON.stringify({
    endpoint: normalizedEndpoint,
    namespace,
    userId: userId ?? "anonymous",
  })
  return `nextdb-client-${hashCacheScope(scope)}`
}

function hashCacheScope(value: string): string {
  let hash = 0x811c9dc5
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }
  return (hash >>> 0).toString(36)
}

function defaultRealtimeTransportFactory(
  kind: NextDbRealtimeTransportKind,
  webTransportOptions?: WebTransportOptions,
): NextDbRealtimeTransportFactory {
  if (kind === "webtransport") {
    return webTransportRealtimeTransport(webTransportOptions)
  }
  if (kind === "jsonl") {
    return jsonLineHttpRealtimeTransport()
  }
  return ({ url }) => new WebSocketRealtimeTransport(url)
}

function connectionTransportParam(kind: NextDbRealtimeTransportKind | "custom"): ConnectionTransport {
  if (kind === "webtransport") {
    return "webTransport"
  }
  if (kind === "custom" || kind === "jsonl") {
    return "custom"
  }
  return "webSocket"
}

function webTransportUrl(url: URL): URL {
  const next = new URL(url)
  if (next.protocol === "ws:") {
    next.protocol = "http:"
  } else if (next.protocol === "wss:") {
    next.protocol = "https:"
  }
  return next
}

function jsonLineHttpUrl(url: URL, connectPath = "/v1/connect/jsonl"): URL {
  const next = new URL(url)
  if (next.protocol === "ws:") {
    next.protocol = "http:"
  } else if (next.protocol === "wss:") {
    next.protocol = "https:"
  }
  next.pathname = connectPath
  return next
}

function jsonLineHttpHeaders(headers?: HeadersInit): Headers {
  const next = new Headers(headers)
  if (!next.has("accept")) {
    next.set("accept", "application/x-ndjson")
  }
  if (!next.has("content-type")) {
    next.set("content-type", "application/x-ndjson")
  }
  return next
}

function sameEndpoint(left: string, right: string): boolean {
  return left.replace(/\/$/, "") === right.replace(/\/$/, "")
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function hasIndexedDb(): boolean {
  return typeof indexedDB !== "undefined"
}

function normalizeObjectBody(body: Blob | ArrayBuffer | Uint8Array | string): BodyInit {
  if (typeof body === "string" || body instanceof Blob || body instanceof ArrayBuffer) {
    return body
  }
  const copy = new Uint8Array(body.byteLength)
  copy.set(body)
  return copy.buffer
}

function normalizePageLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit)) {
    return 50
  }
  return Math.min(500, Math.max(1, Math.floor(limit)))
}

function freshnessSatisfied(options: FreshnessOptions | undefined, lsn: number | undefined): boolean {
  return options?.minLsn === undefined || (lsn !== undefined && lsn >= options.minLsn)
}

function requiresReadQuorum(options: FreshnessOptions | undefined): boolean {
  return options?.consistency === "quorum" || options?.consistency === "all"
}

function recordReadConsistencyRequiresServer(options: FreshnessOptions | undefined): boolean {
  return options?.recordConsistency === "read-your-writes" || options?.recordConsistency === "strong"
}

function maxLsn(items: Array<{ lsn: number }>): number | undefined {
  if (items.length === 0) {
    return undefined
  }
  return Math.max(...items.map((item) => item.lsn))
}

function highestLsnRecord<T>(items: Array<NextDbRecord<T>>): NextDbRecord<T> {
  return items.reduce((highest, item) => item.lsn > highest.lsn ? item : highest)
}

function highestLsnUserProfile(items: NextDbUserProfile[]): NextDbUserProfile {
  return items.reduce((highest, item) => item.lsn > highest.lsn ? item : highest)
}

function mergeUserListQuorum(
  pages: ListUsersResponse[],
  limit: number,
): ListUsersResponse {
  const usersById = new Map<string, NextDbUserProfile>()
  for (const page of pages) {
    for (const user of page.users) {
      const existing = usersById.get(user.userId)
      if (existing === undefined || user.lsn > existing.lsn) {
        usersById.set(user.userId, user)
      }
    }
  }
  const users = [...usersById.values()].sort((left, right) => left.userId.localeCompare(right.userId))
  const page = users.slice(0, limit)
  return {
    users: page,
    nextAfterUserId: page.at(-1)?.userId,
    hasMore: users.length > limit || pages.some((page) => page.hasMore),
  }
}

type RecordListQuorumMerge =
  | { kind: "key" }
  | { kind: "indexRange", fields: string[] }

function mergeRecordListQuorum<T>(
  table: string,
  pages: Array<ListRecordsResponse<T>>,
  limit: number,
  merge: RecordListQuorumMerge = { kind: "key" },
): ListRecordsResponse<T> {
  const recordsByKey = new Map<string, NextDbRecord<T>>()
  for (const page of pages) {
    for (const record of page.records) {
      const existing = recordsByKey.get(record.key)
      if (existing === undefined || record.lsn > existing.lsn) {
        recordsByKey.set(record.key, record)
      }
    }
  }
  const indexed = [...recordsByKey.values()].map((record) => ({
    record,
    values: merge.kind === "indexRange" ? recordIndexValues(record, merge.fields) : undefined,
  }))
  indexed.sort((left, right) => {
    if (merge.kind === "indexRange") {
      return compareIndexValues(left.values ?? [], right.values ?? [])
        || left.record.key.localeCompare(right.record.key)
    }
    return left.record.key.localeCompare(right.record.key)
  })
  const page = indexed.slice(0, limit)
  const records = page.map(({ record }) => record)
  const cursorItem = merge.kind === "indexRange" ? page.at(-1) : undefined
  const hasMore = indexed.length > limit || pages.some((page) => page.hasMore)
  return {
    table,
    records,
    nextAfterKey: records.at(-1)?.key,
    nextCursor: !hasMore || cursorItem?.values === undefined
      ? undefined
      : localIndexRangeCursor(cursorItem.values, cursorItem.record.key),
    hasMore,
  }
}

function mergeObjectListQuorum(
  pages: ListObjectsResponse[],
  limit: number,
): ListObjectsResponse {
  const objectsById = new Map<string, NextDbObjectMetadata>()
  for (const page of pages) {
    for (const object of page.objects) {
      objectsById.set(object.id, object)
    }
  }
  const objects = [...objectsById.values()].sort((left, right) => left.id.localeCompare(right.id))
  const page = objects.slice(0, limit)
  return {
    objects: page,
    nextAfterId: page.at(-1)?.id,
    hasMore: objects.length > limit || pages.some((page) => page.hasMore),
  }
}

function mergeRoomMessagesQuorum(
  roomId: string,
  pages: MessagesResponse[],
  limit: number,
  beforeLsn?: number,
): MessagesResponse {
  const messagesById = new Map<string, NextDbMessage>()
  for (const page of pages) {
    for (const message of page.messages) {
      if (beforeLsn !== undefined && message.lsn >= beforeLsn) {
        continue
      }
      const existing = messagesById.get(message.id)
      if (existing === undefined || message.lsn > existing.lsn) {
        messagesById.set(message.id, message)
      }
    }
  }
  const messages = [...messagesById.values()]
    .sort((left, right) => right.lsn - left.lsn || right.createdAtMs - left.createdAtMs || left.id.localeCompare(right.id))
    .slice(0, limit)
  return {
    roomId,
    source: pages.some((page) => page.source === "live") ? "live" : "chatLog",
    messages,
  }
}

function mergeUserEventsQuorum(
  userId: string,
  pages: UserEventsResponse[],
  limit: number,
  beforeLsn?: number,
): UserEventsResponse {
  const eventsById = new Map<string, NextDbUserEvent>()
  for (const page of pages) {
    for (const event of page.events) {
      if (beforeLsn !== undefined && event.lsn >= beforeLsn) {
        continue
      }
      const existing = eventsById.get(event.id)
      if (existing === undefined || event.lsn > existing.lsn) {
        eventsById.set(event.id, event)
      }
    }
  }
  const events = [...eventsById.values()]
    .sort((left, right) => right.lsn - left.lsn || right.createdAtMs - left.createdAtMs || left.id.localeCompare(right.id))
    .slice(0, limit)
  return {
    userId,
    events,
  }
}

function readQuorumEndpoints(route: ShardRoute, activeEndpoint: string): string[] {
  return uniqueEndpoints([
    route.ownerUrl,
    ...route.replicaUrls,
    activeEndpoint,
  ].filter((endpoint): endpoint is string => typeof endpoint === "string" && endpoint.length > 0))
}

function readQuorumEndpointsForShard(shard: ClusterShard, activeEndpoint: string): string[] {
  const endpoints = [
    shard.ownerUrl,
    ...shard.replicaUrls,
    shard.role === "unassigned" ? undefined : activeEndpoint,
  ].filter((endpoint): endpoint is string => typeof endpoint === "string" && endpoint.length > 0)
  return uniqueEndpoints(endpoints)
}

function recordListShardParams(params: URLSearchParams, shard: number): URLSearchParams {
  const next = new URLSearchParams(params)
  next.set("shard", String(shard))
  return next
}

function clusterRouteKey(options: ClusterRouteOptions): string {
  if (options.key !== undefined && options.key.trim() !== "") {
    return options.key
  }
  if (options.roomId !== undefined && options.roomId.trim() !== "") {
    return options.roomId
  }
  if (options.objectId !== undefined && options.objectId.trim() !== "") {
    return options.objectId
  }
  if (
    options.table !== undefined &&
    options.table.trim() !== "" &&
    options.recordKey !== undefined &&
    options.recordKey.trim() !== ""
  ) {
    return `${options.table}:${options.recordKey}`
  }
  throw new Error("provide key, roomId, objectId, or table plus recordKey")
}

async function localShardIndex(key: string, shardCount: number): Promise<number | undefined> {
  if (shardCount <= 1) {
    return 0
  }
  const subtle = globalThis.crypto?.subtle
  if (subtle === undefined) {
    return undefined
  }
  const digest = new Uint8Array(await subtle.digest("SHA-256", new TextEncoder().encode(key)))
  let first = 0n
  for (let index = 0; index < 8; index += 1) {
    first = (first << 8n) | BigInt(digest[index])
  }
  return Number(first % BigInt(shardCount))
}

function recordTransactionOperationShardKey(operation: RecordTransactionOperation): string {
  if (operation.type === "upsert" || operation.type === "delete") {
    return `${operation.table}:${operation.key}`
  }
  return `${operation.table}:${operation.parentKey}`
}

function combineRecordBatchResponses<T>(responses: Array<RecordBatchResponse<T>>): RecordBatchResponse<T> {
  return {
    lsn: Math.max(0, ...responses.map((response) => response.lsn)),
    transactionCount: responses.reduce((total, response) => total + response.transactionCount, 0),
    operations: responses.flatMap((response) => response.operations),
  }
}

async function recordBatchChildClientMutationId(
  base: string,
  shard: number,
  chunkIndex: number,
  splitTransaction: boolean,
): Promise<string> {
  if (!splitTransaction) {
    return base
  }
  const suffix = `:s${shard}p${chunkIndex}`
  if (base.length + suffix.length <= 160) {
    return `${base}${suffix}`
  }
  const digest = await sha256HexPrefix(base, 16)
  const marker = `:h${digest}`
  const keep = Math.max(0, 160 - marker.length - suffix.length)
  return `${base.slice(0, keep)}${marker}${suffix}`
}

async function sha256HexPrefix(value: string, length: number): Promise<string> {
  const subtle = globalThis.crypto?.subtle
  if (subtle === undefined) {
    return Array.from(value)
      .reduce((hash, char) => ((hash * 31) + char.charCodeAt(0)) >>> 0, 0)
      .toString(16)
      .padStart(length, "0")
      .slice(0, length)
  }
  const digest = new Uint8Array(await subtle.digest("SHA-256", new TextEncoder().encode(value)))
  return [...digest]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("")
    .slice(0, length)
}

function readQuorumRequiredAcks(consistency: ReadConsistency, endpointCount: number): number {
  if (endpointCount <= 0) {
    return 1
  }
  if (consistency === "all") {
    return endpointCount
  }
  if (consistency === "quorum") {
    return Math.floor(endpointCount / 2) + 1
  }
  return 1
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export async function createRealtimeBinaryFrame(
  body: RealtimeBinaryBody,
  options: RealtimeBinaryFrameOptions = {},
): Promise<RealtimeBinaryFramePayload> {
  const bytes = await realtimeBinaryBodyBytes(body)
  const payload: RealtimeBinaryFramePayload = {
    dataBase64: bytesToBase64(bytes),
    byteLength: bytes.byteLength,
    timestampMs: options.timestampMs ?? Date.now(),
  }
  const contentType = options.contentType ?? (body instanceof Blob && body.type ? body.type : undefined)
  if (contentType !== undefined) {
    payload.contentType = contentType
  }
  if (options.codec !== undefined) {
    payload.codec = options.codec
  }
  if (options.metadata !== undefined) {
    payload.metadata = options.metadata
  }
  return payload
}

export function decodeRealtimeBinaryFrame(payload: RealtimeBinaryFramePayload): Uint8Array {
  const bytes = base64ToBytes(payload.dataBase64)
  if (bytes.byteLength !== payload.byteLength) {
    throw new Error("realtime binary frame byteLength does not match dataBase64")
  }
  return bytes
}

async function realtimeBinaryBodyBytes(body: RealtimeBinaryBody): Promise<Uint8Array> {
  if (typeof body === "string") {
    return new TextEncoder().encode(body)
  }
  if (body instanceof Blob) {
    return new Uint8Array(await body.arrayBuffer())
  }
  if (body instanceof ArrayBuffer) {
    return new Uint8Array(body.slice(0))
  }
  const copy = new Uint8Array(body.byteLength)
  copy.set(body)
  return copy
}

function bytesToBase64(bytes: Uint8Array): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
  let out = ""
  let index = 0
  for (; index + 2 < bytes.byteLength; index += 3) {
    const value = (bytes[index] << 16) | (bytes[index + 1] << 8) | bytes[index + 2]
    out += alphabet[(value >> 18) & 63]
      + alphabet[(value >> 12) & 63]
      + alphabet[(value >> 6) & 63]
      + alphabet[value & 63]
  }
  if (index < bytes.byteLength) {
    const first = bytes[index]
    const second = index + 1 < bytes.byteLength ? bytes[index + 1] : 0
    const value = (first << 16) | (second << 8)
    out += alphabet[(value >> 18) & 63]
      + alphabet[(value >> 12) & 63]
      + (index + 1 < bytes.byteLength ? alphabet[(value >> 6) & 63] : "=")
      + "="
  }
  return out
}

function base64ToBytes(value: string): Uint8Array {
  const clean = value.replace(/\s+/g, "")
  if (clean.length % 4 !== 0) {
    throw new Error("invalid base64 length")
  }
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
  let padding = 0
  if (clean.endsWith("==")) {
    padding = 2
  } else if (clean.endsWith("=")) {
    padding = 1
  }
  const out = new Uint8Array((clean.length / 4) * 3 - padding)
  let offset = 0
  for (let index = 0; index < clean.length; index += 4) {
    const values = [clean[index], clean[index + 1], clean[index + 2], clean[index + 3]].map((char) => {
      if (char === "=") {
        return 0
      }
      const decoded = alphabet.indexOf(char)
      if (decoded < 0) {
        throw new Error("invalid base64 character")
      }
      return decoded
    })
    const triple = (values[0] << 18) | (values[1] << 12) | (values[2] << 6) | values[3]
    if (offset < out.byteLength) {
      out[offset++] = (triple >> 16) & 255
    }
    if (offset < out.byteLength) {
      out[offset++] = (triple >> 8) & 255
    }
    if (offset < out.byteLength) {
      out[offset++] = triple & 255
    }
  }
  return out
}

function objectBodyBlob(body: Blob | ArrayBuffer | Uint8Array | string, contentType: string): Blob {
  if (body instanceof Blob) {
    return body
  }
  if (typeof body === "string") {
    return new Blob([body], { type: contentType })
  }
  if (body instanceof ArrayBuffer) {
    return new Blob([body], { type: contentType })
  }
  const copy = new Uint8Array(body.byteLength)
  copy.set(body)
  return new Blob([copy.buffer], { type: contentType })
}

function objectRangeHeader(options: ObjectBodyRangeOptions): string {
  if (options.suffixLength !== undefined) {
    if (options.start !== undefined || options.end !== undefined) {
      throw new Error("suffixLength cannot be combined with start or end")
    }
    if (!Number.isInteger(options.suffixLength) || options.suffixLength <= 0) {
      throw new Error("suffixLength must be a positive integer")
    }
    return `bytes=-${options.suffixLength}`
  }
  if (options.start === undefined) {
    throw new Error("object range reads require start or suffixLength")
  }
  if (!Number.isInteger(options.start) || options.start < 0) {
    throw new Error("range start must be a non-negative integer")
  }
  if (options.end !== undefined && (!Number.isInteger(options.end) || options.end < options.start)) {
    throw new Error("range end must be an integer greater than or equal to start")
  }
  return `bytes=${options.start}-${options.end ?? ""}`
}

function parseObjectContentRange(contentRange: string): { start: number; end: number; byteSize: number } {
  const match = contentRange.match(/^bytes (\d+)-(\d+)\/(\d+)$/)
  if (!match) {
    throw new Error(`invalid object content-range: ${contentRange}`)
  }
  return {
    start: Number(match[1]),
    end: Number(match[2]),
    byteSize: Number(match[3]),
  }
}

function cachedObjectByteRange(
  options: ObjectBodyRangeOptions,
  byteSize: number,
): { start: number; end: number } | undefined {
  if (!Number.isInteger(byteSize) || byteSize <= 0) {
    return undefined
  }
  if (options.suffixLength !== undefined) {
    if (!Number.isInteger(options.suffixLength) || options.suffixLength <= 0) {
      return undefined
    }
    const length = Math.min(options.suffixLength, byteSize)
    return { start: byteSize - length, end: byteSize - 1 }
  }
  if (options.start === undefined || !Number.isInteger(options.start) || options.start < 0 || options.start >= byteSize) {
    return undefined
  }
  const end = options.end === undefined ? byteSize - 1 : Math.min(options.end, byteSize - 1)
  if (!Number.isInteger(end) || end < options.start) {
    return undefined
  }
  return { start: options.start, end }
}

function objectContentRange(start: number, end: number, byteSize: number): string {
  return `bytes ${start}-${end}/${byteSize}`
}

async function objectBodyMatchesMetadata(body: Blob, metadata: NextDbObjectMetadata): Promise<boolean> {
  if (body.size !== metadata.byteSize) {
    return false
  }
  return (await objectBodySha256(body)) === metadata.sha256
}

function objectMetadataContentMatches(left: NextDbObjectMetadata, right: NextDbObjectMetadata): boolean {
  return left.byteSize === right.byteSize &&
    left.contentType === right.contentType &&
    left.sha256 === right.sha256
}

function objectBodyRangeMatchesMetadata(
  response: ObjectBodyRangeResponse,
  metadata: NextDbObjectMetadata,
): boolean {
  return response.byteSize === metadata.byteSize
    && response.contentType === metadata.contentType
    && response.start <= response.end
    && response.body.size === response.end - response.start + 1
}

async function objectBodySha256(body: Blob): Promise<string | undefined> {
  const subtle = globalThis.crypto?.subtle
  if (!subtle) {
    return undefined
  }
  const digest = await subtle.digest("SHA-256", await body.arrayBuffer())
  return hexLower(new Uint8Array(digest))
}

function selectObjectIdsToTrim(
  objects: Array<NextDbObjectMetadata & { cachedBytes?: number }>,
  maxObjects: number,
  maxBytes: number,
): string[] {
  const objectLimit = Math.max(0, Math.floor(maxObjects))
  const byteLimit = Math.max(0, Math.floor(maxBytes))
  if (objectLimit <= 0 && byteLimit <= 0) {
    return []
  }

  const newestFirst = [...objects].sort((left, right) => {
    const created = right.createdAtMs - left.createdAtMs
    return created !== 0 ? created : right.id.localeCompare(left.id)
  })
  const deleteIds = new Set<string>()
  const retained = objectLimit > 0 ? newestFirst.slice(0, objectLimit) : newestFirst
  for (const object of newestFirst.slice(retained.length)) {
    deleteIds.add(object.id)
  }

  if (byteLimit > 0) {
    let totalBytes = retained.reduce((sum, object) => sum + Math.max(0, object.cachedBytes ?? object.byteSize), 0)
    for (const object of [...retained].reverse()) {
      if (totalBytes <= byteLimit) {
        break
      }
      deleteIds.add(object.id)
      totalBytes -= Math.max(0, object.cachedBytes ?? object.byteSize)
    }
  }

  return [...deleteIds]
}

function nextClientId(prefix: string): string {
  const random = typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
  return `${prefix}-${random}`
}

function nextObjectId(): string {
  return nextClientId("object")
}

function isNetworkFailure(error: unknown): boolean {
  return error instanceof TypeError || (typeof error === "object" && error !== null && "name" in error && error.name === "TypeError")
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function fromStoredMessage(value: unknown): NextDbMessage {
  return value as NextDbMessage
}

function encodeIndexValue(value: unknown): string {
  if (typeof value === "string") {
    return value
  }
  return JSON.stringify(value)
}

function normalizeIndexQueryValues(options: QueryRecordsByIndexOptions): unknown[] {
  if (options.values !== undefined) {
    return options.values
  }
  if (options.value !== undefined) {
    return [options.value]
  }
  throw new Error("index query requires value or values")
}

function isIndexRangeQuery(options: QueryRecordsByIndexOptions): boolean {
  return options.lower !== undefined
    || options.upper !== undefined
    || options.lowerValues !== undefined
    || options.upperValues !== undefined
    || options.afterCursor !== undefined
}

function localIndexQueryFromOptions(
  fields: string[],
  options: QueryRecordsByIndexOptions,
  limit: number,
  label: string,
): LocalIndexQuery | undefined {
  if (isIndexRangeQuery(options)) {
    const lowerValues = normalizeIndexBoundValues(options.lower, options.lowerValues, fields, "lower", label)
    const upperValues = normalizeIndexBoundValues(options.upper, options.upperValues, fields, "upper", label)
    return {
      fields,
      lowerValues,
      upperValues,
      limit,
    }
  }
  const values = normalizeIndexQueryValues(options)
  if (values.length !== fields.length) {
    return undefined
  }
  return {
    fields,
    values,
    limit,
  }
}

function localIndexQueryFromFrame(
  fields: string[],
  frame: Extract<ClientFrame, { type: "subscribeQuery" }>,
  limit: number,
): LocalIndexQuery | undefined {
  try {
    if (
      frame.lower !== undefined ||
      frame.upper !== undefined ||
      frame.lowerValues !== undefined ||
      frame.upperValues !== undefined ||
      frame.afterCursor !== undefined
    ) {
      const lowerValues = parseOptionalFrameIndexValues(frame.lower, frame.lowerValues)
      const upperValues = parseOptionalFrameIndexValues(frame.upper, frame.upperValues)
      if (
        (lowerValues !== undefined && lowerValues.length !== fields.length) ||
        (upperValues !== undefined && upperValues.length !== fields.length)
      ) {
        return undefined
      }
      return {
        fields,
        lowerValues,
        upperValues,
        limit,
        afterCursor: frame.afterCursor,
      }
    }

    const values = frame.values !== undefined
      ? parseFrameIndexValues(frame.values)
      : frame.value === undefined
        ? undefined
        : [parseFrameIndexValue(frame.value)]
    if (values === undefined || values.length !== fields.length) {
      return undefined
    }
    return {
      fields,
      values,
      limit,
      afterKey: frame.afterKey,
    }
  } catch {
    return undefined
  }
}

function parseOptionalFrameIndexValues(value: string | undefined, values: string | undefined): unknown[] | undefined {
  if (values !== undefined) {
    return parseFrameIndexValues(values)
  }
  return value === undefined ? undefined : [parseFrameIndexValue(value)]
}

function parseFrameIndexValues(values: string): unknown[] {
  const parsed = JSON.parse(values) as unknown
  if (!Array.isArray(parsed) || parsed.some((value) => !isScalarIndexValue(value))) {
    throw new Error("index values must be a scalar JSON array")
  }
  return parsed
}

function parseFrameIndexValue(value: string): unknown {
  try {
    const parsed = JSON.parse(value) as unknown
    return isScalarIndexValue(parsed) ? parsed : value
  } catch {
    return value
  }
}

function isScalarIndexValue(value: unknown): boolean {
  return value === null || ["string", "number", "boolean"].includes(typeof value)
}

function normalizeIndexBoundValues(
  value: unknown,
  values: unknown[] | undefined,
  fields: string[],
  boundName: string,
  label: string,
): unknown[] | undefined {
  const boundValues = values ?? (value === undefined ? undefined : [value])
  if (boundValues === undefined) {
    return undefined
  }
  if (boundValues.length !== fields.length) {
    throw new Error(`${label} ${boundName} bound must contain ${fields.length} value(s)`)
  }
  return boundValues
}

function setIndexQueryParams(params: URLSearchParams, options: QueryRecordsByIndexOptions, label: string): void {
  if (isIndexRangeQuery(options)) {
    if (options.lowerValues !== undefined) {
      params.set("lowerValues", JSON.stringify(options.lowerValues))
    } else if (options.lower !== undefined) {
      params.set("lower", encodeIndexValue(options.lower))
    }
    if (options.upperValues !== undefined) {
      params.set("upperValues", JSON.stringify(options.upperValues))
    } else if (options.upper !== undefined) {
      params.set("upper", encodeIndexValue(options.upper))
    }
    return
  }
  if (options.values !== undefined) {
    params.set("values", JSON.stringify(options.values))
  } else if (options.value !== undefined) {
    params.set("value", encodeIndexValue(options.value))
  } else {
    throw new Error(`${label} requires value/values or lower/upper range bounds`)
  }
}

function setRecordPredicateParam(params: URLSearchParams, predicate: RecordPredicate | undefined): void {
  if (predicate !== undefined) {
    params.set("predicate", JSON.stringify(predicate))
  }
}

function setRecordReadConsistencyParams(params: URLSearchParams, options: FreshnessOptions | undefined): void {
  if (options?.recordConsistency !== undefined) {
    params.set("consistency", options.recordConsistency)
  }
  if (options?.minLsn !== undefined && options.recordConsistency === "read-your-writes") {
    params.set("minLsn", String(Math.max(0, Math.floor(options.minLsn))))
  }
}

function subscribeQueryFrame(options: RecordLiveQueryOptions & { queryId: string }): Extract<ClientFrame, { type: "subscribeQuery" }> {
  const frame: Extract<ClientFrame, { type: "subscribeQuery" }> = {
    type: "subscribeQuery",
    queryId: options.queryId,
    table: options.table,
  }
  if (options.parentKey !== undefined) {
    frame.parentKey = options.parentKey
  }
  if (options.nested !== undefined) {
    frame.nested = options.nested
  }
  if (options.indexName !== undefined) {
    frame.indexName = options.indexName
  }
  if (options.afterKey !== undefined) {
    frame.afterKey = options.afterKey
  }
  if (options.afterCursor !== undefined) {
    frame.afterCursor = options.afterCursor
  }
  if (options.limit !== undefined) {
    frame.limit = options.limit
  }
  if (options.order !== undefined) {
    frame.order = options.order
  }
  if (options.predicate !== undefined) {
    frame.predicate = options.predicate
  }
  if (options.resultId !== undefined) {
    frame.resultId = options.resultId
  }
  frame.diff = options.diff ?? true
  if (options.indexName !== undefined) {
    const params = new URLSearchParams()
    setIndexQueryParams(params, options, "subscribeQuery")
    for (const [key, value] of params) {
      if (key === "value") {
        frame.value = value
      } else if (key === "values") {
        frame.values = value
      } else if (key === "lower") {
        frame.lower = value
      } else if (key === "upper") {
        frame.upper = value
      } else if (key === "lowerValues") {
        frame.lowerValues = value
      } else if (key === "upperValues") {
        frame.upperValues = value
      }
    }
  }
  return frame
}

function mergeLiveQueryDiff(
  previous: ListRecordsResponse | undefined,
  diff: RecordLiveQueryDiff,
): ListRecordsResponse {
  const recordsByKey = new Map<string, NextDbRecord>()
  for (const record of previous?.records ?? []) {
    recordsByKey.set(record.key, record)
  }
  for (const removed of diff.removed) {
    recordsByKey.delete(removed.key)
  }
  for (const record of diff.added) {
    recordsByKey.set(record.key, record)
  }
  for (const record of diff.updated) {
    recordsByKey.set(record.key, record)
  }

  return {
    table: previous?.table ?? diff.table,
    records: diff.keys
      .map((key) => recordsByKey.get(key))
      .filter((record): record is NextDbRecord => record !== undefined),
    nextAfterKey: diff.nextAfterKey,
    nextCursor: diff.nextCursor,
    hasMore: diff.hasMore,
  }
}

function nestedRecordTable(table: string, nested: string): string {
  return `${table}.${nested}`
}

function recordShardKey(table: string, key: string): string {
  return `${table}:${key}`
}

function nestedRecordKey(parentKey: string, nestedKey: string): string {
  return `${parentKey}:${nestedKey}`
}

function nestedTableCursorId(table: string, parentKey: string, nested: string): string {
  return nestedTableCursorKey(nestedRecordTable(table, nested), parentKey)
}

function nestedTableCursorKey(logicalTable: string, parentKey: string): string {
  return `${nestedTableCursorIdPrefix(logicalTable)}:${encodeURIComponent(parentKey)}`
}

function nestedTableCursorIdPrefix(logicalTable: string): string {
  return `nested:${encodeURIComponent(logicalTable)}`
}

function nestedCoverageKey(table: string, parentKey: string, nested: string): string {
  return nestedCoverageKeyFromLogical(nestedRecordTable(table, nested), parentKey)
}

function nestedCoverageKeyFromTarget(target: { table: string; parentKey: string; nested: string }): string {
  return nestedCoverageKey(target.table, target.parentKey, target.nested)
}

function nestedCoverageKeyFromLogical(logicalTable: string, parentKey: string): string {
  return `${encodeURIComponent(logicalTable)}/${encodeURIComponent(parentKey)}`
}

function parseNestedCoverageKey(key: string): {
  logicalTable: string
  table: string
  parentKey: string
  nested: string
} | undefined {
  const separator = key.indexOf("/")
  if (separator <= 0) {
    return undefined
  }
  const logicalTable = decodeURIComponent(key.slice(0, separator))
  const parentKey = decodeURIComponent(key.slice(separator + 1))
  const nestedSeparator = logicalTable.lastIndexOf(".")
  if (nestedSeparator <= 0 || nestedSeparator >= logicalTable.length - 1) {
    return undefined
  }
  return {
    logicalTable,
    table: logicalTable.slice(0, nestedSeparator),
    parentKey,
    nested: logicalTable.slice(nestedSeparator + 1),
  }
}

function parentKeyFromNestedRecordPrefix(keyPrefix: string): string {
  return keyPrefix.endsWith(":") ? keyPrefix.slice(0, -1) : keyPrefix
}

function sortedStrings(values: Iterable<string>): string[] {
  return [...new Set(values)].sort((left, right) => left.localeCompare(right))
}

function incrementCount(counts: Map<string, number>, key: string): void {
  counts.set(key, (counts.get(key) ?? 0) + 1)
}

function sumRecordValues(record: Record<string, number>): number {
  return Object.values(record).reduce((sum, value) => sum + value, 0)
}

function sumNestedRecordValues(record: Record<string, Record<string, number>>): number {
  return Object.values(record).reduce((sum, nested) => sum + sumRecordValues(nested), 0)
}

function nestedRecordPrefix(parentKey: string): string {
  return `${parentKey}:`
}

function nestedRecordKeyPrefix(record: Pick<NextDbRecord, "table" | "key">): string | undefined {
  if (!record.table.includes(".")) {
    return undefined
  }
  const separator = record.key.indexOf(":")
  if (separator <= 0) {
    return undefined
  }
  return record.key.slice(0, separator + 1)
}

function nestedRecordPath(table: string, parentKey: string, nested: string, nestedKey: string): string {
  return `tables/${table}/${parentKey}/${nested}/${nestedKey}`
}

function nestedKeyFromLogicalKey(parentKey: string, logicalKey: string | undefined): string | undefined {
  const prefix = nestedRecordPrefix(parentKey)
  return logicalKey?.startsWith(prefix) ? logicalKey.slice(prefix.length) : undefined
}

function parseNestedSchemaOrder(schema: NextDbSchema, table: string, nested: string): RecordOrderTerm[] | undefined {
  const tables = schema.tables as Record<string, unknown>
  const tableSchema = asRecord(tables[table])
  const nestedTables = asRecord(tableSchema?.nested)
  const nestedSchema = asRecord(nestedTables?.[nested])
  const storage = asRecord(nestedSchema?.storage)
  const order = Array.isArray(storage?.order) ? storage.order : undefined
  if (order === undefined) {
    return undefined
  }
  const terms = order
    .filter((term): term is string => typeof term === "string")
    .map(parseRecordOrderTerm)
  return terms.length > 0 ? terms : undefined
}

function parseRecordIndexFields(schema: NextDbSchema, table: string, indexName: string): string[] | undefined {
  const tables = schema.tables as Record<string, unknown>
  const [tableName, nestedName] = table.split(".", 2)
  const tableSchema = asRecord(tables[tableName])
  const indexedSchema = nestedName === undefined
    ? tableSchema
    : asRecord(asRecord(tableSchema?.nested)?.[nestedName])
  const indexes = asRecord(indexedSchema?.indexes)
  const index = asRecord(indexes?.[indexName])
  const fields = Array.isArray(index?.fields) ? index.fields : undefined
  const strings = fields?.filter((field): field is string => typeof field === "string")
  return strings !== undefined && strings.length === fields?.length ? strings : undefined
}

function parseRecordOrderTerm(term: string): RecordOrderTerm {
  const desc = term.match(/^desc\((.+)\)$/)
  if (desc?.[1]) {
    return { field: desc[1], direction: "desc" }
  }
  return { field: term, direction: "asc" }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" ? value as Record<string, unknown> : undefined
}

function localOrderId(table: string, keyPrefix: string, order: RecordOrderTerm[]): string {
  const encoded = JSON.stringify({ table, keyPrefix, order })
  return hexLower(utf8Bytes(encoded))
}

function localRecordOrderCursor(record: NextDbRecord, order: RecordOrderTerm[]): string {
  const stem = order
    .map((term) => localOrderValueComponent(valueAtPath(record.value, term.field), term.direction))
    .join("_")
  return `${stem}__${hexLower(utf8Bytes(record.key))}`
}

function valueAtPath(value: unknown, field: string): unknown {
  if (value === null || typeof value !== "object") {
    return undefined
  }
  return (value as Record<string, unknown>)[field]
}

function recordMatchesIndexQuery(record: NextDbRecord, fields: string[], values: unknown[]): boolean {
  if (fields.length !== values.length) {
    return false
  }
  return fields.every((field, index) => scalarValuesEqual(valueAtPath(record.value, field), values[index]))
}

function localQueryRecordsByIndex<T = unknown>(
  records: NextDbRecord[],
  query: LocalIndexQuery,
): LocalIndexedRecordsResponse<T> {
  const rangeQuery = query.lowerValues !== undefined
    || query.upperValues !== undefined
    || query.afterCursor !== undefined
  const afterCursor = query.afterCursor === undefined ? undefined : parseLocalIndexRangeCursor(query.afterCursor)
  const scoped = records.filter((record) => query.keyPrefix === undefined || record.key.startsWith(query.keyPrefix))
  const sorted = rangeQuery
    ? scoped
      .map((record) => ({ record, values: recordIndexValues(record, query.fields) }))
      .filter(({ values }) => query.lowerValues === undefined || compareIndexValues(values, query.lowerValues) >= 0)
      .filter(({ values }) => query.upperValues === undefined || compareIndexValues(values, query.upperValues) <= 0)
      .filter(({ values, record }) => {
        if (afterCursor === undefined) {
          return true
        }
        return (compareIndexValues(values, afterCursor.values) || record.key.localeCompare(afterCursor.key)) > 0
      })
      .sort((left, right) => compareIndexValues(left.values, right.values) || left.record.key.localeCompare(right.record.key))
    : scoped
      .filter((record) => query.afterKey === undefined || record.key > query.afterKey)
      .filter((record) => query.values !== undefined && recordMatchesIndexQuery(record, query.fields, query.values))
      .sort((left, right) => left.key.localeCompare(right.key))
      .map((record) => ({ record, values: recordIndexValues(record, query.fields) }))
  const page = sorted.slice(0, query.limit)
  const hasMore = sorted.length > query.limit
  const recordsPage = page.map(({ record }) => record as NextDbRecord<T>)
  return {
    records: recordsPage,
    nextCursor: rangeQuery && hasMore ? localIndexRangeCursor(page[query.limit - 1].values, page[query.limit - 1].record.key) : undefined,
    hasMore,
  }
}

function recordIndexValues(record: NextDbRecord, fields: string[]): unknown[] {
  return fields.map((field) => valueAtPath(record.value, field))
}

function compareIndexValues(left: unknown[], right: unknown[]): number {
  for (let index = 0; index < Math.min(left.length, right.length); index += 1) {
    const ordering = compareIndexValue(left[index], right[index])
    if (ordering !== 0) {
      return ordering
    }
  }
  return Math.sign(left.length - right.length)
}

function compareIndexValue(left: unknown, right: unknown): number {
  const rank = indexValueRank(left) - indexValueRank(right)
  if (rank !== 0) {
    return Math.sign(rank)
  }
  if (left === null && right === null) {
    return 0
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return left === right ? 0 : left ? 1 : -1
  }
  if (typeof left === "number" && typeof right === "number") {
    return Number.isNaN(left) || Number.isNaN(right) ? 0 : Math.sign(left - right)
  }
  if (typeof left === "string" && typeof right === "string") {
    return left.localeCompare(right)
  }
  return JSON.stringify(left).localeCompare(JSON.stringify(right))
}

function indexValueRank(value: unknown): number {
  if (value === null || value === undefined) {
    return 0
  }
  if (typeof value === "boolean") {
    return 1
  }
  if (typeof value === "number") {
    return 2
  }
  if (typeof value === "string") {
    return 3
  }
  return 4
}

function localIndexRangeCursor(values: unknown[], key: string): string {
  return hexLower(utf8Bytes(JSON.stringify([values, key])))
}

function parseLocalIndexRangeCursor(cursor: string): { values: unknown[], key: string } {
  const parsed = JSON.parse(new TextDecoder().decode(hexToBytes(cursor)))
  if (!Array.isArray(parsed) || !Array.isArray(parsed[0]) || typeof parsed[1] !== "string") {
    throw new Error("invalid index range cursor")
  }
  return { values: parsed[0], key: parsed[1] }
}

function scalarValuesEqual(left: unknown, right: unknown): boolean {
  if (left === null || right === null) {
    return left === right
  }
  if (["string", "number", "boolean"].includes(typeof left) && typeof left === typeof right) {
    return left === right
  }
  return false
}

function localOrderValueComponent(value: unknown, direction: RecordOrderDirection): string {
  const asc = (() => {
    if (typeof value === "number") {
      if (Number.isInteger(value) && value >= 0) {
        return `2u${String(value).padStart(20, "0")}`
      }
      if (Number.isInteger(value)) {
        return `2i${String(value - Number.MIN_SAFE_INTEGER).padStart(20, "0")}`
      }
      return `2f${String(value).padStart(24, "0")}`
    }
    if (typeof value === "string") {
      return `3s${hexLower(utf8Bytes(value))}`
    }
    if (typeof value === "boolean") {
      return `1b${value ? 1 : 0}`
    }
    if (value === null || value === undefined) {
      return "0"
    }
    return `4j${hexLower(utf8Bytes(JSON.stringify(value)))}`
  })()
  return direction === "asc" ? asc : invertSortComponent(asc)
}

function invertSortComponent(value: string): string {
  return hexLower([...utf8Bytes(value)].map((byte) => 255 - byte))
}

function utf8Bytes(value: string): number[] {
  return [...new TextEncoder().encode(value)]
}

function hexLower(bytes: Iterable<number>): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function hexToBytes(value: string): Uint8Array {
  if (value.length % 2 !== 0) {
    throw new Error("invalid hex cursor")
  }
  const bytes = new Uint8Array(value.length / 2)
  for (let index = 0; index < value.length; index += 2) {
    const byte = Number.parseInt(value.slice(index, index + 2), 16)
    if (Number.isNaN(byte)) {
      throw new Error("invalid hex cursor")
    }
    bytes[index / 2] = byte
  }
  return bytes
}

function tableEventId(event: TableDeliveryEvent): string {
  if (event.type === "recordUpserted") {
    return `upsert:${event.key}`
  }
  return `delete:${event.key}`
}

function isMetadataRecord(value: unknown): value is { metadata: ClientCacheMetadata } {
  return typeof value === "object" && value !== null && "metadata" in value
}

function isObjectBodyRecord(value: unknown): value is { id: string; body: Blob } {
  return typeof value === "object" && value !== null && "body" in value && value.body instanceof Blob
}

function isObjectBodyRangeRecord(value: unknown): value is CachedObjectBodyRange {
  return typeof value === "object" &&
    value !== null &&
    "objectId" in value &&
    "start" in value &&
    "end" in value &&
    "byteSize" in value &&
    "contentType" in value &&
    "sha256" in value &&
    "body" in value &&
    typeof value.objectId === "string" &&
    typeof value.start === "number" &&
    typeof value.end === "number" &&
    typeof value.byteSize === "number" &&
    typeof value.contentType === "string" &&
    typeof value.sha256 === "string" &&
    value.body instanceof Blob
}

function subscriptionOptionsFromFrame(frame: ClientFrame): { afterLsn?: number; catchUpLimit?: number } {
  const options: { afterLsn?: number; catchUpLimit?: number } = {}
  if ("afterLsn" in frame && frame.afterLsn !== undefined) {
    options.afterLsn = frame.afterLsn
  }
  if ("catchUpLimit" in frame && frame.catchUpLimit !== undefined) {
    options.catchUpLimit = frame.catchUpLimit
  }
  return options
}

function idbTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("messages", mode)
    const store = transaction.objectStore("messages")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbUserEventTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("userEvents", mode)
    const store = transaction.objectStore("userEvents")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function deleteUserEvents(db: IDBDatabase, userId: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("userEvents", "readwrite")
    const store = transaction.objectStore("userEvents")
    const index = store.index("byUserLsn")
    const range = IDBKeyRange.bound([userId, 0], [userId, Number.MAX_SAFE_INTEGER])
    const request = index.openCursor(range)
    let removed = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      removed += 1
      cursor.delete()
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(removed)
  })
}

function trimUserEvents(db: IDBDatabase, userId: string, keepLatest: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("userEvents", "readwrite")
    const store = transaction.objectStore("userEvents")
    const index = store.index("byUserLsn")
    const range = IDBKeyRange.bound([userId, 0], [userId, Number.MAX_SAFE_INTEGER])
    const request = index.openCursor(range, "prev")
    let seen = 0
    let deleted = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      if (seen >= keepLatest) {
        cursor.delete()
        deleted += 1
      }
      seen += 1
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(deleted)
  })
}

function idbObjectTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (metadata: IDBObjectStore, bodies: IDBObjectStore, ranges: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["objectMetadata", "objectBodies", "objectBodyRanges"], mode)
    const metadata = transaction.objectStore("objectMetadata")
    const bodies = transaction.objectStore("objectBodies")
    const ranges = transaction.objectStore("objectBodyRanges")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(metadata, bodies, ranges)
  })
}

function idbObjectRangeTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("objectBodyRanges", mode)
    const store = transaction.objectStore("objectBodyRanges")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbGetObjectBodyRange(
  db: IDBDatabase,
  metadata: NextDbObjectMetadata,
  start: number,
  end: number,
): Promise<ObjectBodyRangeResponse | undefined> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("objectBodyRanges", "readonly")
    const store = transaction.objectStore("objectBodyRanges")
    const index = store.index("byObjectStart")
    const range = IDBKeyRange.bound([metadata.id, 0], [metadata.id, start])
    const request = index.openCursor(range, "prev")
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        resolve(undefined)
        return
      }
      const cached = cursor.value
      if (
        isObjectBodyRangeRecord(cached) &&
        cached.start <= start &&
        cached.end >= end &&
        cached.byteSize === metadata.byteSize &&
        cached.contentType === metadata.contentType &&
        cached.sha256 === metadata.sha256
      ) {
        resolve({
          body: cached.body.slice(start - cached.start, end - cached.start + 1, metadata.contentType),
          contentRange: objectContentRange(start, end, metadata.byteSize),
          start,
          end,
          byteSize: metadata.byteSize,
          contentType: metadata.contentType,
        })
        return
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbObjectCacheStats(db: IDBDatabase): Promise<{ bytes: number; rangeChunks: number }> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["objectBodies", "objectBodyRanges"], "readonly")
    const bodyRequest = transaction.objectStore("objectBodies").openCursor()
    const rangeRequest = transaction.objectStore("objectBodyRanges").openCursor()
    let bytes = 0
    let rangeChunks = 0
    let bodiesDone = false
    let rangesDone = false
    const maybeResolve = () => {
      if (bodiesDone && rangesDone) {
        resolve({ bytes, rangeChunks })
      }
    }
    bodyRequest.onerror = () => reject(bodyRequest.error)
    bodyRequest.onsuccess = () => {
      const cursor = bodyRequest.result
      if (!cursor) {
        bodiesDone = true
        maybeResolve()
        return
      }
      const row = cursor.value
      if (isObjectBodyRecord(row)) {
        bytes += row.body.size
      }
      cursor.continue()
    }
    rangeRequest.onerror = () => reject(rangeRequest.error)
    rangeRequest.onsuccess = () => {
      const cursor = rangeRequest.result
      if (!cursor) {
        rangesDone = true
        maybeResolve()
        return
      }
      const row = cursor.value
      if (isObjectBodyRangeRecord(row)) {
        bytes += row.body.size
        rangeChunks += 1
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbObjectCachedBytesById(db: IDBDatabase): Promise<Map<string, number>> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["objectBodies", "objectBodyRanges"], "readonly")
    const bodyRequest = transaction.objectStore("objectBodies").openCursor()
    const rangeRequest = transaction.objectStore("objectBodyRanges").openCursor()
    const bytesById = new Map<string, number>()
    let bodiesDone = false
    let rangesDone = false
    const maybeResolve = () => {
      if (bodiesDone && rangesDone) {
        resolve(bytesById)
      }
    }
    bodyRequest.onerror = () => reject(bodyRequest.error)
    bodyRequest.onsuccess = () => {
      const cursor = bodyRequest.result
      if (!cursor) {
        bodiesDone = true
        maybeResolve()
        return
      }
      const row = cursor.value
      if (isObjectBodyRecord(row) && typeof row.id === "string") {
        bytesById.set(row.id, (bytesById.get(row.id) ?? 0) + row.body.size)
      }
      cursor.continue()
    }
    rangeRequest.onerror = () => reject(rangeRequest.error)
    rangeRequest.onsuccess = () => {
      const cursor = rangeRequest.result
      if (!cursor) {
        rangesDone = true
        maybeResolve()
        return
      }
      const row = cursor.value
      if (isObjectBodyRangeRecord(row)) {
        bytesById.set(row.objectId, (bytesById.get(row.objectId) ?? 0) + row.body.size)
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function deleteObjectBodyRangeRows(store: IDBObjectStore, objectId: string): void {
  const index = store.index("byObjectStart")
  const range = IDBKeyRange.bound([objectId, 0], [objectId, Number.MAX_SAFE_INTEGER])
  const request = index.openCursor(range)
  request.onsuccess = () => {
    const cursor = request.result
    if (!cursor) {
      return
    }
    cursor.delete()
    cursor.continue()
  }
}

function deleteStaleObjectBodyRangeRows(store: IDBObjectStore, metadata: NextDbObjectMetadata): void {
  const index = store.index("byObjectStart")
  const range = IDBKeyRange.bound([metadata.id, 0], [metadata.id, Number.MAX_SAFE_INTEGER])
  const request = index.openCursor(range)
  request.onsuccess = () => {
    const cursor = request.result
    if (!cursor) {
      return
    }
    const cached = cursor.value
    if (
      !isObjectBodyRangeRecord(cached) ||
      cached.byteSize !== metadata.byteSize ||
      cached.contentType !== metadata.contentType ||
      cached.sha256 !== metadata.sha256
    ) {
      cursor.delete()
    }
    cursor.continue()
  }
}

function objectBodyRangeCacheId(objectId: string, start: number, end: number): string {
  return `${objectId}:${start}:${end}`
}

function idbListObjects(
  db: IDBDatabase,
  limit: number,
  afterId?: string,
): Promise<NextDbObjectMetadata[]> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("objectMetadata", "readonly")
    const store = transaction.objectStore("objectMetadata")
    const range = afterId === undefined ? undefined : IDBKeyRange.lowerBound(afterId, true)
    const request = range === undefined ? store.openCursor() : store.openCursor(range)
    const objects: NextDbObjectMetadata[] = []
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor || objects.length >= limit) {
        resolve(objects)
        return
      }
      objects.push(cursor.value as NextDbObjectMetadata)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbListAllObjects(db: IDBDatabase): Promise<NextDbObjectMetadata[]> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("objectMetadata", "readonly")
    const store = transaction.objectStore("objectMetadata")
    const request = store.openCursor()
    const objects: NextDbObjectMetadata[] = []
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        resolve(objects)
        return
      }
      objects.push(cursor.value as NextDbObjectMetadata)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbListUserProfiles(
  db: IDBDatabase,
  limit: number,
  afterUserId?: string,
): Promise<NextDbUserProfile[]> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("userProfiles", "readonly")
    const store = transaction.objectStore("userProfiles")
    const range = afterUserId === undefined ? undefined : IDBKeyRange.lowerBound(afterUserId, true)
    const request = range === undefined ? store.openCursor() : store.openCursor(range)
    const users: NextDbUserProfile[] = []
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor || users.length >= limit) {
        resolve(users)
        return
      }
      users.push(cursor.value as NextDbUserProfile)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbRecordTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("records", mode)
    const store = transaction.objectStore("records")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbRecordOrderTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (metadata: IDBObjectStore, orders: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["recordOrderMetadata", "recordOrders"], mode)
    const metadata = transaction.objectStore("recordOrderMetadata")
    const orders = transaction.objectStore("recordOrders")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(metadata, orders)
  })
}

function idbGetRecordByTableKey<T = unknown>(
  db: IDBDatabase,
  table: string,
  key: string,
): Promise<NextDbRecord<T> | undefined> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("records", "readonly")
    const store = transaction.objectStore("records")
    const index = store.index("byTableKey")
    const request = index.get([table, key])
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result as NextDbRecord<T> | undefined)
    transaction.onerror = () => reject(transaction.error)
  })
}

async function idbUpdateRecordOrderEntries(db: IDBDatabase, records: NextDbRecord[]): Promise<void> {
  if (records.length === 0 || !db.objectStoreNames.contains("recordOrderMetadata")) {
    return
  }
  const metadata = await idbListRecordOrderMetadata(db)
  for (const record of records) {
    await idbDeleteRecordOrderEntries(db, record.path)
    const matching = metadata.filter((entry) => entry.table === record.table && record.key.startsWith(entry.keyPrefix))
    if (matching.length === 0) {
      continue
    }
    await idbRecordOrderTransaction(db, "readwrite", (_metadata, orders) => {
      for (const entry of matching) {
        const cursor = localRecordOrderCursor(record, entry.order)
        orders.put({
          id: `${entry.orderId}:${cursor}`,
          orderId: entry.orderId,
          table: entry.table,
          keyPrefix: entry.keyPrefix,
          cursor,
          recordPath: record.path,
          record,
        } satisfies StoredRecordOrderEntry)
      }
    })
  }
}

function idbListRecordOrderMetadata(db: IDBDatabase): Promise<StoredRecordOrderMetadata[]> {
  if (!db.objectStoreNames.contains("recordOrderMetadata")) {
    return Promise.resolve([])
  }
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("recordOrderMetadata", "readonly")
    const store = transaction.objectStore("recordOrderMetadata")
    const request = store.openCursor()
    const records: StoredRecordOrderMetadata[] = []
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        resolve(records)
        return
      }
      records.push(cursor.value as StoredRecordOrderMetadata)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

async function idbMaterializeRecordOrder(
  db: IDBDatabase,
  table: string,
  keyPrefix: string,
  order: RecordOrderTerm[],
): Promise<string> {
  const orderId = localOrderId(table, keyPrefix, order)
  const existing = await idbGetRecordOrderMetadata(db, orderId)
  if (existing !== undefined) {
    return orderId
  }

  const records = await idbListRecordsByKeyPrefix(db, table, keyPrefix, Number.MAX_SAFE_INTEGER)
  await idbRecordOrderTransaction(db, "readwrite", (metadata, orders) => {
    metadata.put({ orderId, table, keyPrefix, order } satisfies StoredRecordOrderMetadata)
    for (const record of records) {
      const cursor = localRecordOrderCursor(record, order)
      orders.put({
        id: `${orderId}:${cursor}`,
        orderId,
        table,
        keyPrefix,
        cursor,
        recordPath: record.path,
        record,
      } satisfies StoredRecordOrderEntry)
    }
  })
  return orderId
}

function idbGetRecordOrderMetadata(
  db: IDBDatabase,
  orderId: string,
): Promise<StoredRecordOrderMetadata | undefined> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("recordOrderMetadata", "readonly")
    const store = transaction.objectStore("recordOrderMetadata")
    const request = store.get(orderId)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result as StoredRecordOrderMetadata | undefined)
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbListRecordOrder<T = unknown>(
  db: IDBDatabase,
  orderId: string,
  limit: number,
  afterCursor?: string,
): Promise<LocalOrderedRecordsResponse<T>> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("recordOrders", "readonly")
    const store = transaction.objectStore("recordOrders")
    const index = store.index("byOrderCursor")
    const range = afterCursor === undefined
      ? IDBKeyRange.bound([orderId, ""], [orderId, "\uffff"])
      : IDBKeyRange.bound([orderId, afterCursor], [orderId, "\uffff"], true, false)
    const request = index.openCursor(range)
    const entries: StoredRecordOrderEntry[] = []

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor || entries.length >= limit + 1) {
        const page = entries.slice(0, limit)
        resolve({
          records: page.map((entry) => entry.record as NextDbRecord<T>),
          nextCursor: page.at(-1)?.cursor,
          hasMore: entries.length > limit,
        })
        return
      }
      entries.push(cursor.value as StoredRecordOrderEntry)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbListRecordsByKeyPrefix<T = unknown>(
  db: IDBDatabase,
  table: string,
  keyPrefix: string,
  limit: number,
  afterKey?: string,
): Promise<Array<NextDbRecord<T>>> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("records", "readonly")
    const store = transaction.objectStore("records")
    const index = store.index("byTableKey")
    const lower = afterKey === undefined ? keyPrefix : afterKey
    const range = IDBKeyRange.bound(
      [table, lower],
      [table, `${keyPrefix}\uffff`],
      afterKey !== undefined,
      false,
    )
    const request = index.openCursor(range)
    const records: Array<NextDbRecord<T>> = []
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor || records.length >= limit) {
        resolve(records)
        return
      }
      const record = cursor.value as NextDbRecord<T>
      if (record.key.startsWith(keyPrefix)) {
        records.push(record)
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbQueryRecordsByIndex<T = unknown>(
  db: IDBDatabase,
  table: string,
  query: LocalIndexQuery,
): Promise<LocalIndexedRecordsResponse<T>> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("records", "readonly")
    const store = transaction.objectStore("records")
    const index = store.index("byTableKey")
    const rangeQuery = query.lowerValues !== undefined
      || query.upperValues !== undefined
      || query.afterCursor !== undefined
    const lowerKey = rangeQuery ? query.keyPrefix ?? "" : query.afterKey ?? query.keyPrefix ?? ""
    const upperKey = query.keyPrefix === undefined ? "\uffff" : `${query.keyPrefix}\uffff`
    const range = IDBKeyRange.bound(
      [table, lowerKey],
      [table, upperKey],
      !rangeQuery && query.afterKey !== undefined,
      false,
    )
    const request = index.openCursor(range)
    const records: NextDbRecord[] = []

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor || (!rangeQuery && records.length > query.limit)) {
        resolve(localQueryRecordsByIndex<T>(records, query))
        return
      }
      const record = cursor.value as NextDbRecord<T>
      if (
        (query.keyPrefix === undefined || record.key.startsWith(query.keyPrefix))
        && (rangeQuery || (query.values !== undefined && recordMatchesIndexQuery(record, query.fields, query.values)))
      ) {
        records.push(record)
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbDeleteRecordOrderEntries(db: IDBDatabase, recordPath: string): Promise<void> {
  if (!db.objectStoreNames.contains("recordOrders")) {
    return Promise.resolve()
  }
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("recordOrders", "readwrite")
    const store = transaction.objectStore("recordOrders")
    const index = store.index("byRecordPath")
    const request = index.openCursor(IDBKeyRange.only(recordPath))
    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      cursor.delete()
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
  })
}

function idbDeleteRecordOrdersForTable(db: IDBDatabase, table: string): Promise<void> {
  if (!db.objectStoreNames.contains("recordOrders")) {
    return Promise.resolve()
  }
  return idbRecordOrderTransaction(db, "readwrite", (metadata, orders) => {
    const metadataIndex = metadata.openCursor()
    metadataIndex.onsuccess = () => {
      const cursor = metadataIndex.result
      if (!cursor) {
        return
      }
      const value = cursor.value as StoredRecordOrderMetadata
      if (value.table === table) {
        cursor.delete()
      }
      cursor.continue()
    }

    const orderIndex = orders.index("byTable").openCursor(IDBKeyRange.only(table))
    orderIndex.onsuccess = () => {
      const cursor = orderIndex.result
      if (!cursor) {
        return
      }
      cursor.delete()
      cursor.continue()
    }
  })
}

function idbGet(
  db: IDBDatabase,
  storeName: "messages" | "records" | "pendingWrites" | "metadata" | "objectMetadata" | "objectBodies" | "userProfiles" | "subscriptions",
  key: IDBValidKey,
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, "readonly")
    const store = transaction.objectStore(storeName)
    const request = store.get(key)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result)
    transaction.onerror = () => reject(transaction.error)
  })
}

function subscriptionOptionsForStorage(options: SubscriptionOptions): SubscriptionOptions {
  const stored: SubscriptionOptions = {}
  if (options.catchUp !== undefined) {
    stored.catchUp = options.catchUp
  }
  if (options.catchUpLimit !== undefined) {
    stored.catchUpLimit = options.catchUpLimit
  }
  if (options.keyRange !== undefined) {
    stored.keyRange = {
      lowerKey: options.keyRange.lowerKey,
      upperKey: options.keyRange.upperKey,
    }
  }
  if (options.indexPrefix !== undefined) {
    stored.indexPrefix = {
      indexName: options.indexPrefix.indexName,
      fields: options.indexPrefix.fields === undefined ? undefined : [...options.indexPrefix.fields],
      values: [...options.indexPrefix.values],
    }
  }
  if (options.serverSnapshot !== undefined) {
    stored.serverSnapshot = options.serverSnapshot
  }
  if (options.snapshotLimit !== undefined) {
    stored.snapshotLimit = options.snapshotLimit
  }
  return stored
}

function mapToRecord(map: Map<string, number>): Record<string, number> {
  return Object.fromEntries(map.entries())
}

function mapRealtimeChannelStateVersions(
  map: Map<string, RealtimeChannelStateSnapshot>,
): Record<string, { version: number; updatedAtMs: number }> {
  return Object.fromEntries(
    [...map.entries()].map(([channelId, snapshot]) => [
      channelId,
      { version: snapshot.version, updatedAtMs: snapshot.updatedAtMs },
    ]),
  )
}

function mapRealtimeChannelMemberSummaries(
  map: Map<string, RealtimeMember[]>,
): Record<string, { memberCount: number; updatedAtMs?: number }> {
  return Object.fromEntries(
    [...map.entries()].map(([channelId, members]) => [
      channelId,
      {
        memberCount: members.length,
        updatedAtMs: latestRealtimeMemberUpdatedAtMs(members),
      },
    ]),
  )
}

function mapRealtimeChannelEventSummaries(
  map: Map<string, RealtimeChannelEvent[]>,
): Record<string, { eventCount: number; latestSequence?: number; latestTimestampMs?: number }> {
  return Object.fromEntries(
    [...map.entries()].map(([channelId, events]) => {
      const latest = events.at(-1)
      return [
        channelId,
        {
          eventCount: events.length,
          latestSequence: latest?.sequence,
          latestTimestampMs: latest?.timestampMs,
        },
      ]
    }),
  )
}

function mapRealtimeChannelSignalSummaries(
  map: Map<string, RealtimeSignal[]>,
): Record<string, { signalCount: number; latestSequence?: number; latestTimestampMs?: number }> {
  return Object.fromEntries(
    [...map.entries()].map(([channelId, signals]) => {
      const latest = signals.at(-1)
      return [
        channelId,
        {
          signalCount: signals.length,
          latestSequence: latest?.sequence,
          latestTimestampMs: latest?.timestampMs,
        },
      ]
    }),
  )
}

function latestRealtimeMemberUpdatedAtMs(members: RealtimeMember[]): number | undefined {
  let latest: number | undefined
  for (const member of members) {
    latest = latest === undefined ? member.updatedAtMs : Math.max(latest, member.updatedAtMs)
  }
  return latest
}

function realtimeMemberKey(member: RealtimeMember): string {
  return `${member.userId}\u0000${member.sessionId ?? ""}`
}

function sameRealtimeMembers(a: RealtimeMember[], b: RealtimeMember[]): boolean {
  if (a.length !== b.length) {
    return false
  }
  const aByKey = new Map(a.map((member) => [realtimeMemberKey(member), member]))
  for (const member of b) {
    const current = aByKey.get(realtimeMemberKey(member))
    if (!current || JSON.stringify(current) !== JSON.stringify(member)) {
      return false
    }
  }
  return true
}

function buildConnectionListResponse(
  sessions: ConnectionSession[],
  options: ListConnectionsOptions = {},
): ConnectionListResponse {
  const filtered = sessions
    .filter((session) => connectionSessionMatches(session, options))
    .sort((left, right) => left.sessionId.localeCompare(right.sessionId))
  return {
    sessions: filtered,
    total: filtered.length,
    users: new Set(filtered.map((session) => session.userId).filter((userId): userId is string => userId !== undefined)).size,
    transports: connectionTransportCounts(filtered),
    userSummaries: connectionUserSummaries(filtered),
  }
}

function connectionSessionMatches(session: ConnectionSession, options: ListConnectionsOptions): boolean {
  return (
    (options.userId === undefined || session.userId === options.userId) &&
    (options.transport === undefined || session.transport === options.transport)
  )
}

function connectionTransportCounts(sessions: ConnectionSession[]): Record<ConnectionTransport, number> {
  return {
    webSocket: sessions.filter((session) => session.transport === "webSocket").length,
    webTransport: sessions.filter((session) => session.transport === "webTransport").length,
    custom: sessions.filter((session) => session.transport === "custom").length,
  }
}

function connectionUserSummaries(sessions: ConnectionSession[]): ConnectionUserSummary[] {
  const byUser = new Map<string, ConnectionSession[]>()
  for (const session of sessions) {
    if (session.userId === undefined) {
      continue
    }
    byUser.set(session.userId, [...(byUser.get(session.userId) ?? []), session])
  }
  return [...byUser.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([userId, userSessions]) => ({
      userId,
      sessionCount: userSessions.length,
      sessionIds: userSessions.map((session) => session.sessionId).sort(),
      transports: connectionTransportCounts(userSessions),
      subscribedRooms: uniqueSorted(userSessions.flatMap((session) => session.subscribedRooms)),
      subscribedTables: uniqueSorted(userSessions.flatMap((session) => session.subscribedTables)),
      subscribedNestedTables: uniqueSorted(userSessions.flatMap((session) => session.subscribedNestedTables)),
      subscribedQueries: uniqueSorted(userSessions.flatMap((session) => session.subscribedQueries)),
      subscribedQueryTables: connectionQueryTableCounts(userSessions),
      userEventSessions: userSessions.filter((session) => session.subscribedUserEvents).length,
      objectSessions: userSessions.filter((session) => session.subscribedObjects).length,
      lastSeenAtMs: Math.max(...userSessions.map((session) => session.lastSeenAtMs)),
    }))
}

function connectionQueryTableCounts(sessions: ConnectionSession[]): Record<string, number> {
  const counts: Record<string, number> = {}
  for (const session of sessions) {
    for (const [table, count] of Object.entries(session.subscribedQueryTables ?? {})) {
      counts[table] = (counts[table] ?? 0) + count
    }
  }
  return counts
}

function connectionSessionStatus(
  map: Map<string, ConnectionSession>,
): { sessionCount: number; userCount: number; updatedAtMs?: number } {
  const sessions = [...map.values()]
  return {
    sessionCount: sessions.length,
    userCount: new Set(sessions.map((session) => session.userId).filter((userId): userId is string => userId !== undefined)).size,
    updatedAtMs: latestConnectionSessionSeenAtMs(sessions),
  }
}

function latestConnectionSessionSeenAtMs(sessions: ConnectionSession[]): number | undefined {
  let latest: number | undefined
  for (const session of sessions) {
    latest = latest === undefined ? session.lastSeenAtMs : Math.max(latest, session.lastSeenAtMs)
  }
  return latest
}

function sameConnectionSession(left: ConnectionSession, right: ConnectionSession): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values)].sort()
}

function storedRoomSubscriptionId(roomId: string): string {
  return `room:${roomId}`
}

function storedTableSubscriptionId(table: string): string {
  return `table:${tableSubscriptionTargetId(table)}`
}

function tableSubscriptionTargetId(table: string, options: SubscriptionOptions = {}): string {
  const range = options.keyRange
  const indexPrefix = options.indexPrefix
  if (range?.lowerKey === undefined && range?.upperKey === undefined && indexPrefix === undefined) {
    return table
  }
  const rangePart = range?.lowerKey === undefined && range?.upperKey === undefined
    ? ""
    : `[${encodeURIComponent(range.lowerKey ?? "")}..${encodeURIComponent(range.upperKey ?? "")})`
  const indexPart = indexPrefix === undefined
    ? ""
    : `@${encodeURIComponent(indexPrefix.indexName)}=${encodeURIComponent(encodeTableSubscriptionIndexValues(indexPrefix.values))}`
  return `${table}${rangePart}${indexPart}`
}

function tableSubscriptionFrame(
  table: string,
  options: SubscriptionOptions = {},
): Extract<ClientFrame, { type: "subscribeTable" }> {
  const indexValues = options.indexPrefix === undefined
    ? undefined
    : encodeTableSubscriptionIndexValues(options.indexPrefix.values)
  return {
    type: "subscribeTable",
    table,
    lowerKey: options.keyRange?.lowerKey,
    upperKey: options.keyRange?.upperKey,
    indexName: options.indexPrefix?.indexName,
    indexValues,
    snapshotLimit: tableSubscriptionSupportsServerSnapshot(options)
      ? normalizePageLimit(options.snapshotLimit)
      : undefined,
  }
}

function tableSubscriptionSupportsServerSnapshot(options: SubscriptionOptions): boolean {
  return options.serverSnapshot === true
}

function unsubscribeTableFrame(
  table: string,
  options: SubscriptionOptions = {},
): Extract<ClientFrame, { type: "unsubscribeTable" }> {
  return {
    type: "unsubscribeTable",
    table,
    lowerKey: options.keyRange?.lowerKey,
    upperKey: options.keyRange?.upperKey,
    indexName: options.indexPrefix?.indexName,
    indexValues: options.indexPrefix === undefined
      ? undefined
      : encodeTableSubscriptionIndexValues(options.indexPrefix.values),
  }
}

function tableSubscriptionFrameId(
  frame: Extract<ClientFrame, { type: "subscribeTable" | "unsubscribeTable" }>,
): string {
  return tableSubscriptionTargetId(frame.table, {
    keyRange: {
      lowerKey: frame.lowerKey,
      upperKey: frame.upperKey,
    },
    indexPrefix: frame.indexName === undefined || frame.indexValues === undefined
      ? undefined
      : {
          indexName: frame.indexName,
          values: parseFrameIndexValues(frame.indexValues),
        },
  })
}

function tableEventMatchesSubscription(
  event: TableDeliveryEvent,
  options: SubscriptionOptions,
): boolean {
  return event.type === "recordUpserted"
    ? tableRecordMatchesSubscription(event.record, options)
    : tableKeyMatchesSubscription(event.key, options)
}

function tableRecordMatchesSubscription(
  record: NextDbRecord,
  options: SubscriptionOptions,
): boolean {
  return tableKeyMatchesSubscription(record.key, options) &&
    tableRecordMatchesIndexPrefix(record, options)
}

function tableKeyMatchesSubscription(
  key: string,
  options: SubscriptionOptions,
): boolean {
  const range = options.keyRange
  return (range?.lowerKey === undefined || key >= range.lowerKey) &&
    (range?.upperKey === undefined || key < range.upperKey)
}

function tableRecordMatchesIndexPrefix(record: NextDbRecord, options: SubscriptionOptions): boolean {
  const prefix = options.indexPrefix
  if (prefix === undefined) {
    return true
  }
  if (prefix.fields === undefined) {
    return true
  }
  const values = prefix.values
  return values.length > 0 &&
    recordIndexValues(record, prefix.fields).slice(0, values.length)
      .every((value, index) => value === values[index])
}

function encodeTableSubscriptionIndexValues(values: unknown[]): string {
  if (values.length === 0 || values.some((value) => !isScalarIndexValue(value))) {
    throw new Error("table subscription indexPrefix values must be a non-empty scalar array")
  }
  return JSON.stringify(values)
}

function storedNestedTableSubscriptionId(table: string, parentKey: string, nested: string): string {
  return `nested:${encodeURIComponent(table)}/${encodeURIComponent(parentKey)}/${encodeURIComponent(nested)}`
}

function nestedTableSubscriptionLabel(subscription: { table: string; parentKey: string; nested: string }): string {
  return `${subscription.table}/${subscription.parentKey}/${subscription.nested}`
}

function syncNestedTableTargetParam(target: SyncNestedTableTarget): string {
  return `${target.table}:${target.parentKey}:${target.nested}`
}

function storedQuerySubscriptionId(queryId: string): string {
  return `query:${queryId}`
}

function storedUserSubscriptionId(userId: string): string {
  return `user:${userId}`
}

function storedObjectSubscriptionId(): string {
  return "objects"
}

function aggregateSumSubscriptionId(table: string, field: string): string {
  return `${table}\0${field}`
}

function aggregateSumSubscriptionFromId(id: string): { table: string; field: string } {
  const separator = id.indexOf("\0")
  if (separator < 0) {
    return { table: id, field: "" }
  }
  return {
    table: id.slice(0, separator),
    field: id.slice(separator + 1),
  }
}

function idbNamedTransaction(
  db: IDBDatabase,
  storeName: "metadata" | "userProfiles",
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, mode)
    const store = transaction.objectStore(storeName)
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbCursorTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("cursors", mode)
    const store = transaction.objectStore("cursors")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbPendingWriteTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("pendingWrites", mode)
    const store = transaction.objectStore("pendingWrites")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbSubscriptionTransaction(
  db: IDBDatabase,
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("subscriptions", mode)
    const store = transaction.objectStore("subscriptions")
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
    run(store)
  })
}

function idbListPendingWrites(db: IDBDatabase, limit: number): Promise<NextDbPendingWrite[]> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("pendingWrites", "readonly")
    const store = transaction.objectStore("pendingWrites")
    const index = store.index("byCreatedAt")
    const request = index.openCursor()
    const writes: NextDbPendingWrite[] = []

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor || writes.length >= limit) {
        resolve(writes)
        return
      }
      writes.push(cursor.value as NextDbPendingWrite)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbListSubscriptions(db: IDBDatabase): Promise<NextDbStoredSubscription[]> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("subscriptions", "readonly")
    const store = transaction.objectStore("subscriptions")
    const index = store.index("byUpdatedAt")
    const request = index.openCursor()
    const subscriptions: NextDbStoredSubscription[] = []

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        resolve(subscriptions)
        return
      }
      subscriptions.push(cursor.value as NextDbStoredSubscription)
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
  })
}

function idbGetCursor(db: IDBDatabase, key: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("cursors", "readonly")
    const store = transaction.objectStore("cursors")
    const request = store.get(key)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(Number(request.result?.lsn ?? 0))
    transaction.onerror = () => reject(transaction.error)
  })
}

function deleteNestedTableCursorsForLogicalTable(db: IDBDatabase, logicalTable: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("cursors", "readwrite")
    const store = transaction.objectStore("cursors")
    const request = store.openCursor()
    const prefix = `${nestedTableCursorIdPrefix(logicalTable)}:`
    let deleted = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      const key = String(cursor.key)
      if (key.startsWith(prefix)) {
        cursor.delete()
        deleted += 1
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(deleted)
  })
}

function idbCount(
  db: IDBDatabase,
  storeName: "objectMetadata" | "messages" | "userEvents" | "userProfiles" | "records" | "pendingWrites" | "subscriptions",
): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, "readonly")
    const store = transaction.objectStore(storeName)
    const request = store.count()
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result)
    transaction.onerror = () => reject(transaction.error)
  })
}

function deleteTableRecords(db: IDBDatabase, table: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("records", "readwrite")
    const store = transaction.objectStore("records")
    const index = store.index("byTableKey")
    const range = IDBKeyRange.bound([table, ""], [table, "\uffff"])
    const request = index.openCursor(range)
    let deleted = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      cursor.delete()
      deleted += 1
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(deleted)
  })
}

async function deleteTableRecordsByKeyPrefix(db: IDBDatabase, table: string, keyPrefix: string): Promise<number> {
  const deletedPaths = await new Promise<string[]>((resolve, reject) => {
    const transaction = db.transaction("records", "readwrite")
    const store = transaction.objectStore("records")
    const index = store.index("byTableKey")
    const range = IDBKeyRange.bound([table, keyPrefix], [table, `${keyPrefix}\uffff`])
    const request = index.openCursor(range)
    const paths: string[] = []

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      const record = cursor.value as NextDbRecord
      if (record.key.startsWith(keyPrefix)) {
        paths.push(record.path)
        cursor.delete()
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(paths)
  })

  for (const path of deletedPaths) {
    await idbDeleteRecordOrderEntries(db, path)
  }
  return deletedPaths.length
}

function deleteRoomMessages(db: IDBDatabase, roomId: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("messages", "readwrite")
    const store = transaction.objectStore("messages")
    const index = store.index("byRoomLsn")
    const range = IDBKeyRange.bound([roomId, 0], [roomId, Number.MAX_SAFE_INTEGER])
    const request = index.openCursor(range)
    let deleted = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      cursor.delete()
      deleted += 1
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(deleted)
  })
}

function trimRoomMessages(db: IDBDatabase, roomId: string, keepLatest: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("messages", "readwrite")
    const store = transaction.objectStore("messages")
    const index = store.index("byRoomLsn")
    const range = IDBKeyRange.bound([roomId, 0], [roomId, Number.MAX_SAFE_INTEGER])
    const request = index.openCursor(range, "prev")
    let seen = 0
    let deleted = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      if (seen >= keepLatest) {
        cursor.delete()
        deleted += 1
      }
      seen += 1
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(deleted)
  })
}

async function trimTableRecords(db: IDBDatabase, table: string, keepLatest: number): Promise<number> {
  const deletedPaths = await new Promise<string[]>((resolve, reject) => {
    const transaction = db.transaction("records", "readwrite")
    const store = transaction.objectStore("records")
    const index = store.index("byTableLsn")
    const range = IDBKeyRange.bound([table, 0], [table, Number.MAX_SAFE_INTEGER])
    const request = index.openCursor(range, "prev")
    const paths: string[] = []
    let seen = 0

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      const record = cursor.value as NextDbRecord
      if (seen >= keepLatest) {
        paths.push(record.path)
        cursor.delete()
      }
      seen += 1
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(paths)
  })

  for (const path of deletedPaths) {
    await idbDeleteRecordOrderEntries(db, path)
  }
  return deletedPaths.length
}

async function trimTableRecordsByKeyPrefix(
  db: IDBDatabase,
  table: string,
  keyPrefix: string,
  keepLatest: number,
): Promise<number> {
  const records = await idbListRecordsByKeyPrefix(db, table, keyPrefix, Number.MAX_SAFE_INTEGER)
  const retained = new Set(
    records
      .sort((left, right) => right.lsn - left.lsn || left.key.localeCompare(right.key))
      .slice(0, Math.max(0, keepLatest))
      .map((record) => record.path),
  )
  const deletePaths = records
    .filter((record) => !retained.has(record.path))
    .map((record) => record.path)
  if (deletePaths.length === 0) {
    return 0
  }
  const deletePathSet = new Set(deletePaths)
  await new Promise<void>((resolve, reject) => {
    const transaction = db.transaction("records", "readwrite")
    const store = transaction.objectStore("records")
    for (const path of deletePathSet) {
      store.delete(path)
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve()
  })
  for (const path of deletePaths) {
    await idbDeleteRecordOrderEntries(db, path)
  }
  return deletePaths.length
}

async function trimNestedTableRecordPartitions(
  db: IDBDatabase,
  table: string,
  keepPartitions: number,
  keepLatestPerPartition: number,
): Promise<number> {
  const partitions = await listNestedTablePartitionLsns(db, table)
  const retained = new Set(
    [...partitions.entries()]
      .sort(([leftPrefix, leftLsn], [rightPrefix, rightLsn]) => rightLsn - leftLsn || leftPrefix.localeCompare(rightPrefix))
      .slice(0, keepPartitions)
      .map(([keyPrefix]) => keyPrefix),
  )
  let removed = 0
  for (const keyPrefix of partitions.keys()) {
    if (retained.has(keyPrefix)) {
      if (keepLatestPerPartition > 0) {
        removed += await trimTableRecordsByKeyPrefix(db, table, keyPrefix, keepLatestPerPartition)
      }
      continue
    }
    removed += await deleteTableRecordsByKeyPrefix(db, table, keyPrefix)
    await deleteNestedTableCursorByKeyPrefix(db, table, keyPrefix)
  }
  return removed
}

function listNestedTablePartitionLsns(db: IDBDatabase, table: string): Promise<Map<string, number>> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("records", "readonly")
    const store = transaction.objectStore("records")
    const index = store.index("byTableKey")
    const range = IDBKeyRange.bound([table, ""], [table, "\uffff"])
    const request = index.openCursor(range)
    const partitions = new Map<string, number>()

    request.onerror = () => reject(request.error)
    request.onsuccess = () => {
      const cursor = request.result
      if (!cursor) {
        return
      }
      const record = cursor.value as NextDbRecord
      const keyPrefix = nestedRecordKeyPrefix(record)
      if (keyPrefix !== undefined) {
        partitions.set(keyPrefix, Math.max(partitions.get(keyPrefix) ?? 0, record.lsn))
      }
      cursor.continue()
    }
    transaction.onerror = () => reject(transaction.error)
    transaction.oncomplete = () => resolve(partitions)
  })
}

function deleteNestedTableCursorByKeyPrefix(
  db: IDBDatabase,
  logicalTable: string,
  keyPrefix: string,
): Promise<void> {
  const parentKey = keyPrefix.slice(0, -1)
  return idbCursorTransaction(db, "readwrite", (store) => {
    store.delete(nestedTableCursorKey(logicalTable, parentKey))
  })
}
