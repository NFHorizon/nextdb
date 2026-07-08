export type Durability = "strict" | "relaxed"

export type ActorKind = "room" | "scope" | "table" | "view" | "aggregate"

export interface BehaviorInvokeRequest<TInput = unknown> {
  behavior: string
  mutation: string
  userId?: string
  clientMutationId?: string
  input: TInput
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

export interface BehaviorReadPlan {
  records?: Array<{ table: string; key: string }>
  nestedRecords?: Array<{ table: string; parentKey: string; nested: string; nestedKey: string }>
  latestMessages?: Array<{ roomId: string; limit?: number }>
  objects?: Array<{ objectId: string }>
  objectBodies?: Array<{ objectId: string }>
  realtimeChannelMembers?: Array<{ channelId: string }>
  realtimeChannelStates?: Array<{ channelId: string }>
  connectionSessions?: Array<{ userId?: string; sessionId?: string; transport?: "webSocket" | "webTransport" | "custom" }>
  auditTraces?: BehaviorAuditTraceRead[]
  auditReplays?: BehaviorAuditReplayRead[]
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
  replyTo?: BehaviorContinuationReplyTarget
  callChainId?: string
  callDepth?: number
  maxDepth?: number
  deadlineMs?: number
  path?: string[]
}

export interface BehaviorContinuationReplyTarget {
  actorKind: ActorKind
  actorKey: string
  reminderId?: string
  continuation: BehaviorContinuationPayload
}

export interface BehaviorReminderOptions {
  reminderId?: string
  dueAtMs?: number
  delayMs?: number
  userId?: string
  clientMutationId?: string
  input?: unknown
  read?: BehaviorReadPlan
  context?: unknown
  replyTo?: BehaviorContinuationReplyTarget
  callChainId?: string
  callDepth?: number
  maxDepth?: number
  deadlineMs?: number
  path?: string[]
}

export interface BehaviorHostHttpScopes {
  allowUrlPrefixes: string[]
}

export type BehaviorAuditTraceRead =
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

export type BehaviorAuditReplayRead =
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

export interface BehaviorInvokeOutput<TResult = unknown> {
  commands: BehaviorCommand[]
  result: TResult
}

export type BehaviorRecordPredicateOp = "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "contains" | "startsWith" | "exists"

export interface BehaviorRecordPredicate {
  all: BehaviorRecordPredicateTerm[]
}

export interface BehaviorRecordPredicateTerm {
  field: string
  op: BehaviorRecordPredicateOp
  value?: unknown
}

export type BehaviorCommand =
  | {
      type: "sendMessage"
      roomId: string
      body: string
      attachments: string[]
      durability: Durability
    }
  | {
      type: "publishVolatile"
      roomId: string
      name: string
      payload: unknown
    }
  | {
      type: "publishUserVolatile"
      userId: string
      name: string
      payload: unknown
    }
  | {
      type: "publishUserEvent"
      userId: string
      name: string
      payload: unknown
      durability: Durability
      clientMutationId?: string
    }
  | {
      type: "putObject"
      bodyBase64: string
      contentType: string
      objectId?: string
      clientMutationId?: string
    }
  | {
      type: "deleteObject"
      objectId: string
      force?: boolean
      clientMutationId?: string
    }
  | {
      type: "upsertRecord"
      table: string
      key: string
      value: unknown
      durability: Durability
      expectedLsn?: number
    }
  | {
      type: "deleteRecord"
      table: string
      key: string
      durability: Durability
      expectedLsn?: number
    }
  | {
      type: "recordTransaction"
      operations: BehaviorRecordTransactionOperation[]
      durability: Durability
    }
  | {
      type: "broadcastRealtimeChannel"
      channelId: string
      kind: string
      payload: unknown
      includeSelf?: boolean
    }
  | {
      type: "updateRealtimeChannelState"
      channelId: string
      state: unknown
      expectedVersion?: number
    }
  | {
      type: "updateRealtimePresence"
      channelId: string
      metadata: unknown
      sessionId?: string
    }
  | {
      type: "disconnectConnections"
      userId?: string
      sessionId?: string
      reason?: string
    }
  | {
      type: "activateRuntimeRecords"
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
      predicate?: BehaviorRecordPredicate
    }
  | {
      type: "evictRuntimeRecords"
      table: string
      parentKey?: string
      nested?: string
      key?: string
      keys?: string[]
      afterKey?: string
      limit?: number
    }
  | {
      type: "activateRuntimeRoom"
      roomId: string
      limit?: number
    }
  | {
      type: "evictRuntimeRoom"
      roomId: string
      limit?: number
    }
  | {
      type: "scheduleActorReminder"
      kind: ActorKind
      key: string
      reminderId?: string
      dueAtMs?: number
      delayMs?: number
      payload?: unknown
    }
  | {
      type: "requestHostHttp"
      requestId?: string
      method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | string
      url: string
      headers?: Record<string, string>
      body?: unknown
      bodyBase64?: string
      timeoutMs?: number
      actorKind: ActorKind
      actorKey: string
      reminderId?: string
      continuation: BehaviorContinuationPayload
    }

export type BehaviorRecordTransactionOperation =
  | {
      type: "upsert"
      table: string
      key: string
      value: unknown
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
      value: unknown
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

export interface BehaviorManifest {
  name: string
  version: string
  modulePath: string
  abiEncoding?: BehaviorAbiEncoding
  mutations: string[]
  inputs?: Record<string, BehaviorFieldSchema>
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

export type BehaviorFieldSchema = {
  type: unknown
  optional?: boolean
  [key: string]: unknown
}

export type BehaviorHandler<TInput = unknown, TResult = unknown> = (
  request: BehaviorInvokeRequest<TInput>,
) => BehaviorInvokeOutput<TResult>

export function defineBehavior<TInput, TResult>(
  handler: BehaviorHandler<TInput, TResult>,
): BehaviorHandler<TInput, TResult> {
  return handler
}

export function output<TResult>(
  result: TResult,
  commands: BehaviorCommand[] = [],
): BehaviorInvokeOutput<TResult> {
  return { commands, result }
}

export function runtimeContext(request: BehaviorInvokeRequest): BehaviorRuntimeContext | undefined {
  if (!isObject(request.context)) {
    return undefined
  }
  const requestContext = request.context.requestContext
  const ctx = isObject(requestContext) ? requestContext.ctx : request.context.ctx
  if (!isObject(ctx) || typeof ctx.timestampMs !== "number" || typeof ctx.rngSeed !== "string") {
    return undefined
  }
  return ctx as unknown as BehaviorRuntimeContext
}

export function sendMessage(
  roomId: string,
  body: string,
  options: {
    attachments?: string[]
    durability?: Durability
  } = {},
): BehaviorCommand {
  return {
    type: "sendMessage",
    roomId,
    body,
    attachments: options.attachments ?? [],
    durability: options.durability ?? "strict",
  }
}

export function publishVolatile(roomId: string, name: string, payload: unknown): BehaviorCommand {
  return {
    type: "publishVolatile",
    roomId,
    name,
    payload,
  }
}

export function publishUserVolatile(userId: string, name: string, payload: unknown): BehaviorCommand {
  return {
    type: "publishUserVolatile",
    userId,
    name,
    payload,
  }
}

export function publishUserEvent(
  userId: string,
  name: string,
  payload: unknown,
  options: {
    durability?: Exclude<Durability, "volatile">
    clientMutationId?: string
  } = {},
): BehaviorCommand {
  return {
    type: "publishUserEvent",
    userId,
    name,
    payload,
    durability: options.durability ?? "strict",
    ...(options.clientMutationId === undefined ? {} : { clientMutationId: options.clientMutationId }),
  }
}

export function putObject(
  body: string | Uint8Array,
  options: {
    contentType?: string
    objectId?: string
    clientMutationId?: string
  } = {},
): BehaviorCommand {
  return {
    type: "putObject",
    bodyBase64: Buffer.from(body).toString("base64"),
    contentType: options.contentType ?? "application/octet-stream",
    ...(options.objectId === undefined ? {} : { objectId: options.objectId }),
    ...(options.clientMutationId === undefined ? {} : { clientMutationId: options.clientMutationId }),
  }
}

export function deleteObject(
  objectId: string,
  options: {
    force?: boolean
    clientMutationId?: string
  } = {},
): BehaviorCommand {
  return {
    type: "deleteObject",
    objectId,
    ...(options.force === undefined ? {} : { force: options.force }),
    ...(options.clientMutationId === undefined ? {} : { clientMutationId: options.clientMutationId }),
  }
}

export function upsertRecord(
  table: string,
  key: string,
  value: unknown,
  options: {
    durability?: Durability
    expectedLsn?: number
  } = {},
): BehaviorCommand {
  return {
    type: "upsertRecord",
    table,
    key,
    value,
    durability: options.durability ?? "strict",
    ...(options.expectedLsn === undefined ? {} : { expectedLsn: options.expectedLsn }),
  }
}

export function deleteRecord(
  table: string,
  key: string,
  options: {
    durability?: Durability
    expectedLsn?: number
  } = {},
): BehaviorCommand {
  return {
    type: "deleteRecord",
    table,
    key,
    durability: options.durability ?? "strict",
    ...(options.expectedLsn === undefined ? {} : { expectedLsn: options.expectedLsn }),
  }
}

export function recordTransaction(
  operations: BehaviorRecordTransactionOperation[],
  options: {
    durability?: Durability
  } = {},
): BehaviorCommand {
  return {
    type: "recordTransaction",
    operations,
    durability: options.durability ?? "strict",
  }
}

export function updateRealtimeChannelState(
  channelId: string,
  state: unknown,
  options: {
    expectedVersion?: number
  } = {},
): BehaviorCommand {
  return {
    type: "updateRealtimeChannelState",
    channelId,
    state,
    ...(options.expectedVersion === undefined ? {} : { expectedVersion: options.expectedVersion }),
  }
}

export function updateRealtimePresence(
  channelId: string,
  metadata: unknown,
  options: {
    sessionId?: string
  } = {},
): BehaviorCommand {
  return {
    type: "updateRealtimePresence",
    channelId,
    metadata,
    ...(options.sessionId === undefined ? {} : { sessionId: options.sessionId }),
  }
}

export function broadcastRealtimeChannel(
  channelId: string,
  kind: string,
  payload: unknown,
  options: {
    includeSelf?: boolean
  } = {},
): BehaviorCommand {
  return {
    type: "broadcastRealtimeChannel",
    channelId,
    kind,
    payload,
    ...(options.includeSelf === undefined ? {} : { includeSelf: options.includeSelf }),
  }
}

export function disconnectConnections(
  options: {
    userId?: string
    sessionId?: string
    reason?: string
  },
): BehaviorCommand {
  return {
    type: "disconnectConnections",
    ...(options.userId === undefined ? {} : { userId: options.userId }),
    ...(options.sessionId === undefined ? {} : { sessionId: options.sessionId }),
    ...(options.reason === undefined ? {} : { reason: options.reason }),
  }
}

export function activateRuntimeRecords(
  table: string,
  options: {
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
    predicate?: BehaviorRecordPredicate
  } = {},
): BehaviorCommand {
  return {
    type: "activateRuntimeRecords",
    table,
    ...(options.parentKey === undefined ? {} : { parentKey: options.parentKey }),
    ...(options.nested === undefined ? {} : { nested: options.nested }),
    ...(options.key === undefined ? {} : { key: options.key }),
    ...(options.keys === undefined ? {} : { keys: options.keys }),
    ...(options.indexName === undefined ? {} : { indexName: options.indexName }),
    ...(options.value === undefined ? {} : { value: options.value }),
    ...(options.values === undefined ? {} : { values: options.values }),
    ...(options.lower === undefined ? {} : { lower: options.lower }),
    ...(options.upper === undefined ? {} : { upper: options.upper }),
    ...(options.lowerValues === undefined ? {} : { lowerValues: options.lowerValues }),
    ...(options.upperValues === undefined ? {} : { upperValues: options.upperValues }),
    ...(options.afterKey === undefined ? {} : { afterKey: options.afterKey }),
    ...(options.afterCursor === undefined ? {} : { afterCursor: options.afterCursor }),
    ...(options.order === undefined ? {} : { order: options.order }),
    ...(options.limit === undefined ? {} : { limit: options.limit }),
    ...(options.predicate === undefined ? {} : { predicate: options.predicate }),
  }
}

export function evictRuntimeRecords(
  table: string,
  options: {
    parentKey?: string
    nested?: string
    key?: string
    keys?: string[]
    afterKey?: string
    limit?: number
  } = {},
): BehaviorCommand {
  return {
    type: "evictRuntimeRecords",
    table,
    ...(options.parentKey === undefined ? {} : { parentKey: options.parentKey }),
    ...(options.nested === undefined ? {} : { nested: options.nested }),
    ...(options.key === undefined ? {} : { key: options.key }),
    ...(options.keys === undefined ? {} : { keys: options.keys }),
    ...(options.afterKey === undefined ? {} : { afterKey: options.afterKey }),
    ...(options.limit === undefined ? {} : { limit: options.limit }),
  }
}

export function activateRuntimeRoom(roomId: string, options: { limit?: number } = {}): BehaviorCommand {
  return {
    type: "activateRuntimeRoom",
    roomId,
    ...(options.limit === undefined ? {} : { limit: options.limit }),
  }
}

export function evictRuntimeRoom(roomId: string, options: { limit?: number } = {}): BehaviorCommand {
  return {
    type: "evictRuntimeRoom",
    roomId,
    ...(options.limit === undefined ? {} : { limit: options.limit }),
  }
}

export function scheduleActorReminder(
  kind: ActorKind,
  key: string,
  options: {
    reminderId?: string
    dueAtMs?: number
    delayMs?: number
    payload?: unknown
  } = {},
): BehaviorCommand {
  return {
    type: "scheduleActorReminder",
    kind,
    key,
    ...(options.reminderId === undefined ? {} : { reminderId: options.reminderId }),
    ...(options.dueAtMs === undefined ? {} : { dueAtMs: options.dueAtMs }),
    ...(options.delayMs === undefined ? {} : { delayMs: options.delayMs }),
    ...(options.payload === undefined ? {} : { payload: options.payload }),
  }
}

export function scheduleBehaviorReminder(
  kind: ActorKind,
  key: string,
  behavior: string,
  mutation: string,
  options: BehaviorReminderOptions = {},
): BehaviorCommand {
  return scheduleActorReminder(kind, key, {
    reminderId: options.reminderId,
    dueAtMs: options.dueAtMs,
    delayMs: options.delayMs,
    payload: behaviorContinuation(behavior, mutation, {
      userId: options.userId,
      clientMutationId: options.clientMutationId,
      input: options.input,
      read: options.read,
      context: options.context,
      callChainId: options.callChainId,
      callDepth: options.callDepth,
      maxDepth: options.maxDepth,
      deadlineMs: options.deadlineMs,
      path: options.path,
    }),
  })
}

export function requestHostHttp(
  method: string,
  url: string,
  actorKind: ActorKind,
  actorKey: string,
  continuation: BehaviorContinuationPayload,
  options: {
    requestId?: string
    headers?: Record<string, string>
    body?: unknown
    bodyBase64?: string
    timeoutMs?: number
    reminderId?: string
  } = {},
): BehaviorCommand {
  return {
    type: "requestHostHttp",
    method,
    url,
    actorKind,
    actorKey,
    continuation,
    ...(options.requestId === undefined ? {} : { requestId: options.requestId }),
    ...(options.headers === undefined ? {} : { headers: options.headers }),
    ...(options.body === undefined ? {} : { body: options.body }),
    ...(options.bodyBase64 === undefined ? {} : { bodyBase64: options.bodyBase64 }),
    ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
    ...(options.reminderId === undefined ? {} : { reminderId: options.reminderId }),
  }
}

export function behaviorContinuation(
  behavior: string,
  mutation: string,
  options: {
    userId?: string
    clientMutationId?: string
    input?: unknown
    read?: BehaviorReadPlan
    context?: unknown
    replyTo?: BehaviorContinuationReplyTarget
    callChainId?: string
    callDepth?: number
    maxDepth?: number
    deadlineMs?: number
    path?: string[]
  } = {},
): BehaviorContinuationPayload {
  return {
    type: "behaviorContinuation",
    behavior,
    mutation,
    ...(options.userId === undefined ? {} : { userId: options.userId }),
    ...(options.clientMutationId === undefined ? {} : { clientMutationId: options.clientMutationId }),
    ...(options.input === undefined ? {} : { input: options.input }),
    ...(options.read === undefined ? {} : { read: options.read }),
    ...(options.context === undefined ? {} : { context: options.context }),
    ...(options.replyTo === undefined ? {} : { replyTo: options.replyTo }),
    ...(options.callChainId === undefined ? {} : { callChainId: options.callChainId }),
    ...(options.callDepth === undefined ? {} : { callDepth: options.callDepth }),
    ...(options.maxDepth === undefined ? {} : { maxDepth: options.maxDepth }),
    ...(options.deadlineMs === undefined ? {} : { deadlineMs: options.deadlineMs }),
    ...(options.path === undefined ? {} : { path: options.path }),
  }
}

export function behaviorReplyTo(
  actorKind: ActorKind,
  actorKey: string,
  continuation: BehaviorContinuationPayload,
  options: { reminderId?: string } = {},
): BehaviorContinuationReplyTarget {
  return {
    actorKind,
    actorKey,
    ...(options.reminderId === undefined ? {} : { reminderId: options.reminderId }),
    continuation,
  }
}

export function nestedUpsert(
  table: string,
  parentKey: string,
  nested: string,
  nestedKey: string,
  value: unknown,
  options: {
    expectedLsn?: number
  } = {},
): BehaviorRecordTransactionOperation {
  return {
    type: "nestedUpsert",
    table,
    parentKey,
    nested,
    nestedKey,
    value,
    ...(options.expectedLsn === undefined ? {} : { expectedLsn: options.expectedLsn }),
  }
}

export function nestedDelete(
  table: string,
  parentKey: string,
  nested: string,
  nestedKey: string,
  options: {
    expectedLsn?: number
  } = {},
): BehaviorRecordTransactionOperation {
  return {
    type: "nestedDelete",
    table,
    parentKey,
    nested,
    nestedKey,
    ...(options.expectedLsn === undefined ? {} : { expectedLsn: options.expectedLsn }),
  }
}

export function validateManifest(value: unknown): BehaviorManifest {
  if (!isObject(value)) {
    throw new Error("manifest must be an object")
  }

  const manifest = value as Record<string, unknown>
  const name = requiredString(manifest, "name")
  const version = requiredString(manifest, "version")
  const modulePath = requiredString(manifest, "modulePath")
  const abiEncoding = optionalAbiEncoding(manifest, "abiEncoding")
  const mutations = requiredStringArray(manifest, "mutations")
  const inputs = optionalInputSchemas(manifest, "inputs", mutations)
  const reads = optionalReadCapabilities(manifest, "reads")
  const recordScopes = optionalRecordScopes(manifest, "recordScopes")
  const objectScopes = optionalWildcardScopes(manifest, "objectScopes")
  const realtimeScopes = optionalRealtimeScopes(manifest, "realtimeScopes")
  const connectionScopes = optionalConnectionScopes(manifest, "connectionScopes")
  const userScopes = optionalUserScopes(manifest, "userScopes")
  const eventScopes = optionalEventScopes(manifest, "eventScopes")
  const hostHttpScopes = optionalHostHttpScopes(manifest, "hostHttpScopes")
  const commands = optionalCommandCapabilities(manifest, "commands")
  const maxFuel = optionalNumber(manifest, "maxFuel")

  if (mutations.length === 0) {
    throw new Error("manifest.mutations must not be empty")
  }

  return {
    name,
    version,
    modulePath,
    ...(abiEncoding === undefined ? {} : { abiEncoding }),
    mutations,
    ...(inputs === undefined ? {} : { inputs }),
    ...(reads === undefined ? {} : { reads }),
    ...(recordScopes === undefined ? {} : { recordScopes }),
    ...(objectScopes === undefined ? {} : { objectScopes }),
    ...(realtimeScopes === undefined ? {} : { realtimeScopes }),
    ...(connectionScopes === undefined ? {} : { connectionScopes }),
    ...(userScopes === undefined ? {} : { userScopes }),
    ...(eventScopes === undefined ? {} : { eventScopes }),
    ...(hostHttpScopes === undefined ? {} : { hostHttpScopes }),
    ...(commands === undefined ? {} : { commands }),
    ...(maxFuel === undefined ? {} : { maxFuel }),
  }
}

function optionalAbiEncoding(
  value: Record<string, unknown>,
  field: string,
): BehaviorAbiEncoding | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (fieldValue !== "json" && fieldValue !== "postcard" && fieldValue !== "postcardTypedSchema") {
    throw new Error(`manifest.${field} must be "json", "postcard", or "postcardTypedSchema"`)
  }
  return fieldValue
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function requiredString(value: Record<string, unknown>, field: string): string {
  const fieldValue = value[field]
  if (typeof fieldValue !== "string" || fieldValue.trim() === "") {
    throw new Error(`manifest.${field} must be a non-empty string`)
  }
  return fieldValue
}

function requiredStringArray(value: Record<string, unknown>, field: string): string[] {
  const fieldValue = value[field]
  if (!Array.isArray(fieldValue) || fieldValue.some((item) => typeof item !== "string" || item.trim() === "")) {
    throw new Error(`manifest.${field} must be a non-empty string array`)
  }
  return fieldValue as string[]
}

function optionalNumber(value: Record<string, unknown>, field: string): number | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (typeof fieldValue !== "number" || !Number.isFinite(fieldValue) || fieldValue <= 0) {
    throw new Error(`manifest.${field} must be a positive number`)
  }
  return fieldValue
}

function optionalInputSchemas(
  value: Record<string, unknown>,
  field: string,
  mutations: string[],
): Record<string, BehaviorFieldSchema> | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!isObject(fieldValue)) {
    throw new Error(`manifest.${field} must be an object`)
  }
  const inputs: Record<string, BehaviorFieldSchema> = {}
  const mutationSet = new Set(mutations)
  for (const [mutation, input] of Object.entries(fieldValue)) {
    if (!mutationSet.has(mutation)) {
      throw new Error(`manifest.${field}.${mutation} must be listed in manifest.mutations`)
    }
    if (!isObject(input) || input.type === undefined) {
      throw new Error(`manifest.${field}.${mutation} must be a field schema with a type`)
    }
    inputs[mutation] = input as BehaviorFieldSchema
  }
  return inputs
}

function optionalRecordScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorRecordScopes | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!isObject(fieldValue)) {
    throw new Error(`manifest.${field} must be an object`)
  }
  const scopes: BehaviorRecordScopes = {}
  for (const key of ["read", "write", "nestedRead", "nestedWrite"] as const) {
    const list = optionalScopeStringArray(fieldValue, key, `manifest.${field}`)
    if (list !== undefined) {
      scopes[key] = list
    }
  }
  return scopes
}

function optionalWildcardScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorObjectScopes | BehaviorRealtimeScopes | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!isObject(fieldValue)) {
    throw new Error(`manifest.${field} must be an object`)
  }
  const scopes: BehaviorObjectScopes | BehaviorRealtimeScopes = {}
  for (const key of ["read", "write"] as const) {
    const list = optionalScopeStringArray(fieldValue, key, `manifest.${field}`, validateWildcardScope)
    if (list !== undefined) {
      scopes[key] = list
    }
  }
  return scopes
}

function optionalRealtimeScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorRealtimeScopes | undefined {
  return optionalWildcardScopes(value, field) as BehaviorRealtimeScopes | undefined
}

function optionalConnectionScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorConnectionScopes | undefined {
  return optionalWildcardScopes(value, field) as BehaviorConnectionScopes | undefined
}

function optionalUserScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorUserScopes | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!isObject(fieldValue)) {
    throw new Error(`manifest.${field} must be an object`)
  }
  const scopes: BehaviorUserScopes = {}
  for (const key of ["read", "publish"] as const) {
    const list = optionalScopeStringArray(fieldValue, key, `manifest.${field}`, validateWildcardScope)
    if (list !== undefined) {
      scopes[key] = list
    }
  }
  return scopes
}

function optionalEventScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorEventScopes | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!isObject(fieldValue)) {
    throw new Error(`manifest.${field} must be an object`)
  }
  const scopes: BehaviorEventScopes = {}
  for (const key of ["publish", "realtimeBroadcast"] as const) {
    const list = optionalScopeStringArray(fieldValue, key, `manifest.${field}`, validateWildcardScope)
    if (list !== undefined) {
      scopes[key] = list
    }
  }
  return scopes
}

function optionalHostHttpScopes(
  value: Record<string, unknown>,
  field: string,
): BehaviorHostHttpScopes | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!isObject(fieldValue)) {
    throw new Error(`manifest.${field} must be an object`)
  }
  const allowUrlPrefixes = optionalScopeStringArray(
    fieldValue,
    "allowUrlPrefixes",
    `manifest.${field}`,
    validateHostHttpUrlPrefix,
  )
  if (allowUrlPrefixes === undefined || allowUrlPrefixes.length === 0) {
    throw new Error(`manifest.${field}.allowUrlPrefixes must be a non-empty array`)
  }
  return { allowUrlPrefixes }
}

function optionalScopeStringArray(
  value: Record<string, unknown>,
  field: string,
  label: string,
  validateItem?: (value: string, fieldLabel: string) => void,
): string[] | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!Array.isArray(fieldValue)) {
    throw new Error(`${label}.${field} must be an array`)
  }
  const seen = new Set<string>()
  for (const item of fieldValue) {
    if (typeof item !== "string" || item.trim() === "") {
      throw new Error(`${label}.${field} must contain non-empty strings`)
    }
    validateItem?.(item, `${label}.${field}`)
    if (seen.has(item)) {
      throw new Error(`${label}.${field} contains a duplicate value`)
    }
    seen.add(item)
  }
  return fieldValue as string[]
}

function validateWildcardScope(value: string, fieldLabel: string): void {
  const wildcardCount = value.split("*").length - 1
  if (wildcardCount > 0 && (value !== "*" && (wildcardCount !== 1 || !value.endsWith("*")))) {
    throw new Error(`${fieldLabel} wildcard must be '*' or a trailing prefix wildcard`)
  }
}

function validateHostHttpUrlPrefix(value: string, fieldLabel: string): void {
  if (!value.startsWith("https://") && !value.startsWith("http://")) {
    throw new Error(`${fieldLabel} values must start with http:// or https://`)
  }
}

const behaviorReadCapabilities = new Set<BehaviorReadCapability>([
  "records",
  "nestedRecords",
  "latestMessages",
  "objects",
  "objectBodies",
  "realtimeChannelMembers",
  "realtimeChannelStates",
  "connectionSessions",
  "auditTraces",
  "auditReplays",
])

function optionalReadCapabilities(
  value: Record<string, unknown>,
  field: string,
): BehaviorReadCapability[] | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!Array.isArray(fieldValue)) {
    throw new Error(`manifest.${field} must be an array`)
  }
  const seen = new Set<string>()
  for (const item of fieldValue) {
    if (typeof item !== "string" || !behaviorReadCapabilities.has(item as BehaviorReadCapability)) {
      throw new Error(`manifest.${field} contains an unknown read capability`)
    }
    if (seen.has(item)) {
      throw new Error(`manifest.${field} contains a duplicate read capability`)
    }
    seen.add(item)
  }
  return fieldValue as BehaviorReadCapability[]
}

const behaviorCommandCapabilities = new Set<BehaviorCommandCapability>([
  "sendMessage",
  "publishVolatile",
  "publishUserVolatile",
  "publishUserEvent",
  "putObject",
  "deleteObject",
  "upsertRecord",
  "deleteRecord",
  "recordTransaction",
  "broadcastRealtimeChannel",
  "updateRealtimeChannelState",
  "updateRealtimePresence",
  "disconnectConnections",
  "activateRuntimeRecords",
  "evictRuntimeRecords",
  "activateRuntimeRoom",
  "evictRuntimeRoom",
  "scheduleActorReminder",
  "requestHostHttp",
])

function optionalCommandCapabilities(
  value: Record<string, unknown>,
  field: string,
): BehaviorCommandCapability[] | undefined {
  const fieldValue = value[field]
  if (fieldValue === undefined) {
    return undefined
  }
  if (!Array.isArray(fieldValue)) {
    throw new Error(`manifest.${field} must be an array`)
  }
  const seen = new Set<string>()
  for (const item of fieldValue) {
    if (typeof item !== "string" || !behaviorCommandCapabilities.has(item as BehaviorCommandCapability)) {
      throw new Error(`manifest.${field} contains an unknown command capability`)
    }
    if (seen.has(item)) {
      throw new Error(`manifest.${field} contains a duplicate command capability`)
    }
    seen.add(item)
  }
  return fieldValue as BehaviorCommandCapability[]
}
