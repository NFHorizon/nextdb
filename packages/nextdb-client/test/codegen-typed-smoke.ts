import { NEXTDB_SCHEMA_VERSION, typedNextDb, type Id, type NextDbNestedTables } from "./generated/nextdb.schema"

declare const raw: unknown

const db = typedNextDb(raw)
db.withSchemaVersion(NEXTDB_SCHEMA_VERSION)
const health = await db.health()
health.ok satisfies boolean
health.connectionLayer.supportedTransports.forEach((transport) => {
  transport satisfies "webSocket" | "webTransport" | "custom"
})
const readiness = await db.readiness()
readiness.readReady satisfies boolean
readiness.writeReady satisfies boolean
readiness.realtimeReady satisfies boolean
readiness.checks.forEach((check) => {
  check.name.toUpperCase()
  check.ok satisfies boolean
})
const metrics = await db.metrics()
metrics.toUpperCase()
const objects = db.objectStore("Object")
const objectId = "object-typed-1" as Id<"Object">
const cacheStats = await db.cacheStats()
cacheStats.totalRecords.toFixed()
cacheStats.nestedTables["rooms.messages"]?.["general:"]?.toFixed()
const cacheCoverage = await db.cacheCoverage()
cacheCoverage.globalCursor.toFixed()
cacheCoverage.objects.cachedByteSize.toFixed()
cacheCoverage.tables.rooms?.records.toFixed()
const localStatus = await db.localDataStatus()
localStatus.pendingWrites.total.toFixed()
localStatus.connectionTransport satisfies "webSocket" | "webTransport" | "custom"
const stopLocalDataStatus = db.watchLocalDataStatus((snapshot) => {
  snapshot.status.coverage.objects.objects.toFixed()
  snapshot.pendingQueue.autoFlush.enabled satisfies boolean
  snapshot.source satisfies string
}, { limit: 5 })
stopLocalDataStatus()
const pendingQueue = await db.pendingWriteQueueStatus(5)
pendingQueue.stats.total.toFixed()
pendingQueue.autoFlush.inFlight satisfies boolean
pendingQueue.writes.forEach((write) => {
  write.id.toUpperCase()
  write.createdAtMs.toFixed()
})
const stopPendingWrites = db.watchPendingWrites((snapshot) => {
  snapshot.queue.stats.failed.toFixed()
}, { limit: 5 })
stopPendingWrites()
const pendingStats = await db.pendingWriteStats()
pendingStats.overMaxWrites satisfies boolean
pendingStats.objectPutBytes.toFixed()
const flushedPending = await db.flushPendingWrites(5)
flushedPending.errors.forEach((entry) => {
  entry.id.toUpperCase()
  entry.retryable satisfies boolean
})
const resetPending = await db.resetPendingWrite("pending-id")
resetPending.write?.attempts.toFixed()
const discardedPending = await db.discardPendingWrite("pending-id", { removeOptimistic: true })
discardedPending.removedOptimistic satisfies boolean
const clearedPending = await db.clearPendingWrites()
clearedPending.toFixed()
const storedSubscriptions = await db.listStoredSubscriptions()
storedSubscriptions.forEach((subscription) => {
  subscription.id.toUpperCase()
  subscription.updatedAtMs.toFixed()
  if (subscription.kind === "room") {
    subscription.roomId satisfies Id<"Room">
  }
  if (subscription.kind === "table") {
    subscription.table satisfies "rooms"
  }
  if (subscription.kind === "userEvents") {
    subscription.userId satisfies Id<"User">
  }
})
const restoredSubscriptions = await db.restoreSubscriptions()
restoredSubscriptions.forEach((subscription) => subscription.createdAtMs.toFixed())
const clearedSubscriptions = await db.clearStoredSubscriptions()
clearedSubscriptions.toFixed()
const lease = await db.refreshCacheLease()
lease.profile.version.toFixed()
lease.lease.clientId.toUpperCase()
lease.invalidations.forEach((entry) => entry.minValidLsn.toFixed())
const clearedCache = await db.clearCache()
clearedCache.toFixed()
const cacheEnforced = await db.enforceLocalCacheProfile({
  profile: {
    version: 7,
    leaseTtlMs: 60_000,
    maxObjects: 10,
    maxObjectBytes: 1024,
    maxRoomMessages: 100,
    maxUserEvents: 100,
    maxRecordsPerTable: 100,
    maxNestedPartitions: 10,
    maxPendingWrites: 20,
    maxPendingWriteBytes: 1024 * 1024,
    offlineWrites: true,
  },
})
cacheEnforced.removed.total.toFixed()
cacheEnforced.after.totalObjectCachedBytes.toFixed()

const metadata = await objects.put("typed body", {
  contentType: "text/plain",
  objectId,
  clientMutationId: "typed-object-put-1",
})
metadata.sha256.toUpperCase()
metadata.byteSize.toFixed()
metadata.id satisfies Id<"Object">

const loaded = await objects.getMetadata(objectId, { minLsn: 1, timeoutMs: 1000, consistency: "quorum" })
loaded.createdAtMs.toFixed()
const body = await objects.getBody(objectId, { minLsn: 1, consistency: "all" })
body.size.toFixed()
const bodyRange = await objects.getBodyRange(objectId, { start: 0, end: 4, minLsn: 1, consistency: "local" })
bodyRange.start.toFixed()
bodyRange.end.toFixed()
bodyRange.byteSize.toFixed()
bodyRange.body.size.toFixed()
const refs = await objects.getReferences(objectId)
refs.sources.map((source) => source.toUpperCase())
const page = await objects.list({ limit: 10, minLsn: 1 })
page.objects.forEach((object) => object.contentType.toUpperCase())
const cachedObjectPage = await objects.listCached({ limit: 10, afterId: objectId })
cachedObjectPage.objects.forEach((object) => object.id satisfies Id<"Object">)
const directCachedObjectPage = await db.listCachedObjects<"Object">({ limit: 10 })
directCachedObjectPage.objects.forEach((object) => object.sha256.toUpperCase())
const cachedMetadata = await objects.getCachedMetadata(objectId)
if (cachedMetadata) {
  cachedMetadata.id satisfies Id<"Object">
  cachedMetadata.sha256.toUpperCase()
}
const cachedBody = await objects.getCachedBody(objectId)
cachedBody?.size.toFixed()
const directCachedMetadata = await db.getCachedObjectMetadata<"Object">(objectId)
if (directCachedMetadata) {
  directCachedMetadata.byteSize.toFixed()
}
const directCachedBody = await db.getCachedObjectBody<"Object">(objectId)
directCachedBody?.size.toFixed()
objects.subscribe((event) => {
  if (event.type === "objectCommitted") {
    event.object.path.toUpperCase()
  } else {
    event.objectId satisfies Id<"Object">
  }
})
objects.watchList((snapshot) => {
  snapshot.objects.forEach((object) => object.id satisfies Id<"Object">)
  snapshot.nextAfterId satisfies Id<"Object"> | undefined
})
objects.watch(objectId, (snapshot) => {
  snapshot.objectId satisfies Id<"Object">
  snapshot.metadata?.contentType.toUpperCase()
  snapshot.cachedBody?.size.toFixed()
}, { includeBody: true })
await objects.delete(objectId, { force: true })
await objects.sync({ limit: 20 })

const userId = "alice" as Id<"User">
const channelId = "call-general" as Id<"RealtimeChannel">
const roomId = "general" as Id<"Room">
const messageId = "m1" as Id<"Message">
const activatedRuntimeRecords = await db.activateRuntimeRecords({ table: "rooms", key: roomId })
activatedRuntimeRecords.after.recordCount.toFixed()
const evictedRuntimeRecords = await db.evictRuntimeRecords({ table: "rooms", keys: [roomId] })
evictedRuntimeRecords.evicted.toFixed()
const runtimeActivation = await db.runtimeActivationStatus()
runtimeActivation.hotRoomIdleTtlMs.toFixed()
runtimeActivation.hotRoomMaintenanceIntervalMs.toFixed()
runtimeActivation.hotRoomIdleMaintenance.lastSweepAtMs?.toFixed()
runtimeActivation.hotRoomIdleMaintenance.lastEvicted.toFixed()
runtimeActivation.hotRoomIdleMaintenance.totalEvicted.toFixed()
runtimeActivation.recordHotMaintenanceIntervalMs.toFixed()
runtimeActivation.rooms.forEach((room) => {
  room.roomId satisfies Id<"Room">
  room.messages.toFixed()
})
runtimeActivation.recordHotCache.recordCount.toFixed()
runtimeActivation.recordHotCache.volatileRecords.toFixed()
runtimeActivation.recordHotCache.tables.forEach((table) => table.volatileRecords.toFixed())
runtimeActivation.recordHotCache.durableIdleTtlMs.toFixed()
runtimeActivation.recordHotCache.durableIdleLastSweepAtMs?.toFixed()
runtimeActivation.recordHotCache.durableIdleLastEvicted.toFixed()
runtimeActivation.recordHotCache.durableIdleTotalEvicted.toFixed()
const activatedRuntimeRoom = await db.activateRuntimeRoom({ roomId, limit: 2 })
activatedRuntimeRoom.afterRoomCount.toFixed()
const evictedRuntimeRoom = await db.evictRuntimeRoom({ roomId })
evictedRuntimeRoom.evicted satisfies boolean
cacheCoverage.rooms[roomId]?.messages.toFixed()
cacheCoverage.realtimeChannels[channelId]?.activeSubscription satisfies boolean
cacheCoverage.realtimeChannels[channelId]?.latestSignalSequence?.toFixed()
localStatus.coverage.realtimeChannels[channelId]?.recentEvents.toFixed()
const typedUser = await db.getUser(userId, { minLsn: 1, timeoutMs: 1000, consistency: "quorum" })
typedUser.userId satisfies Id<"User">
typedUser.lsn.toFixed()
typedUser.metadata satisfies unknown
const upsertedUser = await db.upsertUser(userId, {
  displayName: "Alice",
  metadata: { role: "typed" },
  clientMutationId: "typed-user-upsert-1",
})
upsertedUser.userId satisfies Id<"User">
upsertedUser.updatedAtMs.toFixed()
const typedUsers = await db.listUsers({ limit: 10, afterUserId: userId, minLsn: 1, consistency: "all" })
typedUsers.users.forEach((user) => user.userId satisfies Id<"User">)
typedUsers.nextAfterUserId satisfies Id<"User"> | undefined
const typedConnections = await db.listConnections({ userId, transport: "webSocket" })
typedConnections.sessions.forEach((session) => {
  session.userId satisfies string | undefined
  session.metadata satisfies unknown
  session.subscribedQueryTables satisfies Record<string, number>
})
const cachedConnections = db.cachedConnections({ userId })
cachedConnections?.sessions.forEach((session) => session.sessionId.toUpperCase())
const stopConnectionWatcher = db.watchConnections((snapshot) => {
  snapshot.connections?.total.toFixed()
}, { userId, immediate: false })
stopConnectionWatcher()
const stopConnectionEvents = db.onConnectionEvent((event) => {
  if (event.eventType === "metadataUpdated") {
    event.session?.metadata satisfies unknown
  }
})
db.updateConnectionMetadata({ device: "typed", capabilities: ["audio"] })
stopConnectionEvents()
const cachedUsers = await db.listCachedUsers({ limit: 10, afterUserId: userId })
cachedUsers.users.forEach((user) => user.userId satisfies Id<"User">)
const cachedUser = await db.getCachedUser(userId)
if (cachedUser) {
  cachedUser.userId satisfies Id<"User">
  cachedUser.updatedAtMs.toFixed()
}
await db.clearUserProfileCache(userId)
await db.clearUserEventCache(userId)
await db.clearUserCache(userId)
const roomValue = {
  id: roomId,
  title: "General",
}
const messageValue: NextDbNestedTables["rooms"]["messages"] = {
  id: messageId,
  roomId,
  senderId: userId,
  body: "hello",
  attachments: [metadata],
  createdAtMs: Date.now(),
  path: "rooms/general/messages/m1",
}

const typedRoom = db.room(roomId)
typedRoom.roomId satisfies Id<"Room">
const sentRoomMessage = await typedRoom.messages.send("typed message", {
  attachments: [objectId],
  clientMutationId: "typed-room-message-1",
})
sentRoomMessage.body.toUpperCase()
sentRoomMessage.lsn.toFixed()
sentRoomMessage.attachments.forEach((attachment) => attachment.id satisfies Id<"Object">)
const latestRoomMessages = await typedRoom.messages.latest({ limit: 10, minLsn: 1, consistency: "quorum" })
latestRoomMessages.messages.forEach((message) => message.senderId satisfies Id<"User">)
const activatedRoomMessageWindow = await typedRoom.messages.activateRuntime({ key: messageId })
activatedRoomMessageWindow.table.toUpperCase()
await typedRoom.messages.activateRuntime({ order: "schema", limit: 10 })
const previousRoomMessages = await typedRoom.messages.before(10, { limit: 5, consistency: "all" })
previousRoomMessages.messages.forEach((message) => message.roomId satisfies Id<"Room">)
const cachedRoomMessages = await typedRoom.messages.cached({ limit: 5, beforeLsn: 10 })
cachedRoomMessages.messages.forEach((message) => message.id satisfies Id<"Message">)
await db.listCachedRoomMessages(roomId, { limit: 5 })
typedRoom.messages.subscribe((event) => {
  if (event.type === "messageCreated") {
    event.message.id satisfies Id<"Message">
  } else {
    event.roomId satisfies Id<"Room">
    event.payload satisfies unknown
  }
})
typedRoom.messages.watchLatest((snapshot) => {
  snapshot.messages.forEach((message) => message.createdAtMs.toFixed())
})
await typedRoom.messages.sync({ limit: 20 })
const roomVolatile = await typedRoom.publishVolatile("presence.ping", { at: Date.now() })
roomVolatile.delivered.toFixed()
await typedRoom.publishVolatile("custom.room.event", { freeform: true })
await typedRoom.cache.clear()
await typedRoom.cache.trim(100)

const rooms = db.table("rooms")
const activatedRoomHandleRecords = await rooms.activateRuntime({ key: roomId })
activatedRoomHandleRecords.after.recordCount.toFixed()
const evictedRoomHandleRecords = await rooms.evictRuntime({ keys: [roomId] })
evictedRoomHandleRecords.evicted.toFixed()
const loadedRoom = await rooms.get(roomId, { minLsn: 1, timeoutMs: 1000, consistency: "quorum" })
loadedRoom.value.title.toUpperCase()
const projectedRoom = await rooms.get(roomId, { minLsn: 1, recordConsistency: "read-your-writes" })
projectedRoom.value.title.toUpperCase()
const loadedRooms = await rooms.list({ limit: 10, minLsn: 1, consistency: "all" })
loadedRooms.records.forEach((record) => record.value.title.toUpperCase())
const stronglyProjectedRooms = await rooms.list({ limit: 10, recordConsistency: "strong" })
stronglyProjectedRooms.records.forEach((record) => record.value.title.toUpperCase())
const cachedRooms = await rooms.cache.list({ limit: 10, afterKey: roomId })
cachedRooms.records.forEach((record) => record.value.id satisfies Id<"Room">)
await db.listCachedRecords("rooms", { limit: 10 })
const cachedRoom = await rooms.cache.get(roomId)
if (cachedRoom) {
  cachedRoom.value.title.toUpperCase()
}
const directCachedRoom = await db.getCachedRecord("rooms", roomId)
if (directCachedRoom) {
  directCachedRoom.value.id satisfies Id<"Room">
}
const predicateRooms = await rooms.list({
  limit: 10,
  predicate: { all: [{ field: "title", op: "startsWith", value: "Gen" }] },
})
predicateRooms.records.forEach((record) => record.value.id satisfies Id<"Room">)
await rooms.index("byTitle", {
  value: "General",
  predicate: { all: [{ field: "id", op: "eq", value: roomId }] },
})
rooms.subscribeQuery((result) => {
  result.response.records.forEach((record) => record.value.title.toUpperCase())
}, {
  predicate: { all: [{ field: "title", op: "contains", value: "Gen" }] },
})
rooms.subscribeQuery((result) => {
  result.resultId.toUpperCase()
}, {
  indexName: "byTitle",
  value: "General",
  predicate: { all: [{ field: "title", op: "eq", value: "General" }] },
})
rooms.watchList((snapshot) => {
  snapshot.records.forEach((record) => record.value.title.toUpperCase())
})
rooms.watch(roomId, (snapshot) => {
  snapshot.key satisfies Id<"Room">
  snapshot.record?.value.title.toUpperCase()
})
const roomTransaction = await rooms.transaction([
  { type: "upsert", key: roomId, value: roomValue },
  { type: "delete", key: roomId },
])
roomTransaction.operations.forEach((operation) => {
  if (operation.type === "recordUpserted") {
    operation.record.value.title.toUpperCase()
  }
})
const roomBatch = await rooms.upsertMany([
  { key: roomId, value: roomValue },
])
roomBatch.forEach((record) => record.value.title.toUpperCase())

const roomMessages = db.nestedTable("rooms", roomId, "messages")
const activatedMessageHandleRecords = await roomMessages.activateRuntime({ key: messageId })
activatedMessageHandleRecords.parentKey satisfies Id<"Room"> | undefined
activatedMessageHandleRecords.nested?.toUpperCase()
await roomMessages.activateRuntime({ order: "schema", limit: 10 })
const loadedMessage = await roomMessages.get(messageId, { minLsn: 1 })
loadedMessage.value.body.toUpperCase()
const loadedMessages = await roomMessages.listBySchemaOrder({ limit: 10, minLsn: 1 })
loadedMessages.records.forEach((record) => record.value.body.toUpperCase())
loadedMessages.records.forEach((record) => {
  record.value.attachments.forEach((attachment) => {
    attachment.id satisfies Id<"Object">
    attachment.sha256.toUpperCase()
    attachment.byteSize.toFixed()
  })
})
const cachedRoomRecords = await roomMessages.cache.list({ limit: 10, afterKey: messageId })
cachedRoomRecords.records.forEach((record) => record.value.roomId satisfies Id<"Room">)
const cachedRoomRecordsBySchema = await roomMessages.cache.listBySchemaOrder({ limit: 10 })
cachedRoomRecordsBySchema.records.forEach((record) => record.value.id satisfies Id<"Message">)
await db.listCachedNestedRecords("rooms", roomId, "messages", { limit: 10 })
const cachedRoomMessage = await roomMessages.cache.get(messageId)
if (cachedRoomMessage) {
  cachedRoomMessage.value.senderId satisfies Id<"User">
}
const directCachedRoomMessage = await db.getCachedNestedRecord("rooms", roomId, "messages", messageId)
if (directCachedRoomMessage) {
  directCachedRoomMessage.value.body.toUpperCase()
}
await roomMessages.cache.clear()
await db.clearNestedTableCache("rooms", roomId, "messages")
await roomMessages.listBySchemaOrder({
  limit: 10,
  predicate: {
    all: [
      { field: "senderId", op: "eq", value: userId },
      { field: "createdAtMs", op: "gte", value: Date.now() - 1_000 },
      { field: "body", op: "contains", value: "hello" },
      { field: "attachments", op: "contains", value: metadata },
    ],
  },
})
roomMessages.subscribeQuery((result) => {
  result.response.records.forEach((record) => record.value.senderId satisfies Id<"User">)
}, {
  order: "schema",
  predicate: { all: [{ field: "senderId", op: "eq", value: userId }] },
})
roomMessages.watchList((snapshot) => {
  snapshot.records.forEach((record) => record.value.body.toUpperCase())
})
roomMessages.watch(messageId, (snapshot) => {
  snapshot.key satisfies Id<"Message">
  if (snapshot.record) {
    snapshot.record.value.senderId satisfies Id<"User">
  }
})
const messageTransaction = await roomMessages.transaction([
  { type: "upsert", key: messageId, value: messageValue },
  { type: "delete", key: messageId },
])
messageTransaction.operations.forEach((operation) => {
  if (operation.type === "recordUpserted") {
    operation.record.value.body.toUpperCase()
  }
})
const messageBatch = await roomMessages.upsertMany([
  { key: messageId, value: messageValue },
])
messageBatch.forEach((record) => record.value.senderId satisfies Id<"User">)

await db.recordTransaction([
  { type: "upsert", table: "rooms", key: roomId, value: roomValue },
  {
    type: "nestedUpsert",
    table: "rooms",
    parentKey: roomId,
    nested: "messages",
    nestedKey: messageId,
    value: messageValue,
  },
])
const typedRecordBatch = await db.recordBatch([
  { type: "upsert", table: "rooms", key: roomId, value: roomValue },
  {
    type: "nestedUpsert",
    table: "rooms",
    parentKey: roomId,
    nested: "messages",
    nestedKey: messageId,
    value: messageValue,
  },
])
typedRecordBatch.transactionCount.toFixed()
typedRecordBatch.operations.forEach((operation) => {
  if (operation.type === "recordUpserted") {
    if ("title" in operation.record.value) {
      operation.record.value.title.toUpperCase()
    }
    if ("body" in operation.record.value) {
      operation.record.value.body.toUpperCase()
    }
  }
})

const roomTrace = await db.traceEntity({ kind: "record", table: "rooms", id: roomId, limit: 10, afterLsn: 1 })
roomTrace.nextAfterLsn.toFixed()
roomTrace.records.forEach((record) => record satisfies unknown)
await db.traceEntity({
  kind: "nestedRecord",
  table: "rooms",
  parentKey: roomId,
  nested: "messages",
  nestedKey: messageId,
})
await db.traceEntity({ kind: "room", id: roomId })
await db.traceEntity({ kind: "user", id: userId })
await db.traceEntity({ kind: "object", id: objectId })
await db.traceEntity({ kind: "path", path: "rooms/general/messages/m1" })
await db.traceEntity({ kind: "clientMutation", clientMutationId: "typed-user-event-1" })
const replayedRoom = await db.replayEntity({ kind: "record", table: "rooms", recordKey: roomId, atLsn: 10 })
replayedRoom.record?.value.title.toUpperCase()
replayedRoom.delete?.deletedAtMs.toFixed()
const replayedMessage = await db.replayEntity({
  kind: "nestedRecord",
  table: "rooms",
  parentKey: roomId,
  nested: "messages",
  id: messageId,
})
replayedMessage.record?.value.body.toUpperCase()
replayedMessage.record?.value.senderId satisfies Id<"User"> | undefined
const replayedUser = await db.replayEntity({ kind: "user", id: userId })
replayedUser.user?.displayName?.toUpperCase()
const replayedObject = await db.replayEntity({ kind: "object", id: objectId })
replayedObject.object?.sha256.toUpperCase()

const durableEvent = await db.publishUserEvent(userId, "notification.created", { text: "hello" }, {
  durability: "strict",
  clientMutationId: "typed-user-event-1",
})
durableEvent.payload.text.toUpperCase()
await db.publishUserEvent(userId, "notification.created", { text: "hello relaxed" }, "relaxed")
const volatilePublished = await db.publishUserVolatile(userId, "presence.ping", { at: Date.now() })
volatilePublished.delivered.toFixed()
const typedInbox = await db.listUserEvents<"notification.created">(userId, {
  limit: 10,
  minLsn: 1,
  consistency: "quorum",
})
typedInbox.forEach((event) => event.payload.text.toUpperCase())
const typedCachedInbox = await db.listCachedUserEvents<"notification.created">(userId, { limit: 10 })
typedCachedInbox.forEach((event) => event.payload.text.toUpperCase())
const typedCachedCurrentInbox = await db.listCachedCurrentUserEvents<"notification.created">({ limit: 10 })
typedCachedCurrentInbox.forEach((event) => event.payload.text.toUpperCase())
const typedHistoricalInbox = await db.listUserEvents<"notification.created">(userId, {
  limit: 5,
  beforeLsn: 10,
  consistency: "all",
})
typedHistoricalInbox.forEach((event) => event.lsn.toFixed())
db.onUserEvent<"presence.ping">((event) => {
  if (event.type === "volatileUserEvent") {
    event.payload.at.toFixed()
  }
})
db.watchCurrentUserEvents<"notification.created">((snapshot) => {
  snapshot.events.forEach((event) => event.payload.text.toUpperCase())
})

const channel = db.realtimeChannel(channelId)
type CallState = { phase: "lobby" | "started"; tick: number }
type CallPresence = { media: string[]; muted: boolean; ready: boolean }
const joinedCall = await channel.join<CallPresence>({ media: ["audio"], muted: false, ready: true })
joinedCall.channelId satisfies Id<"RealtimeChannel">
joinedCall.member.userId satisfies Id<"User">
joinedCall.member.metadata.ready satisfies boolean
joinedCall.member.updatedAtMs.toFixed()
const currentCallMembers = await channel.members<CallPresence>()
currentCallMembers.channelId satisfies Id<"RealtimeChannel">
currentCallMembers.members.forEach((member) => {
  member.userId satisfies Id<"User">
  member.metadata.muted satisfies boolean
})
channel.cachedMembers<CallPresence>()?.members.forEach((member) => {
  member.metadata.ready satisfies boolean
})
channel.watchMembers<CallPresence>((snapshot) => {
  snapshot.channelId satisfies Id<"RealtimeChannel">
  snapshot.snapshot?.members.forEach((member) => member.metadata.media.forEach((entry) => entry.toUpperCase()))
})
const updatedPresence = await channel.updatePresence<CallPresence>({ media: ["audio", "video"], muted: true, ready: true })
updatedPresence.channelId satisfies Id<"RealtimeChannel">
updatedPresence.member.metadata.media.forEach((entry) => entry.toUpperCase())
updatedPresence.sequence.toFixed()
updatedPresence.delivered.toFixed()
const channelState = await channel.state<CallState>()
channelState.channelId satisfies Id<"RealtimeChannel">
channelState.state.state.phase satisfies "lobby" | "started"
await channel.updateState<CallState>({ phase: "lobby", tick: 1 }, { expectedVersion: channelState.state.version })
channel.cachedState<CallState>()?.state.tick.toFixed()
channel.watchState<CallState>((snapshot) => {
  snapshot.channelId satisfies Id<"RealtimeChannel">
  snapshot.snapshot?.state.tick.toFixed()
})
await channel.signal(userId, "offer", { sdp: "..." })
await channel.broadcast("gameInput", { buttons: ["jump"], frame: 42 })
channel.cachedRecentEvents({ kind: "gameInput", limit: 5 }).forEach((event) => {
  event.channelId satisfies Id<"RealtimeChannel">
  event.kind satisfies "gameInput"
  event.sequence.toFixed()
})
channel.watchRecentEvents((snapshot) => {
  snapshot.channelId satisfies Id<"RealtimeChannel">
  snapshot.events.forEach((event) => {
    event.kind satisfies "gameInput"
    event.timestampMs.toFixed()
  })
}, { kind: "gameInput", limit: 5 })
channel.cachedRecentSignals({ kind: "offer", limit: 5 }).forEach((signal) => {
  signal.channelId satisfies Id<"RealtimeChannel">
  signal.toUserId satisfies Id<"User">
  signal.kind satisfies "offer"
})
channel.watchRecentSignals((snapshot) => {
  snapshot.channelId satisfies Id<"RealtimeChannel">
  snapshot.signals.forEach((signal) => {
    signal.kind satisfies "offer"
    signal.sequence.toFixed()
  })
}, { kind: "offer", limit: 5 })
await channel.sendGameInputFrame(new Uint8Array([1, 2, 3]), {
  contentType: "application/x.nextdb.game-input",
  metadata: { frame: 43 },
  includeSelf: false,
})
await channel.sendVoiceFrame(new Uint8Array([4, 5, 6]), {
  contentType: "audio/opus",
  codec: "opus",
})
await channel.sendVideoFrame("idr-frame", {
  contentType: "video/h264",
  codec: "h264",
  metadata: { keyframe: true },
})
channel.onSignal((signal) => {
  signal.channelId satisfies Id<"RealtimeChannel">
  signal.fromUserId satisfies Id<"User">
})
channel.onSignalKind("offer", (signal) => {
  signal.kind satisfies "offer"
  signal.toUserId satisfies Id<"User">
})
channel.onEvent((event) => {
  event.sequence.toFixed()
  event.timestampMs.toFixed()
})
channel.onEventKind("gameInput", (event) => {
  event.kind satisfies "gameInput"
  event.channelId satisfies Id<"RealtimeChannel">
})
channel.onState<CallState>((event) => {
  event.channelId satisfies Id<"RealtimeChannel">
  event.state.state.phase satisfies "lobby" | "started"
})
channel.onMemberUpdated((event) => {
  event.channelId satisfies Id<"RealtimeChannel">
  event.member.userId satisfies Id<"User">
  event.member.updatedAtMs.toFixed()
  event.sequence.toFixed()
})

await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  userId,
  input: {
    roomId,
    body: "typed behavior",
  },
  read: {
    records: [{ table: "rooms", key: roomId }],
    nestedRecords: [{
      table: "rooms",
      parentKey: roomId,
      nested: "messages",
      nestedKey: messageId,
    }],
    latestMessages: [{ roomId, limit: 10 }],
    objects: [{ object: "Object", objectId }],
    objectBodies: [{ object: "Object", objectId }],
    realtimeChannelMembers: [{ channelId }],
    realtimeChannelStates: [{ channelId }],
    connectionSessions: [{ userId, sessionId: "typed-session", transport: "webSocket" }],
    auditTraces: [{ kind: "record", table: "rooms", id: roomId, limit: 10 }],
    auditReplays: [{ kind: "nestedRecord", table: "rooms", parentKey: roomId, nested: "messages", id: messageId }],
  },
})

// @ts-expect-error object store name is schema-bound
const missing = db.objectStore("MissingObject")
void missing
// @ts-expect-error object id is branded by object schema
await objects.getMetadata("plain-object-id")
// @ts-expect-error object watcher id is branded by object schema
objects.watch("plain-object-id", () => undefined)
// @ts-expect-error objectId option is branded by object schema
await objects.put("typed body", { objectId: "plain-object-id" })
// @ts-expect-error generated object metadata does not have arbitrary fields
metadata.missingField
// @ts-expect-error ObjectRef fields require object metadata, not a plain object id
messageValue.attachments = [objectId]
// @ts-expect-error event name is schema-bound
await db.publishUserEvent(userId, "missing.event", { text: "hello" })
// @ts-expect-error notification.created requires text
await db.publishUserEvent(userId, "notification.created", { at: Date.now() })
// @ts-expect-error durable user events do not accept volatile durability
await db.publishUserEvent(userId, "notification.created", { text: "hello" }, "volatile")
// @ts-expect-error presence.ping requires numeric at
await db.publishUserVolatile(userId, "presence.ping", { at: "now" })
// @ts-expect-error room id is branded by room table schema
db.room("plain-room-id")
// @ts-expect-error realtime channel id is branded by realtime channel entity schema
db.realtimeChannel("plain-channel-id")
// @ts-expect-error audit record trace table is schema-bound
await db.traceEntity({ kind: "record", table: "missing", id: roomId })
// @ts-expect-error audit record trace key is branded by table schema
await db.traceEntity({ kind: "record", table: "rooms", id: "plain-room-id" })
await db.traceEntity({
  kind: "nestedRecord",
  table: "rooms",
  parentKey: roomId,
  // @ts-expect-error audit nested trace name is schema-bound
  nested: "missing",
  nestedKey: messageId,
})
await db.replayEntity({
  kind: "nestedRecord",
  table: "rooms",
  // @ts-expect-error audit nested replay parent key is branded by parent table
  parentKey: "plain-room-id",
  nested: "messages",
  id: messageId,
})
await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  // @ts-expect-error behavior user id is branded by user entity schema
  userId: "plain-user-id",
  input: {
    roomId,
    body: "bad user",
  },
})
await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  input: {
    roomId,
    body: "bad read plan",
  },
  read: {
    latestMessages: [
      // @ts-expect-error behavior latestMessages room id is branded by room table schema
      { roomId: "plain-room-id", limit: 1 },
    ],
    realtimeChannelMembers: [
      // @ts-expect-error behavior realtime members channel id is branded by realtime channel entity schema
      { channelId: "plain-channel-id" },
    ],
    realtimeChannelStates: [
      // @ts-expect-error behavior realtime state channel id is branded by realtime channel entity schema
      { channelId: "plain-channel-id" },
    ],
  },
})
// @ts-expect-error replayed room records do not expose message fields
replayedRoom.record?.value.body
// @ts-expect-error typed realtime channel state requires tick
await channel.updateState<CallState>({ phase: "lobby" })
await typedRoom.messages.send("bad attachment", {
  // @ts-expect-error room message attachments are typed object ids
  attachments: ["plain-object-id"],
})
// @ts-expect-error declared volatile payload is schema-bound
await typedRoom.publishVolatile("presence.ping", { at: "now" })
await rooms.list({
  predicate: {
    all: [{
      // @ts-expect-error predicate field is schema-bound
      field: "missing",
      op: "eq",
      value: "General",
    }],
  },
})
await rooms.list({
  predicate: {
    all: [{
      field: "title",
      op: "eq",
      // @ts-expect-error predicate value is bound to field type
      value: 123,
    }],
  },
})
await rooms.index("byTitle", {
  value: "General",
  predicate: {
    all: [{
      field: "title",
      op: "contains",
      // @ts-expect-error string contains predicate requires string value
      value: 123,
    }],
  },
})
await roomMessages.listBySchemaOrder({
  predicate: {
    all: [
      // @ts-expect-error nested predicate value is bound to Id field type
      {
      field: "senderId",
      op: "eq",
      value: "plain-user-id",
    }],
  },
})
await roomMessages.listBySchemaOrder({
  predicate: {
    all: [
      // @ts-expect-error list contains predicate uses element type, not object id
      {
      field: "attachments",
      op: "contains",
      value: objectId,
    }],
  },
})
await rooms.transaction([{
  type: "upsert",
  key: roomId,
  // @ts-expect-error room transaction value is schema-bound
  value: { id: roomId },
}])
await rooms.transaction([{
  type: "upsert",
  // @ts-expect-error room transaction key is branded by table schema
  key: "plain-room-id",
  value: roomValue,
}])
// @ts-expect-error table watcher key is branded by table schema
rooms.watch("plain-room-id", () => undefined)
await roomMessages.transaction([{
  type: "upsert",
  key: messageId,
  value: {
    ...messageValue,
    // @ts-expect-error nested transaction value is schema-bound
    senderId: "plain-user-id",
  },
}])
// @ts-expect-error nested watcher key is branded by nested table schema
roomMessages.watch("plain-message-id", () => undefined)
await db.recordTransaction([{
  type: "upsert",
  // @ts-expect-error record transaction table is schema-bound
  table: "missing",
  key: roomId,
  value: roomValue,
}])
await db.recordTransaction([{
  type: "nestedUpsert",
  table: "rooms",
  parentKey: roomId,
  // @ts-expect-error record transaction nested table is schema-bound
  nested: "missing",
  nestedKey: messageId,
  value: messageValue,
}])
await db.recordTransaction([{
  type: "nestedUpsert",
  table: "rooms",
  parentKey: roomId,
  nested: "messages",
  nestedKey: messageId,
  value: {
    ...messageValue,
    // @ts-expect-error record transaction nested value is schema-bound
    createdAtMs: "now",
  },
}])
await db.recordBatch([{
  type: "upsert",
  // @ts-expect-error record batch table is schema-bound
  table: "missing",
  key: roomId,
  value: roomValue,
}])
await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  input: { roomId, body: "typed behavior" },
  read: { records: [{
    // @ts-expect-error behavior read plan table is schema-bound
    table: "missing",
    key: roomId,
  }] },
})
await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  input: { roomId, body: "typed behavior" },
  read: { nestedRecords: [{
    table: "rooms",
    parentKey: roomId,
    // @ts-expect-error nested read plan nested table is schema-bound
    nested: "missing",
    nestedKey: messageId,
  }] },
})
await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  input: { roomId, body: "typed behavior" },
  read: { objects: [{
    object: "Object",
    // @ts-expect-error behavior object read id is schema-bound
    objectId: "plain-object-id",
  }] },
})
