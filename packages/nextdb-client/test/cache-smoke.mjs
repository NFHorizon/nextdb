import assert from "node:assert/strict"
import { createServer } from "node:http"

import "fake-indexeddb/auto"

import {
  IndexedDbLocalCache,
  MemoryLocalCache,
  NextDbClient,
  NextDbPendingWriteLimitError,
} from "../dist/index.js"

const metadata = {
  clientId: "cache-smoke-client",
  sessionId: "cache-smoke-session",
  profileVersion: 3,
  schemaVersion: 7,
  invalidationGeneration: 11,
  leaseExpiresAtMs: Date.now() + 60_000,
  lastValidatedAtMs: Date.now(),
}

const pendingWrite = {
  id: "pending-1",
  type: "recordUpsert",
  createdAtMs: 1,
  attempts: 0,
  table: "rooms",
  key: "cache-smoke",
  value: { id: "cache-smoke", title: "Cache Smoke" },
  durability: "strict",
  expectedLsn: 4,
  clientMutationId: "cache-smoke-mutation",
}

const message = {
  id: "msg-1",
  roomId: "cache-room",
  senderId: "cache-user",
  body: "cached",
  attachments: [],
  createdAtMs: Date.now(),
  lsn: 9,
  path: "rooms/cache-room/messages/msg-1",
}

const laterMessage = {
  ...message,
  id: "msg-2",
  body: "cached later",
  createdAtMs: message.createdAtMs + 1,
  lsn: 12,
  path: "rooms/cache-room/messages/msg-2",
}

const userEvent = {
  id: "event-1",
  userId: "cache-user",
  name: "notification.created",
  payload: { text: "cached event" },
  createdAtMs: Date.now(),
  lsn: 13,
  path: "users/cache-user/events/event-1",
}

const laterUserEvent = {
  ...userEvent,
  id: "event-2",
  payload: { text: "cached event later" },
  createdAtMs: userEvent.createdAtMs + 1,
  lsn: 14,
  path: "users/cache-user/events/event-2",
}

const userProfile = {
  userId: "cache-user",
  displayName: "Cache User",
  metadata: { role: "tester" },
  createdAtMs: Date.now(),
  updatedAtMs: Date.now(),
  lsn: 15,
  path: "users/cache-user",
}

const record = {
  table: "rooms",
  key: "cache-room",
  value: { id: "cache-room", title: "Cache Room" },
  updatedAtMs: Date.now(),
  lsn: 10,
  path: "tables/rooms/cache-room",
}

const nestedRecords = [
  nestedRecord("m1", "cache-user", 10),
  nestedRecord("m2", "cache-user", 30),
  nestedRecord("m3", "other-user", 20),
]

const objectMetadata = {
  id: "object-1",
  path: "objects/object-1",
  contentType: "text/plain",
  byteSize: 11,
  sha256: "object-sha",
  createdAtMs: Date.now(),
}

const laterObjectMetadata = {
  ...objectMetadata,
  id: "object-2",
  path: "objects/object-2",
  byteSize: 15,
  sha256: "object-sha-2",
  createdAtMs: objectMetadata.createdAtMs + 1,
}

const newestObjectMetadata = {
  ...objectMetadata,
  id: "object-3",
  path: "objects/object-3",
  byteSize: 20,
  sha256: "object-sha-3",
  createdAtMs: objectMetadata.createdAtMs + 2,
}

class CountingRoomMessageCache extends MemoryLocalCache {
  putRoomMessagesCalls = []
  putRecordsCalls = []
  putUserEventsCalls = []
  setGlobalCursorCalls = []
  setRoomCursorCalls = []
  setUserCursorCalls = []
  setTableCursorCalls = []
  setNestedTableCursorCalls = []

  async putRoomMessages(roomId, messages) {
    this.putRoomMessagesCalls.push({ roomId, messages: [...messages] })
    await super.putRoomMessages(roomId, messages)
  }

  async putRecords(records) {
    this.putRecordsCalls.push([...records])
    await super.putRecords(records)
  }

  async putUserEvents(userId, events) {
    this.putUserEventsCalls.push({ userId, events: [...events] })
    await super.putUserEvents(userId, events)
  }

  async setGlobalCursor(lsn) {
    this.setGlobalCursorCalls.push(lsn)
    await super.setGlobalCursor(lsn)
  }

  async setRoomCursor(roomId, lsn) {
    this.setRoomCursorCalls.push({ roomId, lsn })
    await super.setRoomCursor(roomId, lsn)
  }

  async setUserCursor(userId, lsn) {
    this.setUserCursorCalls.push({ userId, lsn })
    await super.setUserCursor(userId, lsn)
  }

  async setTableCursor(table, lsn) {
    this.setTableCursorCalls.push({ table, lsn })
    await super.setTableCursor(table, lsn)
  }

  async setNestedTableCursor(table, parentKey, nested, lsn) {
    this.setNestedTableCursorCalls.push({ table, parentKey, nested, lsn })
    await super.setNestedTableCursor(table, parentKey, nested, lsn)
  }
}

await testMemoryClearAllClearsEveryCursor()
await testMemoryNestedTableCursor()
await testMemoryClearUserEventsClearsCursor()
await testClientUserCacheManagement()
await testClientCachedListApis()
await testSyncPullBatchesRoomMessageCacheWrites()
await testSyncPullBatchesConsecutiveRecordUpserts()
await testSyncPullBatchesNestedRecordUpsertCursors()
await testSyncPullBatchesUserEventCacheWrites()
await testClientObjectBodyRangeUsesCachedBody()
await testClientObjectBodyRangeUsesCachedPartialRange()
await testMemoryObjectBodyInvalidatesWhenMetadataChanges()
await testMemoryUserProfiles()
await testMemoryTrimObjectsKeepsNewestWithinLimits()
await testMemoryTrimObjectsUsesActualCachedBytes()
await testMemoryOfflineObjectWritesQueue()
await testMemoryOfflineUserWritesFlush()
await testMemoryOfflineRecordTransactionFlush()
await testMemoryPendingWriteManagement()
await testMemoryPendingWriteLimitAdmission()
await testMemoryWatchPendingWrites()
await testMemoryWatchLocalDataStatus()
await testMemoryWatchRecordDetail()
await testMemoryWatchNestedRecordDetail()
await testMemoryWatchObjectDetail()
await testMemoryAutoFlushPendingObjectWrite()
await testMemoryCacheCoverage()
await testMemoryTrimUserEventsKeepsNewestEvents()
await testMemoryTrimTableKeepsNewestRecords()
await testMemoryTrimRecordsByKeyPrefixKeepsNewestPerParent()
await testMemoryTrimNestedTablePartitionsKeepsHottestParents()
await testMemoryIndexRangeQueryPaginates()
await testMemorySubscriptionRegistry()
await testClientIndexRangeUsesLocalCache()
await testIndexedDbRehydratesAcrossInstances()
await testIndexedDbObjectBodyRangeRehydrates()
await testIndexedDbObjectBodyInvalidatesWhenMetadataChanges()
await testIndexedDbNestedTableCursorRehydrates()
await testDefaultIndexedDbCacheScopesByEndpointUserNamespace()
await testIndexedDbSubscriptionRegistryRehydrates()
await testIndexedDbPendingObjectBlobRehydrates()
await testIndexedDbPendingWriteManagement()
await testIndexedDbCacheCoverage()
await testIndexedDbClearUserEventsClearsCursor()
await testIndexedDbUserProfiles()
await testIndexedDbTrimObjectsKeepsNewestWithinLimits()
await testIndexedDbTrimObjectsUsesActualCachedBytes()
await testIndexedDbTrimUserEventsKeepsNewestEvents()
await testIndexedDbTrimTableKeepsNewestRecords()
await testIndexedDbTrimRecordsByKeyPrefixKeepsNewestPerParent()
await testIndexedDbTrimNestedTablePartitionsKeepsHottestParents()
await testIndexedDbClearRecordsByKeyPrefix()
await testIndexedDbIndexRangeQueryPaginates()
await testIndexedDbPendingClearPreservesMetadata()
await testIndexedDbClearAllClearsMetadataAndCursors()

console.log("cache smoke ok")

async function testMemoryClearAllClearsEveryCursor() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata(metadata)
  await cache.setGlobalCursor(99)
  await cache.setObjectCursor(100)
  await cache.setRoomCursor("cache-room", 98)
  await cache.setUserCursor("cache-user", 97)
  await cache.setTableCursor("rooms", 96)
  await cache.putPendingWrite(pendingWrite)
  await cache.putRoomMessages("cache-room", [message])
  await cache.putUserEvents("cache-user", [userEvent])
  await cache.putRecords([record])

  const removed = await cache.clearAll()

  assert.equal(removed, 4)
  assert.equal(await cache.getGlobalCursor(), 0)
  assert.equal(await cache.getObjectCursor(), 0)
  assert.equal(await cache.getRoomCursor("cache-room"), 0)
  assert.equal(await cache.getUserCursor("cache-user"), 0)
  assert.equal(await cache.getTableCursor("rooms"), 0)
  assert.equal(await cache.getMetadata(), undefined)
  assert.deepEqual(await cache.listPendingWrites(), [])
}

async function testMemoryNestedTableCursor() {
  const cache = new MemoryLocalCache()
  await cache.setNestedTableCursor("rooms", "cache-room", "messages", 123)
  await cache.setNestedTableCursor("rooms", "other-room", "messages", 456)

  assert.equal(await cache.getNestedTableCursor("rooms", "cache-room", "messages"), 123)
  assert.equal(await cache.getNestedTableCursor("rooms", "other-room", "messages"), 456)

  await cache.clearTable("rooms.messages")
  assert.equal(await cache.getNestedTableCursor("rooms", "cache-room", "messages"), 0)
  assert.equal(await cache.getNestedTableCursor("rooms", "other-room", "messages"), 0)
}

async function testMemorySubscriptionRegistry() {
  const cache = new MemoryLocalCache()
  await cache.putSubscription({
    id: "room:cache-room",
    kind: "room",
    roomId: "cache-room",
    options: { catchUpLimit: 25 },
    createdAtMs: 1,
    updatedAtMs: 2,
  })
  await cache.putSubscription({
    id: "objects",
    kind: "objects",
    options: {},
    createdAtMs: 3,
    updatedAtMs: 4,
  })

  const subscriptions = await cache.listSubscriptions()
  assert.deepEqual(subscriptions.map((subscription) => subscription.id), ["room:cache-room", "objects"])
  assert.equal((await cache.stats()).subscriptions, 2)
  await cache.deleteSubscription("objects")
  assert.deepEqual((await cache.listSubscriptions()).map((subscription) => subscription.id), ["room:cache-room"])
  assert.equal(await cache.clearSubscriptions(), 1)
  assert.deepEqual(await cache.listSubscriptions(), [])
}

async function testMemoryClearUserEventsClearsCursor() {
  const cache = new MemoryLocalCache()
  await cache.setUserCursor("cache-user", 97)
  await cache.putUserEvents("cache-user", [userEvent, laterUserEvent])

  const removed = await cache.clearUserEvents("cache-user")

  assert.equal(removed, 2)
  assert.equal(await cache.getUserCursor("cache-user"), 0)
  assert.deepEqual(await cache.getUserEvents("cache-user", 10), [])
}

async function testClientUserCacheManagement() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "cache-user",
    cache,
  })
  const changes = []
  client.onCacheChange((change) => changes.push(change))

  await cache.setUserCursor("cache-user", 97)
  await cache.putUserProfile(userProfile)
  await cache.putUserEvents("cache-user", [userEvent, laterUserEvent])

  let page = await client.listCachedUsers({ limit: 1 })
  assert.deepEqual(page.users.map((user) => user.userId), ["cache-user"])
  assert.equal(page.hasMore, false)
  assert.equal(page.nextAfterUserId, "cache-user")

  const removedProfile = await client.clearUserProfileCache("cache-user")
  assert.equal(removedProfile, 1)
  assert.equal(await cache.getUserProfile("cache-user"), undefined)
  assert.equal(changes.at(-1).type, "userProfileDeleted")

  await cache.putUserProfile(userProfile)
  const removedUser = await client.clearUserCache("cache-user")
  assert.equal(removedUser, 3)
  assert.equal(await cache.getUserProfile("cache-user"), undefined)
  assert.deepEqual(await cache.getUserEvents("cache-user", 10), [])
  assert.equal(await cache.getUserCursor("cache-user"), 0)
  assert(changes.some((change) => change.type === "userInvalidated" && change.source === "manual"))

  page = await client.listCachedUsers()
  assert.deepEqual(page.users, [])
}

async function testClientCachedListApis() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "cache-user",
    cache,
  })
  await cache.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))
  await cache.putObject(laterObjectMetadata, new Blob(["later cache"], { type: "text/plain" }))
  await cache.putRoomMessages("cache-room", [message, laterMessage])
  await cache.putUserEvents("cache-user", [userEvent, laterUserEvent])
  await cache.putUserProfile(userProfile)
  await cache.putRecords([
    record,
    nestedRecord("cached-nested-1", "cache-user", 10),
    nestedRecord("cached-nested-2", "cache-user", 20),
    nestedRecordForParent("other-room", "other-nested-1", "cache-user", 30),
  ])
  await cache.setTableCursor("rooms.messages", 99)

  assert.deepEqual(await client.getCachedUser(), userProfile)
  assert.equal(await client.getCachedUser("missing-user"), undefined)

  assert.deepEqual(await client.getCachedObjectMetadata("object-1"), objectMetadata)
  const cachedObjectBody = await client.getCachedObjectBody("object-1")
  assert(cachedObjectBody)
  assert.equal(await cachedObjectBody.text(), "hello cache")
  assert.deepEqual(await client.objectStore("Object").getCachedMetadata("object-2"), laterObjectMetadata)
  assert.equal(await client.objectStore("Object").getCachedBody("missing-object"), undefined)

  const objects = await client.listCachedObjects({ limit: 1 })
  assert.deepEqual(objects.objects.map((object) => object.id), ["object-1"])
  assert.equal(objects.hasMore, true)
  assert.equal((await client.objectStore("Object").listCached({ limit: 2 })).objects.length, 2)

  const roomMessages = await client.listCachedRoomMessages("cache-room", { limit: 1 })
  assert.equal(roomMessages.source, "cache")
  assert.deepEqual(roomMessages.messages.map((row) => row.id), ["msg-2"])
  assert.deepEqual((await client.room("cache-room").messages.cached({ limit: 2 })).messages.map((row) => row.id), ["msg-2", "msg-1"])

  const cachedEvents = await client.listCachedCurrentUserEvents({ limit: 1 })
  assert.deepEqual(cachedEvents.map((event) => event.id), ["event-2"])
  assert.deepEqual((await client.listCachedUserEvents("cache-user", { beforeLsn: 14 })).map((event) => event.id), ["event-1"])

  const records = await client.listCachedRecords("rooms", { limit: 1 })
  assert.deepEqual(records.records.map((row) => row.key), ["cache-room"])
  assert.equal(records.hasMore, false)
  assert.deepEqual(await client.getCachedRecord("rooms", "cache-room"), record)
  assert.deepEqual((await client.table("rooms").cache.list({ limit: 1 })).records.map((row) => row.key), ["cache-room"])
  assert.deepEqual(await client.table("rooms").cache.get("cache-room"), record)
  assert.equal(await client.table("rooms").cache.get("missing-room"), undefined)

  const nested = await client.listCachedNestedRecords("rooms", "cache-room", "messages", { limit: 1 })
  assert.deepEqual(nested.records.map((row) => row.value.id), ["cached-nested-1"])
  assert.equal(nested.hasMore, true)
  assert.equal((await client.getCachedNestedRecord("rooms", "cache-room", "messages", "cached-nested-1")).value.id, "cached-nested-1")
  assert.deepEqual((await client.nestedTable("rooms", "cache-room", "messages").cache.list({ afterKey: "cached-nested-1" })).records.map((row) => row.value.id), ["cached-nested-2"])
  assert.equal((await client.nestedTable("rooms", "cache-room", "messages").cache.get("cached-nested-2")).value.id, "cached-nested-2")
  assert.equal(await client.nestedTable("rooms", "cache-room", "messages").cache.get("missing-message"), undefined)

  assert.equal(await client.clearNestedTableCache("rooms", "cache-room", "messages"), 2)
  assert.equal(await cache.getTableCursor("rooms.messages"), 0)
  assert.deepEqual((await client.listCachedNestedRecords("rooms", "cache-room", "messages")).records, [])
  assert.deepEqual((await client.listCachedNestedRecords("rooms", "other-room", "messages")).records.map((row) => row.value.id), ["other-nested-1"])
  assert.equal(await client.nestedTable("rooms", "other-room", "messages").cache.clear(), 1)
  assert.deepEqual((await client.listCachedNestedRecords("rooms", "other-room", "messages")).records, [])
}

async function testSyncPullBatchesRoomMessageCacheWrites() {
  const cache = new CountingRoomMessageCache()
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const roomId = "sync-batch-room"
  const events = [1, 2, 3].map((lsn) => ({
    type: "messageCreated",
    roomId,
    message: {
      id: `sync-message-${lsn}`,
      roomId,
      senderId: "cache-user",
      body: `sync message ${lsn}`,
      attachments: [],
      createdAtMs: 1000 + lsn,
      lsn,
      path: `rooms/${roomId}/messages/sync-message-${lsn}`,
    },
  }))
  const server = await startSyncPullServer(port, events)
  const client = new NextDbClient({
    endpoint,
    userId: "cache-user",
    cache,
  })
  const changes = []
  client.onCacheChange((change) => changes.push(change))

  try {
    const response = await client.syncPull({ rooms: [roomId], limit: 10 })

    assert.equal(response.events.length, 3)
    assert.equal(cache.putRoomMessagesCalls.length, 1)
    assert.equal(cache.putRoomMessagesCalls[0].roomId, roomId)
    assert.deepEqual(cache.putRoomMessagesCalls[0].messages.map((row) => row.id), [
      "sync-message-1",
      "sync-message-2",
      "sync-message-3",
    ])
    assert.deepEqual((await cache.getRoomMessages(roomId, 3)).map((row) => row.id), [
      "sync-message-3",
      "sync-message-2",
      "sync-message-1",
    ])
    assert.equal(await cache.getRoomCursor(roomId), 3)
    assert.equal(await cache.getGlobalCursor(), 3)
    assert.deepEqual(cache.setRoomCursorCalls, [{ roomId, lsn: 3 }])
    assert.deepEqual(cache.setGlobalCursorCalls, [3])
    assert.deepEqual(
      changes
        .filter((change) => change.type === "messageUpserted")
        .map((change) => change.key),
      ["sync-message-1", "sync-message-2", "sync-message-3"],
    )
  } finally {
    await closeServer(server)
  }
}

async function testSyncPullBatchesConsecutiveRecordUpserts() {
  const cache = new CountingRoomMessageCache()
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const events = [1, 2, 3].map((lsn) => ({
    type: "recordUpserted",
    table: "rooms",
    key: `sync-room-${lsn}`,
    record: {
      table: "rooms",
      key: `sync-room-${lsn}`,
      value: { id: `sync-room-${lsn}`, title: `Sync Room ${lsn}` },
      updatedAtMs: 2000 + lsn,
      lsn,
      path: `tables/rooms/sync-room-${lsn}`,
    },
  }))
  const server = await startSyncPullServer(port, events)
  const client = new NextDbClient({
    endpoint,
    userId: "cache-user",
    cache,
  })
  const changes = []
  const tableEvents = []
  client.onCacheChange((change) => changes.push(change))
  const unsubscribe = client.subscribeTable("rooms", (event) => tableEvents.push(event))

  try {
    const response = await client.syncPull({ tables: ["rooms"], limit: 10 })

    assert.equal(response.events.length, 3)
    assert.equal(cache.putRecordsCalls.length, 1)
    assert.deepEqual(cache.putRecordsCalls[0].map((row) => row.key), [
      "sync-room-1",
      "sync-room-2",
      "sync-room-3",
    ])
    assert.deepEqual((await cache.listRecords("rooms", 3)).map((row) => row.key), [
      "sync-room-1",
      "sync-room-2",
      "sync-room-3",
    ])
    assert.equal(await cache.getTableCursor("rooms"), 3)
    assert.equal(await cache.getGlobalCursor(), 3)
    assert.deepEqual(cache.setTableCursorCalls, [{ table: "rooms", lsn: 3 }])
    assert.deepEqual(cache.setGlobalCursorCalls, [3])
    assert.deepEqual(
      changes
        .filter((change) => change.type === "recordUpserted")
        .map((change) => change.key),
      ["sync-room-1", "sync-room-2", "sync-room-3"],
    )
    assert.deepEqual(tableEvents.map((event) => event.key), [
      "sync-room-1",
      "sync-room-2",
      "sync-room-3",
    ])
  } finally {
    unsubscribe()
    await closeServer(server)
  }
}

async function testSyncPullBatchesNestedRecordUpsertCursors() {
  const cache = new CountingRoomMessageCache()
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const parentKey = "sync-room"
  const events = [1, 2, 3].map((lsn) => {
    const record = nestedRecordForParent(parentKey, `sync-message-${lsn}`, "cache-user", lsn)
    return {
      type: "recordUpserted",
      table: record.table,
      key: record.key,
      record,
    }
  })
  const server = await startSyncPullServer(port, events)
  const client = new NextDbClient({
    endpoint,
    userId: "cache-user",
    cache,
  })

  try {
    const response = await client.syncPull({
      nestedTables: [{ table: "rooms", parentKey, nested: "messages" }],
      limit: 10,
    })

    assert.equal(response.events.length, 3)
    assert.equal(cache.putRecordsCalls.length, 1)
    assert.deepEqual(cache.putRecordsCalls[0].map((row) => row.key), [
      "sync-room:sync-message-1",
      "sync-room:sync-message-2",
      "sync-room:sync-message-3",
    ])
    assert.deepEqual((await cache.listRecords("rooms.messages", 3)).map((row) => row.key), [
      "sync-room:sync-message-1",
      "sync-room:sync-message-2",
      "sync-room:sync-message-3",
    ])
    assert.equal(await cache.getNestedTableCursor("rooms", parentKey, "messages"), 3)
    assert.equal(await cache.getGlobalCursor(), 3)
    assert.deepEqual(cache.setNestedTableCursorCalls, [{ table: "rooms", parentKey, nested: "messages", lsn: 3 }])
    assert.deepEqual(cache.setTableCursorCalls, [])
    assert.deepEqual(cache.setGlobalCursorCalls, [3])
  } finally {
    await closeServer(server)
  }
}

async function testSyncPullBatchesUserEventCacheWrites() {
  const cache = new CountingRoomMessageCache()
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const userId = "sync-user"
  const events = [1, 2, 3].map((index) => ({
    type: "userEvent",
    userId,
    event: {
      id: `sync-user-event-${index}`,
      userId,
      name: "notification.created",
      payload: { index },
      createdAtMs: 3000 + index,
      lsn: index,
      path: `users/${userId}/events/sync-user-event-${index}`,
    },
  }))
  const server = await startSyncPullServer(port, events)
  const client = new NextDbClient({
    endpoint,
    userId,
    cache,
  })
  const changes = []
  const userEvents = []
  client.onCacheChange((change) => changes.push(change))
  const unsubscribe = client.onUserEvent((event) => userEvents.push(event))

  try {
    const response = await client.syncPull({ users: [userId], limit: 10 })

    assert.equal(response.events.length, 3)
    assert.equal(cache.putUserEventsCalls.length, 1)
    assert.equal(cache.putUserEventsCalls[0].userId, userId)
    assert.deepEqual(cache.putUserEventsCalls[0].events.map((row) => row.id), [
      "sync-user-event-1",
      "sync-user-event-2",
      "sync-user-event-3",
    ])
    assert.deepEqual((await cache.getUserEvents(userId, 3)).map((row) => row.id), [
      "sync-user-event-3",
      "sync-user-event-2",
      "sync-user-event-1",
    ])
    assert.equal(await cache.getUserCursor(userId), 3)
    assert.deepEqual(cache.setUserCursorCalls, [{ userId, lsn: 3 }])
    assert.deepEqual(
      changes
        .filter((change) => change.type === "userEventUpserted")
        .map((change) => change.key),
      ["sync-user-event-1", "sync-user-event-2", "sync-user-event-3"],
    )
    assert.deepEqual(userEvents.map((event) => event.event.id), [
      "sync-user-event-1",
      "sync-user-event-2",
      "sync-user-event-3",
    ])
  } finally {
    unsubscribe()
    await closeServer(server)
  }
}

async function testClientObjectBodyRangeUsesCachedBody() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "cache-user",
    cache,
  })
  await cache.setMetadata(metadata)
  await cache.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))

  const range = await client.getObjectBodyRange("object-1", { start: 6, end: 10 })
  assert.equal(range.contentRange, "bytes 6-10/11")
  assert.equal(range.start, 6)
  assert.equal(range.end, 10)
  assert.equal(range.byteSize, 11)
  assert.equal(range.contentType, "text/plain")
  assert.equal(await range.body.text(), "cache")

  const openEnded = await client.getObjectBodyRange("object-1", { start: 6 })
  assert.equal(openEnded.contentRange, "bytes 6-10/11")
  assert.equal(await openEnded.body.text(), "cache")

  const suffix = await client.objectStore("Object").getBodyRange("object-1", { suffixLength: 5 })
  assert.equal(suffix.contentRange, "bytes 6-10/11")
  assert.equal(await suffix.body.text(), "cache")

  client.close()
}

async function testClientObjectBodyRangeUsesCachedPartialRange() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "cache-user",
    cache,
  })
  await cache.setMetadata(metadata)
  await cache.putObject(objectMetadata)
  await cache.putObjectBodyRange(objectMetadata, {
    body: new Blob(["hello cache"], { type: "text/plain" }),
    contentRange: "bytes 0-10/11",
    start: 0,
    end: 10,
    byteSize: 11,
    contentType: "text/plain",
  })
  let stats = await cache.stats()
  assert.equal(stats.totalObjectCachedBytes, 11)
  assert.equal(stats.totalObjectRangeChunks, 1)

  const range = await client.getObjectBodyRange("object-1", { start: 6, end: 10 })
  assert.equal(range.contentRange, "bytes 6-10/11")
  assert.equal(range.start, 6)
  assert.equal(range.end, 10)
  assert.equal(range.byteSize, 11)
  assert.equal(range.contentType, "text/plain")
  assert.equal(await range.body.text(), "cache")
  assert.equal(await client.getCachedObjectBody("object-1"), undefined)

  await cache.putObject({ ...objectMetadata, sha256: "object-sha-replaced" })
  stats = await cache.stats()
  assert.equal(stats.totalObjectCachedBytes, 0)
  assert.equal(stats.totalObjectRangeChunks, 0)
  await assert.rejects(
    () => client.getObjectBodyRange("object-1", { start: 6, end: 10 }),
    /fetch failed|bad port|ECONNREFUSED|NextDB object range request failed/,
  )

  client.close()
}

async function testMemoryObjectBodyInvalidatesWhenMetadataChanges() {
  const cache = new MemoryLocalCache()
  await cache.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))
  await cache.putObject({ ...objectMetadata, sha256: "object-sha-replaced" })

  const stats = await cache.stats()
  assert.equal(await cache.getObjectBody("object-1"), undefined)
  assert.equal(stats.totalObjectCachedBytes, 0)
}

async function testMemoryUserProfiles() {
  const cache = new MemoryLocalCache()
  await cache.putUserProfile(userProfile)

  assert.deepEqual(await cache.getUserProfile("cache-user"), userProfile)
  assert.deepEqual((await cache.listUserProfiles(10)).map((row) => row.userId), ["cache-user"])
  assert.equal((await cache.stats()).totalUserProfiles, 1)
  assert.equal(await cache.deleteUserProfile("cache-user"), true)
  assert.equal(await cache.getUserProfile("cache-user"), undefined)
}

async function testMemoryTrimObjectsKeepsNewestWithinLimits() {
  const cache = new MemoryLocalCache()
  await cache.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))
  await cache.putObject(laterObjectMetadata, new Blob(["x".repeat(15)], { type: "text/plain" }))
  await cache.putObject(newestObjectMetadata, new Blob(["x".repeat(20)], { type: "text/plain" }))

  const removed = await cache.trimObjects(2, 25)

  assert.equal(removed, 2)
  assert.deepEqual((await cache.listObjects(10)).map((object) => object.id), ["object-3"])
  assert.equal(await cache.getObjectBody("object-1"), undefined)
  assert.equal(await cache.getObjectBody("object-2"), undefined)
  assert.equal((await cache.getObjectBody("object-3")).size, 20)
}

async function testMemoryTrimObjectsUsesActualCachedBytes() {
  const cache = new MemoryLocalCache()
  await seedPartialObjectRanges(cache)

  const removed = await cache.trimObjects(0, 22)
  const stats = await cache.stats()

  assert.equal(removed, 1)
  assert.deepEqual((await cache.listObjects(10)).map((object) => object.id), ["object-2", "object-3"])
  assert.equal(stats.totalObjectBytes, 35)
  assert.equal(stats.totalObjectCachedBytes, 22)
  assert.equal(stats.totalObjectRangeChunks, 2)
}

async function testMemoryOfflineObjectWritesQueue() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })

  const metadata = await client.putObject("offline object", {
    contentType: "text/plain",
    objectId: "offline-object-1",
    clientMutationId: "offline-object-put-1",
  })

  assert.equal(metadata.id, "offline-object-1")
  assert.equal(metadata.byteSize, 14)
  assert.equal((await cache.getObjectBody("offline-object-1")).size, 14)
  let writes = await cache.listPendingWrites()
  assert.equal(writes.length, 1)
  assert.equal(writes[0].type, "objectPut")
  assert.equal(writes[0].objectId, "offline-object-1")
  assert.equal(writes[0].body.size, 14)

  const deleted = await client.deleteObject("offline-object-1", {
    clientMutationId: "offline-object-delete-1",
  })

  assert.equal(deleted.deleted, true)
  assert.equal(deleted.lsn, 0)
  assert.equal(await cache.getObjectMetadata("offline-object-1"), undefined)
  writes = await cache.listPendingWrites()
  assert.deepEqual(writes.map((write) => write.type), ["objectPut", "objectDelete"])
  const stats = await client.pendingWriteStats()
  assert.equal(stats.byType.objectPut, 1)
  assert.equal(stats.byType.objectDelete, 1)
}

async function testMemoryOfflineUserWritesFlush() {
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint,
    userId: "offline-user",
    cache,
    offlineWrites: true,
  })

  const profile = await client.upsertUser("offline-user", {
    displayName: "Offline User",
    metadata: { mode: "offline" },
    clientMutationId: "offline-user-profile-1",
  })
  assert.equal(profile.userId, "offline-user")
  assert.equal(profile.displayName, "Offline User")
  assert.equal(profile.lsn, 0)
  assert.equal((await cache.getUserProfile("offline-user")).lsn, 0)

  const event = await client.publishUserEvent("offline-user", "notification.created", { text: "queued" }, {
    clientMutationId: "offline-user-event-1",
  })
  assert.equal(event.userId, "offline-user")
  assert.equal(event.name, "notification.created")
  assert.equal(event.lsn, 0)
  assert.deepEqual(await cache.getUserEvents("offline-user", 10), [])

  let writes = await cache.listPendingWrites()
  assert.deepEqual(writes.map((write) => write.type), ["userProfileUpsert", "userEvent"])
  const stats = await client.pendingWriteStats()
  assert.equal(stats.byType.userProfileUpsert, 1)
  assert.equal(stats.byType.userEvent, 1)

  const requests = []
  const server = await startUserWriteServer(port, requests)
  try {
    const flushed = await client.flushPendingWrites()

    assert.equal(flushed.committed, 2)
    assert.equal(flushed.remaining, 0)
    assert.deepEqual(flushed.errors, [])
    assert.deepEqual(requests.map((request) => request.type), ["userProfileUpsert", "userEvent"])
    assert.equal(requests[0].body.clientMutationId, "offline-user-profile-1")
    assert.equal(requests[1].body.clientMutationId, "offline-user-event-1")

    writes = await cache.listPendingWrites()
    assert.deepEqual(writes, [])
    assert.equal((await cache.getUserProfile("offline-user")).lsn, 101)
    const events = await cache.getUserEvents("offline-user", 10)
    assert.equal(events.length, 1)
    assert.equal(events[0].payload.text, "queued")
    assert.equal(events[0].lsn, 102)
  } finally {
    client.close()
    await closeServer(server)
  }
}

async function testMemoryOfflineRecordTransactionFlush() {
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint,
    cache,
    offlineWrites: true,
  })
  await cache.putRecords([{
    table: "rooms",
    key: "offline-transaction-delete",
    value: { id: "offline-transaction-delete", title: "Delete Me" },
    updatedAtMs: 1,
    lsn: 1,
    path: "tables/rooms/offline-transaction-delete",
  }])

  const operations = [
    {
      type: "upsert",
      table: "rooms",
      key: "offline-transaction-upsert",
      value: { id: "offline-transaction-upsert", title: "Queued Transaction" },
    },
    {
      type: "delete",
      table: "rooms",
      key: "offline-transaction-delete",
      expectedLsn: 1,
    },
  ]
  const queued = await client.recordTransaction(operations, {
    clientMutationId: "offline-record-transaction-1",
  })

  assert.equal(queued.lsn, 0)
  assert.deepEqual(queued.operations, [])
  assert.equal(await cache.getRecord("rooms", "offline-transaction-upsert"), undefined)
  assert.notEqual(await cache.getRecord("rooms", "offline-transaction-delete"), undefined)
  let writes = await cache.listPendingWrites()
  assert.equal(writes.length, 1)
  assert.equal(writes[0].type, "recordTransaction")
  assert.equal(writes[0].clientMutationId, "offline-record-transaction-1")
  assert.equal((await client.pendingWriteStats()).byType.recordTransaction, 1)

  const requests = []
  const server = await startRecordTransactionServer(port, requests)
  try {
    const flushed = await client.flushPendingWrites()

    assert.equal(flushed.committed, 1)
    assert.equal(flushed.remaining, 0)
    assert.deepEqual(flushed.errors, [])
    assert.equal(requests.length, 1)
    assert.equal(requests[0].clientMutationId, "offline-record-transaction-1")
    assert.deepEqual(requests[0].operations.map((operation) => operation.type), ["upsert", "delete"])
    assert.equal((await cache.getRecord("rooms", "offline-transaction-upsert")).lsn, 201)
    assert.equal(await cache.getRecord("rooms", "offline-transaction-delete"), undefined)
    writes = await cache.listPendingWrites()
    assert.deepEqual(writes, [])
  } finally {
    client.close()
    await closeServer(server)
  }
}

async function testMemoryPendingWriteManagement() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const changes = []
  client.onCacheChange((change) => changes.push(change))

  await client.putObject("discard me", {
    contentType: "text/plain",
    objectId: "discard-object-memory",
    clientMutationId: "discard-object-memory-put",
  })
  await cache.setMetadata({
    clientId: "pending-memory",
    profileVersion: 1,
    schemaVersion: 1,
    maxPendingWrites: 1,
    maxPendingWriteBytes: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })

  let status = await client.pendingWriteQueueStatus()
  assert.equal(status.stats.total, 1)
  assert.equal(status.stats.byType.objectPut, 1)
  assert.equal(status.stats.objectPutBytes, 10)
  assert(status.stats.estimatedBytes > status.stats.objectPutBytes)
  assert.equal(status.stats.failed, 1)
  assert.equal(status.stats.totalAttempts, 0)
  assert.equal(status.stats.maxWrites, 1)
  assert.equal(status.stats.maxBytes, 1)
  assert.equal(status.stats.overMaxWrites, false)
  assert.equal(status.stats.overMaxBytes, true)
  assert.equal(status.stats.oldestCreatedAtMs, status.writes[0].createdAtMs)
  assert.equal(status.stats.newestCreatedAtMs, status.writes[0].createdAtMs)
  assert.equal(status.writes[0].type, "objectPut")
  assert.equal(status.autoFlush.enabled, false)
  assert.equal(status.autoFlush.inFlight, false)
  assert.equal(changes.find((change) => change.type === "pendingWriteQueued")?.write.type, "objectPut")
  assert.equal(changes.find((change) => change.type === "pendingWriteQueued")?.stats.total, 1)

  const pendingId = status.writes[0].id
  await cache.putPendingWrite({
    ...status.writes[0],
    attempts: 3,
    lastError: "previous failure",
  })
  status = await client.pendingWriteQueueStatus()
  assert.equal(status.stats.failed, 1)
  assert.equal(status.stats.totalAttempts, 3)
  const reset = await client.resetPendingWrite(pendingId)
  assert.equal(reset.reset, true)
  assert.equal(reset.write.attempts, 0)
  assert.equal(reset.write.lastError, undefined)
  assert.equal(changes.find((change) => change.type === "pendingWriteReset")?.write.id, pendingId)
  status = await client.pendingWriteQueueStatus()
  assert.equal(status.stats.failed, 0)
  assert.equal(status.stats.totalAttempts, 0)

  const discarded = await client.discardPendingWrite(pendingId, { removeOptimistic: true })
  assert.equal(discarded.discarded, true)
  assert.equal(discarded.removedOptimistic, true)
  assert.equal(await cache.getObjectMetadata("discard-object-memory"), undefined)
  assert.equal(await cache.getObjectBody("discard-object-memory"), undefined)
  assert.equal((await client.pendingWriteQueueStatus()).stats.total, 0)
  const discardedEvent = changes.find((change) => change.type === "pendingWriteDiscarded")
  assert.equal(discardedEvent?.write.id, pendingId)
  assert.equal(discardedEvent?.removedOptimistic, true)
  assert.equal(discardedEvent?.stats.total, 0)

  const missing = await client.discardPendingWrite(pendingId)
  assert.equal(missing.discarded, false)
}

async function testMemoryPendingWriteLimitAdmission() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "pending-limit-memory",
    profileVersion: 1,
    schemaVersion: 1,
    maxPendingWrites: 1,
    maxPendingWriteBytes: 0,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const changes = []
  client.onCacheChange((change) => changes.push(change))

  await client.putObject("first limited write", {
    contentType: "text/plain",
    objectId: "pending-limit-first",
    clientMutationId: "pending-limit-first",
  })
  try {
    await client.putObject("second limited write", {
      contentType: "text/plain",
      objectId: "pending-limit-second",
      clientMutationId: "pending-limit-second",
    })
    assert.fail("expected pending write count limit")
  } catch (error) {
    assert(error instanceof NextDbPendingWriteLimitError)
    assert.equal(error.limitKind, "maxPendingWrites")
    assert.equal(error.writeType, "objectPut")
    assert.equal(error.currentWrites, 1)
    assert.equal(error.nextWrites, 2)
    assert.equal(error.maxWrites, 1)
    assert(error.writeBytes > 0)
    assert(error.nextBytes > error.currentBytes)
  }

  const status = await client.pendingWriteQueueStatus()
  assert.equal(status.stats.total, 1)
  assert.equal(status.stats.overMaxWrites, false)
  assert.equal(await cache.getObjectMetadata("pending-limit-second"), undefined)
  assert.equal(await cache.getObjectBody("pending-limit-second"), undefined)
  const rejected = changes.find((change) => change.type === "pendingWriteRejected")
  assert.equal(rejected?.limit.limitKind, "maxPendingWrites")
  assert.equal(rejected?.limit.nextWrites, 2)
  assert.equal(rejected?.stats.total, 1)

  const bytesCache = new MemoryLocalCache()
  await bytesCache.setMetadata({
    clientId: "pending-byte-limit-memory",
    profileVersion: 1,
    schemaVersion: 1,
    maxPendingWrites: 0,
    maxPendingWriteBytes: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  const bytesClient = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache: bytesCache,
    offlineWrites: true,
  })
  const byteChanges = []
  bytesClient.onCacheChange((change) => byteChanges.push(change))
  try {
    await bytesClient.putObject("byte limited write", {
      contentType: "text/plain",
      objectId: "pending-byte-limit",
      clientMutationId: "pending-byte-limit",
    })
    assert.fail("expected pending write byte limit")
  } catch (error) {
    assert(error instanceof NextDbPendingWriteLimitError)
    assert.equal(error.limitKind, "maxPendingWriteBytes")
    assert.equal(error.writeType, "objectPut")
    assert.equal(error.currentWrites, 0)
    assert.equal(error.nextWrites, 1)
    assert.equal(error.maxBytes, 1)
    assert(error.writeBytes > error.maxBytes)
    assert.equal(error.nextBytes, error.writeBytes)
  }
  assert.equal((await bytesClient.pendingWriteQueueStatus()).stats.total, 0)
  assert.equal(await bytesCache.getObjectMetadata("pending-byte-limit"), undefined)
  assert.equal(await bytesCache.getObjectBody("pending-byte-limit"), undefined)
  const byteRejected = byteChanges.find((change) => change.type === "pendingWriteRejected")
  assert.equal(byteRejected?.limit.limitKind, "maxPendingWriteBytes")
  assert.equal(byteRejected?.stats.total, 0)
}

async function testMemoryWatchPendingWrites() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const snapshots = []
  const stop = client.watchPendingWrites((snapshot) => snapshots.push(snapshot), { limit: 10 })
  await waitUntil(() => snapshots.length === 1)
  assert.equal(snapshots[0].source, "cache")
  assert.equal(snapshots[0].queue.stats.total, 0)

  await client.putObject("watched object", {
    contentType: "text/plain",
    objectId: "watched-pending-object",
    clientMutationId: "watched-pending-object-put",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "pendingWriteQueued"))
  const queued = snapshots.find((snapshot) => snapshot.change?.type === "pendingWriteQueued")
  assert.equal(queued.queue.stats.total, 1)
  assert.equal(queued.queue.writes[0].type, "objectPut")

  const pendingId = queued.queue.writes[0].id
  await client.resetPendingWrite(pendingId)
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "pendingWriteReset"))
  const reset = snapshots.find((snapshot) => snapshot.change?.type === "pendingWriteReset")
  assert.equal(reset.queue.stats.total, 1)

  await client.discardPendingWrite(pendingId, { removeOptimistic: true })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "pendingWriteDiscarded"))
  const discarded = snapshots.find((snapshot) => snapshot.change?.type === "pendingWriteDiscarded")
  assert.equal(discarded.queue.stats.total, 0)

  stop()
  const countAfterStop = snapshots.length
  await client.putObject("unwatched object", {
    contentType: "text/plain",
    objectId: "unwatched-pending-object",
    clientMutationId: "unwatched-pending-object-put",
  })
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(snapshots.length, countAfterStop)
}

async function testMemoryWatchLocalDataStatus() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const snapshots = []
  const stop = client.watchLocalDataStatus((snapshot) => snapshots.push(snapshot), { limit: 10 })
  await waitUntil(() => snapshots.length === 1)
  assert.equal(snapshots[0].source, "cache")
  assert.equal(snapshots[0].status.pendingWrites.total, 0)
  assert.equal(snapshots[0].pendingQueue.stats.total, 0)

  await client.putObject("local status watched", {
    contentType: "text/plain",
    objectId: "local-status-watched-object",
    clientMutationId: "local-status-watched-object-put",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "pendingWriteQueued"))
  const queued = snapshots.find((snapshot) => snapshot.change?.type === "pendingWriteQueued")
  assert.equal(queued.status.pendingWrites.total, 1)
  assert.equal(queued.pendingQueue.stats.total, 1)
  assert.equal(queued.pendingQueue.writes[0].type, "objectPut")

  stop()
  const countAfterStop = snapshots.length
  await client.putObject("local status unwatched", {
    contentType: "text/plain",
    objectId: "local-status-unwatched-object",
    clientMutationId: "local-status-unwatched-object-put",
  })
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(snapshots.length, countAfterStop)
}

async function testMemoryWatchRecordDetail() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const snapshots = []
  const stop = client.table("rooms").watch("watched-room", (snapshot) => snapshots.push(snapshot))
  await waitUntil(() => snapshots.length === 1)
  assert.equal(snapshots[0].source, "cache")
  assert.equal(snapshots[0].table, "rooms")
  assert.equal(snapshots[0].key, "watched-room")
  assert.equal(snapshots[0].record, undefined)

  await client.upsertRecord("rooms", "watched-room", {
    id: "watched-room",
    title: "Watched Room",
  }, {
    clientMutationId: "watched-room-upsert",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "recordUpserted"))
  const upserted = snapshots.find((snapshot) => snapshot.change?.type === "recordUpserted")
  assert.equal(upserted.record.key, "watched-room")
  assert.equal(upserted.record.value.title, "Watched Room")

  await client.deleteRecord("rooms", "watched-room", {
    clientMutationId: "watched-room-delete",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "recordDeleted"))
  const deleted = snapshots.find((snapshot) => snapshot.change?.type === "recordDeleted")
  assert.equal(deleted.record, undefined)

  stop()
  const countAfterStop = snapshots.length
  await client.upsertRecord("rooms", "watched-room", {
    id: "watched-room",
    title: "Unwatched Room",
  }, {
    clientMutationId: "watched-room-unwatched-upsert",
  })
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(snapshots.length, countAfterStop)
}

async function testMemoryWatchNestedRecordDetail() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const snapshots = []
  const messages = client.nestedTable("rooms", "watched-parent", "messages")
  const stop = messages.watch("watched-message", (snapshot) => snapshots.push(snapshot))
  await waitUntil(() => snapshots.length === 1)
  assert.equal(snapshots[0].source, "cache")
  assert.equal(snapshots[0].table, "rooms.messages")
  assert.equal(snapshots[0].key, "watched-parent:watched-message")
  assert.equal(snapshots[0].record, undefined)

  await messages.upsert("watched-message", {
    id: "watched-message",
    roomId: "watched-parent",
    senderId: "cache-user",
    body: "watched nested",
    attachments: [],
    createdAtMs: Date.now(),
    path: "tables/rooms/watched-parent/messages/watched-message",
  }, {
    clientMutationId: "watched-nested-upsert",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "recordUpserted"))
  const upserted = snapshots.find((snapshot) => snapshot.change?.type === "recordUpserted")
  assert.equal(upserted.record.key, "watched-parent:watched-message")
  assert.equal(upserted.record.value.body, "watched nested")

  await messages.delete("watched-message", {
    clientMutationId: "watched-nested-delete",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "recordDeleted"))
  const deleted = snapshots.find((snapshot) => snapshot.change?.type === "recordDeleted")
  assert.equal(deleted.record, undefined)

  stop()
  const countAfterStop = snapshots.length
  await messages.upsert("watched-message", {
    id: "watched-message",
    roomId: "watched-parent",
    senderId: "cache-user",
    body: "unwatched nested",
    attachments: [],
    createdAtMs: Date.now(),
    path: "tables/rooms/watched-parent/messages/watched-message",
  }, {
    clientMutationId: "watched-nested-unwatched-upsert",
  })
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(snapshots.length, countAfterStop)
}

async function testMemoryWatchObjectDetail() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })
  const snapshots = []
  const stop = client.objectStore("Object").watch(
    "watched-detail-object",
    (snapshot) => snapshots.push(snapshot),
    { includeBody: true },
  )
  await waitUntil(() => snapshots.length === 1)
  assert.equal(snapshots[0].source, "cache")
  assert.equal(snapshots[0].metadata, undefined)
  assert.equal(snapshots[0].cachedBodyAvailable, false)

  await client.putObject("watched detail body", {
    contentType: "text/plain",
    objectId: "watched-detail-object",
    clientMutationId: "watched-detail-object-put",
  })
  await waitUntil(() => snapshots.some((snapshot) => snapshot.change?.type === "objectUpserted"))
  const upserted = snapshots.find((snapshot) => snapshot.change?.type === "objectUpserted")
  assert.equal(upserted.metadata.id, "watched-detail-object")
  assert.equal(upserted.cachedBodyAvailable, true)
  assert.equal(await upserted.cachedBody.text(), "watched detail body")

  stop()
  const countAfterStop = snapshots.length
  await client.deleteObject("watched-detail-object", {
    clientMutationId: "watched-detail-object-delete",
  })
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(snapshots.length, countAfterStop)
}

async function testMemoryAutoFlushPendingObjectWrite() {
  const port = await reservePort()
  const endpoint = `http://127.0.0.1:${port}`
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint,
    cache,
    offlineWrites: true,
  })
  const changes = []
  client.onCacheChange((change) => changes.push(change))

  await client.putObject("auto flush object", {
    contentType: "text/plain",
    objectId: "auto-flush-object",
    clientMutationId: "auto-flush-object-put",
  })
  assert.equal((await cache.listPendingWrites()).length, 1)

  const requests = []
  const server = await startObjectPutServer(port, requests)
  try {
    client.startPendingWriteAutoFlush({
      intervalMs: 10,
      limit: 5,
      retryOnStart: true,
    })
    await waitUntil(async () => (await cache.listPendingWrites()).length === 0)

    assert.equal(requests.length, 1)
    assert.equal(requests[0].objectId, "auto-flush-object")
    assert.equal(requests[0].body, "auto flush object")
    assert.equal((await client.pendingWriteStats()).total, 0)
    assert.equal((await cache.getObjectMetadata("auto-flush-object")).sha256, "committed-auto-flush-sha")
    const committed = changes.find((change) => change.type === "pendingWriteCommitted")
    assert.equal(committed?.write.type, "objectPut")
    assert.equal(committed?.stats.total, 0)
  } finally {
    client.close()
    await closeServer(server)
  }
}

async function testMemoryCacheCoverage() {
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "coverage-user",
    cache,
  })
  await seedCacheCoverage(cache)
  await assertCacheCoverage(client)
}

async function testMemoryTrimUserEventsKeepsNewestEvents() {
  const cache = new MemoryLocalCache()
  await cache.putUserEvents("cache-user", [userEvent, laterUserEvent])

  const removed = await cache.trimUserEvents("cache-user", 1)

  assert.equal(removed, 1)
  assert.deepEqual((await cache.getUserEvents("cache-user", 10)).map((row) => row.id), ["event-2"])
}

async function testMemoryTrimTableKeepsNewestRecords() {
  const cache = new MemoryLocalCache()
  await cache.putRecords([
    { ...record, key: "old", lsn: 1, path: "tables/rooms/old" },
    { ...record, key: "new", lsn: 2, path: "tables/rooms/new" },
  ])

  const removed = await cache.trimTable("rooms", 1)

  assert.equal(removed, 1)
  assert.deepEqual((await cache.listRecords("rooms", 10)).map((row) => row.key), ["new"])
}

async function testMemoryTrimRecordsByKeyPrefixKeepsNewestPerParent() {
  const cache = new MemoryLocalCache()
  const order = [
    { field: "createdAtMs", direction: "desc" },
    { field: "id", direction: "asc" },
  ]
  await cache.putRecords([
    nestedRecordForParent("trim-room-a", "old", "cache-user", 1),
    nestedRecordForParent("trim-room-a", "new", "cache-user", 2),
    nestedRecordForParent("trim-room-b", "old", "cache-user", 3),
    nestedRecordForParent("trim-room-b", "new", "cache-user", 4),
  ])

  await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-a:", order, 10)
  const removed = await cache.trimRecordsByKeyPrefix("rooms.messages", "trim-room-a:", 1)
  const stats = await cache.stats()

  assert.equal(removed, 1)
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-a:", 10)).map((row) => row.value.id), ["new"])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-b:", 10)).map((row) => row.value.id), ["new", "old"])
  assert.deepEqual((await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-a:", order, 10)).records.map((row) => row.value.id), ["new"])
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-a:"], 1)
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-b:"], 2)
}

async function testMemoryTrimNestedTablePartitionsKeepsHottestParents() {
  const cache = new MemoryLocalCache()
  await cache.putRecords([
    nestedRecordForParent("trim-room-a", "old", "cache-user", 1),
    nestedRecordForParent("trim-room-a", "new", "cache-user", 2),
    nestedRecordForParent("trim-room-b", "old", "cache-user", 3),
    nestedRecordForParent("trim-room-b", "new", "cache-user", 4),
    nestedRecordForParent("trim-room-c", "old", "cache-user", 5),
    nestedRecordForParent("trim-room-c", "new", "cache-user", 6),
  ])
  await cache.setNestedTableCursor("rooms", "trim-room-a", "messages", 101)
  await cache.setNestedTableCursor("rooms", "trim-room-b", "messages", 102)
  await cache.setNestedTableCursor("rooms", "trim-room-c", "messages", 103)

  const removed = await cache.trimNestedTablePartitions("rooms.messages", 2, 1)
  const stats = await cache.stats()

  assert.equal(removed, 4)
  assert.deepEqual(await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-a:", 10), [])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-b:", 10)).map((row) => row.value.id), ["new"])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-c:", 10)).map((row) => row.value.id), ["new"])
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-a:"], undefined)
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-b:"], 1)
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-c:"], 1)
  assert.equal(await cache.getNestedTableCursor("rooms", "trim-room-a", "messages"), 0)
  assert.equal(await cache.getNestedTableCursor("rooms", "trim-room-b", "messages"), 102)
  assert.equal(await cache.getNestedTableCursor("rooms", "trim-room-c", "messages"), 103)
}

async function testMemoryIndexRangeQueryPaginates() {
  const cache = new MemoryLocalCache()
  await cache.putRecords(nestedRecords)

  const first = await cache.queryRecordsByIndex("rooms.messages", {
    fields: ["createdAtMs"],
    lowerValues: [20],
    upperValues: [30],
    keyPrefix: "cache-room:",
    limit: 1,
  })

  assert.deepEqual(first.records.map((row) => row.key), ["cache-room:m3"])
  assert.equal(first.hasMore, true)
  assert.equal(typeof first.nextCursor, "string")

  const second = await cache.queryRecordsByIndex("rooms.messages", {
    fields: ["createdAtMs"],
    lowerValues: [20],
    upperValues: [30],
    keyPrefix: "cache-room:",
    afterCursor: first.nextCursor,
    limit: 1,
  })

  assert.deepEqual(second.records.map((row) => row.key), ["cache-room:m2"])
  assert.equal(second.hasMore, false)
  assert.equal(second.nextCursor, undefined)
}

async function testClientIndexRangeUsesLocalCache() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "range-cache-smoke",
    sessionId: "range-cache-smoke-session",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.putRecords([
    {
      table: "rooms",
      key: "range-sdk-1",
      value: { id: "range-sdk-1", title: "~sdk-range-001" },
      updatedAtMs: 1,
      lsn: 1,
      path: "tables/rooms/range-sdk-1",
    },
    {
      table: "rooms",
      key: "range-sdk-2",
      value: { id: "range-sdk-2", title: "~sdk-range-002" },
      updatedAtMs: 2,
      lsn: 2,
      path: "tables/rooms/range-sdk-2",
    },
  ])
  const client = new NextDbClient({ endpoint: "http://127.0.0.1:9", cache })
  client.getSchema = async () => ({
    version: 1,
    entities: {},
    objects: {},
    tables: {
      rooms: {
        fields: { id: { kind: "Id", entity: "Room" }, title: { kind: "Text" } },
        indexes: { byTitle: { fields: ["title"] } },
      },
    },
    behaviors: {},
    events: {},
  })

  const first = await client.table("rooms").index("byTitle", {
    lower: "~sdk-range-001",
    upper: "~sdk-range-002",
    limit: 1,
  })

  assert.deepEqual(first.records.map((row) => row.key), ["range-sdk-1"])
  assert.equal(first.hasMore, true)
  assert.equal(typeof first.nextCursor, "string")

  const second = await client.table("rooms").index("byTitle", {
    lower: "~sdk-range-001",
    upper: "~sdk-range-002",
    afterCursor: first.nextCursor,
    limit: 1,
  })

  assert.deepEqual(second.records.map((row) => row.key), ["range-sdk-2"])
  assert.equal(second.hasMore, false)
}

async function testIndexedDbPendingClearPreservesMetadata() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-pending-${Date.now()}`)
  await cache.setMetadata(metadata)
  await cache.putPendingWrite(pendingWrite)

  const removed = await cache.clearPendingWrites()

  assert.equal(removed, 1)
  assert.deepEqual(await cache.listPendingWrites(), [])
  assert.deepEqual(await cache.getMetadata(), metadata)
}

async function testIndexedDbPendingObjectBlobRehydrates() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-pending-object-${Date.now()}`)
  await cache.putPendingWrite({
    id: "pending-object-1",
    type: "objectPut",
    createdAtMs: 1,
    attempts: 0,
    objectId: "pending-object-idb",
    contentType: "text/plain",
    body: new Blob(["indexed object"], { type: "text/plain" }),
    clientMutationId: "pending-object-idb-mutation",
  })

  const writes = await cache.listPendingWrites()
  assert.equal(writes.length, 1)
  assert.equal(writes[0].type, "objectPut")
  assert.equal(writes[0].body.size, 14)
  assert.equal(await writes[0].body.text(), "indexed object")
}

async function testIndexedDbPendingWriteManagement() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-pending-manage-${Date.now()}`)
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    offlineWrites: true,
  })

  await client.putObject("indexed discard", {
    contentType: "text/plain",
    objectId: "discard-object-idb",
    clientMutationId: "discard-object-idb-put",
  })
  await cache.setMetadata({
    clientId: "pending-idb",
    profileVersion: 1,
    schemaVersion: 1,
    maxPendingWrites: 1,
    maxPendingWriteBytes: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })

  const status = await client.pendingWriteQueueStatus()
  assert.equal(status.stats.total, 1)
  assert.equal(status.stats.objectPutBytes, 15)
  assert(status.stats.estimatedBytes > status.stats.objectPutBytes)
  assert.equal(status.stats.failed, 1)
  assert.equal(status.stats.maxWrites, 1)
  assert.equal(status.stats.maxBytes, 1)
  assert.equal(status.stats.overMaxWrites, false)
  assert.equal(status.stats.overMaxBytes, true)
  assert.equal(status.writes[0].type, "objectPut")
  assert.equal((await cache.getObjectBody("discard-object-idb")).size, 15)

  const pendingId = status.writes[0].id
  const reset = await client.resetPendingWrite(pendingId)
  assert.equal(reset.reset, true)
  assert.equal(reset.write.attempts, 0)

  const discarded = await client.discardPendingWrite(pendingId, { removeOptimistic: true })
  assert.equal(discarded.discarded, true)
  assert.equal(discarded.removedOptimistic, true)
  assert.deepEqual(await cache.listPendingWrites(), [])
  assert.equal(await cache.getObjectMetadata("discard-object-idb"), undefined)
  assert.equal(await cache.getObjectBody("discard-object-idb"), undefined)
}

async function testIndexedDbCacheCoverage() {
  const dbName = `nextdb-cache-smoke-coverage-${Date.now()}`
  const writer = new IndexedDbLocalCache(dbName)
  await seedCacheCoverage(writer)
  const reader = new IndexedDbLocalCache(dbName)
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "coverage-user",
    cache: reader,
  })
  await assertCacheCoverage(client)
}

async function testIndexedDbClearUserEventsClearsCursor() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-user-clear-${Date.now()}`)
  await cache.setUserCursor("cache-user", 97)
  await cache.putUserEvents("cache-user", [userEvent, laterUserEvent])

  const removed = await cache.clearUserEvents("cache-user")

  assert.equal(removed, 2)
  assert.equal(await cache.getUserCursor("cache-user"), 0)
  assert.deepEqual(await cache.getUserEvents("cache-user", 10), [])
}

async function testIndexedDbUserProfiles() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-user-profile-${Date.now()}`)
  await cache.putUserProfile(userProfile)

  assert.deepEqual(await cache.getUserProfile("cache-user"), userProfile)
  assert.deepEqual((await cache.listUserProfiles(10)).map((row) => row.userId), ["cache-user"])
  assert.equal((await cache.stats()).totalUserProfiles, 1)
  assert.equal(await cache.deleteUserProfile("cache-user"), true)
  assert.equal(await cache.getUserProfile("cache-user"), undefined)
}

async function testIndexedDbTrimObjectsKeepsNewestWithinLimits() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-object-trim-${Date.now()}`)
  await cache.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))
  await cache.putObject(laterObjectMetadata, new Blob(["x".repeat(15)], { type: "text/plain" }))
  await cache.putObject(newestObjectMetadata, new Blob(["x".repeat(20)], { type: "text/plain" }))

  const removed = await cache.trimObjects(2, 25)

  assert.equal(removed, 2)
  assert.deepEqual((await cache.listObjects(10)).map((object) => object.id), ["object-3"])
  assert.equal(await cache.getObjectBody("object-1"), undefined)
  assert.equal(await cache.getObjectBody("object-2"), undefined)
  assert.equal((await cache.getObjectBody("object-3")).size, 20)
}

async function testIndexedDbTrimObjectsUsesActualCachedBytes() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-object-range-trim-${Date.now()}`)
  await seedPartialObjectRanges(cache)

  const removed = await cache.trimObjects(0, 22)
  const stats = await cache.stats()

  assert.equal(removed, 1)
  assert.deepEqual((await cache.listObjects(10)).map((object) => object.id), ["object-2", "object-3"])
  assert.equal(stats.totalObjectBytes, 35)
  assert.equal(stats.totalObjectCachedBytes, 22)
  assert.equal(stats.totalObjectRangeChunks, 2)
}

async function testIndexedDbTrimUserEventsKeepsNewestEvents() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-user-trim-${Date.now()}`)
  await cache.putUserEvents("cache-user", [userEvent, laterUserEvent])

  const removed = await cache.trimUserEvents("cache-user", 1)

  assert.equal(removed, 1)
  assert.deepEqual((await cache.getUserEvents("cache-user", 10)).map((row) => row.id), ["event-2"])
}

async function testIndexedDbTrimTableKeepsNewestRecords() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-table-trim-${Date.now()}`)
  await cache.putRecords([
    { ...record, key: "old", lsn: 1, path: "tables/rooms/old" },
    { ...record, key: "new", lsn: 2, path: "tables/rooms/new" },
  ])

  const removed = await cache.trimTable("rooms", 1)

  assert.equal(removed, 1)
  assert.deepEqual((await cache.listRecords("rooms", 10)).map((row) => row.key), ["new"])
}

async function testIndexedDbTrimRecordsByKeyPrefixKeepsNewestPerParent() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-prefix-trim-${Date.now()}`)
  const order = [
    { field: "createdAtMs", direction: "desc" },
    { field: "id", direction: "asc" },
  ]
  await cache.putRecords([
    nestedRecordForParent("trim-room-a", "old", "cache-user", 1),
    nestedRecordForParent("trim-room-a", "new", "cache-user", 2),
    nestedRecordForParent("trim-room-b", "old", "cache-user", 3),
    nestedRecordForParent("trim-room-b", "new", "cache-user", 4),
  ])

  await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-a:", order, 10)
  const removed = await cache.trimRecordsByKeyPrefix("rooms.messages", "trim-room-a:", 1)
  const stats = await cache.stats()

  assert.equal(removed, 1)
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-a:", 10)).map((row) => row.value.id), ["new"])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-b:", 10)).map((row) => row.value.id), ["new", "old"])
  assert.deepEqual((await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-a:", order, 10)).records.map((row) => row.value.id), ["new"])
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-a:"], 1)
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-b:"], 2)
}

async function testIndexedDbTrimNestedTablePartitionsKeepsHottestParents() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-partition-trim-${Date.now()}`)
  const order = [
    { field: "createdAtMs", direction: "desc" },
    { field: "id", direction: "asc" },
  ]
  await cache.putRecords([
    nestedRecordForParent("trim-room-a", "old", "cache-user", 1),
    nestedRecordForParent("trim-room-a", "new", "cache-user", 2),
    nestedRecordForParent("trim-room-b", "old", "cache-user", 3),
    nestedRecordForParent("trim-room-b", "new", "cache-user", 4),
    nestedRecordForParent("trim-room-c", "old", "cache-user", 5),
    nestedRecordForParent("trim-room-c", "new", "cache-user", 6),
  ])
  await cache.setNestedTableCursor("rooms", "trim-room-a", "messages", 101)
  await cache.setNestedTableCursor("rooms", "trim-room-b", "messages", 102)
  await cache.setNestedTableCursor("rooms", "trim-room-c", "messages", 103)
  await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-a:", order, 10)
  await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-b:", order, 10)
  await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-c:", order, 10)

  const removed = await cache.trimNestedTablePartitions("rooms.messages", 2, 1)
  const stats = await cache.stats()

  assert.equal(removed, 4)
  assert.deepEqual(await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-a:", 10), [])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-b:", 10)).map((row) => row.value.id), ["new"])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "trim-room-c:", 10)).map((row) => row.value.id), ["new"])
  assert.deepEqual((await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-a:", order, 10)).records, [])
  assert.deepEqual((await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-b:", order, 10)).records.map((row) => row.value.id), ["new"])
  assert.deepEqual((await cache.listRecordsBySchemaOrder("rooms.messages", "trim-room-c:", order, 10)).records.map((row) => row.value.id), ["new"])
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-a:"], undefined)
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-b:"], 1)
  assert.equal(stats.nestedTables["rooms.messages"]["trim-room-c:"], 1)
  assert.equal(await cache.getNestedTableCursor("rooms", "trim-room-a", "messages"), 0)
  assert.equal(await cache.getNestedTableCursor("rooms", "trim-room-b", "messages"), 102)
  assert.equal(await cache.getNestedTableCursor("rooms", "trim-room-c", "messages"), 103)
}

async function testIndexedDbClearRecordsByKeyPrefix() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-prefix-clear-${Date.now()}`)
  const order = [
    { field: "createdAtMs", direction: "desc" },
    { field: "id", direction: "asc" },
  ]
  await cache.putRecords([
    nestedRecordForParent("cache-room", "prefix-1", "cache-user", 10),
    nestedRecordForParent("cache-room", "prefix-2", "cache-user", 20),
    nestedRecordForParent("other-room", "prefix-3", "cache-user", 30),
  ])

  const schemaOrderBefore = await cache.listRecordsBySchemaOrder("rooms.messages", "cache-room:", order, 10)
  assert.deepEqual(schemaOrderBefore.records.map((row) => row.value.id), ["prefix-2", "prefix-1"])

  const removed = await cache.clearRecordsByKeyPrefix("rooms.messages", "cache-room:")

  assert.equal(removed, 2)
  assert.deepEqual(await cache.listRecordsByKeyPrefix("rooms.messages", "cache-room:", 10), [])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "other-room:", 10)).map((row) => row.value.id), ["prefix-3"])
  const schemaOrderAfter = await cache.listRecordsBySchemaOrder("rooms.messages", "cache-room:", order, 10)
  assert.deepEqual(schemaOrderAfter.records, [])
}

async function testIndexedDbIndexRangeQueryPaginates() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-index-range-${Date.now()}`)
  await cache.putRecords(nestedRecords)

  const first = await cache.queryRecordsByIndex("rooms.messages", {
    fields: ["createdAtMs"],
    lowerValues: [20],
    upperValues: [30],
    keyPrefix: "cache-room:",
    limit: 1,
  })

  assert.deepEqual(first.records.map((row) => row.key), ["cache-room:m3"])
  assert.equal(first.hasMore, true)
  assert.equal(typeof first.nextCursor, "string")

  const second = await cache.queryRecordsByIndex("rooms.messages", {
    fields: ["createdAtMs"],
    lowerValues: [20],
    upperValues: [30],
    keyPrefix: "cache-room:",
    afterCursor: first.nextCursor,
    limit: 1,
  })

  assert.deepEqual(second.records.map((row) => row.key), ["cache-room:m2"])
  assert.equal(second.hasMore, false)
  assert.equal(second.nextCursor, undefined)
}

async function testIndexedDbRehydratesAcrossInstances() {
  const dbName = `nextdb-cache-smoke-rehydrate-${Date.now()}`
  const writer = new IndexedDbLocalCache(dbName)
  const order = [
    { field: "createdAtMs", direction: "desc" },
    { field: "id", direction: "asc" },
  ]

  await writer.setMetadata(metadata)
  await writer.setGlobalCursor(99)
  await writer.setObjectCursor(100)
  await writer.setRoomCursor("cache-room", 98)
  await writer.setUserCursor("cache-user", 97)
  await writer.setTableCursor("rooms.messages", 96)
  await writer.putPendingWrite(pendingWrite)
  await writer.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))
  await writer.putRoomMessages("cache-room", [message, laterMessage])
  await writer.putUserEvents("cache-user", [userEvent, laterUserEvent])
  await writer.putUserProfile(userProfile)
  await writer.putRecords(nestedRecords)
  assert.deepEqual(
    (await writer.listRecordsBySchemaOrder("rooms.messages", "cache-room:", order, 10)).records.map((row) => row.key),
    ["cache-room:m2", "cache-room:m3", "cache-room:m1"],
  )

  const reader = new IndexedDbLocalCache(dbName)
  assert.deepEqual(await reader.getMetadata(), metadata)
  assert.equal(await reader.getGlobalCursor(), 99)
  assert.equal(await reader.getObjectCursor(), 100)
  assert.equal(await reader.getRoomCursor("cache-room"), 98)
  assert.equal(await reader.getUserCursor("cache-user"), 97)
  assert.equal(await reader.getTableCursor("rooms.messages"), 96)
  assert.deepEqual((await reader.getRoomMessages("cache-room", 2)).map((row) => row.id), ["msg-2", "msg-1"])
  assert.deepEqual((await reader.getUserEvents("cache-user", 2)).map((row) => row.id), ["event-2", "event-1"])
  assert.deepEqual((await reader.getUserEvents("cache-user", 2, 14)).map((row) => row.id), ["event-1"])
  assert.deepEqual(await reader.getUserProfile("cache-user"), userProfile)
  assert.equal((await reader.getObjectBody("object-1")).size, 11)
  const stats = await reader.stats()
  assert.equal(stats.totalUserEvents, 2)
  assert.equal(stats.totalUserProfiles, 1)
  assert.equal(stats.users["cache-user"], 2)
  assert.deepEqual((await reader.listPendingWrites()).map((write) => write.id), ["pending-1"])
  assert.deepEqual((await reader.listRecordsByKeyPrefix("rooms.messages", "cache-room:", 10)).map((row) => row.key), [
    "cache-room:m1",
    "cache-room:m2",
    "cache-room:m3",
  ])
  assert.deepEqual(
    (await reader.queryRecordsByIndex("rooms.messages", {
      fields: ["senderId"],
      values: ["cache-user"],
      keyPrefix: "cache-room:",
      limit: 10,
    })).records.map((row) => row.key),
    ["cache-room:m1", "cache-room:m2"],
  )
  assert.deepEqual(
    (await reader.listRecordsBySchemaOrder("rooms.messages", "cache-room:", order, 10)).records.map((row) => row.key),
    ["cache-room:m2", "cache-room:m3", "cache-room:m1"],
  )

  await reader.putRecords([nestedRecord("m4", "cache-user", 40)])
  assert.deepEqual(
    (await new IndexedDbLocalCache(dbName).listRecordsBySchemaOrder("rooms.messages", "cache-room:", order, 10)).records.map((row) => row.key),
    ["cache-room:m4", "cache-room:m2", "cache-room:m3", "cache-room:m1"],
  )
}

async function testIndexedDbObjectBodyRangeRehydrates() {
  const dbName = `nextdb-cache-smoke-object-range-${Date.now()}`
  const writerCache = new IndexedDbLocalCache(dbName)
  await writerCache.setMetadata(metadata)
  await writerCache.putObject(objectMetadata)
  await writerCache.putObjectBodyRange(objectMetadata, {
    body: new Blob(["hello cache"], { type: "text/plain" }),
    contentRange: "bytes 0-10/11",
    start: 0,
    end: 10,
    byteSize: 11,
    contentType: "text/plain",
  })

  const readerCache = new IndexedDbLocalCache(dbName)
  const reader = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "cache-user",
    cache: readerCache,
  })
  const range = await reader.getObjectBodyRange("object-1", { start: 6, end: 10 })
  assert.equal(range.contentRange, "bytes 6-10/11")
  assert.equal(await range.body.text(), "cache")
  assert.equal(await reader.getCachedObjectBody("object-1"), undefined)
  reader.close()
}

async function testIndexedDbObjectBodyInvalidatesWhenMetadataChanges() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-object-body-invalidate-${Date.now()}`)
  await cache.putObject(objectMetadata, new Blob(["hello cache"], { type: "text/plain" }))
  await cache.putObject({ ...objectMetadata, sha256: "object-sha-replaced" })

  const stats = await cache.stats()
  assert.equal(await cache.getObjectBody("object-1"), undefined)
  assert.equal(stats.totalObjectCachedBytes, 0)
}

async function testIndexedDbNestedTableCursorRehydrates() {
  const dbName = `nextdb-cache-smoke-nested-cursor-${Date.now()}`
  const writer = new IndexedDbLocalCache(dbName)
  await writer.setNestedTableCursor("rooms", "cache-room", "messages", 123)
  await writer.setNestedTableCursor("rooms", "other-room", "messages", 456)

  const reader = new IndexedDbLocalCache(dbName)
  assert.equal(await reader.getNestedTableCursor("rooms", "cache-room", "messages"), 123)
  assert.equal(await reader.getNestedTableCursor("rooms", "other-room", "messages"), 456)

  await reader.clearTable("rooms.messages")
  assert.equal(await reader.getNestedTableCursor("rooms", "cache-room", "messages"), 0)
  assert.equal(await reader.getNestedTableCursor("rooms", "other-room", "messages"), 0)
}

async function testDefaultIndexedDbCacheScopesByEndpointUserNamespace() {
  const cacheNamespace = `nextdb-cache-smoke-scope-${Date.now()}`
  const endpoint = "http://127.0.0.1:9"
  const otherEndpoint = "http://127.0.0.1:8"
  const aliceWriter = new NextDbClient({
    endpoint,
    userId: "alice",
    cacheNamespace,
    offlineWrites: true,
  })

  await aliceWriter.putObject("scoped object", {
    contentType: "text/plain",
    objectId: "scoped-object-alice",
    clientMutationId: "scoped-object-alice-put",
  })

  const writerStatus = await aliceWriter.localDataStatus()
  assert.equal(writerStatus.cacheScope.kind, "indexedDb")
  assert.equal(writerStatus.cacheScope.namespace, cacheNamespace)
  assert.equal(writerStatus.cacheScope.userId, "alice")
  assert.match(writerStatus.cacheScope.name, /^nextdb-client-/)

  const aliceReader = new NextDbClient({
    endpoint,
    userId: "alice",
    cacheNamespace,
  })
  assert.equal((await aliceReader.pendingWriteQueueStatus()).stats.total, 1)
  const derivedCache = new IndexedDbLocalCache(writerStatus.cacheScope.name)
  assert.equal((await derivedCache.getObjectBody("scoped-object-alice")).size, 13)

  const bobReader = new NextDbClient({
    endpoint,
    userId: "bob",
    cacheNamespace,
  })
  assert.equal((await bobReader.pendingWriteQueueStatus()).stats.total, 0)
  assert.equal((await bobReader.cacheStats()).totalObjects, 0)

  const otherEndpointReader = new NextDbClient({
    endpoint: otherEndpoint,
    userId: "alice",
    cacheNamespace,
  })
  assert.equal((await otherEndpointReader.pendingWriteQueueStatus()).stats.total, 0)
  assert.equal((await otherEndpointReader.cacheStats()).totalObjects, 0)

  await aliceReader.clearCache()
  await bobReader.clearCache()
  await otherEndpointReader.clearCache()
}

async function testIndexedDbSubscriptionRegistryRehydrates() {
  const dbName = `nextdb-cache-smoke-subscriptions-${Date.now()}`
  const writer = new IndexedDbLocalCache(dbName)
  await writer.putSubscription({
    id: "table:rooms",
    kind: "table",
    table: "rooms",
    options: { catchUp: true, catchUpLimit: 50 },
    createdAtMs: 1,
    updatedAtMs: 2,
  })
  await writer.putSubscription({
    id: "query:rooms-latest",
    kind: "query",
    query: {
      type: "subscribeQuery",
      queryId: "rooms-latest",
      table: "rooms",
      limit: 10,
    },
    createdAtMs: 3,
    updatedAtMs: 4,
  })

  const reader = new IndexedDbLocalCache(dbName)
  const subscriptions = await reader.listSubscriptions()
  assert.deepEqual(subscriptions.map((subscription) => subscription.id), ["table:rooms", "query:rooms-latest"])
  assert.equal((await reader.stats()).subscriptions, 2)
  await reader.deleteSubscription("table:rooms")
  assert.deepEqual((await reader.listSubscriptions()).map((subscription) => subscription.id), ["query:rooms-latest"])
}

async function testIndexedDbClearAllClearsMetadataAndCursors() {
  const cache = new IndexedDbLocalCache(`nextdb-cache-smoke-clear-${Date.now()}`)
  await cache.setMetadata(metadata)
  await cache.setGlobalCursor(99)
  await cache.setObjectCursor(100)
  await cache.setRoomCursor("cache-room", 98)
  await cache.setUserCursor("cache-user", 97)
  await cache.setTableCursor("rooms", 96)
  await cache.putPendingWrite(pendingWrite)
  await cache.putRoomMessages("cache-room", [message])
  await cache.putUserEvents("cache-user", [userEvent])
  await cache.putRecords([record])

  const removed = await cache.clearAll()

  assert.equal(removed, 4)
  assert.equal(await cache.getGlobalCursor(), 0)
  assert.equal(await cache.getObjectCursor(), 0)
  assert.equal(await cache.getRoomCursor("cache-room"), 0)
  assert.equal(await cache.getUserCursor("cache-user"), 0)
  assert.equal(await cache.getTableCursor("rooms"), 0)
  assert.equal(await cache.getMetadata(), undefined)
  assert.deepEqual(await cache.getUserEvents("cache-user", 10), [])
  assert.deepEqual(await cache.listPendingWrites(), [])
}

async function seedCacheCoverage(cache) {
  await cache.setGlobalCursor(40)
  await cache.setObjectCursor(41)
  await cache.setRoomCursor("coverage-room", 42)
  await cache.setUserCursor("coverage-user", 43)
  await cache.setTableCursor("rooms", 44)
  await cache.setNestedTableCursor("rooms", "coverage-room", "messages", 45)
  await cache.putObject({
    ...objectMetadata,
    id: "coverage-object",
    path: "objects/coverage-object",
    byteSize: 17,
  })
  await cache.putRoomMessages("coverage-room", [{
    ...message,
    id: "coverage-message",
    roomId: "coverage-room",
    senderId: "coverage-user",
    lsn: 42,
    path: "rooms/coverage-room/messages/coverage-message",
  }])
  await cache.putUserEvents("coverage-user", [{
    ...userEvent,
    id: "coverage-event",
    userId: "coverage-user",
    lsn: 43,
    path: "users/coverage-user/events/coverage-event",
  }])
  await cache.putUserProfile({
    ...userProfile,
    userId: "coverage-user",
    lsn: 43,
    path: "users/coverage-user",
  })
  await cache.putRecords([
    {
      ...record,
      key: "coverage-room",
      value: { id: "coverage-room", title: "Coverage Room" },
      lsn: 44,
      path: "tables/rooms/coverage-room",
    },
    nestedRecordForParent("coverage-room", "coverage-nested-message", "coverage-user", 45),
  ])
  await cache.putPendingWrite({
    id: "coverage-pending-message",
    type: "sendMessage",
    createdAtMs: 1,
    attempts: 0,
    roomId: "coverage-room",
    userId: "coverage-user",
    body: "pending",
    attachments: [],
    durability: "strict",
    clientMutationId: "coverage-pending-message",
  })
  await cache.putPendingWrite({
    id: "coverage-pending-user",
    type: "userEvent",
    createdAtMs: 2,
    attempts: 0,
    userId: "coverage-user",
    name: "coverage.pending",
    payload: {},
    durability: "strict",
    clientMutationId: "coverage-pending-user",
  })
  await cache.putPendingWrite({
    id: "coverage-pending-record",
    type: "recordUpsert",
    createdAtMs: 3,
    attempts: 0,
    table: "rooms",
    key: "coverage-room",
    value: { id: "coverage-room", title: "Pending Coverage Room" },
    durability: "strict",
    clientMutationId: "coverage-pending-record",
  })
  await cache.putPendingWrite({
    id: "coverage-pending-nested",
    type: "nestedRecordDelete",
    createdAtMs: 4,
    attempts: 0,
    table: "rooms",
    parentKey: "coverage-room",
    nested: "messages",
    nestedKey: "coverage-nested-message",
    durability: "strict",
    clientMutationId: "coverage-pending-nested",
  })
  await cache.putPendingWrite({
    id: "coverage-pending-object",
    type: "objectPut",
    createdAtMs: 5,
    attempts: 0,
    objectId: "coverage-object",
    contentType: "text/plain",
    body: new Blob(["coverage pending"], { type: "text/plain" }),
    clientMutationId: "coverage-pending-object",
  })
  await cache.putSubscription({
    id: "room:coverage-room",
    kind: "room",
    roomId: "coverage-room",
    options: {},
    createdAtMs: 1,
    updatedAtMs: 1,
  })
  await cache.putSubscription({
    id: "userEvents:coverage-user",
    kind: "userEvents",
    userId: "coverage-user",
    options: {},
    createdAtMs: 2,
    updatedAtMs: 2,
  })
  await cache.putSubscription({
    id: "table:rooms",
    kind: "table",
    table: "rooms",
    options: {},
    createdAtMs: 3,
    updatedAtMs: 3,
  })
  await cache.putSubscription({
    id: "nested:rooms/coverage-room/messages",
    kind: "nestedTable",
    table: "rooms",
    parentKey: "coverage-room",
    nested: "messages",
    options: {},
    createdAtMs: 4,
    updatedAtMs: 4,
  })
  await cache.putSubscription({
    id: "objects",
    kind: "objects",
    options: {},
    createdAtMs: 5,
    updatedAtMs: 5,
  })
}

async function assertCacheCoverage(client) {
  const coverage = await client.cacheCoverage()
  assert.equal(coverage.globalCursor, 40)
  assert.deepEqual(coverage.objects, {
    objects: 1,
    byteSize: 17,
    cachedByteSize: 0,
    rangeChunks: 0,
    cursor: 41,
    pendingWrites: 1,
    activeSubscription: false,
    persistentSubscription: false,
    storedSubscription: true,
  })
  assert.deepEqual(coverage.rooms["coverage-room"], {
    messages: 1,
    cursor: 42,
    pendingWrites: 1,
    activeSubscription: false,
    persistentSubscription: false,
    storedSubscription: true,
  })
  assert.deepEqual(coverage.users["coverage-user"], {
    events: 1,
    profile: true,
    cursor: 43,
    pendingWrites: 1,
    activeSubscription: false,
    persistentSubscription: false,
    storedSubscription: true,
  })
  assert.deepEqual(coverage.tables.rooms, {
    records: 1,
    cursor: 44,
    pendingWrites: 1,
    activeSubscription: false,
    persistentSubscription: false,
    storedSubscription: true,
  })
  assert.deepEqual(coverage.nestedTables["rooms.messages"]["coverage-room"], {
    records: 1,
    cursor: 45,
    pendingWrites: 1,
    activeSubscription: false,
    persistentSubscription: false,
    storedSubscription: true,
  })
  assert.deepEqual(coverage.realtimeChannels, {})
  const status = await client.localDataStatus()
  assert.deepEqual(status.coverage, coverage)
}

function nestedRecord(id, senderId, createdAtMs) {
  return nestedRecordForParent("cache-room", id, senderId, createdAtMs)
}

function nestedRecordForParent(parentKey, id, senderId, createdAtMs) {
  const key = `${parentKey}:${id}`
  return {
    table: "rooms.messages",
    key,
    value: {
      id,
      roomId: parentKey,
      senderId,
      body: id,
      attachments: [],
      createdAtMs,
      path: `rooms/${parentKey}/messages/${id}`,
    },
    updatedAtMs: createdAtMs,
    lsn: createdAtMs,
    path: `tables/rooms/${parentKey}/messages/${id}`,
  }
}

async function reservePort() {
  const server = createServer()
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  assert.equal(typeof address, "object")
  const port = address.port
  await closeServer(server)
  return port
}

async function startObjectPutServer(port, requests) {
  const server = createServer((request, response) => {
    const url = new URL(request.url, `http://${request.headers.host}`)
    if (request.method !== "POST" || url.pathname !== "/v1/objects") {
      response.writeHead(404, { "content-type": "application/json" })
      response.end(JSON.stringify({ error: "not found" }))
      return
    }

    const chunks = []
    request.on("data", (chunk) => chunks.push(chunk))
    request.on("end", () => {
      const body = Buffer.concat(chunks)
      const objectId = url.searchParams.get("objectId")
      requests.push({
        objectId,
        contentType: url.searchParams.get("contentType"),
        clientMutationId: url.searchParams.get("clientMutationId"),
        body: body.toString("utf8"),
      })
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({
        id: objectId,
        path: `objects/${objectId}`,
        contentType: url.searchParams.get("contentType") ?? "application/octet-stream",
        byteSize: body.byteLength,
        sha256: "committed-auto-flush-sha",
        createdAtMs: Date.now(),
      }))
    })
  })
  await new Promise((resolve) => server.listen(port, "127.0.0.1", resolve))
  return server
}

async function startSyncPullServer(port, events) {
  const server = createServer((request, response) => {
    const url = new URL(request.url, `http://${request.headers.host}`)
    if (request.method === "GET" && url.pathname === "/v1/cache/profile") {
      const now = Date.now()
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({
        runtimeId: "cache-smoke-runtime",
        profile: {
          version: 1,
          leaseTtlMs: 60_000,
          maxObjects: 0,
          maxObjectBytes: 0,
          maxRoomMessages: 0,
          maxUserEvents: 0,
          maxRecordsPerTable: 0,
          maxNestedPartitions: 0,
          maxPendingWrites: 0,
          maxPendingWriteBytes: 0,
          offlineWrites: false,
        },
        lease: {
          clientId: url.searchParams.get("clientId") ?? "cache-smoke-client",
          sessionId: url.searchParams.get("sessionId") ?? undefined,
          issuedAtMs: now,
          expiresAtMs: now + 60_000,
          profileVersion: 1,
        },
        invalidations: [],
        currentLsn: 0,
        schemaVersion: 1,
        resetRequired: false,
      }))
      return
    }

    if (request.method === "GET" && url.pathname === "/v1/sync/pull") {
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({
        events,
        nextAfterLsn: 3,
        currentLsn: 3,
        hasMore: false,
      }))
      return
    }

    response.writeHead(404, { "content-type": "application/json" })
    response.end(JSON.stringify({ error: "not found" }))
  })
  await new Promise((resolve) => server.listen(port, "127.0.0.1", resolve))
  return server
}

async function startUserWriteServer(port, requests) {
  const server = createServer((request, response) => {
    const url = new URL(request.url, `http://${request.headers.host}`)
    if (request.method === "POST" && url.pathname === "/v1/users/offline-user") {
      readJsonRequest(request, (body) => {
        requests.push({ type: "userProfileUpsert", body })
        response.writeHead(200, { "content-type": "application/json" })
        response.end(JSON.stringify({
          user: {
            userId: "offline-user",
            displayName: body.displayName,
            metadata: body.metadata,
            createdAtMs: 1000,
            updatedAtMs: 1001,
            lsn: 101,
            path: "users/offline-user",
          },
        }))
      })
      return
    }

    if (request.method === "POST" && url.pathname === "/v1/mutate") {
      readJsonRequest(request, (body) => {
        requests.push({ type: "userEvent", body })
        response.writeHead(200, { "content-type": "application/json" })
        response.end(JSON.stringify({
          type: "userEventPublished",
          event: {
            id: "offline-user-event-committed",
            userId: body.userId,
            name: body.name,
            payload: body.payload,
            createdAtMs: 1002,
            lsn: 102,
            path: `users/${body.userId}/events/offline-user-event-committed`,
          },
        }))
      })
      return
    }

    response.writeHead(404, { "content-type": "application/json" })
    response.end(JSON.stringify({ error: "not found" }))
  })
  await new Promise((resolve) => server.listen(port, "127.0.0.1", resolve))
  return server
}

async function startRecordTransactionServer(port, requests) {
  const server = createServer((request, response) => {
    const url = new URL(request.url, `http://${request.headers.host}`)
    if (request.method === "POST" && url.pathname === "/v1/records/transaction") {
      readJsonRequest(request, (body) => {
        requests.push(body)
        response.writeHead(200, { "content-type": "application/json" })
        response.end(JSON.stringify({
          lsn: 202,
          operations: [
            {
              type: "recordUpserted",
              record: {
                table: "rooms",
                key: "offline-transaction-upsert",
                value: { id: "offline-transaction-upsert", title: "Queued Transaction" },
                updatedAtMs: 2000,
                lsn: 201,
                path: "tables/rooms/offline-transaction-upsert",
              },
            },
            {
              type: "recordDeleted",
              table: "rooms",
              key: "offline-transaction-delete",
              deletedAtMs: 2001,
              lsn: 202,
              path: "tables/rooms/offline-transaction-delete",
            },
          ],
        }))
      })
      return
    }

    response.writeHead(404, { "content-type": "application/json" })
    response.end(JSON.stringify({ error: "not found" }))
  })
  await new Promise((resolve) => server.listen(port, "127.0.0.1", resolve))
  return server
}

async function seedPartialObjectRanges(cache) {
  await cache.putObject(objectMetadata)
  await cache.putObject(laterObjectMetadata)
  await cache.putObject(newestObjectMetadata)
  await cache.putObjectBodyRange(objectMetadata, {
    body: new Blob(["a"], { type: "text/plain" }),
    contentRange: "bytes 0-0/11",
    start: 0,
    end: 0,
    byteSize: 11,
    contentType: "text/plain",
  })
  await cache.putObjectBodyRange(laterObjectMetadata, {
    body: new Blob(["bb"], { type: "text/plain" }),
    contentRange: "bytes 0-1/15",
    start: 0,
    end: 1,
    byteSize: 15,
    contentType: "text/plain",
  })
  await cache.putObjectBodyRange(newestObjectMetadata, {
    body: new Blob(["x".repeat(20)], { type: "text/plain" }),
    contentRange: "bytes 0-19/20",
    start: 0,
    end: 19,
    byteSize: 20,
    contentType: "text/plain",
  })
}

function readJsonRequest(request, onBody) {
  const chunks = []
  request.on("data", (chunk) => chunks.push(chunk))
  request.on("end", () => {
    onBody(JSON.parse(Buffer.concat(chunks).toString("utf8")))
  })
}

async function closeServer(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve())
  })
}

async function waitUntil(predicate, timeoutMs = 1000) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  assert.equal(await predicate(), true)
}
