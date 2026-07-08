import assert from "node:assert/strict"
import { createServer } from "node:http"

import {
  decodeRealtimeServerFrame,
  encodeRealtimeClientFrame,
  encodeRealtimeClientFrameJsonLine,
  JsonLineHttpRealtimeTransport,
  MemoryLocalCache,
  NextDbClient,
  RealtimeServerFrameJsonLineDecoder,
  jsonLineHttpRealtimeTransport,
  realtimeTransportCompatibility,
  WebTransportRealtimeTransport,
} from "../dist/index.js"

class MemoryRealtimeTransport {
  state = "connecting"
  sent = []
  openListener
  frameListener
  errorListener
  closeListener

  constructor(url) {
    this.url = url
  }

  send(frame) {
    this.sent.push(frame)
  }

  close() {
    this.state = "closed"
    this.closeListener?.()
  }

  onOpen(listener) {
    this.openListener = listener
  }

  onFrame(listener) {
    this.frameListener = listener
  }

  onError(listener) {
    this.errorListener = listener
  }

  onClose(listener) {
    this.closeListener = listener
  }

  async open() {
    this.state = "open"
    await this.openListener?.()
    await new Promise((resolve) => setTimeout(resolve, 0))
  }

  async frame(frame) {
    await this.frameListener?.(frame)
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
}

class CountingRoomMessageCache extends MemoryLocalCache {
  putRoomMessagesCalls = 0
  putRoomMessagesBatchSizes = []

  async putRoomMessages(roomId, messages) {
    this.putRoomMessagesCalls += 1
    this.putRoomMessagesBatchSizes.push(messages.length)
    return super.putRoomMessages(roomId, messages)
  }
}

const cache = new MemoryLocalCache()
await cache.setMetadata({
  clientId: "transport-smoke-client",
  profileVersion: 1,
  schemaVersion: 1,
  invalidationGeneration: 0,
  leaseExpiresAtMs: Date.now() + 60_000,
  lastValidatedAtMs: Date.now(),
})

const transports = []
const client = new NextDbClient({
  endpoint: "http://127.0.0.1:9",
  userId: "transport-user",
  schemaVersion: 7,
  cache,
  realtimeTransport: ({ url }) => {
    const transport = new MemoryRealtimeTransport(url)
    transports.push(transport)
    return transport
  },
})

const roomEvents = []
const tableEvents = []
const recordDetailSnapshots = []
const nestedWatchSnapshots = []
const nestedDetailSnapshots = []
const userEvents = []
const userProfileEvents = []
const objectEvents = []
const objectWatchSnapshots = []
const objectDetailSnapshots = []
const queryResults = []
const cacheEvents = []
const channelEvents = []
const gameInputs = []
const signals = []
const memberJoined = []
const memberLeft = []
client.onCacheChange((event) => cacheEvents.push(event))
const stopRoom = client.subscribeRoom("transport-room", (event) => roomEvents.push(event), { catchUp: false })
const stopTable = client.subscribeTable("rooms", (event) => tableEvents.push(event), { catchUp: false })
const stopRecordDetailWatch = client.table("rooms").watch("transport-room", (snapshot) => recordDetailSnapshots.push(snapshot), { catchUp: false })
const stopNestedWatch = client.nestedTable("rooms", "transport-room", "messages").watchList((snapshot) => nestedWatchSnapshots.push(snapshot), { catchUp: false })
const stopNestedDetailWatch = client.nestedTable("rooms", "transport-room", "messages").watch("transport-message-a", (snapshot) => nestedDetailSnapshots.push(snapshot), { catchUp: false })
const stopQuery = client.table("rooms").subscribeQuery((event) => queryResults.push(event), {
  queryId: "transport-query",
  indexName: "byTitle",
  value: "Transport Room",
  limit: 10,
  resultId: "sha256:client-known-query",
})
const stopObjects = client.subscribeObjects((event) => objectEvents.push(event), { catchUp: false })
const stopObjectWatch = client.objectStore("Object").watchList((snapshot) => objectWatchSnapshots.push(snapshot), { catchUp: false, limit: 10 })
const stopObjectDetailWatch = client.objectStore("Object").watch("transport-object-1", (snapshot) => objectDetailSnapshots.push(snapshot), { catchUp: false })
const stopUserEvents = client.onUserEvent((event) => {
  if (event.type === "userEvent") {
    userEvents.push(event)
  } else if (event.type === "userUpserted") {
    userProfileEvents.push(event)
  }
})
const channel = client.realtimeChannel("transport-channel")
const stopChannelEvents = channel.onEvent((event) => channelEvents.push(event))
const stopGameInputs = channel.onGameInput((event) => gameInputs.push(event))
const stopSignals = channel.onSignal((event) => signals.push(event))
const stopMemberJoined = channel.onMemberJoined((event) => memberJoined.push(event))
const stopMemberLeft = channel.onMemberLeft((event) => memberLeft.push(event))

assert.equal(transports.length, 1)
const transport = transports[0]
assert.equal(transport.url.pathname, "/v1/connect")
assert.equal(transport.url.searchParams.get("userId"), "transport-user")
assert.equal(transport.url.searchParams.get("schemaVersion"), "7")
assert.equal(transport.url.searchParams.get("transport"), "custom")
assert.deepEqual(transport.sent, [])

await transport.open()
await waitForSent(transport, (frame) => frame.type === "subscribeRoom" && frame.roomId === "transport-room")
await waitForSent(transport, (frame) => frame.type === "subscribeTable" && frame.table === "rooms")
await waitForSent(transport, (frame) =>
  frame.type === "subscribeNestedTable" &&
  frame.table === "rooms" &&
  frame.parentKey === "transport-room" &&
  frame.nested === "messages",
)
assert.equal(transport.sent.some((frame) => frame.type === "subscribeTable" && frame.table === "rooms.messages"), false)
await waitForSent(transport, (frame) =>
  frame.type === "subscribeQuery" &&
  frame.queryId === "transport-query" &&
  frame.table === "rooms" &&
  frame.indexName === "byTitle" &&
  frame.value === "Transport Room" &&
  frame.resultId === undefined,
)
await waitForSent(transport, (frame) => frame.type === "subscribeObjects")
await waitForSent(transport, (frame) => frame.type === "subscribeUserEvents")
assert.equal(transport.sent.filter((frame) => frame.type === "subscribeUserEvents").length, 1)
assert.equal(transport.sent.filter((frame) => frame.type === "subscribeObjects").length, 1)

const message = {
  id: "transport-message-1",
  roomId: "transport-room",
  senderId: "transport-user",
  body: "from memory transport",
  attachments: [],
  createdAtMs: Date.now(),
  lsn: 42,
  path: "rooms/transport-room/messages/transport-message-1",
}
await transport.frame({
  type: "event",
  event: {
    type: "messageCreated",
    roomId: "transport-room",
    message,
  },
})

assert.equal(roomEvents.length, 1)
assert.equal(roomEvents[0].message.id, message.id)
assert.deepEqual((await cache.getRoomMessages("transport-room", 10)).map((row) => row.id), [message.id])
assert.equal(cacheEvents.at(-1).type, "messageUpserted")
assert.equal(cacheEvents.at(-1).source, "realtime")

const record = {
  table: "rooms",
  key: "transport-room",
  value: { id: "transport-room", title: "Transport Room" },
  updatedAtMs: Date.now(),
  lsn: 43,
  path: "tables/rooms/transport-room",
}
await transport.frame({
  type: "event",
  event: {
    type: "recordUpserted",
    table: "rooms",
    key: "transport-room",
    record,
  },
})

assert.equal(tableEvents.length, 1)
assert.equal(tableEvents[0].record.key, record.key)
assert.equal((await cache.getRecord("rooms", "transport-room")).key, "transport-room")
assert.equal(cacheEvents.at(-1).type, "recordUpserted")
assert.equal(cacheEvents.at(-1).source, "realtime")
await waitUntil(() => recordDetailSnapshots.some((snapshot) =>
  snapshot.change?.type === "recordUpserted" &&
  snapshot.record?.key === record.key &&
  snapshot.record.value.title === "Transport Room"
))

await transport.frame({
  type: "queryResult",
  queryId: "transport-query",
  currentLsn: 43,
  resultId: "sha256:transport-query-1",
  response: {
    table: "rooms",
    records: [record],
    hasMore: false,
  },
})
assert.equal(queryResults.length, 1)
assert.equal(queryResults[0].queryId, "transport-query")
assert.equal(queryResults[0].resultId, "sha256:transport-query-1")
assert.deepEqual(queryResults[0].response.records.map((row) => row.key), ["transport-room"])
assert.equal((await cache.getRecord("rooms", "transport-room")).key, "transport-room")

await transport.frame({
  type: "queryUnchanged",
  queryId: "transport-query",
  currentLsn: 44,
  resultId: "sha256:transport-query-1",
})
assert.equal(queryResults.length, 1)

await transport.frame({
  type: "event",
  event: {
    type: "recordDeleted",
    table: "rooms",
    key: "transport-room",
    deletedAtMs: Date.now(),
    lsn: 45,
    path: "tables/rooms/transport-room",
  },
})
assert.equal(tableEvents.length, 2)
assert.equal(tableEvents[1].key, "transport-room")
assert.equal(await cache.getRecord("rooms", "transport-room"), undefined)
assert.equal(cacheEvents.at(-1).type, "recordDeleted")
assert.equal(cacheEvents.at(-1).source, "realtime")
await waitUntil(() => recordDetailSnapshots.some((snapshot) =>
  snapshot.change?.type === "recordDeleted" &&
  snapshot.record === undefined
))

const orphanQueryResults = []
const stopOrphanQuery = client.subscribeQuery({
  queryId: "transport-orphan-diff-query",
  table: "rooms",
  resultId: "sha256:orphan-client-known",
}, (event) => orphanQueryResults.push(event))
await waitForSent(transport, (frame) =>
  frame.type === "subscribeQuery" &&
  frame.queryId === "transport-orphan-diff-query" &&
  frame.resultId === undefined,
)
const beforeOrphanDiffSent = transport.sent.length
await transport.frame({
  type: "queryDiff",
  queryId: "transport-orphan-diff-query",
  currentLsn: 45,
  resultId: "sha256:orphan-diff",
  diff: {
    table: "rooms",
    added: [record],
    updated: [],
    removed: [],
    keys: [record.key],
    hasMore: false,
  },
})
await waitUntil(() => transport.sent.length > beforeOrphanDiffSent)
assert.equal(orphanQueryResults.length, 0)
assert(
  transport.sent
    .slice(beforeOrphanDiffSent)
    .some((frame) =>
      frame.type === "subscribeQuery" &&
      frame.queryId === "transport-orphan-diff-query" &&
      frame.resultId === undefined
    ),
)
stopOrphanQuery()

const objectMetadata = {
  id: "transport-object-1",
  path: "objects/transport-object-1",
  contentType: "text/plain",
  byteSize: 5,
  sha256: "transport-sha",
  createdAtMs: Date.now(),
}
await transport.frame({
  type: "event",
  event: {
    type: "objectCommitted",
    object: objectMetadata,
    lsn: 44,
  },
})
assert.equal(objectEvents.length, 1)
assert.equal(objectEvents[0].object.id, objectMetadata.id)
assert.deepEqual(await cache.getObjectMetadata(objectMetadata.id), objectMetadata)
assert.equal(cacheEvents.at(-1).type, "objectUpserted")
assert.equal(cacheEvents.at(-1).source, "realtime")
await waitUntil(() => objectWatchSnapshots.some((snapshot) =>
  snapshot.change?.type === "objectUpserted" &&
  snapshot.objects.some((object) => object.id === objectMetadata.id)
))
await waitUntil(() => objectDetailSnapshots.some((snapshot) =>
  snapshot.change?.type === "objectUpserted" &&
  snapshot.metadata?.id === objectMetadata.id &&
  snapshot.cachedBodyAvailable === false
))

await transport.frame({
  type: "event",
  event: {
    type: "objectDeleted",
    objectId: objectMetadata.id,
    deletedAtMs: Date.now(),
    lsn: 45,
    path: objectMetadata.path,
  },
})
assert.equal(objectEvents.length, 2)
assert.equal(objectEvents[1].objectId, objectMetadata.id)
assert.equal(await cache.getObjectMetadata(objectMetadata.id), undefined)
assert.equal(cacheEvents.at(-1).type, "objectDeleted")
assert.equal(cacheEvents.at(-1).source, "realtime")
await waitUntil(() => objectWatchSnapshots.some((snapshot) =>
  snapshot.change?.type === "objectDeleted" &&
  !snapshot.objects.some((object) => object.id === objectMetadata.id)
))
await waitUntil(() => objectDetailSnapshots.some((snapshot) =>
  snapshot.change?.type === "objectDeleted" &&
  snapshot.metadata === undefined &&
  snapshot.cachedBodyAvailable === false
))

const userEvent = {
  id: "transport-user-event-1",
  userId: "transport-user",
  name: "notification.created",
  payload: { text: "from memory transport" },
  createdAtMs: Date.now(),
  lsn: 46,
  path: "users/transport-user/events/transport-user-event-1",
}
await transport.frame({
  type: "event",
  event: {
    type: "userEvent",
    userId: "transport-user",
    event: userEvent,
  },
})
assert.equal(userEvents.length, 1)
assert.equal(userEvents[0].event.id, userEvent.id)
assert.deepEqual((await cache.getUserEvents("transport-user", 10)).map((row) => row.id), [userEvent.id])
assert.equal(cacheEvents.at(-1).type, "userEventUpserted")
assert.equal(cacheEvents.at(-1).source, "realtime")

const userProfile = {
  userId: "transport-user",
  displayName: "Transport User",
  metadata: { source: "transport-smoke" },
  createdAtMs: Date.now(),
  updatedAtMs: Date.now(),
  lsn: 47,
  path: "users/transport-user",
}
await transport.frame({
  type: "event",
  event: {
    type: "userUpserted",
    userId: "transport-user",
    user: userProfile,
  },
})
assert.equal(userProfileEvents.length, 1)
assert.equal(userProfileEvents[0].user.userId, userProfile.userId)
assert.deepEqual(await cache.getUserProfile("transport-user"), userProfile)
assert.equal(cacheEvents.at(-1).type, "userProfileUpserted")
assert.equal(cacheEvents.at(-1).source, "realtime")

const channelEvent = {
  channelId: "transport-channel",
  fromUserId: "transport-peer",
  kind: "gameInput",
  payload: { button: "jump" },
  sequence: 1,
  timestampMs: Date.now(),
}
await transport.frame({
  type: "event",
  event: {
    type: "volatileUserEvent",
    userId: "transport-user",
    name: "realtime.channel.event",
    payload: channelEvent,
  },
})
assert.equal(channelEvents.length, 1)
assert.deepEqual(channelEvents[0], channelEvent)
assert.equal(gameInputs.length, 1)
assert.deepEqual(gameInputs[0], channelEvent)

const nestedRecord = {
  table: "rooms.messages",
  key: "transport-room:transport-message-a",
  value: {
    id: "transport-message-a",
    roomId: "transport-room",
    senderId: "transport-user",
    body: "target partition",
    attachments: [],
    createdAtMs: Date.now(),
    path: "rooms/transport-room/messages/transport-message-a",
  },
  updatedAtMs: Date.now(),
  lsn: 48,
  path: "tables/rooms/transport-room/messages/transport-message-a",
}
await transport.frame({
  type: "event",
  event: {
    type: "recordUpserted",
    table: "rooms.messages",
    key: "transport-room:transport-message-a",
    record: nestedRecord,
  },
})
await waitUntil(() => nestedWatchSnapshots.some((snapshot) =>
  snapshot.change?.type === "recordUpserted" &&
  snapshot.records.some((row) => row.key === nestedRecord.key)
))
await waitUntil(() => nestedDetailSnapshots.some((snapshot) =>
  snapshot.change?.type === "recordUpserted" &&
  snapshot.record?.key === nestedRecord.key &&
  snapshot.record.value.body === "target partition"
))

await transport.frame({
  type: "event",
  event: {
    type: "recordDeleted",
    table: "rooms.messages",
    key: "transport-room:transport-message-a",
    deletedAtMs: Date.now(),
    lsn: 49,
    path: "tables/rooms/transport-room/messages/transport-message-a",
  },
})
await waitUntil(() => nestedDetailSnapshots.some((snapshot) =>
  snapshot.change?.type === "recordDeleted" &&
  snapshot.record === undefined
))

const signal = {
  channelId: "transport-channel",
  fromUserId: "transport-peer",
  toUserId: "transport-user",
  kind: "offer",
  payload: { sdp: "test" },
}
await transport.frame({
  type: "event",
  event: {
    type: "volatileUserEvent",
    userId: "transport-user",
    name: "realtime.channel.signal",
    payload: signal,
  },
})
assert.deepEqual(signals, [signal])

const joinedMember = {
  userId: "transport-peer",
  sessionId: "transport-peer-session",
  metadata: { role: "peer" },
  joinedAtMs: Date.now(),
}
await transport.frame({
  type: "event",
  event: {
    type: "volatileUserEvent",
    userId: "transport-user",
    name: "realtime.channel.memberJoined",
    payload: {
      channelId: "transport-channel",
      member: joinedMember,
    },
  },
})
assert.equal(memberJoined.length, 1)
assert.deepEqual(memberJoined[0].member, joinedMember)

await transport.frame({
  type: "event",
  event: {
    type: "volatileUserEvent",
    userId: "transport-user",
    name: "realtime.channel.memberLeft",
    payload: {
      channelId: "transport-channel",
      members: [joinedMember],
    },
  },
})
assert.equal(memberLeft.length, 1)
assert.deepEqual(memberLeft[0].members, [joinedMember])

await cache.putRecords([
  {
    table: "rooms.messages",
    key: "transport-room:transport-message-a",
    value: {
      id: "transport-message-a",
      roomId: "transport-room",
      senderId: "transport-user",
      body: "target partition",
      attachments: [],
      createdAtMs: Date.now(),
      path: "rooms/transport-room/messages/transport-message-a",
    },
    updatedAtMs: Date.now(),
    lsn: 50,
    path: "tables/rooms/transport-room/messages/transport-message-a",
  },
  {
    table: "rooms.messages",
    key: "other-transport-room:transport-message-b",
    value: {
      id: "transport-message-b",
      roomId: "other-transport-room",
      senderId: "transport-user",
      body: "other partition",
      attachments: [],
      createdAtMs: Date.now(),
      path: "rooms/other-transport-room/messages/transport-message-b",
    },
    updatedAtMs: Date.now(),
    lsn: 51,
    path: "tables/rooms/other-transport-room/messages/transport-message-b",
  },
])

await transport.frame({
  type: "cacheInvalidated",
  invalidation: {
    id: "transport-cache-invalidation-1",
    generation: 3,
    scope: "table",
    key: "rooms",
    minValidLsn: 44,
    reason: "transport smoke table reset",
    createdAtMs: Date.now(),
  },
})
assert.equal(await cache.getRecord("rooms", "transport-room"), undefined)
assert.equal(await cache.getTableCursor("rooms"), 44)
assert.equal((await cache.getMetadata()).invalidationGeneration, 3)
assert.equal(cacheEvents.at(-1).type, "tableInvalidated")
assert.equal(cacheEvents.at(-1).source, "cacheInvalidation")
assert.deepEqual(
  (await cache.listRecordsByKeyPrefix("rooms.messages", "transport-room:", 10)).map((row) => row.value.id),
  ["transport-message-a"],
)

await transport.frame({
  type: "cacheInvalidated",
  invalidation: {
    id: "transport-cache-invalidation-2",
    generation: 4,
    scope: "nestedTable",
    key: "rooms.messages:transport-room",
    table: "rooms",
    parentKey: "transport-room",
    nested: "messages",
    minValidLsn: 52,
    reason: "transport smoke nested partition reset",
    createdAtMs: Date.now(),
  },
})
assert.deepEqual(await cache.listRecordsByKeyPrefix("rooms.messages", "transport-room:", 10), [])
assert.deepEqual(
  (await cache.listRecordsByKeyPrefix("rooms.messages", "other-transport-room:", 10)).map((row) => row.value.id),
  ["transport-message-b"],
)
assert.equal(await cache.getNestedTableCursor("rooms", "transport-room", "messages"), 52)
assert.equal(await cache.getTableCursor("rooms.messages"), 0)
assert.equal((await cache.getMetadata()).invalidationGeneration, 4)
assert.equal(cacheEvents.at(-1).type, "tableInvalidated")
assert.equal(cacheEvents.at(-1).table, "rooms.messages")
assert.equal(cacheEvents.at(-1).source, "cacheInvalidation")

stopRoom()
stopTable()
stopRecordDetailWatch()
stopNestedWatch()
stopNestedDetailWatch()
stopQuery()
stopObjects()
stopObjectWatch()
stopObjectDetailWatch()
stopUserEvents()
stopChannelEvents()
stopGameInputs()
stopSignals()
stopMemberJoined()
stopMemberLeft()
assert(transport.sent.some((frame) => frame.type === "unsubscribeQuery" && frame.queryId === "transport-query"))
assert(transport.sent.some((frame) => frame.type === "unsubscribeNestedTable" && frame.table === "rooms" && frame.parentKey === "transport-room" && frame.nested === "messages"))
assert(transport.sent.some((frame) => frame.type === "unsubscribeUserEvents"))
assert(transport.sent.some((frame) => frame.type === "unsubscribeObjects"))
client.close()

async function testRealtimeEventsFrameBatchesRoomMessageCacheWrites() {
  const cache = new CountingRoomMessageCache()
  const transports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-events-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })
  const roomEvents = []
  const stopRoom = client.subscribeRoom("events-room", (event) => roomEvents.push(event), { catchUp: false })
  assert.equal(transports.length, 1)
  const transport = transports[0]
  await transport.open()
  await waitForSent(transport, (frame) => frame.type === "subscribeRoom" && frame.roomId === "events-room")

  const first = {
    id: "events-message-1",
    roomId: "events-room",
    senderId: "transport-events-user",
    body: "first batched realtime message",
    attachments: [],
    createdAtMs: Date.now(),
    lsn: 101,
    path: "rooms/events-room/messages/events-message-1",
  }
  const second = {
    ...first,
    id: "events-message-2",
    body: "second batched realtime message",
    createdAtMs: first.createdAtMs + 1,
    lsn: 102,
    path: "rooms/events-room/messages/events-message-2",
  }

  await transport.frame({
    type: "events",
    events: [
      { type: "messageCreated", roomId: "events-room", message: first },
      { type: "messageCreated", roomId: "events-room", message: second },
    ],
  })

  await waitUntil(() => cache.putRoomMessagesCalls === 1 && roomEvents.length === 2)
  assert.equal(cache.putRoomMessagesCalls, 1)
  assert.deepEqual(cache.putRoomMessagesBatchSizes, [2])
  assert.deepEqual((await cache.getRoomMessages("events-room", 10)).map((message) => message.id), [
    "events-message-2",
    "events-message-1",
  ])
  assert.deepEqual(roomEvents.map((event) => event.message.id), ["events-message-1", "events-message-2"])

  stopRoom()
  client.close()
}

async function testQueryResultIdReconnect() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-query-reconnect-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  const queryTransports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-query-reconnect-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      queryTransports.push(transport)
      return transport
    },
  })
  const queryResults = []
  const stopQuery = client.table("rooms").subscribeQuery((event) => queryResults.push(event), {
    queryId: "transport-reconnect-query",
    indexName: "byTitle",
    value: "Transport Reconnect Room",
    limit: 10,
  })
  assert.equal(queryTransports.length, 1)
  const firstTransport = queryTransports[0]
  await firstTransport.open()
  await waitForSent(firstTransport, (frame) =>
    frame.type === "subscribeQuery" &&
    frame.queryId === "transport-reconnect-query" &&
    frame.resultId === undefined,
  )
  await firstTransport.frame({
    type: "queryResult",
    queryId: "transport-reconnect-query",
    currentLsn: 50,
    resultId: "sha256:transport-reconnect-query-1",
    response: {
      table: "rooms",
      records: [],
      hasMore: false,
    },
  })
  assert.equal(queryResults.length, 1)

  firstTransport.close()
  await waitUntil(() => queryTransports.length === 2)
  const secondTransport = queryTransports[1]
  await secondTransport.open()
  await waitForSent(secondTransport, (frame) =>
    frame.type === "subscribeQuery" &&
    frame.queryId === "transport-reconnect-query" &&
    frame.resultId === "sha256:transport-reconnect-query-1",
  )
  stopQuery()
  client.close()
}

async function testPersistentSubscriptionRestore() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-persistent-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  const firstTransports = []
  const firstClient = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-persistent-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      firstTransports.push(transport)
      return transport
    },
  })
  const stopRoom = firstClient.subscribeRoom("persistent-room", () => undefined, {
    catchUp: true,
    catchUpLimit: 7,
    persistent: true,
  })
  const stopNested = firstClient.nestedTable("rooms", "persistent-room", "messages").subscribe(() => undefined, {
    catchUp: true,
    catchUpLimit: 5,
    persistent: true,
  })
  const stopQuery = firstClient.subscribeQuery({
    queryId: "persistent-query",
    table: "rooms",
    limit: 3,
    resultId: "sha256:persistent-query-client-known",
    persistent: true,
  }, () => undefined)
  await waitUntil(async () => (await cache.listSubscriptions()).length === 3)
  firstClient.close()

  const restoredTransports = []
  const restoredClient = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-persistent-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      restoredTransports.push(transport)
      return transport
    },
  })
  const restored = await restoredClient.restoreSubscriptions()
  assert.deepEqual(restored.map((subscription) => subscription.id).sort(), [
    "nested:rooms/persistent-room/messages",
    "query:persistent-query",
    "room:persistent-room",
  ])
  assert.equal(restoredTransports.length, 1)
  const transport = restoredTransports[0]
  await transport.open()
  await waitForSent(transport, (frame) =>
    frame.type === "subscribeRoom" &&
    frame.roomId === "persistent-room" &&
    frame.catchUpLimit === 7,
  )
  await waitForSent(transport, (frame) =>
    frame.type === "subscribeNestedTable" &&
    frame.table === "rooms" &&
    frame.parentKey === "persistent-room" &&
    frame.nested === "messages" &&
    frame.catchUpLimit === 5,
  )
  await waitForSent(transport, (frame) =>
    frame.type === "subscribeQuery" &&
    frame.queryId === "persistent-query" &&
    frame.table === "rooms" &&
    frame.limit === 3 &&
    frame.resultId === undefined,
  )
  restoredClient.close()
  stopRoom()
  stopNested()
  stopQuery()
}

async function testClearStoredSubscriptionsUnsubscribesPersistentFeeds() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-clear-persistent-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.putSubscription({
    id: "room:clear-room",
    kind: "room",
    roomId: "clear-room",
    options: { catchUp: true, catchUpLimit: 7 },
    createdAtMs: 1,
    updatedAtMs: 1,
  })
  await cache.putSubscription({
    id: "table:rooms",
    kind: "table",
    table: "rooms",
    options: { catchUp: false },
    createdAtMs: 2,
    updatedAtMs: 2,
  })
  await cache.putSubscription({
    id: "nested:rooms/clear-room/messages",
    kind: "nestedTable",
    table: "rooms",
    parentKey: "clear-room",
    nested: "messages",
    options: { catchUp: true, catchUpLimit: 5 },
    createdAtMs: 3,
    updatedAtMs: 3,
  })
  await cache.putSubscription({
    id: "query:clear-query",
    kind: "query",
    query: {
      type: "subscribeQuery",
      queryId: "clear-query",
      table: "rooms",
      limit: 10,
    },
    createdAtMs: 4,
    updatedAtMs: 4,
  })
  await cache.putSubscription({
    id: "user:transport-clear-user:events",
    kind: "userEvents",
    userId: "transport-clear-user",
    options: { catchUp: true },
    createdAtMs: 5,
    updatedAtMs: 5,
  })
  await cache.putSubscription({
    id: "objects",
    kind: "objects",
    options: { catchUp: false },
    createdAtMs: 6,
    updatedAtMs: 6,
  })

  const transports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-clear-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })
  const restored = await client.restoreSubscriptions()
  assert.equal(restored.length, 6)
  assert.equal(transports.length, 1)
  const transport = transports[0]
  await transport.open()
  await waitForSent(transport, (frame) => frame.type === "subscribeRoom" && frame.roomId === "clear-room")
  await waitForSent(transport, (frame) => frame.type === "subscribeTable" && frame.table === "rooms")
  await waitForSent(transport, (frame) =>
    frame.type === "subscribeNestedTable" &&
    frame.table === "rooms" &&
    frame.parentKey === "clear-room" &&
    frame.nested === "messages",
  )
  await waitForSent(transport, (frame) => frame.type === "subscribeQuery" && frame.queryId === "clear-query")
  await waitForSent(transport, (frame) => frame.type === "subscribeUserEvents")
  await waitForSent(transport, (frame) => frame.type === "subscribeObjects")

  const removed = await client.clearStoredSubscriptions()
  assert.equal(removed, 6)
  await waitForSent(transport, (frame) => frame.type === "unsubscribeRoom" && frame.roomId === "clear-room")
  await waitForSent(transport, (frame) => frame.type === "unsubscribeTable" && frame.table === "rooms")
  await waitForSent(transport, (frame) =>
    frame.type === "unsubscribeNestedTable" &&
    frame.table === "rooms" &&
    frame.parentKey === "clear-room" &&
    frame.nested === "messages",
  )
  await waitForSent(transport, (frame) => frame.type === "unsubscribeQuery" && frame.queryId === "clear-query")
  await waitForSent(transport, (frame) => frame.type === "unsubscribeUserEvents")
  await waitForSent(transport, (frame) => frame.type === "unsubscribeObjects")
  assert.deepEqual(await cache.listSubscriptions(), [])
  const status = await client.localDataStatus()
  assert.deepEqual(status.activeSubscriptions.rooms, [])
  assert.deepEqual(status.activeSubscriptions.tables, [])
  assert.deepEqual(status.activeSubscriptions.nestedTables, [])
  assert.deepEqual(status.activeSubscriptions.queries, [])
  assert.equal(status.activeSubscriptions.userEvents, false)
  assert.equal(status.activeSubscriptions.objects, false)
  assert.deepEqual(status.persistentSubscriptions.rooms, [])
  assert.deepEqual(status.persistentSubscriptions.tables, [])
  assert.deepEqual(status.persistentSubscriptions.nestedTables, [])
  assert.deepEqual(status.persistentSubscriptions.queries, [])
  assert.equal(status.persistentSubscriptions.userEvents, false)
  assert.equal(status.persistentSubscriptions.objects, false)
  client.close()
}

async function testClearStoredSubscriptionsDropsPendingRestore() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-clear-pending-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.putSubscription({
    id: "room:pending-room",
    kind: "room",
    roomId: "pending-room",
    options: { catchUp: true, catchUpLimit: 11 },
    createdAtMs: 1,
    updatedAtMs: 1,
  })
  await cache.putSubscription({
    id: "query:pending-query",
    kind: "query",
    query: {
      type: "subscribeQuery",
      queryId: "pending-query",
      table: "rooms",
      limit: 5,
    },
    createdAtMs: 2,
    updatedAtMs: 2,
  })
  await cache.putSubscription({
    id: "objects",
    kind: "objects",
    options: { catchUp: true, catchUpLimit: 3 },
    createdAtMs: 3,
    updatedAtMs: 3,
  })

  const transports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })
  await client.restoreSubscriptions()
  assert.equal(transports.length, 1)
  const transport = transports[0]
  assert.deepEqual(transport.sent, [])
  assert.equal(await client.clearStoredSubscriptions(), 3)
  await transport.open()
  assert.deepEqual(transport.sent, [])
  assert.deepEqual(await cache.listSubscriptions(), [])
  const status = await client.localDataStatus()
  assert.deepEqual(status.activeSubscriptions.rooms, [])
  assert.deepEqual(status.activeSubscriptions.queries, [])
  assert.equal(status.activeSubscriptions.objects, false)
  client.close()
}

async function testClearCacheClearsPersistentSubscriptionIntent() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-clear-cache-persistent-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.putSubscription({
    id: "room:cache-clear-room",
    kind: "room",
    roomId: "cache-clear-room",
    options: { catchUp: true, catchUpLimit: 4 },
    createdAtMs: 1,
    updatedAtMs: 1,
  })
  await cache.putSubscription({
    id: "objects",
    kind: "objects",
    options: { catchUp: false },
    createdAtMs: 2,
    updatedAtMs: 2,
  })

  const transports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })
  await client.restoreSubscriptions()
  const transport = transports[0]
  await transport.open()
  await waitForSent(transport, (frame) => frame.type === "subscribeRoom" && frame.roomId === "cache-clear-room")
  await waitForSent(transport, (frame) => frame.type === "subscribeObjects")

  await client.clearCache()
  await waitForSent(transport, (frame) => frame.type === "unsubscribeRoom" && frame.roomId === "cache-clear-room")
  await waitForSent(transport, (frame) => frame.type === "unsubscribeObjects")
  assert.deepEqual(await cache.listSubscriptions(), [])
  const status = await client.localDataStatus()
  assert.deepEqual(status.activeSubscriptions.rooms, [])
  assert.equal(status.activeSubscriptions.objects, false)
  assert.deepEqual(status.persistentSubscriptions.rooms, [])
  assert.equal(status.persistentSubscriptions.objects, false)
  client.close()
}

async function testPersistentQueryBaselineRestore() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-persistent-baseline-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  const firstTransports = []
  const firstClient = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-persistent-baseline-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      firstTransports.push(transport)
      return transport
    },
  })
  const firstEvents = []
  firstClient.subscribeQuery({
    queryId: "persistent-query-baseline",
    table: "rooms",
    limit: 1,
    persistent: true,
  }, (event) => firstEvents.push(event))
  assert.equal(firstTransports.length, 1)
  const firstTransport = firstTransports[0]
  await firstTransport.open()
  await waitForSent(firstTransport, (frame) =>
    frame.type === "subscribeQuery" &&
    frame.queryId === "persistent-query-baseline" &&
    frame.resultId === undefined,
  )
  const record = {
    table: "rooms",
    key: "persistent-baseline-room",
    value: { id: "persistent-baseline-room", title: "Persistent Baseline Room" },
    updatedAtMs: Date.now(),
    lsn: 91,
    path: "tables/rooms/persistent-baseline-room",
  }
  await firstTransport.frame({
    type: "queryResult",
    queryId: "persistent-query-baseline",
    currentLsn: 91,
    resultId: "sha256:persistent-query-baseline-1",
    response: {
      table: "rooms",
      records: [record],
      nextAfterKey: "persistent-baseline-room",
      hasMore: false,
    },
  })
  assert.equal(firstEvents.length, 1)
  await waitUntil(async () =>
    (await cache.listSubscriptions()).some((subscription) =>
      subscription.id === "query:persistent-query-baseline" &&
      subscription.kind === "query" &&
      subscription.query.resultId === "sha256:persistent-query-baseline-1",
    ),
  )
  firstClient.close()

  const restoredTransports = []
  const restoredClient = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "transport-persistent-baseline-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      restoredTransports.push(transport)
      return transport
    },
  })
  const restored = await restoredClient.restoreSubscriptions()
  assert.deepEqual(restored.map((subscription) => subscription.id), ["query:persistent-query-baseline"])
  assert.equal(restoredTransports.length, 1)
  const restoredTransport = restoredTransports[0]
  await restoredTransport.open()
  await waitForSent(restoredTransport, (frame) =>
    frame.type === "subscribeQuery" &&
    frame.queryId === "persistent-query-baseline" &&
    frame.resultId === "sha256:persistent-query-baseline-1",
  )
  restoredClient.close()
}

async function testAutoRestoreSubscriptions() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-auto-restore-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.setRoomCursor("auto-room", 77)
  await cache.putSubscription({
    id: "room:auto-room",
    kind: "room",
    roomId: "auto-room",
    options: { catchUp: true, catchUpLimit: 9 },
    createdAtMs: 1,
    updatedAtMs: 1,
  })
  await cache.putSubscription({
    id: "objects",
    kind: "objects",
    options: { catchUp: false },
    createdAtMs: 2,
    updatedAtMs: 2,
  })

  const { server, endpoint, requests } = await startSyncPullServer()
  try {
    const transports = []
    const client = new NextDbClient({
      endpoint,
      cache,
      autoRestoreSubscriptions: true,
      realtimeTransport: ({ url }) => {
        const transport = new MemoryRealtimeTransport(url)
        transports.push(transport)
        return transport
      },
    })
    await waitUntil(() => transports.length === 1)
    const transport = transports[0]
    await transport.open()
    await waitForSent(transport, (frame) =>
      frame.type === "subscribeRoom" &&
      frame.roomId === "auto-room" &&
      frame.afterLsn === 77 &&
      frame.catchUpLimit === 9,
    )
    await waitForSent(transport, (frame) =>
      frame.type === "subscribeObjects" &&
      frame.afterLsn === undefined,
    )
    const status = await client.localDataStatus()
    assert.deepEqual(status.persistentSubscriptions.rooms, ["auto-room"])
    assert.equal(status.persistentSubscriptions.objects, true)
    client.close()
  } finally {
    await closeServer(server)
  }
}

async function testNestedSyncPullUsesParentScopedFilter() {
  const requests = []
  const server = createServer((request, response) => {
    const url = new URL(request.url, `http://${request.headers.host}`)
    if (request.method === "GET" && url.pathname === "/v1/sync/pull") {
      requests.push(url)
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({
        events: [],
        nextAfterLsn: Number(url.searchParams.get("afterLsn") ?? 0) + 1,
        currentLsn: 88,
        hasMore: false,
      }))
      return
    }
    response.writeHead(404, { "content-type": "application/json" })
    response.end(JSON.stringify({ error: "not found" }))
  })
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  assert.equal(typeof address, "object")
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-nested-sync-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  const client = new NextDbClient({
    endpoint: `http://127.0.0.1:${address.port}`,
    cache,
  })
  try {
    await client.nestedTable("rooms", "transport:room", "messages").sync({ limit: 25, maxPages: 1 })
    assert.equal(requests.at(-1).searchParams.get("nestedTables"), "rooms:transport:room:messages")
    assert.equal(requests.at(-1).searchParams.get("tables"), null)

    await client.syncPull({
      afterLsn: 12,
      nestedTables: [{ table: "rooms", parentKey: "transport-room", nested: "messages" }],
      limit: 10,
    })
    assert.equal(requests.at(-1).searchParams.get("afterLsn"), "12")
    assert.equal(requests.at(-1).searchParams.get("nestedTables"), "rooms:transport-room:messages")
    assert.equal(requests.at(-1).searchParams.get("tables"), null)
  } finally {
    client.close()
    await closeServer(server)
  }
}

async function testNestedSubscriptionUsesParentScopedCursor() {
  const { server, endpoint, requests } = await startSyncPullServer()
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-nested-cursor-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.setTableCursor("rooms.messages", 100)
  await cache.setNestedTableCursor("rooms", "room-a", "messages", 77)
  const transports = []
  const client = new NextDbClient({
    endpoint,
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })

  try {
    const roomBEvents = []
    const stopRoomB = client.nestedTable("rooms", "room-b", "messages").subscribe((event) => roomBEvents.push(event))
    const transport = transports[0]
    await transport.open()
    await waitForSent(transport, (frame) =>
      frame.type === "subscribeNestedTable" &&
      frame.table === "rooms" &&
      frame.parentKey === "room-b" &&
      frame.nested === "messages" &&
      frame.afterLsn === 0,
    )

    const oldRoomBRecord = {
      table: "rooms.messages",
      key: "room-b:old-message",
      value: { id: "old-message", roomId: "room-b", body: "old" },
      updatedAtMs: 50,
      lsn: 50,
      path: "tables/rooms/room-b/messages/old-message",
    }
    await transport.frame({
      type: "event",
      event: {
        type: "recordUpserted",
        table: "rooms.messages",
        key: "room-b:old-message",
        record: oldRoomBRecord,
      },
    })
    assert.equal(roomBEvents.length, 1)
    assert.equal((await cache.getRecord("rooms.messages", "room-b:old-message")).lsn, 50)
    assert.equal(await cache.getNestedTableCursor("rooms", "room-b", "messages"), 50)
    assert.equal(await cache.getTableCursor("rooms.messages"), 100)

    stopRoomB()
    client.close()

    const roomATransports = []
    const roomAClient = new NextDbClient({
      endpoint,
      cache,
      realtimeTransport: ({ url }) => {
        const transport = new MemoryRealtimeTransport(url)
        roomATransports.push(transport)
        return transport
      },
    })
    const stopRoomA = roomAClient.nestedTable("rooms", "room-a", "messages").subscribe(() => undefined)
    const roomATransport = roomATransports[0]
    await roomATransport.open()
    await waitForSent(roomATransport, (frame) =>
      frame.type === "subscribeNestedTable" &&
      frame.parentKey === "room-a" &&
      frame.afterLsn === 77,
    )
    await waitUntil(() => requests.length > 0)
    stopRoomA()
    roomAClient.close()
  } finally {
    client.close()
    await closeServer(server)
  }
}

async function testLocalDataStatus() {
  const cache = new MemoryLocalCache()
  await cache.setMetadata({
    clientId: "transport-status-client",
    profileVersion: 1,
    schemaVersion: 1,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await cache.putRecords([{
    table: "rooms",
    key: "status-room",
    value: { id: "status-room" },
    updatedAtMs: Date.now(),
    lsn: 88,
    path: "tables/rooms/status-room",
  }])
  const transports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "status-user",
    cache,
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })
  const stopRoom = client.subscribeRoom("status-room", () => undefined, {
    catchUp: true,
    persistent: true,
  })
  const stopNested = client.nestedTable("rooms", "status-room", "messages").subscribe(() => undefined, {
    catchUp: true,
    persistent: true,
  })
  await waitUntil(async () => (await cache.listSubscriptions()).length === 2)
  const status = await client.localDataStatus()
  assert.equal(status.endpoint, "http://127.0.0.1:9")
  assert.equal(status.configuredRealtimeTransportKind, "custom")
  assert.equal(status.configuredConnectionTransport, "custom")
  assert.equal(status.realtimeTransportKind, "custom")
  assert.equal(status.connectionTransport, "custom")
  assert.equal(status.cache.totalRecords, 1)
  assert.equal(status.cache.subscriptions, 2)
  assert.deepEqual(status.activeSubscriptions.rooms, ["status-room"])
  assert.deepEqual(status.activeSubscriptions.nestedTables, ["rooms/status-room/messages"])
  assert.deepEqual(status.persistentSubscriptions.rooms, ["status-room"])
  assert.deepEqual(status.persistentSubscriptions.nestedTables, ["rooms/status-room/messages"])
  assert.deepEqual(status.storedSubscriptions.map((subscription) => subscription.id).sort(), [
    "nested:rooms/status-room/messages",
    "room:status-room",
  ])
  stopRoom()
  stopNested()
  client.close()
}

async function testWebTransportRealtimeTransport() {
  const originalWebTransport = globalThis.WebTransport
  Object.defineProperty(globalThis, "WebTransport", {
    configurable: true,
    writable: true,
    value: MockWebTransport,
  })
  try {
    MockWebTransport.instances = []
    const received = []
    const transport = new WebTransportRealtimeTransport(new URL("wss://example.test/v1/connect?userId=transport-user"))
    const opened = new Promise((resolve) => transport.onOpen(resolve))
    transport.onFrame((frame) => received.push(frame))
    transport.send({ type: "subscribeObjects", afterLsn: 12 })

    await opened
    const mock = MockWebTransport.instances.at(-1)
    assert.equal(mock.url, "https://example.test/v1/connect?userId=transport-user")
    await waitUntil(() => mock.sent.length >= 1)
    assert.deepEqual(JSON.parse(mock.sent[0]), { type: "subscribeObjects", afterLsn: 12 })

    transport.send({ type: "subscribeUserEvents", afterLsn: 7 })
    await waitUntil(() => mock.sent.length >= 2)
    assert.deepEqual(JSON.parse(mock.sent[1]), { type: "subscribeUserEvents", afterLsn: 7 })

    mock.frame({ type: "error", message: "from webtransport" })
    await waitUntil(() => received.length === 1)
    assert.deepEqual(received[0], { type: "error", message: "from webtransport" })

    transport.close()
    assert.equal(transport.state, "closed")
  } finally {
    Object.defineProperty(globalThis, "WebTransport", {
      configurable: true,
      writable: true,
      value: originalWebTransport,
    })
  }
}

function testRealtimeFrameCodec() {
  const encoded = encodeRealtimeClientFrame({ type: "subscribeObjects", afterLsn: 12 })
  assert.equal(encoded, '{"type":"subscribeObjects","afterLsn":12}')
  assert.equal(encodeRealtimeClientFrameJsonLine({ type: "unsubscribeObjects" }), '{"type":"unsubscribeObjects"}\n')
  assert.deepEqual(decodeRealtimeServerFrame('{"type":"objectsSubscribed"}'), { type: "objectsSubscribed" })
  assert.deepEqual(decodeRealtimeServerFrame('{"type":"events","events":[]}'), { type: "events", events: [] })

  const decoder = new RealtimeServerFrameJsonLineDecoder()
  assert.deepEqual(decoder.push('{"type":"error"'), [])
  assert.deepEqual(decoder.push(',"message":"split"}\n{"type":"objectsSubscribed"}\n'), [
    { type: "error", message: "split" },
    { type: "objectsSubscribed" },
  ])
  assert.deepEqual(decoder.push('{"type":"userEventsUnsubscribed"}', { flush: true }), [
    { type: "userEventsUnsubscribed" },
  ])
}

async function testJsonLineHttpRealtimeTransport() {
  const encoder = new TextEncoder()
  const decoder = new TextDecoder()
  const frames = []
  const errors = []
  let opened = false
  let closed = false
  let capturedInput
  let capturedInit
  let requestReader
  let responseController
  const responseBody = new ReadableStream({
    start(controller) {
      responseController = controller
    },
  })
  const fetchImpl = async (input, init) => {
    capturedInput = input
    capturedInit = init
    requestReader = init.body.getReader()
    return new Response(responseBody, {
      status: 200,
      headers: { "content-type": "application/x-ndjson" },
    })
  }
  const transport = jsonLineHttpRealtimeTransport({ fetch: fetchImpl })({
    url: new URL("ws://example.test/v1/connect?userId=jsonl-user&sessionId=jsonl-session&transport=custom"),
  })
  assert.ok(transport instanceof JsonLineHttpRealtimeTransport)
  transport.onOpen(() => {
    opened = true
  })
  transport.onFrame((frame) => frames.push(frame))
  transport.onError((error) => errors.push(error))
  transport.onClose(() => {
    closed = true
  })

  await waitForCondition(() => opened)
  assert.equal(String(capturedInput), "http://example.test/v1/connect/jsonl?userId=jsonl-user&sessionId=jsonl-session&transport=custom")
  assert.equal(capturedInit.method, "POST")
  assert.equal(capturedInit.duplex, "half")
  assert.equal(capturedInit.headers.get("accept"), "application/x-ndjson")
  assert.equal(capturedInit.headers.get("content-type"), "application/x-ndjson")

  transport.send({ type: "subscribeObjects", afterLsn: 7, catchUpLimit: 2 })
  const requestChunk = await requestReader.read()
  assert.equal(decoder.decode(requestChunk.value), '{"type":"subscribeObjects","afterLsn":7,"catchUpLimit":2}\n')

  responseController.enqueue(encoder.encode('{"type":"hello","userId":"jsonl-user","sessionId":"jsonl-session"}\n'))
  responseController.enqueue(encoder.encode('{"type":"objectsSubscribed"}\n'))
  await waitForCondition(() => frames.length === 2)
  assert.deepEqual(frames, [
    { type: "hello", userId: "jsonl-user", sessionId: "jsonl-session" },
    { type: "objectsSubscribed" },
  ])
  assert.deepEqual(errors, [])
  responseController.close()
  await waitForCondition(() => closed)
}

function testRealtimeTransportCompatibility() {
  const health = {
    connectionLayer: {
      protocol: "nextdb.realtime.v1",
      frameEncoding: "json",
      connectPath: "/v1/connect",
      supportedTransports: ["webSocket"],
      defaultTransport: "webSocket",
      webSocket: { supported: true, connectPath: "/v1/connect" },
      webTransport: { supported: false, connectPath: null },
      custom: { supported: false, connectPath: null },
    },
  }
  assert.deepEqual(realtimeTransportCompatibility(health, "websocket"), {
    requestedKind: "websocket",
    requestedTransport: "webSocket",
    supported: true,
    status: "supported",
    supportedTransports: ["webSocket"],
    defaultTransport: "webSocket",
  })
  assert.deepEqual(realtimeTransportCompatibility(health, "webtransport"), {
    requestedKind: "webtransport",
    requestedTransport: "webTransport",
    supported: false,
    status: "unsupported",
    supportedTransports: ["webSocket"],
    defaultTransport: "webSocket",
    fallbackTransport: "webSocket",
      reason: "webTransport is not advertised by this node's connectionLayer.supportedTransports",
  })
  assert.deepEqual(realtimeTransportCompatibility(health, "jsonl"), {
    requestedKind: "jsonl",
    requestedTransport: "custom",
    supported: false,
    status: "unsupported",
    supportedTransports: ["webSocket"],
    defaultTransport: "webSocket",
    fallbackTransport: "webSocket",
    reason: "custom is not advertised by this node's connectionLayer.supportedTransports",
  })
  assert.deepEqual(realtimeTransportCompatibility(health, "custom"), {
    requestedKind: "custom",
    requestedTransport: "custom",
    supported: true,
    status: "custom",
    supportedTransports: ["webSocket"],
    defaultTransport: "webSocket",
    fallbackTransport: "webSocket",
    reason: "custom realtime transports are application-owned and may terminate at an external gateway",
  })
  const jsonlHealth = {
    connectionLayer: {
      ...health.connectionLayer,
      supportedTransports: ["webSocket", "custom"],
      custom: { supported: true, connectPath: "/v1/connect/jsonl" },
    },
  }
  assert.deepEqual(realtimeTransportCompatibility(jsonlHealth, "jsonl"), {
    requestedKind: "jsonl",
    requestedTransport: "custom",
    supported: true,
    status: "supported",
    supportedTransports: ["webSocket", "custom"],
    defaultTransport: "webSocket",
  })
}

async function testWebTransportKindDeclaresConnectionTransport() {
  const transports = []
  const client = new NextDbClient({
    endpoint: "http://127.0.0.1:9",
    userId: "webtransport-kind-user",
    realtimeTransportKind: "webtransport",
    realtimeTransport: ({ url }) => {
      const transport = new MemoryRealtimeTransport(url)
      transports.push(transport)
      return transport
    },
  })
  const stop = client.subscribeObjects(() => undefined, { catchUp: false })
  await waitUntil(() => transports.length === 1)
  assert.equal(transports[0].url.searchParams.get("transport"), "webTransport")
  const status = await client.localDataStatus()
  assert.equal(status.realtimeTransportKind, "webtransport")
  assert.equal(status.connectionTransport, "webTransport")
  stop()
  client.close()
}

async function testJsonLineKindUsesBuiltInTransport() {
  const originalFetch = globalThis.fetch
  const decoder = new TextDecoder()
  let capturedInput
  let capturedInit
  let requestReader
  let responseController
  const responseBody = new ReadableStream({
    start(controller) {
      responseController = controller
    },
  })
  Object.defineProperty(globalThis, "fetch", {
    configurable: true,
    writable: true,
    value: async (input, init) => {
      const url = new URL(String(input))
      if (url.pathname === "/v1/cache/profile") {
        return new Response(JSON.stringify({
          runtimeId: "jsonl-kind-runtime",
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
            clientId: "jsonl-kind-client",
            issuedAtMs: Date.now(),
            expiresAtMs: Date.now() + 60_000,
            profileVersion: 1,
          },
          invalidations: [],
          currentLsn: 0,
          schemaVersion: 0,
          resetRequired: false,
        }), { status: 200, headers: { "content-type": "application/json" } })
      }
      capturedInput = input
      capturedInit = init
      requestReader = init.body.getReader()
      return new Response(responseBody, {
        status: 200,
        headers: { "content-type": "application/x-ndjson" },
      })
    },
  })
  try {
    const client = new NextDbClient({
      endpoint: "http://example.test",
      userId: "jsonl-kind-user",
      realtimeTransportKind: "jsonl",
    })
    const stop = client.subscribeObjects(() => undefined, { catchUp: false })
    await waitForCondition(() => requestReader !== undefined)
    assert.equal(String(capturedInput), "http://example.test/v1/connect/jsonl?userId=jsonl-kind-user&transport=custom")
    assert.equal(capturedInit.method, "POST")
    const requestChunk = await requestReader.read()
    assert.equal(decoder.decode(requestChunk.value), '{"type":"subscribeObjects"}\n')
    const status = await client.localDataStatus()
    assert.equal(status.configuredRealtimeTransportKind, "jsonl")
    assert.equal(status.configuredConnectionTransport, "custom")
    assert.equal(status.realtimeTransportKind, "jsonl")
    assert.equal(status.connectionTransport, "custom")
    stop()
    client.close()
    responseController.close()
  } finally {
    Object.defineProperty(globalThis, "fetch", {
      configurable: true,
      writable: true,
      value: originalFetch,
    })
  }
}

async function testConnectCompatibleRealtimeFallsBackToWebSocket() {
  const originalWebSocket = globalThis.WebSocket
  class MockWebSocket {
    static CONNECTING = 0
    static OPEN = 1
    static CLOSING = 2
    static CLOSED = 3
    static instances = []

    readyState = MockWebSocket.CONNECTING
    sent = []
    onopen
    onmessage
    onerror
    onclose

    constructor(url) {
      this.url = url
      MockWebSocket.instances.push(this)
    }

    send(frame) {
      this.sent.push(frame)
    }

    close() {
      this.readyState = MockWebSocket.CLOSED
      this.onclose?.()
    }
  }

  Object.defineProperty(globalThis, "WebSocket", {
    configurable: true,
    writable: true,
    value: MockWebSocket,
  })
  try {
    const client = new NextDbClient({
      endpoint: "http://127.0.0.1:9",
      userId: "compatible-fallback-user",
      realtimeTransportKind: "webtransport",
    })
    client.health = async () => ({
      ok: true,
      runtimeId: "runtime-test",
      draining: false,
      acceptingWrites: true,
      currentLsn: 0,
      lastSnapshotLsn: 0,
      lastCompactionLsn: 0,
      walPaths: [],
      dataPath: "memory",
      limits: {
        maxObjectBytes: 0,
        maxMessageBytes: 0,
        maxUserEventBytes: 0,
        maxRecordValueBytes: 0,
        maxLiveQueriesPerConnection: 0,
        maxLiveQueriesPerTablePerConnection: 0,
        maxLiveQueriesPerUser: 0,
      },
      connectionLayer: {
        protocol: "nextdb.realtime.v1",
        frameEncoding: "json",
        connectPath: "/v1/connect",
        supportedTransports: ["webSocket"],
        defaultTransport: "webSocket",
        webSocket: { supported: true, connectPath: "/v1/connect" },
        webTransport: { supported: false, connectPath: null },
        custom: { supported: false, connectPath: null },
      },
    })

    const result = await client.connectCompatibleRealtime()
    assert.equal(result.requestedKind, "webtransport")
    assert.equal(result.requestedTransport, "webTransport")
    assert.equal(result.supported, false)
    assert.equal(result.fallbackApplied, true)
    assert.equal(result.connected, true)
    assert.equal(result.activeKind, "websocket")
    assert.equal(result.activeTransport, "webSocket")
    assert.equal(MockWebSocket.instances.length, 1)
    assert.equal(MockWebSocket.instances[0].url.searchParams.get("transport"), "webSocket")
    const status = await client.localDataStatus()
    assert.equal(status.configuredRealtimeTransportKind, "webtransport")
    assert.equal(status.configuredConnectionTransport, "webTransport")
    assert.equal(status.realtimeTransportKind, "websocket")
    assert.equal(status.connectionTransport, "webSocket")
    client.close()
  } finally {
    Object.defineProperty(globalThis, "WebSocket", {
      configurable: true,
      writable: true,
      value: originalWebSocket,
    })
  }
}

async function testConnectCompatibleRealtimeFallsBackToJsonLine() {
  const originalFetch = globalThis.fetch
  let capturedInput
  let capturedInit
  let responseController
  const responseBody = new ReadableStream({
    start(controller) {
      responseController = controller
    },
  })
  Object.defineProperty(globalThis, "fetch", {
    configurable: true,
    writable: true,
    value: async (input, init) => {
      const url = new URL(String(input))
      if (url.pathname === "/v1/cache/profile") {
        return new Response(JSON.stringify({
          runtimeId: "compatible-jsonl-runtime",
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
            clientId: "compatible-jsonl-client",
            issuedAtMs: Date.now(),
            expiresAtMs: Date.now() + 60_000,
            profileVersion: 1,
          },
          invalidations: [],
          currentLsn: 0,
          schemaVersion: 0,
          resetRequired: false,
        }), { status: 200, headers: { "content-type": "application/json" } })
      }
      capturedInput = input
      capturedInit = init
      return new Response(responseBody, {
        status: 200,
        headers: { "content-type": "application/x-ndjson" },
      })
    },
  })
  try {
    const client = new NextDbClient({
      endpoint: "http://example.test",
      userId: "compatible-jsonl-user",
      realtimeTransportKind: "webtransport",
    })
    client.health = async () => ({
      ok: true,
      runtimeId: "runtime-test",
      draining: false,
      acceptingWrites: true,
      currentLsn: 0,
      lastSnapshotLsn: 0,
      lastCompactionLsn: 0,
      walPaths: [],
      dataPath: "memory",
      limits: {
        maxObjectBytes: 0,
        maxMessageBytes: 0,
        maxUserEventBytes: 0,
        maxRecordValueBytes: 0,
        maxLiveQueriesPerConnection: 0,
        maxLiveQueriesPerTablePerConnection: 0,
        maxLiveQueriesPerUser: 0,
      },
      connectionLayer: {
        protocol: "nextdb.realtime.v1",
        frameEncoding: "json",
        connectPath: "/v1/connect",
        supportedTransports: ["webSocket", "custom"],
        defaultTransport: "webSocket",
        webSocket: { supported: true, connectPath: "/v1/connect" },
        webTransport: { supported: false, connectPath: null },
        custom: { supported: true, connectPath: "/v1/connect/jsonl" },
      },
    })

    const result = await client.connectCompatibleRealtime({ fallbackTo: "jsonl" })
    assert.equal(result.requestedKind, "webtransport")
    assert.equal(result.requestedTransport, "webTransport")
    assert.equal(result.supported, false)
    assert.equal(result.fallbackTransport, "custom")
    assert.equal(result.fallbackApplied, true)
    assert.equal(result.connected, true)
    assert.equal(result.activeKind, "jsonl")
    assert.equal(result.activeTransport, "custom")
    await waitForCondition(() => capturedInput !== undefined)
    assert.equal(String(capturedInput), "http://example.test/v1/connect/jsonl?userId=compatible-jsonl-user&transport=custom")
    assert.equal(capturedInit.method, "POST")
    const status = await client.localDataStatus()
    assert.equal(status.configuredRealtimeTransportKind, "webtransport")
    assert.equal(status.configuredConnectionTransport, "webTransport")
    assert.equal(status.realtimeTransportKind, "jsonl")
    assert.equal(status.connectionTransport, "custom")
    client.close()
    responseController.close()
  } finally {
    Object.defineProperty(globalThis, "fetch", {
      configurable: true,
      writable: true,
      value: originalFetch,
    })
  }
}

class MockWebTransport {
  static instances = []

  ready = Promise.resolve()
  sent = []

  constructor(url) {
    this.url = url
    this.closed = new Promise((resolve) => {
      this.resolveClosed = resolve
    })
    this.incomingBidirectionalStreams = new ReadableStream({
      start: (controller) => {
        this.incomingBidirectionalController = controller
      },
    })
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => {
        this.incomingUnidirectionalController = controller
      },
    })
    MockWebTransport.instances.push(this)
  }

  async createBidirectionalStream() {
    this.readable = new ReadableStream({
      start: (controller) => {
        this.serverController = controller
      },
    })
    this.writable = new WritableStream({
      write: (chunk) => {
        this.sent.push(new TextDecoder().decode(chunk).trim())
      },
    })
    return {
      readable: this.readable,
      writable: this.writable,
    }
  }

  frame(frame) {
    this.serverController.enqueue(new TextEncoder().encode(`${JSON.stringify(frame)}\n`))
  }

  close() {
    this.serverController?.close()
    this.incomingBidirectionalController?.close()
    this.incomingUnidirectionalController?.close()
    this.resolveClosed()
  }
}

await testQueryResultIdReconnect()
await testPersistentSubscriptionRestore()
await testClearStoredSubscriptionsUnsubscribesPersistentFeeds()
await testClearStoredSubscriptionsDropsPendingRestore()
await testClearCacheClearsPersistentSubscriptionIntent()
await testPersistentQueryBaselineRestore()
await testAutoRestoreSubscriptions()
await testNestedSyncPullUsesParentScopedFilter()
await testNestedSubscriptionUsesParentScopedCursor()
await testLocalDataStatus()
await testRealtimeEventsFrameBatchesRoomMessageCacheWrites()
testRealtimeFrameCodec()
await testJsonLineHttpRealtimeTransport()
testRealtimeTransportCompatibility()
await testWebTransportRealtimeTransport()
await testWebTransportKindDeclaresConnectionTransport()
await testJsonLineKindUsesBuiltInTransport()
await testConnectCompatibleRealtimeFallsBackToWebSocket()
await testConnectCompatibleRealtimeFallsBackToJsonLine()

console.log("transport smoke ok")

async function waitForSent(transport, predicate) {
  const deadline = Date.now() + 1_000
  while (Date.now() < deadline) {
    if (transport.sent.some(predicate)) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
  assert.fail("timed out waiting for transport frame")
}

async function waitForCondition(predicate, message = "timed out waiting for condition") {
  const deadline = Date.now() + 1_000
  while (Date.now() < deadline) {
    if (predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
  assert.fail(message)
}

async function startSyncPullServer() {
  const requests = []
  const server = createServer((request, response) => {
    const url = new URL(request.url, `http://${request.headers.host}`)
    if (request.method === "GET" && url.pathname === "/v1/sync/pull") {
      requests.push(url)
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({
        events: [],
        nextAfterLsn: 77,
        currentLsn: 77,
        hasMore: false,
      }))
      return
    }
    response.writeHead(404, { "content-type": "application/json" })
    response.end(JSON.stringify({ error: "not found" }))
  })
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  assert.equal(typeof address, "object")
  return {
    server,
    endpoint: `http://127.0.0.1:${address.port}`,
    requests,
  }
}

async function closeServer(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve())
  })
}

async function waitUntil(predicate) {
  const deadline = Date.now() + 1_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
  assert.fail("timed out waiting for condition")
}
