import assert from "node:assert/strict"

import { NextDbClient, NextDbHttpError } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
const roomKey = `volatile-record-${suffix}-a`
const cachedFollowerKey = `volatile-record-${suffix}-z`
const listAfterKey = `volatile-record-${suffix}-0`
const chatLogMessageKey = `volatile-message-window-${suffix}`
const alice = new NextDbClient({
  endpoint,
  userId: `alice-${suffix}`,
  sessionId: `alice-${suffix}-session`,
})
const bob = new NextDbClient({
  endpoint,
  userId: `bob-${suffix}`,
  sessionId: `bob-${suffix}-session`,
})

const bobEvents = []
const stopBob = bob.table("rooms").subscribe((event) => {
  bobEvents.push(event)
})
const predicateResults = []
const indexResults = []
const rangeResults = []
const volatileTitle = `Volatile Room ${suffix} A`
const cachedFollowerTitle = `Volatile Room ${suffix} Z`
const titleRangeLower = `Volatile Room ${suffix} 0`
const titleRangeUpper = `Volatile Room ${suffix} z`
const stopPredicateQuery = bob.table("rooms").subscribeQuery(
  (event) => predicateResults.push(event),
  {
    queryId: `volatile-record-predicate-${suffix}`,
    limit: 20,
    predicate: { all: [{ field: "title", op: "eq", value: volatileTitle }] },
  },
)
const stopIndexQuery = bob.table("rooms").subscribeQuery(
  (event) => indexResults.push(event),
  {
    queryId: `volatile-record-index-${suffix}`,
    indexName: "byTitle",
    value: volatileTitle,
    limit: 20,
  },
)
const stopRangeQuery = bob.table("rooms").subscribeQuery(
  (event) => rangeResults.push(event),
  {
    queryId: `volatile-record-range-${suffix}`,
    indexName: "byTitle",
    lower: volatileTitle,
    upper: volatileTitle,
    limit: 20,
  },
)

try {
  await waitFor(async () => {
    const connections = await bob.listConnections(`bob-${suffix}`)
    return connections.sessions.some((session) => session.subscribedTables.includes("rooms"))
  }, "bob rooms subscription")
  await waitFor(() => predicateResults.length > 0, "initial volatile predicate query")
  await waitFor(() => indexResults.length > 0, "initial volatile index query")
  await waitFor(() => rangeResults.length > 0, "initial volatile range query")

  await alice.table("rooms").upsert(
    cachedFollowerKey,
    { id: cachedFollowerKey, title: cachedFollowerTitle },
    { clientMutationId: `volatile-record-cache-seed-${suffix}` },
  )
  const beforeVolatileHealth = await alice.health()
  const beforeVolatileRecords = beforeVolatileHealth.recordHotCache.volatileRecords
  const beforeRoomsVolatileRecords =
    beforeVolatileHealth.recordHotCache.tables.find((table) => table.table === "rooms")?.volatileRecords ?? 0

  const record = await alice.table("rooms").upsert(
    roomKey,
    { id: roomKey, title: volatileTitle },
    {
      durability: "volatile",
      clientMutationId: `volatile-record-${suffix}`,
    },
  )

  assert.equal(record.table, "rooms")
  assert.equal(record.key, roomKey)
  assert.equal(record.lsn, 0)
  assert.match(record.path, /^volatile\/tables\/rooms\//)
  await waitFor(async () => {
    const health = await alice.health()
    const rooms = health.recordHotCache.tables.find((table) => table.table === "rooms")
    return health.recordHotCache.volatileRecords >= beforeVolatileRecords + 1 &&
      (rooms?.volatileRecords ?? 0) >= beforeRoomsVolatileRecords + 1
  }, "volatile record hot counter increments")
  assert.match(await alice.metrics(), /^nextdb_record_hot_volatile_records \d+$/m)
  assert.match(await alice.metrics(), /^nextdb_record_hot_table_volatile_records\{table="rooms"\} \d+$/m)

  await waitFor(() =>
    bobEvents.some((event) =>
      event.type === "recordUpserted" &&
      event.key === roomKey &&
      event.record.lsn === 0 &&
      event.record.path.startsWith("volatile/"),
    ), "bob receives volatile record upsert")

  await waitFor(() =>
    predicateResults.some((event) =>
      event.response.records.some((entry) =>
        entry.key === roomKey &&
        entry.lsn === 0 &&
        entry.path.startsWith("volatile/"),
      ),
    ), "volatile record appears in predicate live query")
  await waitFor(() =>
    indexResults.some((event) =>
      event.response.records.some((entry) =>
        entry.key === roomKey &&
        entry.lsn === 0 &&
        entry.path.startsWith("volatile/"),
      ),
    ), "volatile record appears in indexed live query")
  await waitFor(() =>
    rangeResults.some((event) =>
      event.response.records.some((entry) =>
        entry.key === roomKey &&
        entry.lsn === 0 &&
        entry.path.startsWith("volatile/"),
      ),
    ), "volatile record appears in range live query")

  const updatedRecord = await alice.table("rooms").upsert(
    roomKey,
    { id: roomKey, title: volatileTitle, marker: "updated" },
    {
      durability: "volatile",
      expectedLsn: 0,
      clientMutationId: `volatile-record-update-${suffix}`,
    },
  )
  assert.equal(updatedRecord.lsn, 0)
  assert.match(updatedRecord.path, /^volatile\/tables\/rooms\//)
  await waitFor(() =>
    predicateResults.some((event) =>
      event.diff?.updated.some((entry) =>
        entry.key === roomKey &&
        entry.lsn === 0 &&
        entry.value.marker === "updated",
      ),
    ), "volatile record update appears in predicate live query diff")
  await waitFor(() =>
    indexResults.some((event) =>
      event.diff?.updated.some((entry) =>
        entry.key === roomKey &&
        entry.lsn === 0 &&
        entry.value.marker === "updated",
      ),
    ), "volatile record update appears in indexed live query diff")
  await waitFor(() =>
    rangeResults.some((event) =>
      event.diff?.updated.some((entry) =>
        entry.key === roomKey &&
        entry.lsn === 0 &&
        entry.value.marker === "updated",
      ),
    ), "volatile record update appears in range live query diff")

  const live = await alice.table("rooms").get(roomKey)
  assert.equal(live.key, roomKey)
  assert.equal(live.lsn, 0)
  assert.match(live.path, /^volatile\//)
  assert.equal(live.value.marker, "updated")

  const listed = await alice.table("rooms").list({ limit: 50 })
  assert.equal(listed.records.some((entry) => entry.key === roomKey && entry.lsn === 0), true)

  const listedFirst = await alice.table("rooms").list({ limit: 1, afterKey: listAfterKey })
  assert.equal(listedFirst.records[0]?.key, roomKey)
  assert.equal(listedFirst.records[0]?.lsn, 0)

  const indexedFirst = await alice.table("rooms").index("byTitle", {
    lower: titleRangeLower,
    upper: titleRangeUpper,
    limit: 1,
  })
  assert.equal(indexedFirst.records[0]?.key, roomKey)
  assert.equal(indexedFirst.records[0]?.lsn, 0)

  const beforeChatLogHealth = await alice.health()
  const beforeChatLogHotRecords =
    beforeChatLogHealth.recordHotCache.tables.find((table) => table.table === "rooms.messages")?.records ?? 0
  const chatLogRecord = await alice.nestedTable("rooms", roomKey, "messages").upsert(
    chatLogMessageKey,
    {
      id: chatLogMessageKey,
      roomId: roomKey,
      senderId: `alice-${suffix}`,
      body: "chatLog messages keep a server-side hot window",
      attachments: [],
      createdAtMs: Date.now(),
      path: `tables/rooms/${roomKey}/messages/${chatLogMessageKey}`,
    },
    { clientMutationId: `volatile-record-chatlog-window-${suffix}` },
  )
  assert.equal(chatLogRecord.table, "rooms.messages")
  assert.equal(chatLogRecord.key, `${roomKey}:${chatLogMessageKey}`)
  assert.notEqual(chatLogRecord.lsn, 0)
  assert.match(chatLogRecord.path, new RegExp(`^tables/rooms/${escapeRegExp(roomKey)}/messages/${escapeRegExp(chatLogMessageKey)}$`))

  await waitFor(async () => {
    const health = await alice.health()
    const messages = health.recordHotCache.tables.find((table) => table.table === "rooms.messages")
    return messages !== undefined &&
      messages.maxItems === 5_000 &&
      messages.records >= Math.min(beforeChatLogHotRecords + 1, messages.maxItems)
  }, "chatLog nested records enter the hot live window")
  assert.match(await alice.metrics(), /^nextdb_record_hot_table_records\{table="rooms\.messages"\} \d+$/m)

  const schemaOrderedMessages = await alice.nestedTable("rooms", roomKey, "messages").listBySchemaOrder({ limit: 5 })
  const schemaOrderedMessage = schemaOrderedMessages.records.find((entry) => entry.key === `${roomKey}:${chatLogMessageKey}`)
  assert.equal(schemaOrderedMessage?.value.body, "chatLog messages keep a server-side hot window")
  assert.equal(schemaOrderedMessage?.lsn, chatLogRecord.lsn)

  const manualMessages = alice.nestedTable("rooms", roomKey, "messages")
  const activatedNestedMessage = await manualMessages.activateRuntime({
    key: chatLogMessageKey,
  })
  assert.equal(activatedNestedMessage.table, "rooms.messages")
  assert.equal(activatedNestedMessage.parentKey, roomKey)
  assert.equal(activatedNestedMessage.nested, "messages")
  assert.equal(activatedNestedMessage.found, 1)

  const activatedNestedPage = await manualMessages.activateRuntime({
    order: "schema",
    limit: 5,
  })
  assert.equal(activatedNestedPage.table, "rooms.messages")
  assert.equal(activatedNestedPage.found >= 1, true)
  const activatedRoomMessageWindow = await alice.room(roomKey).messages.activateRuntime({
    order: "schema",
    limit: 5,
  })
  assert.equal(activatedRoomMessageWindow.table, "rooms.messages")
  assert.equal(activatedRoomMessageWindow.parentKey, roomKey)
  assert.equal(activatedRoomMessageWindow.nested, "messages")
  assert.equal(activatedRoomMessageWindow.found >= 1, true)

  assert.equal(await alice.table("rooms").cache.get(roomKey), undefined)

  const audit = await alice.auditWal({
    payloadType: "recordUpserted",
    table: "rooms",
    recordKey: roomKey,
    clientMutationId: `volatile-record-${suffix}`,
    limit: 10,
  })
  assert.equal(audit.records.length, 0)

  const sync = await alice.syncPull({ tables: ["rooms"], afterLsn: 0, limit: 50 })
  assert.equal(sync.events.some((event) => event.type === "recordUpserted" && event.key === roomKey), false)

  const deleted = await alice.table("rooms").delete(roomKey, {
    durability: "volatile",
    expectedLsn: 0,
    clientMutationId: `volatile-record-delete-${suffix}`,
  })
  assert.equal(deleted.deleted, true)
  assert.equal(deleted.lsn, 0)
  assert.match(deleted.path, /^volatile\/tables\/rooms\//)
  await waitFor(async () => {
    const health = await alice.health()
    const rooms = health.recordHotCache.tables.find((table) => table.table === "rooms")
    return health.recordHotCache.volatileRecords === beforeVolatileRecords &&
      (rooms?.volatileRecords ?? 0) === beforeRoomsVolatileRecords
  }, "volatile record hot counter decrements")

  await waitFor(() =>
    bobEvents.some((event) =>
      event.type === "recordDeleted" &&
      event.key === roomKey &&
      event.lsn === 0 &&
      event.path.startsWith("volatile/"),
    ), "bob receives volatile record delete")

  await assert.rejects(
    () => alice.nestedTable("rooms", roomKey, "messages").upsert(
      `volatile-message-record-${suffix}`,
      {
        id: `volatile-message-record-${suffix}`,
        roomId: roomKey,
        senderId: `alice-${suffix}`,
        body: "chatLog nested tables must reject volatile record writes",
        attachments: [],
        createdAtMs: Date.now(),
        path: `rooms/${roomKey}/messages/volatile-message-record-${suffix}`,
      },
      { durability: "volatile" },
    ),
    (error) =>
      error instanceof NextDbHttpError &&
      error.status === 400 &&
      error.message.includes("volatile record writes require"),
  )

  console.log("volatile record smoke ok")
} finally {
  stopPredicateQuery()
  stopIndexQuery()
  stopRangeQuery()
  stopBob()
  alice.close()
  bob.close()
}

async function waitFor(check, label, timeoutMs = 5_000) {
  const started = Date.now()
  let lastError
  while (Date.now() - started < timeoutMs) {
    try {
      if (await check()) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}
