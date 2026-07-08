import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import http from "node:http"

import {
  MemoryLocalCache,
  NextDbClient,
} from "../dist/index.js"

const messages = [
  message("profile-message-1", 1),
  message("profile-message-2", 2),
]

const records = [
  record("profile-room-1", 3),
  record("profile-room-2", 4),
]

const nestedRecords = [
  nestedRecord("profile-room", "profile-nested-1", 9),
  nestedRecord("profile-room", "profile-nested-2", 10),
]

const otherNestedRecords = [
  nestedRecord("other-room", "other-nested-1", 11),
  nestedRecord("other-room", "other-nested-2", 12),
]

const userEvents = [
  userEvent("profile-event-1", 5),
  userEvent("profile-event-2", 6),
]

const syncedUserProfile = {
  userId: "profile-user",
  displayName: "Profile User",
  metadata: { source: "sync" },
  createdAtMs: 4,
  updatedAtMs: 4,
  lsn: 4,
  path: "users/profile-user",
}

const syncedObject = {
  id: "profile-object-sync",
  path: "objects/profile-object-sync",
  contentType: "text/plain",
  byteSize: 11,
  sha256: "profile-object-sync-sha",
  createdAtMs: 7,
}

let objectSequence = 0

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", "http://127.0.0.1")
  response.setHeader("content-type", "application/json")

  if (url.pathname === "/v1/cache/profile") {
    response.end(JSON.stringify({
      profile: {
        version: 1,
        leaseTtlMs: 60_000,
        maxObjects: 1,
        maxObjectBytes: 1_000,
        maxRoomMessages: 1,
        maxUserEvents: 1,
        maxRecordsPerTable: 1,
        maxNestedPartitions: 1,
        maxPendingWrites: 1,
        maxPendingWriteBytes: 1_000,
        offlineWrites: true,
      },
      lease: {
        clientId: url.searchParams.get("clientId") ?? "cache-profile-smoke",
        issuedAtMs: Date.now(),
        expiresAtMs: Date.now() + 60_000,
        profileVersion: 1,
      },
      invalidations: [],
      currentLsn: 4,
      schemaVersion: 1,
      resetRequired: false,
    }))
    return
  }

  if (url.pathname === "/v1/rooms/profile-room/messages/latest") {
    response.end(JSON.stringify({
      roomId: "profile-room",
      source: "live",
      messages,
    }))
    return
  }

  if (url.pathname === "/v1/records/rooms") {
    response.end(JSON.stringify({
      table: "rooms",
      records,
      hasMore: false,
    }))
    return
  }

  if (url.pathname === "/v1/records/rooms/profile-room/messages") {
    response.end(JSON.stringify({
      table: "rooms.messages",
      records: nestedRecords,
      hasMore: false,
    }))
    return
  }

  if (url.pathname === "/v1/records/rooms/other-room/messages") {
    response.end(JSON.stringify({
      table: "rooms.messages",
      records: otherNestedRecords,
      hasMore: false,
    }))
    return
  }

  if (url.pathname === "/v1/sync/pull") {
    if (url.searchParams.get("objects") === "true") {
      const afterLsn = Number(url.searchParams.get("afterLsn") ?? 0)
      if (afterLsn >= 7) {
        response.end(JSON.stringify({
          events: [
            {
              type: "objectDeleted",
              objectId: syncedObject.id,
              deletedAtMs: 8,
              lsn: 8,
              path: syncedObject.path,
            },
          ],
          nextAfterLsn: 8,
          currentLsn: 8,
          hasMore: false,
        }))
        return
      }
      response.end(JSON.stringify({
        events: [
          {
            type: "objectCommitted",
            object: syncedObject,
            lsn: 7,
          },
        ],
        nextAfterLsn: 7,
        currentLsn: 7,
        hasMore: false,
      }))
      return
    }
    response.end(JSON.stringify({
      events: [
        {
          type: "userUpserted",
          userId: "profile-user",
          user: syncedUserProfile,
        },
        ...userEvents.map((event) => ({
          type: "userEvent",
          userId: "profile-user",
          event,
        })),
      ],
      nextAfterLsn: 6,
      currentLsn: 6,
      hasMore: false,
    }))
    return
  }

  if (request.method === "POST" && url.pathname === "/v1/objects") {
    const body = await readBody(request)
    objectSequence += 1
    const id = `profile-object-${objectSequence}`
    response.end(JSON.stringify({
      id,
      path: `objects/${id}`,
      contentType: url.searchParams.get("contentType") ?? "application/octet-stream",
      byteSize: body.byteLength,
      sha256: createHash("sha256").update(body).digest("hex"),
      createdAtMs: objectSequence,
    }))
    return
  }

  response.statusCode = 404
  response.end(JSON.stringify({ error: `missing route ${url.pathname}` }))
})

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))

try {
  const address = server.address()
  assert.equal(typeof address, "object")
  const endpoint = `http://127.0.0.1:${address.port}`
  const cache = new MemoryLocalCache()
  const client = new NextDbClient({ endpoint, userId: "profile-user", cache })

  await client.latestMessages("profile-room", 2)
  assert.deepEqual((await cache.getRoomMessages("profile-room", 10)).map((row) => row.id), ["profile-message-2"])

  await client.listRecords("rooms", 2)
  assert.deepEqual((await cache.listRecords("rooms", 10)).map((row) => row.key), ["profile-room-2"])

  await client.listNestedRecords("rooms", "profile-room", "messages", 2)
  await client.listNestedRecords("rooms", "other-room", "messages", 2)
  assert.deepEqual(await cache.listRecordsByKeyPrefix("rooms.messages", "profile-room:", 10), [])
  assert.deepEqual((await cache.listRecordsByKeyPrefix("rooms.messages", "other-room:", 10)).map((row) => row.value.id), ["other-nested-2"])

  await client.syncCurrentUserEvents({ limit: 2 })
  assert.deepEqual((await cache.getUserEvents("profile-user", 10)).map((row) => row.id), ["profile-event-2"])
  assert.deepEqual(await cache.getUserProfile("profile-user"), syncedUserProfile)
  const profileMetadata = await cache.getMetadata()
  assert.equal(profileMetadata.maxPendingWrites, 1)
  assert.equal(profileMetadata.maxPendingWriteBytes, 1_000)

  await client.putObject("first", "text/plain")
  await client.putObject("second", "text/plain")
  assert.deepEqual((await cache.listObjects(10)).map((object) => object.id), ["profile-object-2"])

  await client.syncPull({ afterLsn: 0, objects: true, limit: 10 })
  assert.deepEqual((await cache.listObjects(10)).map((object) => object.id), ["profile-object-sync"])

  await client.syncPull({ afterLsn: 7, objects: true, limit: 10 })
  assert.deepEqual(await cache.listObjects(10), [])

  const metadata = await cache.getMetadata()
  assert.equal(metadata.maxObjects, 1)
  assert.equal(metadata.maxObjectBytes, 1_000)
  assert.equal(metadata.maxRoomMessages, 1)
  assert.equal(metadata.maxUserEvents, 1)
  assert.equal(metadata.maxRecordsPerTable, 1)
  assert.equal(metadata.maxNestedPartitions, 1)

  const manualCache = new MemoryLocalCache()
  const manualClient = new NextDbClient({ endpoint, userId: "profile-user", cache: manualCache })
  const cacheChanges = []
  const stopCacheChange = manualClient.onCacheChange((change) => cacheChanges.push(change))
  await manualCache.putObject(objectMetadata("manual-object-1", 1), new Blob(["first"], { type: "text/plain" }))
  await manualCache.putObject(objectMetadata("manual-object-2", 2), new Blob(["second"], { type: "text/plain" }))
  await manualCache.putRoomMessages("profile-room", messages)
  await manualCache.putUserEvents("profile-user", userEvents)
  await manualCache.putRecords([
    record("manual-room-1", 1),
    record("manual-room-2", 2),
    nestedRecord("manual-parent-a", "manual-nested-a1", 1),
    nestedRecord("manual-parent-a", "manual-nested-a2", 2),
    nestedRecord("manual-parent-b", "manual-nested-b1", 3),
    nestedRecord("manual-parent-b", "manual-nested-b2", 4),
  ])

  const enforced = await manualClient.enforceLocalCacheProfile({
    profile: {
      version: 99,
      leaseTtlMs: 60_000,
      maxObjects: 1,
      maxObjectBytes: 1_000,
      maxRoomMessages: 1,
      maxUserEvents: 1,
      maxRecordsPerTable: 1,
      maxNestedPartitions: 1,
      maxPendingWrites: 10,
      maxPendingWriteBytes: 100_000,
      offlineWrites: true,
    },
  })
  stopCacheChange()
  assert.equal(enforced.before.totalObjects, 2)
  assert.equal(enforced.before.totalMessages, 2)
  assert.equal(enforced.before.totalUserEvents, 2)
  assert.equal(enforced.before.tables.rooms, 2)
  assert.equal(enforced.before.nestedTables["rooms.messages"]["manual-parent-a:"], 2)
  assert.equal(enforced.before.nestedTables["rooms.messages"]["manual-parent-b:"], 2)
  assert.equal(enforced.removed.objects, 1)
  assert.deepEqual(enforced.removed.roomMessages, { "profile-room": 1 })
  assert.deepEqual(enforced.removed.userEvents, { "profile-user": 1 })
  assert.deepEqual(enforced.removed.records, { rooms: 1 })
  assert.deepEqual(enforced.removed.nestedRecords["rooms.messages"], {
    "manual-parent-a:": 1,
    "manual-parent-b:": 1,
  })
  assert.equal(enforced.removed.nestedPartitions["rooms.messages"], 1)
  assert.equal(enforced.removed.total, 7)
  assert.equal(enforced.after.totalObjects, 1)
  assert.equal(enforced.after.totalMessages, 1)
  assert.equal(enforced.after.totalUserEvents, 1)
  assert.equal(enforced.after.tables.rooms, 1)
  assert.equal(enforced.after.totalRecords, 2)
  assert.equal(cacheChanges.length, 1)
  assert.equal(cacheChanges[0].type, "cacheProfileEnforced")
  assert.equal(cacheChanges[0].result.removed.total, 7)

  console.log("cache profile smoke ok")
} finally {
  await new Promise((resolve) => server.close(resolve))
}

function message(id, lsn) {
  return {
    id,
    roomId: "profile-room",
    senderId: "profile-user",
    body: id,
    attachments: [],
    createdAtMs: lsn,
    lsn,
    path: `rooms/profile-room/messages/${id}`,
  }
}

function userEvent(id, lsn) {
  return {
    id,
    userId: "profile-user",
    name: "notification.created",
    payload: { id },
    createdAtMs: lsn,
    lsn,
    path: `users/profile-user/events/${id}`,
  }
}

function record(key, lsn) {
  return {
    table: "rooms",
    key,
    value: { id: key, title: key },
    updatedAtMs: lsn,
    lsn,
    path: `tables/rooms/${key}`,
  }
}

function nestedRecord(parentKey, id, lsn) {
  return {
    table: "rooms.messages",
    key: `${parentKey}:${id}`,
    value: {
      id,
      roomId: parentKey,
      body: id,
      createdAtMs: lsn,
    },
    updatedAtMs: lsn,
    lsn,
    path: `tables/rooms/${parentKey}/messages/${id}`,
  }
}

function objectMetadata(id, createdAtMs) {
  return {
    id,
    path: `objects/${id}`,
    contentType: "text/plain",
    byteSize: id.length,
    sha256: `${id}-sha`,
    createdAtMs,
  }
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = []
    request.on("data", (chunk) => chunks.push(chunk))
    request.on("error", reject)
    request.on("end", () => resolve(Buffer.concat(chunks)))
  })
}
