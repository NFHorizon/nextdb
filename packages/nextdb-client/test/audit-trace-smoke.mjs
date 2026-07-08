import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
const userId = `trace-user-${suffix}`
const roomId = `trace-room-${suffix}`
const nestedKey = `trace-message-${suffix}`
const objectId = `trace-object-${suffix}`
const replayRecordKey = `replay-room-${suffix}`
const replayObjectId = `replay-object-${suffix}`
const db = new NextDbClient({
  endpoint,
  userId,
  sessionId: `trace-session-${suffix}`,
})

try {
  const object = await db.putObject("trace object body", {
    contentType: "text/plain",
    objectId,
    clientMutationId: `trace-object-put-${suffix}`,
  })

  const room = await db.table("rooms").upsert(
    roomId,
    { id: roomId, title: "Trace Room" },
    { clientMutationId: `trace-room-upsert-${suffix}` },
  )

  const nestedPath = `tables/rooms/${roomId}/messages/${nestedKey}`
  const nested = await db.nestedTable("rooms", roomId, "messages").upsert(
    nestedKey,
    {
      id: nestedKey,
      roomId,
      senderId: userId,
      body: "nested trace",
      createdAtMs: Date.now(),
      attachments: [object],
      path: nestedPath,
    },
    { clientMutationId: `trace-nested-upsert-${suffix}` },
  )

  const message = await db.room(roomId).messages.send("trace room message", {
    attachments: [object.id],
    clientMutationId: `trace-message-send-${suffix}`,
  })

  const userEvent = await db.publishUserEvent(
    userId,
    "notification.created",
    { text: "trace user event" },
    { clientMutationId: `trace-user-event-${suffix}` },
  )
  const userProfile = await db.upsertUser(userId, {
    displayName: "Trace User",
    metadata: { suffix },
    clientMutationId: `trace-user-profile-${suffix}`,
  })

  const recordTrace = await db.traceEntity({
    kind: "record",
    table: "rooms",
    recordKey: roomId,
  })
  assert.equal(recordTrace.target.kind, "record")
  assert.equal(recordTrace.target.table, "rooms")
  assert.equal(recordTrace.target.recordKey, roomId)
  assert(recordTrace.records.some((record) =>
    record.payload.type === "recordUpserted" &&
    record.payload.record.table === "rooms" &&
    record.payload.record.key === roomId &&
    record.payload.record.lsn === undefined,
  ))

  const nestedTrace = await db.traceEntity({
    kind: "nestedRecord",
    table: "rooms",
    parentKey: roomId,
    nested: "messages",
    nestedKey,
  })
  assert.equal(nestedTrace.target.recordKey, `${roomId}:${nestedKey}`)
  assert(nestedTrace.records.some((record) =>
    record.payload.type === "recordUpserted" &&
    record.payload.record.table === "rooms.messages" &&
    record.payload.record.key === `${roomId}:${nestedKey}`,
  ))

  const pathTrace = await db.traceEntity({ kind: "path", path: nested.path })
  assert(pathTrace.records.some((record) =>
    record.payload.type === "recordUpserted" &&
    record.payload.record.path === nested.path,
  ))

  const objectTrace = await db.traceEntity({ kind: "object", id: objectId })
  assert(objectTrace.records.some((record) =>
    record.payload.type === "objectCommitted" &&
    record.payload.object.id === objectId,
  ))
  assert(objectTrace.records.some((record) =>
    record.payload.type === "messageCreated" &&
    record.payload.message.id === message.id,
  ))
  assert(objectTrace.records.some((record) =>
    record.payload.type === "recordUpserted" &&
    record.payload.record.key === `${roomId}:${nestedKey}`,
  ))

  const userTrace = await db.traceEntity({ kind: "user", id: userId })
  assert(userTrace.records.some((record) =>
    record.payload.type === "messageCreated" &&
    record.payload.message.senderId === userId,
  ))
  assert(userTrace.records.some((record) =>
    record.payload.type === "userEventPublished" &&
    record.payload.event.id === userEvent.id,
  ))

  const mutationTrace = await db.traceEntity({
    kind: "clientMutation",
    clientMutationId: `trace-message-send-${suffix}`,
  })
  assert.equal(mutationTrace.records.length, 1)
  assert.equal(mutationTrace.records[0].payload.type, "messageCreated")
  assert.equal(mutationTrace.records[0].payload.message.id, message.id)

  const roomTraceFirst = await db.traceEntity({ kind: "room", id: roomId, limit: 1 })
  assert.equal(roomTraceFirst.records.length, 1)
  assert.equal(roomTraceFirst.hasMore, true)
  const roomTraceNext = await db.traceEntity({
    kind: "room",
    id: roomId,
    afterLsn: roomTraceFirst.nextAfterLsn,
    limit: 20,
  })
  assert(roomTraceNext.records.some((record) => record.lsn > roomTraceFirst.nextAfterLsn))
  const roomPayloadTypes = [...roomTraceFirst.records, ...roomTraceNext.records].map((record) => record.payload.type)
  assert(roomPayloadTypes.includes("recordUpserted"))
  assert(roomPayloadTypes.includes("messageCreated"))

  const emptyTrace = await db.traceEntity({
    kind: "record",
    table: "rooms",
    recordKey: `missing-${suffix}`,
  })
  assert.equal(emptyTrace.records.length, 0)
  assert.equal(emptyTrace.nextAfterLsn, 0)

  const missingReplay = await db.replayEntity({
    kind: "record",
    table: "rooms",
    recordKey: replayRecordKey,
    atLsn: 0,
  })
  assert.equal(missingReplay.status, "missing")
  assert.equal(missingReplay.record, undefined)
  assert.equal(missingReplay.sourceLsn, undefined)

  const replayRecord = await db.table("rooms").upsert(
    replayRecordKey,
    { id: replayRecordKey, title: "Replay Room v1" },
    { clientMutationId: `replay-room-upsert-${suffix}` },
  )
  const replayAtUpsert = await db.replayEntity({
    kind: "record",
    table: "rooms",
    recordKey: replayRecordKey,
    atLsn: replayRecord.lsn,
  })
  assert.equal(replayAtUpsert.status, "exists")
  assert.equal(replayAtUpsert.sourceLsn, replayRecord.lsn)
  assert.equal(replayAtUpsert.record?.key, replayRecordKey)
  assert.equal(replayAtUpsert.record?.value.title, "Replay Room v1")

  const replayDelete = await db.table("rooms").delete(replayRecordKey, {
    clientMutationId: `replay-room-delete-${suffix}`,
  })
  const replayBeforeDelete = await db.replayEntity({
    kind: "record",
    table: "rooms",
    recordKey: replayRecordKey,
    atLsn: replayRecord.lsn,
  })
  assert.equal(replayBeforeDelete.status, "exists")
  assert.equal(replayBeforeDelete.record?.key, replayRecordKey)
  const replayAfterDelete = await db.replayEntity({
    kind: "record",
    table: "rooms",
    recordKey: replayRecordKey,
    atLsn: replayDelete.lsn,
  })
  assert.equal(replayAfterDelete.status, "deleted")
  assert.equal(replayAfterDelete.sourceLsn, replayDelete.lsn)
  assert.equal(replayAfterDelete.delete?.table, "rooms")
  assert.equal(replayAfterDelete.delete?.key, replayRecordKey)

  const nestedReplay = await db.replayEntity({
    kind: "nestedRecord",
    table: "rooms",
    parentKey: roomId,
    nested: "messages",
    nestedKey,
    atLsn: nested.lsn,
  })
  assert.equal(nestedReplay.status, "exists")
  assert.equal(nestedReplay.record?.table, "rooms.messages")
  assert.equal(nestedReplay.record?.key, `${roomId}:${nestedKey}`)
  assert.equal(nestedReplay.record?.value.body, "nested trace")

  const userReplay = await db.replayEntity({
    kind: "user",
    id: userId,
    atLsn: userProfile.lsn,
  })
  assert.equal(userReplay.status, "exists")
  assert.equal(userReplay.sourceLsn, userProfile.lsn)
  assert.equal(userReplay.user?.userId, userId)
  assert.equal(userReplay.user?.displayName, "Trace User")

  const replayObject = await db.putObject("replay object body", {
    contentType: "text/plain",
    objectId: replayObjectId,
    clientMutationId: `replay-object-put-${suffix}`,
  })
  const replayObjectTrace = await db.traceEntity({ kind: "object", id: replayObjectId, limit: 10 })
  const replayObjectCommit = replayObjectTrace.records.find((record) =>
    record.payload.type === "objectCommitted" &&
    record.payload.object.id === replayObjectId,
  )
  assert(replayObjectCommit)
  const objectAtCommit = await db.replayEntity({
    kind: "object",
    id: replayObjectId,
    atLsn: replayObjectCommit.lsn,
  })
  assert.equal(objectAtCommit.status, "exists")
  assert.equal(objectAtCommit.object?.id, replayObject.id)
  const replayObjectDelete = await db.deleteObject(replayObjectId, {
    clientMutationId: `replay-object-delete-${suffix}`,
  })
  const objectAfterDelete = await db.replayEntity({
    kind: "object",
    id: replayObjectId,
    atLsn: replayObjectDelete.lsn,
  })
  assert.equal(objectAfterDelete.status, "deleted")
  assert.equal(objectAfterDelete.sourceLsn, replayObjectDelete.lsn)
  assert.equal(objectAfterDelete.delete?.objectId, replayObjectId)

  assert.equal(room.table, "rooms")
  console.log("audit trace smoke ok")
} finally {
  db.close()
}
