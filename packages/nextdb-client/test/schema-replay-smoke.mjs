import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient, NextDbHttpError } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-replay-"))
const node = {
  url: "http://127.0.0.1:3413",
  addr: "127.0.0.1:3413",
  dataDir: join(tempRoot, "data"),
}
let child

try {
  await mkdir(node.dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const db = new NextDbClient({ endpoint: node.url })
  const current = await db.getSchema()

  const withTopic = structuredClone(current)
  withTopic.version = current.version + 1
  withTopic.tables.rooms.fields.topic = { type: { kind: "string" }, optional: true }

  const added = await db.applySchema(withTopic, { expectedVersion: current.version })
  assert.equal(added.applied, true)
  assert.equal(added.persisted, true)
  assert.equal(added.replayRebuild, false)
  assert.equal(added.breakingReplayAllowed, false)
  assert.equal(added.projectionRebuilt, false)
  assert.equal(added.migration.compatible, true)

  const key = `schema-replay-${Date.now()}`
  await db.table("rooms").upsert(key, {
    id: key,
    title: "Replay-safe field removal",
    topic: "field to remove from schema",
  }, {
    clientMutationId: `${key}-upsert`,
  })

  const withoutTopic = structuredClone(withTopic)
  withoutTopic.version = withTopic.version + 1
  delete withoutTopic.tables.rooms.fields.topic

  await assert.rejects(
    db.applySchema(withoutTopic, { expectedVersion: withTopic.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /tables\.rooms\.fields\.topic cannot be removed/)
      return true
    },
  )

  const removed = await db.applySchema(withoutTopic, {
    expectedVersion: withTopic.version,
    allowBreakingReplay: true,
  })
  assert.equal(removed.applied, true)
  assert.equal(removed.persisted, true)
  assert.equal(removed.replayRebuild, true)
  assert.equal(removed.breakingReplayAllowed, true)
  assert.equal(removed.projectionRebuilt, true)
  assert.equal(removed.migration.compatible, false)
  assert.equal(removed.migration.requiresReplayRebuild, true)
  assert.equal(removed.migration.projectionRebuildRequired, true)
  assert(removed.migration.errors.includes("tables.rooms.fields.topic cannot be removed"))
  assert.deepEqual(removed.migration.replaySafeBreakingChanges, [
    "tables.rooms.fields.topic cannot be removed",
  ])
  assert.deepEqual(removed.migration.unsafeBreakingChanges, [])
  assert.deepEqual(removed.migration.projectionRebuildReasons, [
    "tables.rooms.fields.topic removed",
  ])
  assert(Number.isInteger(removed.schemaAuditLsn))
  assert.equal(removed.projectionStatus.records, 1)

  const latest = await db.getSchema()
  assert.equal(latest.version, withoutTopic.version)
  assert.equal(latest.tables.rooms.fields.topic, undefined)

  const record = await db.table("rooms").get(key)
  assert.equal(record.key, key)
  assert.equal(record.value.id, key)
  assert.equal(record.value.title, "Replay-safe field removal")

  const schemaAudit = await db.auditWal({ payloadType: "schemaApplied", limit: 50 })
  const removalAudit = schemaAudit.records.find((record) => record.lsn === removed.schemaAuditLsn)
  assert(removalAudit)
  assert.equal(removalAudit.payload.schema.version, withoutTopic.version)
  assert.equal(removalAudit.payload.migration.compatible, false)
  assert.equal(removalAudit.payload.migration.requiresReplayRebuild, true)
  assert.equal(removalAudit.payload.migration.projectionRebuildRequired, true)
  assert(removalAudit.payload.migration.errors.includes("tables.rooms.fields.topic cannot be removed"))
  assert.deepEqual(removalAudit.payload.migration.replaySafeBreakingChanges, [
    "tables.rooms.fields.topic cannot be removed",
  ])
  assert.deepEqual(removalAudit.payload.migration.projectionRebuildReasons, [
    "tables.rooms.fields.topic removed",
  ])

  const withReplayAsset = structuredClone(withoutTopic)
  withReplayAsset.version = withoutTopic.version + 1
  withReplayAsset.tables.rooms.fields.replayAsset = {
    type: { kind: "objectRef", object: "Object" },
    optional: true,
  }
  const addedReplayAsset = await db.applySchema(withReplayAsset, {
    expectedVersion: withoutTopic.version,
  })
  assert.equal(addedReplayAsset.applied, true)
  assert.equal(addedReplayAsset.migration.compatible, true)

  const replayObjectId = `schema-replay-object-ref-${Date.now()}`
  const replayObject = await db.putObject("schema replay object ref body", {
    contentType: "text/plain",
    objectId: replayObjectId,
    clientMutationId: `${replayObjectId}-put`,
  })
  const replayObjectRecordKey = `schema-replay-object-ref-record-${Date.now()}`
  const replayObjectRecord = await db.table("rooms").upsert(replayObjectRecordKey, {
    id: replayObjectRecordKey,
    title: "Replay ObjectRef field removal",
    replayAsset: replayObject,
  }, {
    clientMutationId: `${replayObjectRecordKey}-upsert`,
  })
  const replayRefsBefore = await db.getObjectReferences(replayObjectId)
  assert.equal(replayRefsBefore.objectExists, true)
  assert.equal(replayRefsBefore.refCount, 1)
  assert.deepEqual(replayRefsBefore.sources, [replayObjectRecord.path])

  const withoutReplayAsset = structuredClone(withReplayAsset)
  withoutReplayAsset.version = withReplayAsset.version + 1
  delete withoutReplayAsset.tables.rooms.fields.replayAsset

  await assert.rejects(
    db.applySchema(withoutReplayAsset, { expectedVersion: withReplayAsset.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /tables\.rooms\.fields\.replayAsset cannot be removed/)
      return true
    },
  )

  const removedReplayAsset = await db.applySchema(withoutReplayAsset, {
    expectedVersion: withReplayAsset.version,
    allowBreakingReplay: true,
  })
  assert.equal(removedReplayAsset.applied, true)
  assert.equal(removedReplayAsset.persisted, true)
  assert.equal(removedReplayAsset.replayRebuild, true)
  assert.equal(removedReplayAsset.projectionRebuilt, true)
  assert.equal(removedReplayAsset.migration.projectionRebuildRequired, true)
  assert.deepEqual(removedReplayAsset.migration.replaySafeBreakingChanges, [
    "tables.rooms.fields.replayAsset cannot be removed",
  ])
  assert.deepEqual(removedReplayAsset.migration.projectionRebuildReasons, [
    "tables.rooms.fields.replayAsset removed",
  ])
  const replayRefsAfter = await db.getObjectReferences(replayObjectId)
  assert.equal(replayRefsAfter.objectExists, true)
  assert.equal(replayRefsAfter.refCount, 0)
  assert.deepEqual(replayRefsAfter.sources, [])

  const withoutNotificationEvent = structuredClone(withoutReplayAsset)
  withoutNotificationEvent.version = withoutReplayAsset.version + 1
  delete withoutNotificationEvent.events["notification.created"]

  await assert.rejects(
    db.applySchema(withoutNotificationEvent, { expectedVersion: withoutReplayAsset.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /event notification\.created cannot be removed/)
      return true
    },
  )

  const removedEvent = await db.applySchema(withoutNotificationEvent, {
    expectedVersion: withoutReplayAsset.version,
    allowBreakingReplay: true,
  })
  assert.equal(removedEvent.applied, true)
  assert.equal(removedEvent.persisted, true)
  assert.equal(removedEvent.replayRebuild, true)
  assert.equal(removedEvent.breakingReplayAllowed, true)
  assert.equal(removedEvent.migration.compatible, false)
  assert.equal(removedEvent.migration.requiresReplayRebuild, true)
  assert.equal(removedEvent.migration.projectionRebuildRequired, false)
  assert(removedEvent.migration.errors.includes("event notification.created cannot be removed"))
  assert.deepEqual(removedEvent.migration.replaySafeBreakingChanges, [
    "event notification.created cannot be removed",
  ])
  assert.deepEqual(removedEvent.migration.unsafeBreakingChanges, [])
  assert.deepEqual(removedEvent.migration.projectionRebuildReasons, [])
  assert(Number.isInteger(removedEvent.schemaAuditLsn))

  const withoutEventLatest = await db.getSchema()
  assert.equal(withoutEventLatest.version, withoutNotificationEvent.version)
  assert.equal(withoutEventLatest.events["notification.created"], undefined)

  const eventRemovalAudit = (await db.auditWal({ payloadType: "schemaApplied", limit: 50 }))
    .records
    .find((record) => record.lsn === removedEvent.schemaAuditLsn)
  assert(eventRemovalAudit)
  assert.equal(eventRemovalAudit.payload.schema.version, withoutNotificationEvent.version)
  assert.deepEqual(eventRemovalAudit.payload.migration.replaySafeBreakingChanges, [
    "event notification.created cannot be removed",
  ])
  assert.deepEqual(eventRemovalAudit.payload.migration.unsafeBreakingChanges, [])

  const withAvatarObject = structuredClone(withoutNotificationEvent)
  withAvatarObject.version = withoutNotificationEvent.version + 1
  withAvatarObject.objects.Avatar = {
    fields: {
      id: { type: { kind: "id", entity: "Object" } },
      label: { type: { kind: "string" }, optional: true },
    },
  }

  const addedObject = await db.applySchema(withAvatarObject, {
    expectedVersion: withoutNotificationEvent.version,
  })
  assert.equal(addedObject.applied, true)
  assert.equal(addedObject.migration.compatible, true)

  const withoutAvatarObject = structuredClone(withAvatarObject)
  withoutAvatarObject.version = withAvatarObject.version + 1
  delete withoutAvatarObject.objects.Avatar

  await assert.rejects(
    db.applySchema(withoutAvatarObject, { expectedVersion: withAvatarObject.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /object Avatar cannot be removed/)
      return true
    },
  )

  const removedObject = await db.applySchema(withoutAvatarObject, {
    expectedVersion: withAvatarObject.version,
    allowBreakingReplay: true,
  })
  assert.equal(removedObject.applied, true)
  assert.equal(removedObject.persisted, true)
  assert.equal(removedObject.replayRebuild, true)
  assert.equal(removedObject.breakingReplayAllowed, true)
  assert.equal(removedObject.migration.compatible, false)
  assert.equal(removedObject.migration.requiresReplayRebuild, true)
  assert.equal(removedObject.migration.projectionRebuildRequired, false)
  assert(removedObject.migration.errors.includes("object Avatar cannot be removed"))
  assert.deepEqual(removedObject.migration.replaySafeBreakingChanges, [
    "object Avatar cannot be removed",
  ])
  assert.deepEqual(removedObject.migration.unsafeBreakingChanges, [])
  assert.deepEqual(removedObject.migration.projectionRebuildReasons, [])
  assert(Number.isInteger(removedObject.schemaAuditLsn))

  const withoutObjectLatest = await db.getSchema()
  assert.equal(withoutObjectLatest.version, withoutAvatarObject.version)
  assert.equal(withoutObjectLatest.objects.Avatar, undefined)

  const objectRemovalAudit = (await db.auditWal({ payloadType: "schemaApplied", limit: 50 }))
    .records
    .find((record) => record.lsn === removedObject.schemaAuditLsn)
  assert(objectRemovalAudit)
  assert.equal(objectRemovalAudit.payload.schema.version, withoutAvatarObject.version)
  assert.deepEqual(objectRemovalAudit.payload.migration.replaySafeBreakingChanges, [
    "object Avatar cannot be removed",
  ])
  assert.deepEqual(objectRemovalAudit.payload.migration.unsafeBreakingChanges, [])

  const withoutEchoTsBehavior = structuredClone(withoutAvatarObject)
  withoutEchoTsBehavior.version = withoutAvatarObject.version + 1
  delete withoutEchoTsBehavior.behaviors["echo-ts"]

  await assert.rejects(
    db.applySchema(withoutEchoTsBehavior, { expectedVersion: withoutAvatarObject.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /behavior echo-ts cannot be removed/)
      return true
    },
  )

  const removedBehavior = await db.applySchema(withoutEchoTsBehavior, {
    expectedVersion: withoutAvatarObject.version,
    allowBreakingReplay: true,
  })
  assert.equal(removedBehavior.applied, true)
  assert.equal(removedBehavior.persisted, true)
  assert.equal(removedBehavior.replayRebuild, true)
  assert.equal(removedBehavior.breakingReplayAllowed, true)
  assert.equal(removedBehavior.migration.compatible, false)
  assert.equal(removedBehavior.migration.requiresReplayRebuild, true)
  assert.equal(removedBehavior.migration.projectionRebuildRequired, false)
  assert(removedBehavior.migration.errors.includes("behavior echo-ts cannot be removed"))
  assert.deepEqual(removedBehavior.migration.replaySafeBreakingChanges, [
    "behavior echo-ts cannot be removed",
  ])
  assert.deepEqual(removedBehavior.migration.unsafeBreakingChanges, [])
  assert.deepEqual(removedBehavior.migration.projectionRebuildReasons, [])
  assert(Number.isInteger(removedBehavior.schemaAuditLsn))

  const withoutBehaviorLatest = await db.getSchema()
  assert.equal(withoutBehaviorLatest.version, withoutEchoTsBehavior.version)
  assert.equal(withoutBehaviorLatest.behaviors["echo-ts"], undefined)

  const behaviorRemovalAudit = (await db.auditWal({ payloadType: "schemaApplied", limit: 50 }))
    .records
    .find((record) => record.lsn === removedBehavior.schemaAuditLsn)
  assert(behaviorRemovalAudit)
  assert.equal(behaviorRemovalAudit.payload.schema.version, withoutEchoTsBehavior.version)
  assert.deepEqual(behaviorRemovalAudit.payload.migration.replaySafeBreakingChanges, [
    "behavior echo-ts cannot be removed",
  ])
  assert.deepEqual(behaviorRemovalAudit.payload.migration.unsafeBreakingChanges, [])

  const nestedRoomId = `schema-replay-nested-room-${Date.now()}`
  const nestedKey = `schema-replay-nested-${Date.now()}`
  await db.table("rooms").upsert(nestedRoomId, {
    id: nestedRoomId,
    title: "Nested table removal",
  }, {
    clientMutationId: `${nestedRoomId}-upsert`,
  })
  await db.nestedTable("rooms", nestedRoomId, "messages").upsert(
    nestedKey,
    {
      id: nestedKey,
      roomId: nestedRoomId,
      senderId: "schema-replay-user",
      body: "retained nested WAL fact",
      attachments: [],
      createdAtMs: Date.now(),
      path: `tables/rooms/${nestedRoomId}/messages/${nestedKey}`,
    },
    { clientMutationId: `${nestedKey}-upsert` },
  )

  const withoutMessagesNested = structuredClone(withoutEchoTsBehavior)
  withoutMessagesNested.version = withoutEchoTsBehavior.version + 1
  delete withoutMessagesNested.tables.rooms.nested.messages

  await assert.rejects(
    db.applySchema(withoutMessagesNested, { expectedVersion: withoutEchoTsBehavior.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /nested table rooms\.messages cannot be removed/)
      return true
    },
  )

  const removedNested = await db.applySchema(withoutMessagesNested, {
    expectedVersion: withoutEchoTsBehavior.version,
    allowBreakingReplay: true,
  })
  assert.equal(removedNested.applied, true)
  assert.equal(removedNested.persisted, true)
  assert.equal(removedNested.replayRebuild, true)
  assert.equal(removedNested.breakingReplayAllowed, true)
  assert.equal(removedNested.migration.compatible, false)
  assert.equal(removedNested.migration.requiresReplayRebuild, true)
  assert.equal(removedNested.migration.projectionRebuildRequired, false)
  assert(removedNested.migration.errors.includes("nested table rooms.messages cannot be removed"))
  assert.deepEqual(removedNested.migration.replaySafeBreakingChanges, [
    "nested table rooms.messages cannot be removed",
  ])
  assert.deepEqual(removedNested.migration.unsafeBreakingChanges, [])
  assert.deepEqual(removedNested.migration.projectionRebuildReasons, [])
  assert(Number.isInteger(removedNested.schemaAuditLsn))

  const withoutNestedLatest = await db.getSchema()
  assert.equal(withoutNestedLatest.version, withoutMessagesNested.version)
  assert.equal(withoutNestedLatest.tables.rooms.nested.messages, undefined)

  const nestedReadAfterRemoval = await fetch(
    `${node.url}/v1/records/rooms/${encodeURIComponent(nestedRoomId)}/messages/${encodeURIComponent(nestedKey)}`,
  )
  assert.equal(nestedReadAfterRemoval.status, 404)

  const nestedRemovalAudit = (await db.auditWal({ payloadType: "schemaApplied", limit: 50 }))
    .records
    .find((record) => record.lsn === removedNested.schemaAuditLsn)
  assert(nestedRemovalAudit)
  assert.equal(nestedRemovalAudit.payload.schema.version, withoutMessagesNested.version)
  assert.deepEqual(nestedRemovalAudit.payload.migration.replaySafeBreakingChanges, [
    "nested table rooms.messages cannot be removed",
  ])
  assert.deepEqual(nestedRemovalAudit.payload.migration.unsafeBreakingChanges, [])

  const withAuditLogsTable = structuredClone(withoutMessagesNested)
  withAuditLogsTable.version = withoutMessagesNested.version + 1
  withAuditLogsTable.tables.auditLogs = {
    storage: { kind: "disk" },
    fields: {
      id: { type: { kind: "id", entity: "AuditLog" } },
      message: { type: { kind: "string" } },
    },
    nested: {},
    readVisibility: { all: [] },
    indexes: {},
  }

  const addedAuditLogs = await db.applySchema(withAuditLogsTable, {
    expectedVersion: withoutMessagesNested.version,
  })
  assert.equal(addedAuditLogs.applied, true)
  assert.equal(addedAuditLogs.migration.compatible, true)

  const auditLogKey = `schema-replay-audit-log-${Date.now()}`
  await db.table("auditLogs").upsert(auditLogKey, {
    id: auditLogKey,
    message: "retained table WAL fact",
  }, {
    clientMutationId: `${auditLogKey}-upsert`,
  })

  const withoutAuditLogsTable = structuredClone(withAuditLogsTable)
  withoutAuditLogsTable.version = withAuditLogsTable.version + 1
  delete withoutAuditLogsTable.tables.auditLogs

  await assert.rejects(
    db.applySchema(withoutAuditLogsTable, { expectedVersion: withAuditLogsTable.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /table auditLogs cannot be removed/)
      return true
    },
  )

  const removedTable = await db.applySchema(withoutAuditLogsTable, {
    expectedVersion: withAuditLogsTable.version,
    allowBreakingReplay: true,
  })
  assert.equal(removedTable.applied, true)
  assert.equal(removedTable.persisted, true)
  assert.equal(removedTable.replayRebuild, true)
  assert.equal(removedTable.breakingReplayAllowed, true)
  assert.equal(removedTable.migration.compatible, false)
  assert.equal(removedTable.migration.requiresReplayRebuild, true)
  assert.equal(removedTable.migration.projectionRebuildRequired, false)
  assert(removedTable.migration.errors.includes("table auditLogs cannot be removed"))
  assert.deepEqual(removedTable.migration.replaySafeBreakingChanges, [
    "table auditLogs cannot be removed",
  ])
  assert.deepEqual(removedTable.migration.unsafeBreakingChanges, [])
  assert.deepEqual(removedTable.migration.projectionRebuildReasons, [])
  assert(Number.isInteger(removedTable.schemaAuditLsn))

  const withoutTableLatest = await db.getSchema()
  assert.equal(withoutTableLatest.version, withoutAuditLogsTable.version)
  assert.equal(withoutTableLatest.tables.auditLogs, undefined)

  const tableReadAfterRemoval = await fetch(
    `${node.url}/v1/records/auditLogs/${encodeURIComponent(auditLogKey)}`,
  )
  assert.equal(tableReadAfterRemoval.status, 404)

  const tableRemovalAudit = (await db.auditWal({ payloadType: "schemaApplied", limit: 50 }))
    .records
    .find((record) => record.lsn === removedTable.schemaAuditLsn)
  assert(tableRemovalAudit)
  assert.equal(tableRemovalAudit.payload.schema.version, withoutAuditLogsTable.version)
  assert.deepEqual(tableRemovalAudit.payload.migration.replaySafeBreakingChanges, [
    "table auditLogs cannot be removed",
  ])
  assert.deepEqual(tableRemovalAudit.payload.migration.unsafeBreakingChanges, [])

  const incompatibleTypeChange = structuredClone(withoutAuditLogsTable)
  incompatibleTypeChange.version = withoutAuditLogsTable.version + 1
  incompatibleTypeChange.tables.rooms.fields.title.type = { kind: "text", inlineUntil: 128 }
  await assert.rejects(
    db.applySchema(incompatibleTypeChange, {
      expectedVersion: withoutAuditLogsTable.version,
      allowBreakingReplay: true,
    }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /tables\.rooms\.fields\.title type cannot change/)
      return true
    },
  )

  db.close()
  console.log("schema replay smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node) {
  return spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: node.addr,
      NEXTDB_DATA_DIR: node.dataDir,
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
}

async function stopNode(child) {
  if (child.exitCode !== null) {
    return
  }
  child.kill("SIGINT")
  await new Promise((resolve) => child.once("exit", resolve))
}

async function waitForHealth(baseUrl) {
  await waitFor(async () => {
    try {
      const response = await fetch(`${baseUrl}/v1/health`)
      return response.ok
    } catch {
      return false
    }
  }, `health ${baseUrl}`)
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}`)
}
