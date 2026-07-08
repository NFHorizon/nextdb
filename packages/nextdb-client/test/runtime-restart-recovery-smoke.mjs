import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-runtime-restart-"))
const dataDir = join(tempRoot, "data")
const node = {
  url: "http://127.0.0.1:3396",
  addr: "127.0.0.1:3396",
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)
  const first = new NextDbClient({ endpoint: node.url, userId: "alice" })

  const roomId = `restart-room-${Date.now()}`
  const objectId = `restart-object-${Date.now()}`
  const userId = `restart-user-${Date.now()}`
  const objectBody = "restart object body"
  const beforeMessage = "before snapshot"
  const afterMessage = "after snapshot"

  const room = await first.table("rooms").upsert(roomId, {
    id: roomId,
    title: "Restart Room",
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  const object = await first.putObject(objectBody, {
    contentType: "text/plain",
    objectId,
    clientMutationId: `${objectId}-put`,
  })
  assert.equal(object.id, objectId)
  const before = await first.room(roomId).messages.send(beforeMessage, {
    attachments: [objectId],
    clientMutationId: `${roomId}-message-before`,
  })
  assert(before.lsn > room.lsn)
  const userProfile = await first.upsertUser(userId, {
    displayName: "Restart User",
    metadata: { phase: "before-restart" },
    clientMutationId: `${userId}-profile`,
  })
  const firstUserEvent = await first.publishUserEvent(userId, "notification.created", { text: "first" }, {
    clientMutationId: `${userId}-event-first`,
  })
  const secondUserEvent = await first.publishUserEvent(userId, "notification.created", { text: "second" }, {
    clientMutationId: `${userId}-event-second`,
  })
  assert(firstUserEvent.lsn > userProfile.lsn)
  assert(secondUserEvent.lsn > firstUserEvent.lsn)

  const prepared = await first.prepareRestart({
    reason: "runtime restart recovery smoke",
    snapshot: true,
    compactWal: false,
    waitForWritesMs: 1_000,
  })
  assert.equal(prepared.readyForRestart, true)
  assert.equal(prepared.writeWaitTimedOut, false)
  assert(prepared.snapshot)
  assert.equal(prepared.snapshot.lsn, prepared.currentLsn)
  assert(prepared.snapshot.lsn >= before.lsn)
  assert(prepared.snapshot.recordHotTableCount >= 1)
  assert(prepared.snapshot.recordHotRecordCount >= 1)

  await first.setRuntimeDraining(false, "runtime restart recovery smoke writes after snapshot")
  const after = await first.room(roomId).messages.send(afterMessage, {
    clientMutationId: `${roomId}-message-after`,
  })
  assert(after.lsn > prepared.snapshot.lsn)

  await stopNode(child, "SIGKILL")
  child = undefined

  child = startNode(node)
  await waitForHealth(node.url)
  const second = new NextDbClient({ endpoint: node.url, userId: "alice" })
  const health = await second.health()
  assert.equal(health.ok, true)
  assert.equal(health.currentLsn, after.lsn)
  assert.equal(health.startupRecovery.snapshotLoaded, true)
  assert.equal(health.startupRecovery.snapshotLsn, prepared.snapshot.lsn)
  assert.equal(health.startupRecovery.snapshotRecordHotTableCount, prepared.snapshot.recordHotTableCount)
  assert.equal(health.startupRecovery.snapshotRecordHotRecordCount, prepared.snapshot.recordHotRecordCount)
  assert.equal(health.startupRecovery.highestLsn, after.lsn)
  assert(health.startupRecovery.walRecordsAfterSnapshot >= 1)
  assert(health.startupRecovery.rebuiltMessages >= 2)
  assert(health.startupRecovery.rebuiltRecords >= 1)
  assert(health.startupRecovery.rebuiltObjectRefs >= 1)
  assert(health.recordHotCache.recordCount >= 1)

  const restoredRoom = await second.table("rooms").get(roomId, { minLsn: after.lsn })
  assert.equal(restoredRoom.value.title, "Restart Room")

  const restoredUser = await second.getUser(userId, { minLsn: after.lsn })
  assert.equal(restoredUser.displayName, "Restart User")
  assert.equal(restoredUser.lsn, userProfile.lsn)
  const users = await second.listUsers({ limit: 10, minLsn: after.lsn })
  assert(users.users.some((user) => user.userId === userId && user.lsn === userProfile.lsn))
  const userEvents = await second.listUserEvents(userId, { limit: 10, minLsn: after.lsn })
  assert.deepEqual(userEvents.map((event) => event.id), [secondUserEvent.id, firstUserEvent.id])
  const previousUserEvents = await second.listUserEvents(userId, {
    limit: 10,
    beforeLsn: secondUserEvent.lsn,
    minLsn: after.lsn,
  })
  assert.deepEqual(previousUserEvents.map((event) => event.id), [firstUserEvent.id])

  const restoredObjectBody = await second.getObjectBody(objectId, { minLsn: after.lsn })
  assert.equal(await restoredObjectBody.text(), objectBody)

  const messages = await second.room(roomId).messages.latest({ limit: 10, minLsn: after.lsn })
  assert.equal(messages.messages.length, 2)
  assert.deepEqual(messages.messages.map((message) => message.body), [afterMessage, beforeMessage])
  assert.deepEqual(messages.messages.find((message) => message.body === beforeMessage)?.attachments.map((attachment) => attachment.id), [objectId])

  const duplicateAfter = await second.room(roomId).messages.send("duplicate after restart", {
    clientMutationId: `${roomId}-message-after`,
  })
  assert.equal(duplicateAfter.id, after.id)
  assert.equal(duplicateAfter.lsn, after.lsn)
  assert.equal(duplicateAfter.body, afterMessage)

  const duplicateRoom = await second.table("rooms").upsert(roomId, {
    id: roomId,
    title: "Duplicate Restart Room",
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  assert.equal(duplicateRoom.lsn, room.lsn)
  assert.equal(duplicateRoom.value.title, "Restart Room")

  const integrity = await getJson(`${node.url}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, 7)

  console.log("runtime restart recovery smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node) {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: node.dataDir,
      NEXTDB_ADDR: node.addr,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_RUNTIME_RESTART_SMOKE_LOGS === "1") {
      process.stdout.write(`[restart] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_RUNTIME_RESTART_SMOKE_LOGS === "1") {
      process.stderr.write(`[restart] ${chunk}`)
    }
  })
  return child
}

async function stopNode(child, signal = "SIGTERM") {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill(signal)
  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL")
      resolve()
    }, 5_000)
    child.once("exit", () => {
      clearTimeout(timeout)
      resolve()
    })
  })
}

async function waitForHealth(url) {
  await waitFor(async () => {
    const health = await getJson(`${url}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 15_000) {
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
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}
