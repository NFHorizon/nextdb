import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-actor-policy-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)

  const db = new NextDbClient({ endpoint, userId: "schema-actor-policy-user" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const firstRoomId = `schema-actor-a-${suffix}`
  const secondRoomId = `schema-actor-b-${suffix}`

  const firstMessages = await writeRoom(db, firstRoomId, 5)
  let health = await db.health()
  assert(health.hotWindow > 2)
  assert(health.maxHotRooms > 1)
  assert.equal(health.hotRoomCount, 1)

  const schema = await db.getSchema()
  schema.version += 1
  schema.tables.rooms.storage = { kind: "lru", maxItems: 1 }
  schema.tables.rooms.nested.messages.storage.liveWindow = 2
  const applied = await db.applySchema(schema, { expectedVersion: schema.version - 1 })
  assert.equal(applied.applied, true)

  health = await db.health()
  assert.equal(health.hotWindow, 2)
  assert.equal(health.maxHotRooms, 1)
  assert.equal(health.hotRoomCount, 1)
  const policy = await db.getStoragePolicy()
  assert.equal(policy.hotWindow, 2)
  assert.equal(policy.maxHotRooms, 1)

  const firstLatest = await db.room(firstRoomId).messages.latest({ limit: 5 })
  assert.deepEqual(
    firstLatest.messages.map((message) => message.id),
    firstMessages.toReversed().map((message) => message.id),
  )
  assert.deepEqual(
    firstLatest.messages.slice(0, 2).map((message) => message.id),
    firstMessages.slice(-2).reverse().map((message) => message.id),
  )

  const secondMessages = await writeRoom(db, secondRoomId, 3)
  health = await db.health()
  assert.equal(health.hotWindow, 2)
  assert.equal(health.maxHotRooms, 1)
  assert.equal(health.hotRoomCount, 1)

  const firstAfterLru = await db.room(firstRoomId).messages.latest({ limit: 5 })
  assert.deepEqual(
    firstAfterLru.messages.map((message) => message.id),
    firstMessages.toReversed().map((message) => message.id),
  )
  const secondLatest = await db.room(secondRoomId).messages.latest({ limit: 3 })
  assert.deepEqual(
    secondLatest.messages.map((message) => message.id),
    secondMessages.toReversed().map((message) => message.id),
  )

  const prepared = await db.prepareRestart({
    reason: "schema actor policy smoke",
    snapshot: true,
    compactWal: false,
    waitForWritesMs: 1_000,
  })
  assert.equal(prepared.readyForRestart, true)
  assert.equal(prepared.snapshot?.roomCount, 1)
  db.close()

  await stopNode(child, "SIGKILL")
  child = undefined

  child = spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)
  const recovered = new NextDbClient({ endpoint, userId: "schema-actor-policy-user" })
  const recoveredHealth = await recovered.health()
  assert.equal(recoveredHealth.hotWindow, 2)
  assert.equal(recoveredHealth.maxHotRooms, 1)
  assert.equal(recoveredHealth.startupRecovery.snapshotLoaded, true)
  assert.equal(recoveredHealth.hotRoomCount, 1)
  const recoveredFirst = await recovered.room(firstRoomId).messages.latest({ limit: 5 })
  assert.deepEqual(
    recoveredFirst.messages.map((message) => message.id),
    firstMessages.toReversed().map((message) => message.id),
  )
  const recoveredSecond = await recovered.room(secondRoomId).messages.latest({ limit: 3 })
  assert.deepEqual(
    recoveredSecond.messages.map((message) => message.id),
    secondMessages.toReversed().map((message) => message.id),
  )
  recovered.close()

  console.log("schema actor policy smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function writeRoom(client, roomId, count) {
  await client.table("rooms").upsert(roomId, {
    id: roomId,
    title: `Schema Actor ${roomId}`,
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  const messages = []
  for (let index = 0; index < count; index += 1) {
    messages.push(await client.room(roomId).messages.send(`message-${index}`, {
      clientMutationId: `${roomId}-message-${index}`,
    }))
  }
  return messages
}

async function stopNode(child, signal = "SIGTERM") {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill(signal)
  await Promise.race([
    once(child, "exit"),
    new Promise((resolve) => setTimeout(resolve, 5_000)).then(() => {
      child.kill("SIGKILL")
      return once(child, "exit").catch(() => {})
    }),
  ])
}

async function waitForHealth(url) {
  await waitFor(async () => {
    const response = await fetch(`${url}/v1/health`).catch(() => undefined)
    return response?.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 15_000) {
  const started = Date.now()
  while (Date.now() - started < timeoutMs) {
    if (await check()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}`)
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      const port = typeof address === "object" && address ? address.port : undefined
      server.close((error) => {
        if (error) {
          reject(error)
          return
        }
        if (port === undefined) {
          reject(new Error("failed to allocate local port"))
          return
        }
        resolve(port)
      })
    })
  })
}
