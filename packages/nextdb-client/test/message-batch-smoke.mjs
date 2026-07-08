import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, process.env.NEXTDB_SERVER_BIN ?? "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-message-batch-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const batchSize = 16
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const client = new NextDbClient({ endpoint, userId: "batch-writer" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const strict = await writeDurableBatch(client, suffix, "strict")
  const relaxed = await writeDurableBatch(client, suffix, "relaxed")
  const volatile = await writeVolatileBatch(client, suffix)

  const health = await client.health()
  assert.equal(health.ok, true)
  assert.equal(health.currentLsn, Math.max(strict.highestLsn, relaxed.highestLsn))

  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, (batchSize * 2) + 2)
  assert.equal(volatile.messages.every((message) => message.lsn === 0), true)

  client.close()
  console.log("message batch smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function writeDurableBatch(client, suffix, durability) {
  const roomId = `batch-${durability}-room-${suffix}`
  const room = await client.table("rooms").upsert(roomId, {
    id: roomId,
    title: `Batch ${durability} Room`,
  }, {
    durability: "strict",
    clientMutationId: `${roomId}-upsert`,
  })
  const bodies = Array.from({ length: batchSize }, (_, index) => `batch ${durability} message ${index}`)
  const items = bodies.map((body, index) => ({
    body,
    clientMutationId: `${roomId}-message-${index}`,
  }))
  const messages = await client.room(roomId).messages.sendMany(items, { durability })
  assert.equal(messages.length, batchSize)
  assert.deepEqual(messages.map((message) => message.body), bodies)
  assert(messages.every((message) => message.lsn > room.lsn))
  assert.equal(new Set(messages.map((message) => message.lsn)).size, batchSize)
  assert.equal(new Set(messages.map((message) => message.id)).size, batchSize)

  const replayed = await client.room(roomId).messages.sendMany(items, { durability })
  assert.deepEqual(replayed.map((message) => message.id), messages.map((message) => message.id))
  assert.deepEqual(replayed.map((message) => message.lsn), messages.map((message) => message.lsn))

  const latestAfterReplay = await client.room(roomId).messages.latest({ limit: batchSize * 2 })
  assert.equal(latestAfterReplay.messages.length, batchSize)
  assert.equal(new Set(latestAfterReplay.messages.map((message) => message.id)).size, batchSize)

  const highestLsn = Math.max(...messages.map((message) => message.lsn))
  const latest = await client.room(roomId).messages.latest({
    limit: batchSize,
    minLsn: highestLsn,
  })
  assert.deepEqual(
    latest.messages.map((message) => message.id),
    messages.toSorted((left, right) => right.lsn - left.lsn).map((message) => message.id),
  )

  const cached = await client.room(roomId).messages.cached({ limit: batchSize })
  assert.equal(cached.messages.length, batchSize)

  return {
    roomId,
    messages,
    highestLsn,
  }
}

async function writeVolatileBatch(client, suffix) {
  const roomId = `batch-volatile-room-${suffix}`
  const bodies = Array.from({ length: 10 }, (_, index) => `batch volatile message ${index}`)
  const messages = await client.room(roomId).messages.sendMany(bodies, "volatile")
  assert.equal(messages.length, bodies.length)
  assert.deepEqual(messages.map((message) => message.body), bodies)
  assert(messages.every((message) => message.lsn === 0))
  assert(messages.every((message) => message.path.startsWith("volatile/")))

  const latest = await client.room(roomId).messages.latest({ limit: bodies.length })
  assert.equal(latest.source, "live")
  assert.deepEqual(
    latest.messages.map((message) => message.id),
    messages.toReversed().map((message) => message.id),
  )

  return {
    roomId,
    messages,
  }
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_WAL_SHARDS: process.env.NEXTDB_WAL_SHARDS ?? "4",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_MESSAGE_BATCH_SMOKE_LOGS === "1") {
      process.stdout.write(`[message-batch] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_MESSAGE_BATCH_SMOKE_LOGS === "1") {
      process.stderr.write(`[message-batch] ${chunk}`)
    }
  })
  return child
}

async function waitForHealth(endpoint, timeoutMs = 30_000) {
  const started = Date.now()
  while (Date.now() - started < timeoutMs) {
    if (child?.exitCode !== null || child?.signalCode !== null) {
      throw new Error(`nextdb exited before health: code=${child.exitCode} signal=${child.signalCode}`)
    }
    try {
      const health = await getJson(`${endpoint}/v1/health`)
      if (health.ok) {
        return
      }
    } catch {
      // Server is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error("timed out waiting for nextdb health")
}

async function stopNode(child, signal = "SIGTERM") {
  if (child.exitCode !== null || child.signalCode !== null) {
    return
  }
  child.kill(signal)
  await once(child, "exit")
}

async function getJson(url) {
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`${url} failed: ${response.status} ${await response.text()}`)
  }
  return response.json()
}

async function freePort() {
  const server = createServer()
  await new Promise((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const address = server.address()
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
  return address.port
}
