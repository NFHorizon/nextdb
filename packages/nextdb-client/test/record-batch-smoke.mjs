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
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-record-batch-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const db = new NextDbClient({ endpoint, userId: "record-batch-user" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const topology = await db.clusterTopology()
  assert.equal(topology.shardCount, 4)
  const roomId = await keyForShard(db, `record-batch-room-${suffix}`, 0)
  const rooms = db.table("rooms")
  const roomItems = await Promise.all(Array.from({ length: topology.shardCount }, async (_, shard) => {
    const key = shard === 0
      ? roomId
      : await keyForShard(db, `record-batch-peer-${suffix}-${shard}`, shard)
    return {
      key,
      value: {
        id: key,
        title: shard === 0 ? "Record Batch Parent" : `Record Batch Peer ${shard}`,
      },
    }
  }))
  const roomRecords = await rooms.upsertMany(roomItems, {
    clientMutationId: `record-batch-rooms-${suffix}`,
  })
  assert.equal(roomRecords.length, topology.shardCount)
  assert.deepEqual(roomRecords.map((record) => record.key), roomItems.map((record) => record.key))
  assert.equal(new Set(roomRecords.map((record) => record.lsn)).size, topology.shardCount)
  assert(roomRecords.every((record) => record.lsn > 0))
  assert.equal((await rooms.cache.get(roomId))?.value.title, "Record Batch Parent")

  const messages = db.nestedTable("rooms", roomId, "messages")
  const now = Date.now()
  const nestedRecords = await messages.upsertMany([
    {
      key: `message-a-${suffix}`,
      value: {
        id: `message-a-${suffix}`,
        roomId,
        senderId: "record-batch-user",
        body: "batch nested a",
        attachments: [],
        createdAtMs: now,
        path: `rooms/${roomId}/messages/message-a-${suffix}`,
      },
    },
    {
      key: `message-b-${suffix}`,
      value: {
        id: `message-b-${suffix}`,
        roomId,
        senderId: "record-batch-user",
        body: "batch nested b",
        attachments: [],
        createdAtMs: now + 1,
        path: `rooms/${roomId}/messages/message-b-${suffix}`,
      },
    },
  ], {
    clientMutationId: `record-batch-nested-${suffix}`,
  })
  assert.equal(nestedRecords.length, 2)
  assert.equal(new Set(nestedRecords.map((record) => record.lsn)).size, 1)
  assert(nestedRecords.every((record) => record.lsn > roomRecords[0].lsn))

  const listed = await messages.list({ limit: 10 })
  assert.deepEqual(
    listed.records.map((record) => record.key),
    [`${roomId}:message-a-${suffix}`, `${roomId}:message-b-${suffix}`],
  )
  assert.equal((await messages.cache.get(`message-a-${suffix}`))?.value.body, "batch nested a")

  const directBatchItems = await Promise.all(Array.from({ length: topology.shardCount }, async (_, shard) => {
    const key = await keyForShard(db, `record-batch-direct-${suffix}-${shard}`, shard)
    return {
      type: "upsert",
      table: "rooms",
      key,
      value: {
        id: key,
        title: `Record Batch Direct ${shard}`,
      },
    }
  }))
  const directBatch = await db.recordBatch(directBatchItems, {
    clientMutationId: `record-batch-direct-${suffix}`,
  })
  assert.equal(directBatch.transactionCount, topology.shardCount)
  assert.equal(directBatch.operations.length, topology.shardCount)
  assert.equal(new Set(directBatch.operations.map((operation) => operation.record.lsn)).size, topology.shardCount)

  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, topology.shardCount * 2 + 1)

  db.close()
  console.log("record batch smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_WAL_SHARDS: "4",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_RECORD_BATCH_SMOKE_LOGS === "1") {
      process.stdout.write(`[record-batch] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_RECORD_BATCH_SMOKE_LOGS === "1") {
      process.stderr.write(`[record-batch] ${chunk}`)
    }
  })
  return child
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
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
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
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`${url} failed: ${response.status} ${text}`)
  }
  return JSON.parse(text)
}

async function keyForShard(db, prefix, targetShard) {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const key = `${prefix}-${attempt}`
    const route = await db.clusterRoute({ key: `rooms:${key}` })
    if (route.shard === targetShard) {
      return key
    }
  }
  throw new Error(`failed to find key for shard ${targetShard}`)
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
