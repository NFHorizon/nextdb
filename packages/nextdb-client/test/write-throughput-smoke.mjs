import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { performance } from "node:perf_hooks"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, process.env.NEXTDB_SERVER_BIN ?? "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-write-throughput-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const messageCount = Number(process.env.NEXTDB_WRITE_THROUGHPUT_MESSAGES ?? "240")
const concurrency = Number(process.env.NEXTDB_WRITE_THROUGHPUT_CONCURRENCY ?? "64")
const walBatchMax = Number(process.env.NEXTDB_WRITE_THROUGHPUT_WAL_BATCH_MAX ?? "64")
const walBatchWaitMs = Number(process.env.NEXTDB_WRITE_THROUGHPUT_WAL_BATCH_WAIT_MS ?? "1")
const latestLimit = 12
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const client = new NextDbClient({ endpoint, userId: "throughput-writer" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const strict = await writeDurableScenario(client, suffix, "strict")
  const relaxed = await writeDurableScenario(client, suffix, "relaxed")
  const durableHighestLsn = Math.max(strict.highestLsn, relaxed.highestLsn)

  const health = await client.health()
  assert.equal(health.ok, true)
  assert.equal(health.currentLsn, durableHighestLsn)
  assert.equal(health.roomCount, 2)
  assert(health.hotRoomCount >= 2)
  assert.equal(typeof health.checkpointInFlight, "boolean")
  const walLocal = health.walReplicas.map((replica) => replica.remoteStatus)
  assert.equal(walLocal.every((status) => status.batchMax === walBatchMax), true)
  assert.equal(walLocal.every((status) => status.batchWaitMs === walBatchWaitMs), true)
  assert.equal(sum(walLocal, "localRecords"), (messageCount * 2) + 2)
  assert(sum(walLocal, "localBatches") > 0)
  assert(sum(walLocal, "localBytes") > 0)
  assert(sum(walLocal, "queueCapacity") > 0)
  assert(walLocal.some((status) => status.localLastBatchRecords > 0))
  const metrics = await client.metrics()
  assert.match(metrics, /^nextdb_checkpoint_in_flight [01]$/m)
  assert.match(metrics, /^nextdb_wal_batch_max\{shard="0"\} \d+$/m)
  assert.match(metrics, /^nextdb_wal_batch_wait_ms\{shard="0"\} \d+$/m)
  assert.match(metrics, /^nextdb_wal_local_records\{shard="0"\} \d+$/m)
  assert.match(metrics, /^nextdb_wal_queue_depth\{shard="0"\} \d+$/m)

  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, (messageCount * 2) + 2)

  const prepared = await client.prepareRestart({
    reason: "write throughput smoke",
    snapshot: true,
    compactWal: false,
    waitForWritesMs: 1_000,
  })
  assert.equal(prepared.readyForRestart, true)
  assert.equal(prepared.writeWaitTimedOut, false)
  assert.equal(prepared.currentLsn, durableHighestLsn)
  assert.equal(prepared.snapshot?.lsn, durableHighestLsn)

  client.close()
  await stopNode(child, "SIGKILL")
  child = undefined

  child = startNode()
  await waitForHealth(endpoint)
  const recovered = new NextDbClient({ endpoint, userId: "throughput-reader" })
  const recoveredHealth = await recovered.health()
  assert.equal(recoveredHealth.currentLsn, durableHighestLsn)
  assert.equal(recoveredHealth.startupRecovery.snapshotLoaded, true)
  assert.equal(recoveredHealth.startupRecovery.snapshotLsn, durableHighestLsn)
  assert.equal(recoveredHealth.startupRecovery.rebuiltMessages, messageCount * 2)

  await assertLatestIds(recovered, strict.roomId, strict.expectedLatestIds, durableHighestLsn)
  await assertLatestIds(recovered, relaxed.roomId, relaxed.expectedLatestIds, durableHighestLsn)

  const volatile = await writeVolatileScenario(recovered, suffix, durableHighestLsn)
  const afterVolatileHealth = await recovered.health()
  assert.equal(afterVolatileHealth.currentLsn, durableHighestLsn)
  const afterVolatileIntegrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(afterVolatileIntegrity.ok, true)
  assert.equal(afterVolatileIntegrity.recordCount, (messageCount * 2) + 2)
  recovered.close()

  await stopNode(child, "SIGKILL")
  child = undefined

  child = startNode()
  await waitForHealth(endpoint)
  const afterVolatileRestart = new NextDbClient({ endpoint, userId: "throughput-reader" })
  const afterVolatileRestartHealth = await afterVolatileRestart.health()
  assert.equal(afterVolatileRestartHealth.currentLsn, durableHighestLsn)
  await assertLatestIds(afterVolatileRestart, strict.roomId, strict.expectedLatestIds, durableHighestLsn)
  await assertLatestIds(afterVolatileRestart, relaxed.roomId, relaxed.expectedLatestIds, durableHighestLsn)
  const volatileAfterRestart = await afterVolatileRestart.room(volatile.roomId).messages.latest({ limit: latestLimit })
  assert.equal(volatileAfterRestart.messages.length, 0)
  afterVolatileRestart.close()

  console.log([
    "write throughput smoke ok:",
    formatResult(strict),
    formatResult(relaxed),
    formatResult(volatile),
  ].join(" "))
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function writeDurableScenario(client, suffix, durability) {
  const roomId = `throughput-${durability}-room-${suffix}`
  const room = await client.table("rooms").upsert(roomId, {
    id: roomId,
    title: `Throughput ${durability} Room`,
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  assert(room.lsn > 0)

  const started = performance.now()
  const messages = await writeMessages(client, roomId, messageCount, concurrency, durability)
  const elapsedMs = performance.now() - started
  assert.equal(messages.length, messageCount)

  const lsns = messages.map((message) => message.lsn)
  assert.equal(new Set(lsns).size, messageCount)
  assert.equal(lsns.every((lsn) => lsn > room.lsn), true)
  const highestLsn = Math.max(...lsns)
  const expectedLatestIds = messages
    .toSorted((left, right) => right.lsn - left.lsn)
    .slice(0, latestLimit)
    .map((message) => message.id)
  await assertLatestIds(client, roomId, expectedLatestIds, highestLsn)

  return {
    durability,
    roomId,
    messages,
    elapsedMs,
    highestLsn,
    expectedLatestIds,
  }
}

async function writeVolatileScenario(client, suffix, durableHighestLsn) {
  const roomId = `throughput-volatile-room-${suffix}`
  const started = performance.now()
  const messages = await writeMessages(client, roomId, messageCount, concurrency, "volatile")
  const elapsedMs = performance.now() - started
  assert.equal(messages.length, messageCount)
  assert.equal(messages.every((message) => message.lsn === 0), true)
  assert.equal(messages.every((message) => message.path.startsWith("volatile/")), true)

  const messageIds = new Set(messages.map((message) => message.id))
  const latest = await client.room(roomId).messages.latest({ limit: latestLimit })
  assert.equal(latest.source, "live")
  assert.equal(latest.messages.length, latestLimit)
  assert.equal(latest.messages.every((message) => message.lsn === 0 && messageIds.has(message.id)), true)

  const sync = await client.syncPull({ rooms: [roomId], afterLsn: 0, limit: messageCount + 10 })
  assert.equal(sync.events.some((event) => event.type === "messageCreated" && messageIds.has(event.message.id)), false)
  const health = await client.health()
  assert.equal(health.currentLsn, durableHighestLsn)

  return {
    durability: "volatile",
    roomId,
    messages,
    elapsedMs,
    highestLsn: 0,
    expectedLatestIds: latest.messages.map((message) => message.id),
  }
}

async function assertLatestIds(client, roomId, expectedIds, minLsn) {
  const latest = await client.room(roomId).messages.latest({
    limit: expectedIds.length,
    minLsn,
  })
  assert.deepEqual(latest.messages.map((message) => message.id), expectedIds)
}

async function writeMessages(client, roomId, count, parallelism, durability) {
  const pending = new Set()
  const messages = []
  for (let index = 0; index < count; index += 1) {
    const task = client.room(roomId).messages.send(`throughput message ${index}`, {
      durability,
      clientMutationId: `${roomId}-${durability}-message-${index}`,
    }).then((message) => {
      messages.push(message)
    }).finally(() => {
      pending.delete(task)
    })
    pending.add(task)
    if (pending.size >= parallelism) {
      await Promise.race(pending)
    }
  }
  await Promise.all(pending)
  return messages
}

function formatResult(result) {
  const opsPerSecond = Math.round(messageCount / (result.elapsedMs / 1000))
  return `${result.durability}=${messageCount} messages/${result.elapsedMs.toFixed(1)}ms/${opsPerSecond} msg/s`
}

function sum(items, key) {
  return items.reduce((total, item) => total + item[key], 0)
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
      NEXTDB_WAL_BATCH_MAX: String(walBatchMax),
      NEXTDB_WAL_BATCH_WAIT_MS: String(walBatchWaitMs),
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_WRITE_THROUGHPUT_SMOKE_LOGS === "1") {
      process.stdout.write(`[throughput] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_WRITE_THROUGHPUT_SMOKE_LOGS === "1") {
      process.stderr.write(`[throughput] ${chunk}`)
    }
  })
  return child
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
