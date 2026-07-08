import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:net"
import { spawn } from "node:child_process"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { performance } from "node:perf_hooks"
import { fileURLToPath } from "node:url"

import { NextDbClient } from "../packages/nextdb-client/dist/index.js"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-soak-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const wsEndpoint = `ws://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const config = {
  durationMs: numberEnv("NEXTDB_SOAK_DURATION_MS", 30_000),
  concurrency: numberEnv("NEXTDB_SOAK_CONCURRENCY", 16),
  healthIntervalMs: numberEnv("NEXTDB_SOAK_HEALTH_INTERVAL_MS", 2_500),
  objectBytes: numberEnv("NEXTDB_SOAK_OBJECT_BYTES", 512),
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const roomId = `soak-room-${suffix}`
  const client = new NextDbClient({ endpoint, userId: `soak-writer-${suffix}` })
  const subscription = await openRoomSocket({ userId: `soak-live-${suffix}`, roomId })
  const room = await client.table("rooms").upsert(roomId, {
    id: roomId,
    title: "Soak Room",
  }, {
    durability: "strict",
    clientMutationId: `${roomId}-upsert`,
  })
  assert(room.lsn > 0)

  let samplingError
  const healthSamples = []
  const readinessSamples = []
  const sampleTimer = setInterval(() => {
    void sampleRuntime(client, healthSamples, readinessSamples).catch((error) => {
      samplingError ??= error
    })
  }, config.healthIntervalMs)

  const durableMessageIds = new Set()
  const stats = new Map()
  let durableOperationCount = 1
  let operationIndex = 0
  const pending = new Set()
  const started = performance.now()
  const deadline = started + config.durationMs

  try {
    while (performance.now() < deadline) {
      while (pending.size < config.concurrency && performance.now() < deadline) {
        const index = operationIndex++
        const operation = operationFor(index)
        const task = runOperation({
          client,
          suffix,
          roomId,
          index,
          operation,
          durableMessageIds,
          stats,
        }).then((result) => {
          if (result.durable) {
            durableOperationCount += 1
          }
        }).finally(() => {
          pending.delete(task)
        })
        pending.add(task)
      }
      if (pending.size > 0) {
        await Promise.race(pending)
      }
    }
    await Promise.all(pending)
  } finally {
    clearInterval(sampleTimer)
  }

  if (samplingError) {
    throw samplingError
  }
  await sampleRuntime(client, healthSamples, readinessSamples)
  await waitFor(
    () => [...durableMessageIds].every((id) => subscription.deliveredMessageIds.has(id)),
    "durable message subscription delivery",
    5_000,
  )

  const health = await client.health()
  const readiness = await client.readiness()
  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  const wait = await client.waitForLsn(durableOperationCount, { timeoutMs: 1_000 })
  const projectionStatus = await client.getProjectionStatus()

  assert.equal(health.ok, true)
  assert.equal(readiness.ok, true)
  assert.equal(readiness.writeReady, true)
  assert.equal(wait.caughtUp, true)
  assert.equal(health.currentLsn, durableOperationCount)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, durableOperationCount)
  assert(projectionStatus.records >= 1, JSON.stringify(projectionStatus))
  assert(projectionStatus.keyOrderEntries >= 1, JSON.stringify(projectionStatus))
  assert(healthSamples.length >= 1)
  assert(readinessSamples.length >= 1)
  assert([...stats.values()].every((entry) => entry.count > 0), "all configured operation classes should run")

  const elapsedMs = performance.now() - started
  const result = {
    ok: true,
    endpoint,
    config,
    measuredAtMs: Date.now(),
    elapsedMs: round(elapsedMs),
    operations: {
      total: [...stats.values()].reduce((sum, entry) => sum + entry.count, 0),
      durable: durableOperationCount,
      durableMessagesDelivered: durableMessageIds.size,
      subscriptionDelivered: subscription.deliveredMessageIds.size,
      opsPerSecond: round([...stats.values()].reduce((sum, entry) => sum + entry.count, 0) / (elapsedMs / 1000)),
    },
    health: {
      currentLsn: health.currentLsn,
      roomCount: health.roomCount,
      hotRoomCount: health.hotRoomCount,
      walShardCount: health.walShardCount,
      runtimeId: health.runtimeId,
      samples: healthSamples.length,
    },
    readiness: {
      samples: readinessSamples.length,
      readReady: readiness.readReady,
      writeReady: readiness.writeReady,
      realtimeReady: readiness.realtimeReady,
    },
    wal: {
      recordCount: integrity.recordCount,
    },
    projections: projectionStatus,
    scenarios: [...stats.values()].map(summarizeStats),
  }

  if (process.env.NEXTDB_SOAK_OUT) {
    const outputPath = resolve(process.env.NEXTDB_SOAK_OUT)
    await mkdir(dirname(outputPath), { recursive: true })
    await writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`)
  }

  subscription.socket.close()
  client.close()

  console.log("nextdb soak ok")
  console.log(JSON.stringify(result, null, 2))
} finally {
  if (child) {
    await stopNode(child)
  }
  if (process.env.NEXTDB_SOAK_KEEP_DATA !== "true") {
    await rm(tempRoot, { recursive: true, force: true })
  } else {
    console.log(`kept soak data at ${dataDir}`)
  }
}

function operationFor(index) {
  if (index % 19 === 0) {
    return "record:list:keyOrder"
  }
  if (index % 17 === 0) {
    return "object:put"
  }
  if (index % 13 === 0) {
    return "record:index:exact"
  }
  if (index % 7 === 0) {
    return "record:strict"
  }
  if (index % 5 === 0) {
    return "message:volatile"
  }
  if (index % 2 === 0) {
    return "message:relaxed"
  }
  return "message:strict"
}

async function runOperation({ client, suffix, roomId, index, operation, durableMessageIds, stats }) {
  const started = performance.now()
  try {
    if (operation === "message:strict" || operation === "message:relaxed" || operation === "message:volatile") {
      const durability = operation.split(":")[1]
      const message = await client.room(roomId).messages.send(`soak ${operation} ${index}`, {
        durability,
        clientMutationId: `${roomId}-${operation}-${index}`,
      })
      if (durability === "volatile") {
        assert.equal(message.lsn, 0)
      } else {
        assert(message.lsn > 0)
        durableMessageIds.add(message.id)
      }
      recordStats(stats, operation, performance.now() - started)
      return { durable: durability !== "volatile" }
    }
    if (operation === "record:strict") {
      const key = `soak-record-${suffix}-${index}`
      const record = await client.table("rooms").upsert(key, {
        id: key,
        title: `Soak Record ${index}`,
      }, {
        durability: "strict",
        clientMutationId: key,
      })
      assert(record.lsn > 0)
      recordStats(stats, operation, performance.now() - started)
      return { durable: true }
    }
    if (operation === "record:list:keyOrder") {
      const page = await client.table("rooms").list({ limit: 32 })
      assert(page.records.length > 0)
      assert.deepEqual(
        page.records.map((record) => record.key),
        page.records.map((record) => record.key).toSorted(),
      )
      recordStats(stats, operation, performance.now() - started)
      return { durable: false }
    }
    if (operation === "record:index:exact") {
      const page = await client.table("rooms").index("byTitle", {
        value: "Soak Room",
        limit: 8,
      })
      assert(page.records.some((record) => record.key === roomId))
      recordStats(stats, operation, performance.now() - started)
      return { durable: false }
    }
    if (operation === "object:put") {
      const object = await client.putObject("x".repeat(config.objectBytes), {
        contentType: "text/plain",
        objectId: `soak-object-${suffix}-${index}`,
        clientMutationId: `soak-object-${suffix}-${index}`,
      })
      assert.equal(object.byteSize, config.objectBytes)
      recordStats(stats, operation, performance.now() - started)
      return { durable: true }
    }
    throw new Error(`unknown operation ${operation}`)
  } catch (error) {
    error.message = `${operation} ${index} failed: ${error.message}`
    throw error
  }
}

async function sampleRuntime(client, healthSamples, readinessSamples) {
  const [health, readiness] = await Promise.all([
    client.health(),
    client.readiness(),
  ])
  assert.equal(health.ok, true)
  assert.equal(readiness.ok, true)
  assert.equal(readiness.readReady, true)
  healthSamples.push({
    sampledAtMs: Date.now(),
    currentLsn: health.currentLsn,
    roomCount: health.roomCount,
    hotRoomCount: health.hotRoomCount,
  })
  readinessSamples.push({
    sampledAtMs: Date.now(),
    readReady: readiness.readReady,
    writeReady: readiness.writeReady,
    realtimeReady: readiness.realtimeReady,
  })
}

async function openRoomSocket({ userId, roomId }) {
  const socket = new WebSocket(`${wsEndpoint}/v1/connect?userId=${encodeURIComponent(userId)}`)
  const deliveredMessageIds = new Set()
  const frames = []
  socket.addEventListener("message", (event) => {
    const frame = JSON.parse(event.data)
    frames.push(frame)
    if (frame.type === "event" && frame.event?.type === "messageCreated" && frame.event.message?.lsn > 0) {
      deliveredMessageIds.add(frame.event.message.id)
    }
  })
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true })
    socket.addEventListener("error", reject, { once: true })
  })
  await waitFor(() => frames.some((frame) => frame.type === "hello"), "websocket hello")
  socket.send(JSON.stringify({
    type: "subscribeRoom",
    roomId,
    afterLsn: 0,
    catchUpLimit: 100,
  }))
  await waitFor(
    () => frames.some((frame) => frame.type === "subscribed" && frame.roomId === roomId),
    "room subscription",
  )
  return { socket, deliveredMessageIds }
}

function recordStats(stats, name, latencyMs) {
  const entry = stats.get(name) ?? {
    name,
    count: 0,
    latenciesMs: [],
  }
  entry.count += 1
  entry.latenciesMs.push(latencyMs)
  stats.set(name, entry)
}

function summarizeStats(entry) {
  const sorted = entry.latenciesMs.toSorted((left, right) => left - right)
  const elapsedMs = sorted.reduce((sum, value) => sum + value, 0)
  return {
    name: entry.name,
    count: entry.count,
    avgLatencyMs: round(elapsedMs / entry.count),
    latencyMs: {
      min: percentile(sorted, 0),
      p50: percentile(sorted, 0.50),
      p95: percentile(sorted, 0.95),
      p99: percentile(sorted, 0.99),
      max: percentile(sorted, 1),
    },
  }
}

function percentile(sorted, p) {
  if (sorted.length === 0) {
    return 0
  }
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1))
  return round(sorted[index])
}

function round(value) {
  return Math.round(value * 100) / 100
}

function numberEnv(name, fallback) {
  const raw = process.env[name]
  if (raw === undefined) {
    return fallback
  }
  const value = Number(raw)
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number`)
  }
  return Math.floor(value)
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_SOAK_LOGS === "1") {
      process.stdout.write(`[soak] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_SOAK_LOGS === "1") {
      process.stderr.write(`[soak] ${chunk}`)
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

async function waitFor(predicate, label, timeoutMs = 15_000) {
  const started = Date.now()
  let lastError
  while (Date.now() - started < timeoutMs) {
    try {
      if (await predicate()) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
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
