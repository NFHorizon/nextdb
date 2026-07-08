import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:net"
import { spawn } from "node:child_process"
import { createHash } from "node:crypto"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { performance } from "node:perf_hooks"
import { fileURLToPath } from "node:url"

import { NextDbClient } from "../packages/nextdb-client/dist/index.js"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const serverBin = resolve(root, process.env.NEXTDB_SERVER_BIN ?? "target/release/nextdb-server")
const externalEndpoint = process.env.NEXTDB_BENCH_ENDPOINT
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-benchmark-"))
const port = externalEndpoint ? undefined : await freePort()
const endpoint = externalEndpoint ?? `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const config = {
  messageCount: numberEnv("NEXTDB_BENCH_MESSAGES", 480),
  messageBatchSize: numberEnv("NEXTDB_BENCH_MESSAGE_BATCH_SIZE", 80),
  recordCount: numberEnv("NEXTDB_BENCH_RECORDS", 240),
  recordBatchSize: numberEnv("NEXTDB_BENCH_RECORD_BATCH_SIZE", 80),
  recordListPages: numberEnv("NEXTDB_BENCH_RECORD_LIST_PAGES", 32),
  recordListPageSize: numberEnv("NEXTDB_BENCH_RECORD_LIST_PAGE_SIZE", 20),
  walShards: numberEnv("NEXTDB_WAL_SHARDS", 4),
  objectCount: numberEnv("NEXTDB_BENCH_OBJECTS", 64),
  concurrency: numberEnv("NEXTDB_BENCH_CONCURRENCY", 24),
  objectBytes: numberEnv("NEXTDB_BENCH_OBJECT_BYTES", 1024),
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  if (!externalEndpoint) {
    child = startNode()
  }
  await waitForHealth(endpoint)
  const initialHealth = await getJson(`${endpoint}/v1/health`)
  const initialIntegrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)

  const client = new NextDbClient({ endpoint, userId: "benchmark-writer" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const strict = await benchMessages(client, suffix, "strict")
  const relaxed = await benchMessages(client, suffix, "relaxed")
  const volatile = await benchMessages(client, suffix, "volatile")
  const strictBatch = await benchMessagesBatch(client, suffix, "strict")
  const relaxedBatch = await benchMessagesBatch(client, suffix, "relaxed")
  const volatileBatch = await benchMessagesBatch(client, suffix, "volatile")
  const records = await benchRecords(client, suffix)
  const recordsBatch = await benchRecordsBatch(client, suffix)
  const recordsCrossShardBatch = await benchRecordsCrossShardBatch(client, suffix)
  const objects = await benchObjects(client, suffix)
  const recordListCold = await benchRecordListCold(client)
  const recordListWarm = await benchRecordListWarm(client)
  const projectionStatus = await client.getProjectionStatus()
  const health = await client.health()
  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  const durableOperationCount =
    strict.count +
    relaxed.count +
    strictBatch.count +
    relaxedBatch.count +
    records.walRecordCount +
    recordsBatch.walRecordCount +
    recordsCrossShardBatch.walRecordCount +
    objects.count +
    4

  assert.equal(health.ok, true)
  assert.equal(integrity.ok, true, JSON.stringify(integrity))
  assert.equal(
    integrity.recordCount - initialIntegrity.recordCount,
    durableOperationCount,
    JSON.stringify({ initialIntegrity, integrity }),
  )
  assert.equal(
    health.currentLsn - initialHealth.currentLsn,
    durableOperationCount,
    JSON.stringify({ initialHealth, health, integrity }),
  )
  assert(projectionStatus.keyOrderEntries >= config.recordCount, JSON.stringify(projectionStatus))
  assert.equal(volatile.highestLsn, 0)
  assert.equal(volatileBatch.highestLsn, 0)

  client.close()

  const result = {
    ok: true,
    endpoint,
    config,
    measuredAtMs: Date.now(),
    health: {
      currentLsn: health.currentLsn,
      roomCount: health.roomCount,
      hotRoomCount: health.hotRoomCount,
      walShardCount: health.walShardCount,
      runtimeId: health.runtimeId,
    },
    wal: {
      recordCount: integrity.recordCount,
      durableOperationCount,
    },
    projections: projectionStatus,
    scenarios: [
      strict,
      relaxed,
      volatile,
      strictBatch,
      relaxedBatch,
      volatileBatch,
      records,
      recordsBatch,
      recordsCrossShardBatch,
      objects,
      recordListCold,
      recordListWarm,
    ],
  }

  if (process.env.NEXTDB_BENCH_OUT) {
    const outputPath = resolve(process.env.NEXTDB_BENCH_OUT)
    await mkdir(dirname(outputPath), { recursive: true })
    await writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`)
  }

  console.log("nextdb benchmark ok")
  console.log(JSON.stringify(result, null, 2))
} finally {
  if (child) {
    await stopNode(child)
  }
  if (process.env.NEXTDB_BENCH_KEEP_DATA !== "true") {
    await rm(tempRoot, { recursive: true, force: true })
  } else {
    console.log(`kept benchmark data at ${dataDir}`)
  }
}

async function benchMessages(client, suffix, durability) {
  const count = config.messageCount
  const roomId = `bench-${durability}-room-${suffix}`
  const room = durability === "volatile"
    ? undefined
    : await client.table("rooms").upsert(roomId, {
      id: roomId,
      title: `Benchmark ${durability} Room`,
    }, {
      durability: "strict",
      clientMutationId: `${roomId}-upsert`,
    })
  const result = await runBatch({
    name: `messages:${durability}`,
    count,
    concurrency: config.concurrency,
    run: (index) => client.room(roomId).messages.send(`benchmark ${durability} message ${index}`, {
      durability,
      clientMutationId: `${roomId}-message-${index}`,
    }),
  })
  const lsns = result.values.map((message) => message.lsn)
  if (durability === "volatile") {
    assert(lsns.every((lsn) => lsn === 0))
  } else {
    assert(lsns.every((lsn) => lsn > room.lsn))
    assert.equal(new Set(lsns).size, count)
    const latest = await client.room(roomId).messages.latest({
      limit: Math.min(20, count),
      minLsn: Math.max(...lsns),
    })
    assert.equal(latest.messages.length, Math.min(20, count))
  }
  return summarize(result, {
    durability,
    highestLsn: Math.max(...lsns),
  })
}

async function benchMessagesBatch(client, suffix, durability) {
  const count = config.messageCount
  const batchSize = Math.min(config.messageBatchSize, count)
  const batchCount = Math.ceil(count / batchSize)
  const roomId = `bench-${durability}-batch-room-${suffix}`
  const room = durability === "volatile"
    ? undefined
    : await client.table("rooms").upsert(roomId, {
      id: roomId,
      title: `Benchmark ${durability} Batch Room`,
    }, {
      durability: "strict",
      clientMutationId: `${roomId}-upsert`,
    })
  const result = await runBatch({
    name: `messages:${durability}:batch`,
    count: batchCount,
    concurrency: config.concurrency,
    run: (batchIndex) => {
      const offset = batchIndex * batchSize
      const size = Math.min(batchSize, count - offset)
      const messages = Array.from({ length: size }, (_, index) =>
        `benchmark ${durability} batch message ${offset + index}`)
      return client.room(roomId).messages.sendMany(messages, { durability })
    },
  })
  const messages = result.values.flat()
  assert.equal(messages.length, count, `${result.name} completed message count`)
  const lsns = messages.map((message) => message.lsn)
  if (durability === "volatile") {
    assert(lsns.every((lsn) => lsn === 0))
  } else {
    assert(lsns.every((lsn) => lsn > room.lsn))
    assert.equal(new Set(lsns).size, count)
    const latest = await client.room(roomId).messages.latest({
      limit: Math.min(20, count),
      minLsn: Math.max(...lsns),
    })
    assert.equal(latest.messages.length, Math.min(20, count))
  }
  return summarize({
    ...result,
    count,
    latenciesMs: result.latenciesMs.flatMap((latency, index) => {
      const offset = index * batchSize
      const size = Math.min(batchSize, count - offset)
      return Array.from({ length: size }, () => latency)
    }),
    values: messages,
  }, {
    durability,
    batchSize,
    batchCount,
    highestLsn: Math.max(...lsns),
  })
}

async function benchRecords(client, suffix) {
  const table = "rooms"
  const result = await runBatch({
    name: "records:strict",
    count: config.recordCount,
    concurrency: config.concurrency,
    run: (index) => client.table(table).upsert(`bench-record-${suffix}-${index}`, {
      id: `bench-record-${suffix}-${index}`,
      title: `Benchmark Record ${index}`,
    }, {
      durability: "strict",
      clientMutationId: `bench-record-${suffix}-${index}`,
    }),
  })
  const lsns = result.values.map((record) => record.lsn)
  assert.equal(new Set(lsns).size, config.recordCount)
  return summarize(result, {
    durability: "strict",
    highestLsn: Math.max(...lsns),
    walRecordCount: config.recordCount,
  })
}

async function benchRecordsBatch(client, suffix) {
  const table = "rooms"
  const count = config.recordCount
  const batchSize = Math.min(config.recordBatchSize, count)
  const batchCount = Math.ceil(count / batchSize)
  const result = await runBatch({
    name: "records:strict:batch",
    count: batchCount,
    concurrency: config.concurrency,
    run: (batchIndex) => {
      const offset = batchIndex * batchSize
      const size = Math.min(batchSize, count - offset)
      const records = Array.from({ length: size }, (_, index) => {
        const recordIndex = offset + index
        const id = keyForShard(
          `bench-record-batch-${suffix}-${recordIndex}`,
          config.walShards,
          batchIndex % config.walShards,
        )
        return {
          key: id,
          value: {
            id,
            title: `Benchmark Batch Record ${recordIndex}`,
          },
        }
      })
      return client.table(table).upsertMany(records, {
        durability: "strict",
        clientMutationId: `bench-record-batch-${suffix}-${batchIndex}`,
      })
    },
  })
  const records = result.values.flat()
  assert.equal(records.length, count, `${result.name} completed record count`)
  const lsns = records.map((record) => record.lsn)
  assert.equal(new Set(records.map((record) => record.key)).size, count)
  assert.equal(new Set(lsns).size, batchCount)
  return summarize({
    ...result,
    count,
    latenciesMs: result.latenciesMs.flatMap((latency, index) => {
      const offset = index * batchSize
      const size = Math.min(batchSize, count - offset)
      return Array.from({ length: size }, () => latency)
    }),
    values: records,
  }, {
    durability: "strict",
    batchSize,
    batchCount,
    highestLsn: Math.max(...lsns),
    walRecordCount: batchCount,
  })
}

async function benchRecordsCrossShardBatch(client, suffix) {
  const table = "rooms"
  const count = config.recordCount
  const batchSize = Math.min(config.recordBatchSize, count)
  const batchCount = Math.ceil(count / batchSize)
  const shardsPerFullBatch = Math.min(config.walShards, batchSize)
  const result = await runBatch({
    name: "records:strict:batch:crossShard",
    count: batchCount,
    concurrency: config.concurrency,
    run: (batchIndex) => {
      const offset = batchIndex * batchSize
      const size = Math.min(batchSize, count - offset)
      const records = Array.from({ length: size }, (_, index) => {
        const recordIndex = offset + index
        const targetShard = index % config.walShards
        const id = keyForShard(
          `bench-record-cross-shard-batch-${suffix}-${recordIndex}`,
          config.walShards,
          targetShard,
        )
        return {
          key: id,
          value: {
            id,
            title: `Benchmark Cross Shard Batch Record ${recordIndex}`,
          },
        }
      })
      return client.table(table).upsertMany(records, {
        durability: "strict",
        clientMutationId: `bench-record-cross-shard-batch-${suffix}-${batchIndex}`,
      })
    },
  })
  const records = result.values.flat()
  assert.equal(records.length, count, `${result.name} completed record count`)
  const lsns = records.map((record) => record.lsn)
  assert.equal(new Set(records.map((record) => record.key)).size, count)
  const expectedWalRecordCount = Array.from({ length: batchCount }, (_, batchIndex) => {
    const offset = batchIndex * batchSize
    const size = Math.min(batchSize, count - offset)
    return Math.min(config.walShards, size)
  }).reduce((total, value) => total + value, 0)
  assert.equal(new Set(lsns).size, expectedWalRecordCount)
  return summarize({
    ...result,
    count,
    latenciesMs: result.latenciesMs.flatMap((latency, index) => {
      const offset = index * batchSize
      const size = Math.min(batchSize, count - offset)
      return Array.from({ length: size }, () => latency)
    }),
    values: records,
  }, {
    durability: "strict",
    batchSize,
    batchCount,
    shardsPerFullBatch,
    highestLsn: Math.max(...lsns),
    walRecordCount: expectedWalRecordCount,
  })
}

async function benchObjects(client, suffix) {
  const payload = "x".repeat(config.objectBytes)
  const result = await runBatch({
    name: "objects:put",
    count: config.objectCount,
    concurrency: config.concurrency,
    run: async (index) => {
      const object = await client.putObject(payload, {
        contentType: "text/plain",
        objectId: `bench-object-${suffix}-${index}`,
        clientMutationId: `bench-object-${suffix}-${index}`,
      })
      assert.equal(object.byteSize, config.objectBytes)
      return object
    },
  })
  assert.equal(new Set(result.values.map((object) => object.id)).size, config.objectCount)
  return summarize(result, {
    durability: "strict",
    bytesPerObject: config.objectBytes,
    totalBytes: config.objectBytes * config.objectCount,
  })
}

async function benchRecordListCold(client) {
  const table = client.table("rooms")
  const pageSize = config.recordListPageSize
  const started = performance.now()
  const page = await table.list({ limit: pageSize })
  const elapsedMs = performance.now() - started
  assert(page.records.length > 0, "cold record list returned records")
  return summarize({
    name: "records:list:keyOrder:cold",
    count: 1,
    concurrency: 1,
    elapsedMs,
    latenciesMs: [elapsedMs],
    values: [page],
  }, {
    pageSize,
    recordsRead: page.records.length,
    nextAfterKey: page.nextAfterKey,
  })
}

async function benchRecordListWarm(client) {
  const table = client.table("rooms")
  const pageSize = config.recordListPageSize
  const latenciesMs = []
  let recordsRead = 0
  let afterKey
  const started = performance.now()
  for (let index = 0; index < config.recordListPages; index += 1) {
    const pageStarted = performance.now()
    const page = await table.list({ limit: pageSize, afterKey })
    latenciesMs.push(performance.now() - pageStarted)
    recordsRead += page.records.length
    if (page.records.length === 0 || !page.nextAfterKey) {
      afterKey = undefined
    } else {
      afterKey = page.nextAfterKey
    }
  }
  const elapsedMs = performance.now() - started
  assert(recordsRead > 0, "warm record list read records")
  return summarize({
    name: "records:list:keyOrder:warm",
    count: config.recordListPages,
    concurrency: 1,
    elapsedMs,
    latenciesMs,
    values: [],
  }, {
    pageSize,
    recordsRead,
  })
}

async function runBatch({ name, count, concurrency, run }) {
  const pending = new Set()
  const latenciesMs = []
  const values = []
  const started = performance.now()
  for (let index = 0; index < count; index += 1) {
    const operationStarted = performance.now()
    const task = Promise.resolve()
      .then(() => run(index))
      .then((value) => {
        values.push(value)
        latenciesMs.push(performance.now() - operationStarted)
      })
      .finally(() => {
        pending.delete(task)
      })
    pending.add(task)
    if (pending.size >= concurrency) {
      await Promise.race(pending)
    }
  }
  await Promise.all(pending)
  const elapsedMs = performance.now() - started
  assert.equal(values.length, count, `${name} completed operation count`)
  return {
    name,
    count,
    concurrency,
    elapsedMs,
    latenciesMs,
    values,
  }
}

function summarize(result, extra = {}) {
  const sorted = result.latenciesMs.toSorted((left, right) => left - right)
  return {
    name: result.name,
    count: result.count,
    concurrency: result.concurrency,
    elapsedMs: round(result.elapsedMs),
    opsPerSecond: round(result.count / (result.elapsedMs / 1000)),
    latencyMs: {
      min: percentile(sorted, 0),
      p50: percentile(sorted, 0.50),
      p95: percentile(sorted, 0.95),
      p99: percentile(sorted, 0.99),
      max: percentile(sorted, 1),
    },
    ...extra,
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

function keyForShard(prefix, shardCount, targetShard) {
  for (let attempt = 0; attempt < 100_000; attempt += 1) {
    const key = `${prefix}-${attempt}`
    if (shardIndex(`rooms:${key}`, shardCount) === targetShard) {
      return key
    }
  }
  throw new Error(`failed to find key for shard ${targetShard}`)
}

function shardIndex(value, shardCount) {
  if (shardCount <= 1) {
    return 0
  }
  const digest = createHash("sha256").update(value).digest()
  const first = digest.readBigUInt64BE(0)
  return Number(first % BigInt(shardCount))
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: endpoint.replace("http://", ""),
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_WAL_SHARDS: String(config.walShards),
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_BENCH_LOGS === "1") {
      process.stdout.write(`[benchmark] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_BENCH_LOGS === "1") {
      process.stderr.write(`[benchmark] ${chunk}`)
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
