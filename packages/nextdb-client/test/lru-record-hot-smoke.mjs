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
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-lru-record-hot-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const db = new NextDbClient({ endpoint })
  const schema = await db.getSchema()
  schema.version += 1
  schema.tables.rooms.storage = { kind: "lru", maxItems: 1 }
  schema.tables.rooms.indexes.byTitle.unique = true
  const apply = await db.applySchema(schema, { expectedVersion: schema.version - 1 })
  assert.equal(apply.applied, true)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const firstKey = `lru-first-${suffix}`
  const secondKey = `lru-second-${suffix}`
  const rooms = db.table("rooms")

  await rooms.upsert(firstKey, { id: firstKey, title: "LRU First" }, {
    clientMutationId: `${firstKey}-upsert`,
  })
  await rooms.upsert(secondKey, { id: secondKey, title: "LRU Second" }, {
    clientMutationId: `${secondKey}-upsert`,
  })

  await assertHotRoomRecordCount(endpoint, 1)

  const evictedSecond = await db.evictRuntimeRecords({ table: "rooms", key: secondKey })
  assert.equal(evictedSecond.evicted, 1)
  assert.equal(evictedSecond.after.recordCount, 0)
  await assertHotRoomRecordCount(endpoint, 0)

  const prepared = await db.prepareRestart({
    reason: "lru record hot prewarm smoke",
    snapshot: true,
    compactWal: false,
    waitForWritesMs: 1_000,
  })
  assert.equal(prepared.readyForRestart, true)
  db.close()
  await stopNode(child)
  child = undefined

  child = startNode({ NEXTDB_RECORD_HOT_PREWARM_LIMIT: "1" })
  await waitForHealth(endpoint)
  const prewarmDb = new NextDbClient({ endpoint })
  await waitFor(async () => {
    const health = await prewarmDb.health()
    return health.recordHotPrewarm.enabled
      && health.recordHotPrewarm.lastFinishedAtMs
      && health.recordHotPrewarm.totalFound >= 1
      && health.recordHotCache.tables.some((table) => table.table === "rooms" && table.records === 1)
  }, "record hot startup prewarm")
  const prewarmHealth = await prewarmDb.health()
  assert.equal(prewarmHealth.recordHotPrewarm.limitPerTable, 1)
  assert.equal(prewarmHealth.recordHotPrewarm.tables.some((table) => table.table === "rooms" && table.found >= 1), true)
  assert.match(await prewarmDb.metrics(), /^nextdb_record_hot_prewarm_total_found \d+$/m)
  prewarmDb.close()

  const dbAfterPrewarm = new NextDbClient({ endpoint })
  const roomsAfterPrewarm = dbAfterPrewarm.table("rooms")
  const activatedFirst = await dbAfterPrewarm.activateRuntimeRecords({ table: "rooms", key: firstKey })
  assert.equal(activatedFirst.found, 1)
  assert.equal(activatedFirst.after.recordCount, 1)
  await assertHotRoomRecordCount(endpoint, 1)

  const first = await roomsAfterPrewarm.get(firstKey)
  assert.equal(first.key, firstKey)
  assert.equal(first.value.title, "LRU First")
  await assertHotRoomRecordCount(endpoint, 1)

  const second = await roomsAfterPrewarm.get(secondKey)
  assert.equal(second.key, secondKey)
  assert.equal(second.value.title, "LRU Second")
  await assertHotRoomRecordCount(endpoint, 1)

  const page = await roomsAfterPrewarm.list({ limit: 20 })
  assert.equal(page.records.some((record) => record.key === firstKey), true)
  assert.equal(page.records.some((record) => record.key === secondKey), true)
  await assertHotRoomRecordCount(endpoint, 1)

  const indexedFirst = await roomsAfterPrewarm.index("byTitle", {
    value: "LRU First",
    limit: 20,
  })
  assert.equal(indexedFirst.records.some((record) => record.key === firstKey), true)
  await assertHotRoomRecordCount(endpoint, 1)

  const evictedBeforeLiveQuery = await dbAfterPrewarm.evictRuntimeRecords({ table: "rooms", limit: 1 })
  assert.equal(evictedBeforeLiveQuery.evicted, 1)
  await assertHotRoomRecordCount(endpoint, 0)
  const activatedByIndex = await dbAfterPrewarm.activateRuntimeRecords({
    table: "rooms",
    indexName: "byTitle",
    value: "LRU First",
    limit: 1,
  })
  assert.equal(activatedByIndex.found, 1)
  assert.equal(activatedByIndex.after.recordCount, 1)
  assert.equal(activatedByIndex.after.tables.find((table) => table.table === "rooms")?.hydrateDurableTotal >= 1, true)
  await assertHotRoomRecordCount(endpoint, 1)
  const evictedAfterIndexActivation = await dbAfterPrewarm.evictRuntimeRecords({ table: "rooms", limit: 1 })
  assert.equal(evictedAfterIndexActivation.evicted, 1)
  await assertHotRoomRecordCount(endpoint, 0)
  const liveQueryEvents = []
  const unsubscribeLiveQuery = dbAfterPrewarm.subscribeQuery({
    queryId: `lru-live-query-${suffix}`,
    table: "rooms",
    indexName: "byTitle",
    value: "LRU First",
    limit: 1,
  }, (event) => liveQueryEvents.push(event))
  await waitFor(
    () => liveQueryEvents.some((event) => event.response.records.some((record) => record.key === firstKey)),
    "live query result for LRU record",
  )
  await assertHotRoomRecordCount(endpoint, 1)
  unsubscribeLiveQuery()

  const overlayKey = `lru-overlay-${suffix}`
  await roomsAfterPrewarm.upsert(overlayKey, { id: overlayKey, title: "Overlay Durable" }, {
    clientMutationId: `${overlayKey}-durable`,
  })
  const volatileOverlay = await roomsAfterPrewarm.upsert(
    overlayKey,
    { id: overlayKey, title: "Overlay Volatile" },
    {
      durability: "volatile",
      clientMutationId: `${overlayKey}-volatile`,
    },
  )
  assert.equal(volatileOverlay.lsn, 0)
  assert.match(volatileOverlay.path, /^volatile\//)

  const reusedDurableTitleKey = `lru-reused-durable-title-${suffix}`
  await assert.rejects(
    () => roomsAfterPrewarm.upsert(
      reusedDurableTitleKey,
      { id: reusedDurableTitleKey, title: "Overlay Durable" },
      { clientMutationId: `${reusedDurableTitleKey}-upsert` },
    ),
    (error) => error?.status === 409 && error.message.includes("unique index violation"),
  )

  const duplicateVolatileTitleKey = `lru-duplicate-volatile-title-${suffix}`
  await assert.rejects(
    () => roomsAfterPrewarm.upsert(
      duplicateVolatileTitleKey,
      { id: duplicateVolatileTitleKey, title: "Overlay Volatile" },
      { clientMutationId: `${duplicateVolatileTitleKey}-upsert` },
    ),
    (error) => error?.status === 409 && error.message.includes("unique index violation"),
  )
  await assert.rejects(
    () => roomsAfterPrewarm.upsert(
      `${duplicateVolatileTitleKey}-volatile`,
      { id: `${duplicateVolatileTitleKey}-volatile`, title: "Overlay Volatile" },
      {
        durability: "volatile",
        clientMutationId: `${duplicateVolatileTitleKey}-volatile-upsert`,
      },
    ),
    (error) => error?.status === 409 && error.message.includes("unique index violation"),
  )

  const durableIndex = await roomsAfterPrewarm.index("byTitle", {
    value: "Overlay Durable",
    limit: 20,
  })
  assert.equal(durableIndex.records.some((record) => record.key === overlayKey), false)
  const durableRange = await roomsAfterPrewarm.index("byTitle", {
    lower: "Overlay Durable",
    upper: "Overlay Durable",
    limit: 20,
  })
  assert.equal(durableRange.records.some((record) => record.key === overlayKey), false)
  const volatileIndex = await roomsAfterPrewarm.index("byTitle", {
    value: "Overlay Volatile",
    limit: 20,
  })
  assert.equal(volatileIndex.records.some((record) => record.key === overlayKey && record.lsn === 0), true)
  const volatileRange = await roomsAfterPrewarm.index("byTitle", {
    lower: "Overlay Volatile",
    upper: "Overlay Volatile",
    limit: 20,
  })
  assert.equal(volatileRange.records.some((record) => record.key === overlayKey && record.lsn === 0), true)
  const overlayAfterIndex = await roomsAfterPrewarm.get(overlayKey)
  assert.equal(overlayAfterIndex.lsn, 0)
  assert.equal(overlayAfterIndex.value.title, "Overlay Volatile")

  const committedOverlay = await roomsAfterPrewarm.upsert(overlayKey, { id: overlayKey, title: "Overlay Committed" }, {
    clientMutationId: `${overlayKey}-committed`,
  })
  assert(committedOverlay.lsn > 0)
  assert.equal(committedOverlay.value.title, "Overlay Committed")
  const cachedCommittedOverlay = await roomsAfterPrewarm.cache.get(overlayKey)
  assert.equal(cachedCommittedOverlay?.lsn, committedOverlay.lsn)
  assert.equal(cachedCommittedOverlay?.value.title, "Overlay Committed")
  const hotMetricsHealth = await dbAfterPrewarm.health()
  assert(hotMetricsHealth.recordHotCache.getTotal >= 3)
  assert(hotMetricsHealth.recordHotCache.getHitTotal >= 2)
  assert(hotMetricsHealth.recordHotCache.getMissTotal >= 1)
  assert(hotMetricsHealth.recordHotCache.listTotal >= 1)
  assert(hotMetricsHealth.recordHotCache.hydrateDurableTotal >= 1)
  assert(hotMetricsHealth.recordHotCache.upsertTotal >= 1)
  assert(hotMetricsHealth.recordHotCache.hydrateDurableTotal > hotMetricsHealth.recordHotCache.upsertTotal)
  assert(hotMetricsHealth.recordHotCache.evictTotal >= 1)
  assert(hotMetricsHealth.recordHotCache.lruEvictedTotal >= 1)
  assert(hotMetricsHealth.recordHotCache.volatileRecords >= 0)
  const roomHotMetrics = hotMetricsHealth.recordHotCache.tables.find((table) => table.table === "rooms")
  assert(roomHotMetrics)
  assert.equal(roomHotMetrics.volatileRecords, hotMetricsHealth.recordHotCache.volatileRecords)
  assert.equal(roomHotMetrics.getTotal, hotMetricsHealth.recordHotCache.getTotal)
  assert.equal(roomHotMetrics.getHitTotal, hotMetricsHealth.recordHotCache.getHitTotal)
  assert.equal(roomHotMetrics.getMissTotal, hotMetricsHealth.recordHotCache.getMissTotal)
  assert.equal(roomHotMetrics.hydrateDurableTotal, hotMetricsHealth.recordHotCache.hydrateDurableTotal)
  assert.equal(roomHotMetrics.upsertTotal, hotMetricsHealth.recordHotCache.upsertTotal)
  assert.equal(roomHotMetrics.evictTotal, hotMetricsHealth.recordHotCache.evictTotal)
  assert.equal(roomHotMetrics.lruEvictedTotal, hotMetricsHealth.recordHotCache.lruEvictedTotal)
  const hotMetrics = await dbAfterPrewarm.metrics()
  assert.match(hotMetrics, /^nextdb_record_projection_key_order_entries \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_projection_recent_entries \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_projection_partition_entries \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_projection_order_entries \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_get_total \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_get_hit_total \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_get_miss_total \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_volatile_records \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_hydrate_durable_total \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_lru_evicted_total \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_table_volatile_records\{table="rooms"\} \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_table_get_total\{table="rooms"\} \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_table_get_hit_total\{table="rooms"\} \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_table_hydrate_durable_total\{table="rooms"\} \d+$/m)
  assert.match(hotMetrics, /^nextdb_record_hot_table_lru_evicted_total\{table="rooms"\} \d+$/m)

  dbAfterPrewarm.close()
  console.log("lru record hot smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(env = {}) {
  return spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      ...env,
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
}

async function stopNode(process) {
  process.kill("SIGINT")
  await once(process, "exit").catch(() => {})
}

async function assertHotRoomRecordCount(endpoint, expected) {
  const health = await getJson(`${endpoint}/v1/health`)
  const roomTable = health.recordHotCache.tables.find((table) => table.table === "rooms")
  assert.equal(roomTable?.storage.kind, "lru")
  assert.equal(roomTable?.maxItems, 1)
  assert.equal(roomTable?.records, expected)
}

async function waitForHealth(endpoint) {
  await waitFor(async () => {
    try {
      const response = await fetch(`${endpoint}/v1/health`)
      return response.ok
    } catch {
      return false
    }
  }, `health ${endpoint}`)
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return JSON.parse(text)
}

async function waitFor(predicate, label, timeoutMs = 5_000) {
  const started = Date.now()
  while (Date.now() - started < timeoutMs) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
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
