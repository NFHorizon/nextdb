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
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-record-hot-idle-"))
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
      NEXTDB_RECORD_HOT_DURABLE_IDLE_TTL_MS: "50",
      NEXTDB_RECORD_HOT_MAINTENANCE_INTERVAL_MS: "25",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)

  const db = new NextDbClient({ endpoint })
  const schema = await db.getSchema()
  schema.version += 1
  schema.tables.rooms.storage = { kind: "lru", maxItems: 10 }
  const apply = await db.applySchema(schema, { expectedVersion: schema.version - 1 })
  assert.equal(apply.applied, true)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const durableKey = `record-hot-idle-durable-${suffix}`
  const volatileKey = `record-hot-idle-volatile-${suffix}`
  const rooms = db.table("rooms")

  await rooms.upsert(durableKey, { id: durableKey, title: "Durable Idle" }, {
    clientMutationId: `${durableKey}-upsert`,
  })
  const volatile = await rooms.upsert(
    volatileKey,
    { id: volatileKey, title: "Volatile Idle" },
    {
      durability: "volatile",
      clientMutationId: `${volatileKey}-volatile`,
    },
  )
  assert.equal(volatile.lsn, 0)
  await assertHotRoomRecordState(2, 50, 25)

  await sleep(500)
  const idleState = await assertHotRoomRecordState(1, 50, 25)
  assert(idleState.durableIdleLastSweepAtMs > 0)
  assert(idleState.durableIdleTotalEvicted >= 1)

  const coldDurable = (await getRecord("rooms", durableKey)).record
  assert.equal(coldDurable.key, durableKey)
  assert.equal(coldDurable.value.title, "Durable Idle")
  assert(coldDurable.lsn > 0)
  await assertHotRoomRecordState(2, 50, 25)

  const volatileAfter = (await getRecord("rooms", volatileKey)).record
  assert.equal(volatileAfter.key, volatileKey)
  assert.equal(volatileAfter.value.title, "Volatile Idle")
  assert.equal(volatileAfter.lsn, 0)

  db.close()
  console.log("record hot idle smoke ok")
} finally {
  if (child) {
    child.kill("SIGINT")
    await once(child, "exit").catch(() => {})
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function assertHotRoomRecordState(expectedRecords, expectedTtlMs, expectedIntervalMs) {
  const health = await getJson(`${endpoint}/v1/health`)
  const roomTable = health.recordHotCache.tables.find((table) => table.table === "rooms")
  assert.equal(health.recordHotCache.durableIdleTtlMs, expectedTtlMs)
  assert.equal(health.recordHotMaintenanceIntervalMs, expectedIntervalMs)
  assert.equal(roomTable?.storage.kind, "lru")
  assert.equal(roomTable?.records, expectedRecords)
  return health.recordHotCache
}

async function getRecord(table, key) {
  const response = await fetch(`${endpoint}/v1/records/${encodeURIComponent(table)}/${encodeURIComponent(key)}`)
  if (response.status !== 200) {
    assert.equal(response.status, 200, await response.text())
  }
  return response.json()
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
  if (!response.ok) {
    throw new Error(`${url} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await sleep(50)
  }
  throw new Error(`timed out waiting for ${label}`)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      if (address && typeof address === "object") {
        server.close(() => resolve(address.port))
      } else {
        server.close(() => reject(new Error("failed to allocate record hot idle smoke port")))
      }
    })
  })
}
