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
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-volatile-overlay-restart-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = await startServer()
  const firstHealth = await getJson(`${endpoint}/v1/health`)

  const db = new NextDbClient({ endpoint })
  const schema = await db.getSchema()
  schema.version += 1
  schema.tables.rooms.storage = { kind: "lru", maxItems: 8 }
  const apply = await db.applySchema(schema, { expectedVersion: schema.version - 1 })
  assert.equal(apply.applied, true)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const overlayKey = `restart-overlay-${suffix}`
  const rooms = db.table("rooms")
  const durable = await rooms.upsert(overlayKey, { id: overlayKey, title: "Restart Durable" }, {
    clientMutationId: `${overlayKey}-durable`,
  })
  assert(durable.lsn > 0)

  const volatile = await rooms.upsert(overlayKey, { id: overlayKey, title: "Restart Volatile" }, {
    durability: "volatile",
    clientMutationId: `${overlayKey}-volatile`,
  })
  assert.equal(volatile.lsn, 0)

  const beforeRestart = await rooms.get(overlayKey)
  assert.equal(beforeRestart.value.title, "Restart Volatile")

  await stopServer()
  child = await startServer()
  const secondHealth = await getJson(`${endpoint}/v1/health`)
  assert.notEqual(secondHealth.runtimeId, firstHealth.runtimeId)

  const indexedDurable = await rooms.index("byTitle", {
    value: "Restart Durable",
    limit: 20,
  })
  assert.equal(indexedDurable.records.some((record) => record.key === overlayKey), true)

  const cached = await rooms.cache.get(overlayKey)
  assert.equal(cached?.lsn, durable.lsn)
  assert.equal(cached?.value.title, "Restart Durable")

  const afterRestart = await rooms.get(overlayKey)
  assert.equal(afterRestart.lsn, durable.lsn)
  assert.equal(afterRestart.value.title, "Restart Durable")

  db.close()
  console.log("volatile overlay restart smoke ok")
} finally {
  await stopServer()
  await rm(tempRoot, { recursive: true, force: true })
}

async function startServer() {
  const next = spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)
  return next
}

async function stopServer() {
  if (!child) {
    return
  }
  const current = child
  child = undefined
  current.kill("SIGINT")
  await once(current, "exit").catch(() => {})
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
