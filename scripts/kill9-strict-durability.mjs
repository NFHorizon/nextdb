import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

import { NextDbClient } from "../packages/nextdb-client/dist/index.js"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const serverBin = resolve(root, process.env.NEXTDB_SERVER_BIN ?? "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-kill9-strict-"))
const dataDir = join(tempRoot, "data")
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const addr = `127.0.0.1:${port}`
const iterations = numberEnv("NEXTDB_KILL9_ITERATIONS", 100)
const keepData = process.env.NEXTDB_KILL9_KEEP_DATA === "true"
const verbose = process.env.NEXTDB_KILL9_LOGS === "1"

let child
let highestLsn = 0
let durableOperationCount = 0
const expected = []

try {
  await mkdir(dataDir, { recursive: true })

  for (let index = 0; index < iterations; index += 1) {
    child = startNode()
    await waitForHealth()
    await assertRecovered(`before iteration ${index}`)

    const client = new NextDbClient({ endpoint, userId: "kill9-writer" })
    const roomId = `kill9-room-${index}`
    const title = `Kill9 Room ${index}`
    const body = `kill9 strict message ${index}`
    const room = await client.table("rooms").upsert(
      roomId,
      { id: roomId, title },
      {
        durability: "strict",
        clientMutationId: `kill9-room-${index}`,
      },
    )
    const message = await client.room(roomId).messages.send(body, {
      durability: "strict",
      clientMutationId: `kill9-message-${index}`,
    })
    assert(message.lsn > room.lsn)
    highestLsn = message.lsn
    durableOperationCount += 2
    expected.push({ roomId, title, body, roomLsn: room.lsn, messageLsn: message.lsn })
    client.close()

    await stopNode(child, "SIGKILL")
    child = undefined
  }

  child = startNode()
  await waitForHealth()
  await assertRecovered("final restart")

  const client = new NextDbClient({ endpoint, userId: "kill9-reader" })
  for (const item of expected) {
    const room = await client.table("rooms").get(item.roomId, { minLsn: highestLsn })
    assert.equal(room.key, item.roomId)
    assert.equal(room.value.title, item.title)
    assert.equal(room.lsn, item.roomLsn)

    const latest = await client.room(item.roomId).messages.latest({ limit: 1, minLsn: highestLsn })
    assert.equal(latest.messages.length, 1)
    assert.equal(latest.messages[0].body, item.body)
    assert.equal(latest.messages[0].lsn, item.messageLsn)
  }
  client.close()

  console.log("kill -9 strict durability loop ok")
  console.log(JSON.stringify({
    ok: true,
    iterations,
    endpoint,
    highestLsn,
    durableOperationCount,
    verifiedRooms: expected.length,
  }, null, 2))
} finally {
  if (child) {
    await stopNode(child, "SIGKILL")
  }
  if (keepData) {
    console.log(`kept kill -9 data at ${dataDir}`)
  } else {
    await rm(tempRoot, { recursive: true, force: true })
  }
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: addr,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_WAL_SHARDS: process.env.NEXTDB_WAL_SHARDS ?? "4",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (verbose) {
      process.stdout.write(`[kill9] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (verbose) {
      process.stderr.write(`[kill9] ${chunk}`)
    }
  })
  return child
}

async function stopNode(child, signal) {
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

async function assertRecovered(label) {
  const health = await getJson(`${endpoint}/v1/health`)
  assert.equal(health.ok, true, label)
  assert.equal(health.currentLsn, highestLsn, JSON.stringify({ label, health, highestLsn }))
  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true, JSON.stringify({ label, integrity }))
  assert.equal(
    integrity.recordCount,
    durableOperationCount,
    JSON.stringify({ label, integrity, durableOperationCount }),
  )
}

async function waitForHealth() {
  await waitFor(async () => {
    const health = await getJson(`${endpoint}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${endpoint}`)
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

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}

async function freePort() {
  const server = createServer()
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  const port = address.port
  server.close()
  await once(server, "close")
  return port
}

function numberEnv(name, fallback) {
  const value = process.env[name]
  if (!value) {
    return fallback
  }
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }
  return parsed
}
