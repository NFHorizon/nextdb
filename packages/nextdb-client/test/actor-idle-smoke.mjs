import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-actor-idle-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const client = new NextDbClient({ endpoint, userId: "actor-idle-user" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const hotRoomId = `actor-idle-hot-${suffix}`
  const idleRoomId = `actor-idle-cold-${suffix}`

  await writeRoom(client, hotRoomId)
  await writeRoom(client, idleRoomId)

  let health = await client.health()
  assert.equal(health.hotRoomIdleTtlMs, 50)
  assert.equal(health.hotRoomMaintenanceIntervalMs, 25)
  assert.equal(health.hotRoomCount, 2)
  let activationStatus = await client.runtimeActivationStatus()
  assert.equal(activationStatus.hotRoomIdleTtlMs, 50)
  assert.equal(activationStatus.hotRoomMaintenanceIntervalMs, 25)
  assert.equal(activationStatus.roomCount, 2)

  await sleep(500)
  const reactivatedHot = await latestMessages(hotRoomId)
  assert.equal(reactivatedHot.source, "chatLog")
  assert.equal(reactivatedHot.messages[0]?.body, "hello")

  activationStatus = await client.runtimeActivationStatus()
  assert.equal(activationStatus.roomCount, 1)
  assert.equal(activationStatus.rooms[0]?.roomId, hotRoomId)
  assert(activationStatus.hotRoomIdleMaintenance.lastSweepAtMs > 0)
  assert(activationStatus.hotRoomIdleMaintenance.totalEvicted >= 2)

  const coldReadable = await latestMessages(idleRoomId)
  assert.equal(coldReadable.source, "chatLog")
  assert.equal(coldReadable.messages[0]?.body, "hello")

  health = await client.health()
  assert.equal(health.hotRoomCount, 2)
  assert.equal(health.roomCount, 2)

  client.close()
  console.log("actor idle smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function writeRoom(client, roomId) {
  await client.table("rooms").upsert(roomId, {
    id: roomId,
    title: `Actor Idle ${roomId}`,
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  await client.room(roomId).messages.send("hello", {
    clientMutationId: `${roomId}-message`,
  })
}

async function latestMessages(roomId) {
  const response = await fetch(`${endpoint}/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?limit=1`)
  if (response.status !== 200) {
    assert.equal(response.status, 200, await response.text())
  }
  return response.json()
}

function startNode() {
  return spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_HOT_WINDOW: "2",
      NEXTDB_MAX_HOT_ROOMS: "10",
      NEXTDB_HOT_ROOM_IDLE_TTL_MS: "50",
      NEXTDB_HOT_ROOM_MAINTENANCE_INTERVAL_MS: "25",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
}

async function stopNode(child) {
  if (child.exitCode !== null) {
    return
  }
  child.kill("SIGINT")
  await new Promise((resolve) => child.once("exit", resolve))
}

async function waitForHealth(baseUrl) {
  await waitFor(async () => {
    try {
      const response = await fetch(`${baseUrl}/v1/health`)
      return response.ok
    } catch {
      return false
    }
  }, `health ${baseUrl}`)
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
        server.close(() => reject(new Error("failed to allocate actor idle smoke port")))
      }
    })
  })
}
