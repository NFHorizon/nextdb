import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:net"
import { spawn } from "node:child_process"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-runtime-chaos-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const wsEndpoint = `ws://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const preCrashMessageCount = Number(process.env.NEXTDB_RUNTIME_CHAOS_MESSAGES ?? "16")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth(endpoint)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const roomId = `chaos-room-${suffix}`
  const writer = new NextDbClient({ endpoint, userId: `chaos-writer-${suffix}` })

  const room = await writer.table("rooms").upsert(roomId, {
    id: roomId,
    title: "Runtime Chaos Room",
  }, {
    durability: "strict",
    clientMutationId: `${roomId}-upsert`,
  })
  assert(room.lsn > 0)

  const live = await openRoomSocket({
    userId: `chaos-live-${suffix}`,
    roomId,
    afterLsn: 0,
  })

  const preCrashMessages = await Promise.all(
    Array.from({ length: preCrashMessageCount }, (_, index) =>
      writer.room(roomId).messages.send(`pre-crash ${index}`, {
        durability: "strict",
        clientMutationId: `${roomId}-pre-${index}`,
      }),
    ),
  )
  const preCrashHighestLsn = Math.max(...preCrashMessages.map((message) => message.lsn))
  assert.equal(new Set(preCrashMessages.map((message) => message.lsn)).size, preCrashMessageCount)
  await waitForRoomMessageEvents(live.frames, preCrashMessages.map((message) => message.id), "pre-crash live events")

  await stopNode(child, "SIGKILL")
  child = undefined
  await waitForSocketClose(live.socket)
  writer.close()

  child = startNode()
  await waitForHealth(endpoint)
  const recovered = new NextDbClient({ endpoint, userId: `chaos-recovered-${suffix}` })
  const recoveredHealth = await recovered.health()
  assert.equal(recoveredHealth.ok, true)
  assert.equal(recoveredHealth.currentLsn, preCrashHighestLsn)
  assert.equal(recoveredHealth.startupRecovery.highestLsn, preCrashHighestLsn)
  assert(recoveredHealth.startupRecovery.rebuiltMessages >= preCrashMessageCount)

  const latestAfterRestart = await recovered.room(roomId).messages.latest({
    limit: preCrashMessageCount,
    minLsn: preCrashHighestLsn,
  })
  assert.deepEqual(
    latestAfterRestart.messages.map((message) => message.id).toSorted(),
    preCrashMessages.map((message) => message.id).toSorted(),
  )

  const missedWhileDisconnected = await recovered.room(roomId).messages.send("missed while disconnected", {
    durability: "strict",
    clientMutationId: `${roomId}-missed`,
  })
  assert(missedWhileDisconnected.lsn > preCrashHighestLsn)

  const resumed = await openRoomSocket({
    userId: `chaos-resumed-${suffix}`,
    roomId,
    afterLsn: preCrashHighestLsn,
    catchUpLimit: 8,
  })
  await waitForFrame(
    resumed.frames,
    (frame) => frame.type === "subscriptionCatchUp" && frame.rooms?.includes(roomId),
    "room catch-up completion after restart",
  )
  await waitForRoomMessageEvents(resumed.frames, [missedWhileDisconnected.id], "missed catch-up event")

  const liveAfterResume = await recovered.room(roomId).messages.send("live after resume", {
    durability: "strict",
    clientMutationId: `${roomId}-live-after-resume`,
  })
  await waitForRoomMessageEvents(resumed.frames, [liveAfterResume.id], "post-resume live event")

  const finalHealth = await recovered.health()
  assert.equal(finalHealth.currentLsn, liveAfterResume.lsn)
  const wait = await recovered.waitForLsn(liveAfterResume.lsn, { timeoutMs: 1_000 })
  assert.equal(wait.caughtUp, true)

  const trace = await recovered.traceEntity({
    kind: "clientMutation",
    clientMutationId: `${roomId}-live-after-resume`,
  })
  assert.equal(trace.records.length, 1)
  assert.equal(trace.records[0].payload.type, "messageCreated")
  assert.equal(trace.records[0].payload.message.id, liveAfterResume.id)

  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, preCrashMessageCount + 3)

  resumed.socket.close()
  recovered.close()

  console.log(`runtime chaos smoke ok: recovered ${preCrashMessageCount} messages and resumed from lsn ${preCrashHighestLsn}`)
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
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_RUNTIME_CHAOS_SMOKE_LOGS === "1") {
      process.stdout.write(`[chaos] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_RUNTIME_CHAOS_SMOKE_LOGS === "1") {
      process.stderr.write(`[chaos] ${chunk}`)
    }
  })
  return child
}

async function openRoomSocket({ userId, roomId, afterLsn, catchUpLimit }) {
  const socket = new WebSocket(`${wsEndpoint}/v1/connect?userId=${encodeURIComponent(userId)}`)
  const frames = []
  socket.addEventListener("message", (event) => frames.push(JSON.parse(event.data)))
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true })
    socket.addEventListener("error", reject, { once: true })
  })
  await waitForFrame(frames, (frame) => frame.type === "hello", "websocket hello")
  socket.send(JSON.stringify({
    type: "subscribeRoom",
    roomId,
    afterLsn,
    catchUpLimit,
  }))
  await waitForFrame(
    frames,
    (frame) => frame.type === "subscribed" && frame.roomId === roomId,
    "room subscription",
  )
  return { socket, frames }
}

async function waitForRoomMessageEvents(frames, messageIds, label) {
  const expected = new Set(messageIds)
  await waitForFrame(
    frames,
    () => {
      const seen = new Set(
        frames
          .filter((frame) => frame.type === "event" && frame.event?.type === "messageCreated")
          .map((frame) => frame.event.message.id),
      )
      return [...expected].every((id) => seen.has(id))
    },
    label,
    5_000,
  )
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

async function waitForSocketClose(socket) {
  if (socket.readyState === WebSocket.CLOSED) {
    return
  }
  await Promise.race([
    new Promise((resolve) => socket.addEventListener("close", resolve, { once: true })),
    new Promise((resolve) => setTimeout(resolve, 1_000)),
  ])
}

async function waitForHealth(url) {
  await waitFor(async () => {
    const health = await getJson(`${url}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${url}`)
}

async function waitForFrame(frames, predicate, label, timeoutMs = 5_000) {
  await waitFor(() => {
    const frame = frames.find(predicate)
    return frame !== undefined
  }, label, timeoutMs)
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
