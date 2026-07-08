import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-room-volatile-"))
const dataDir = join(tempRoot, "data")
const node = {
  url: "http://127.0.0.1:3398",
  addr: "127.0.0.1:3398",
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const roomId = `volatile-room-${suffix}`
  const eventName = `typing.${suffix}`
  const alice = new NextDbClient({
    endpoint: node.url,
    userId: `alice-${suffix}`,
    sessionId: `alice-session-${suffix}`,
  })
  const bob = new NextDbClient({
    endpoint: node.url,
    userId: `bob-${suffix}`,
    sessionId: `bob-session-${suffix}`,
  })
  const carol = new NextDbClient({
    endpoint: node.url,
    userId: `carol-${suffix}`,
    sessionId: `carol-session-${suffix}`,
  })

  const schema = await alice.getSchema()
  schema.version += 1
  schema.events[eventName] = {
    payload: {
      type: {
        kind: "object",
        fields: {
          nonce: { type: { kind: "string" }, optional: false },
          state: { type: { kind: "string" }, optional: false },
          attachment: { type: { kind: "objectRef", object: "Object" }, optional: false },
        },
      },
      optional: false,
    },
  }
  const applied = await alice.applySchema(schema, { expectedVersion: schema.version - 1 })
  assert.equal(applied.applied, true)

  const objectId = `volatile-event-object-${suffix}`
  const object = await alice.putObject("volatile event object", {
    contentType: "text/plain",
    objectId,
    clientMutationId: `${objectId}-put`,
  })
  const missingObject = {
    ...object,
    id: `${objectId}-missing`,
    path: `objects/${objectId}-missing`,
  }

  const aliceEvents = []
  const bobEvents = []
  const carolEvents = []
  const stopAlice = alice.room(roomId).messages.subscribe((event) => aliceEvents.push(event))
  const stopBob = bob.room(roomId).messages.subscribe((event) => bobEvents.push(event))
  const stopCarol = carol.room(`${roomId}-other`).messages.subscribe((event) => carolEvents.push(event))

  try {
    await waitFor(() => subscribedRoomSessions(node.url, roomId).then((count) => count === 2), "two room subscribers")
    const nonce = `volatile-${suffix}`
    await assert.rejects(
      () => alice.room(roomId).publishVolatile(eventName, { nonce, state: "typing", attachment: missingObject }),
      (error) => error?.status === 404 && error.message.includes("object ref not found"),
    )
    await assert.rejects(
      () => alice.room(roomId).publishVolatile(eventName, {
        nonce,
        state: "typing",
        attachment: { ...object, sha256: "not-the-object-sha" },
      }),
      (error) => error?.status === 400 && error.message.includes("object ref metadata does not match"),
    )

    const published = await alice.room(roomId).publishVolatile(eventName, { nonce, state: "typing", attachment: object })
    assert.equal(published.delivered, 2)

    await waitFor(
      () => aliceEvents.some((event) => event.type === "volatileRoomEvent" && event.payload?.nonce === nonce),
      "alice receives volatile room event",
    )
    await waitFor(
      () => bobEvents.some((event) => event.type === "volatileRoomEvent" && event.payload?.nonce === nonce),
      "bob receives volatile room event",
    )
    assert.equal(carolEvents.some((event) => event.type === "volatileRoomEvent" && event.payload?.nonce === nonce), false)

    const audit = await getJson(`${node.url}/v1/audit/wal?afterLsn=0&limit=20`)
    assert.equal(
      audit.records.some((record) => JSON.stringify(record).includes(nonce)),
      false,
      "volatile room event must not be persisted to WAL",
    )

    console.log("room volatile smoke ok")
  } finally {
    stopAlice()
    stopBob()
    stopCarol()
    alice.close()
    bob.close()
    carol.close()
  }
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function subscribedRoomSessions(baseUrl, roomId) {
  const response = await getJson(`${baseUrl}/v1/admin/connections`)
  return response.sessions.filter((session) => session.subscribedRooms.includes(roomId)).length
}

function startNode(node) {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: node.dataDir,
      NEXTDB_ADDR: node.addr,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_ROOM_VOLATILE_SMOKE_LOGS === "1") {
      process.stdout.write(`[room-volatile] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_ROOM_VOLATILE_SMOKE_LOGS === "1") {
      process.stderr.write(`[room-volatile] ${chunk}`)
    }
  })
  return child
}

async function stopNode(child) {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill("SIGTERM")
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

async function waitForHealth(url) {
  await waitFor(async () => {
    const response = await fetch(`${url}/v1/health`).catch(() => undefined)
    if (!response?.ok) {
      return false
    }
    const health = await response.json()
    return health.ok === true
  }, `health at ${url}`)
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return JSON.parse(text)
}

async function waitFor(check, label, timeoutMs = 5_000) {
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
