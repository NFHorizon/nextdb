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
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-runtime-drain-connection-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const sockets = []
let child

try {
  if (typeof WebSocket === "undefined") {
    throw new Error("global WebSocket is required; run this smoke with Node.js 22 or newer")
  }

  await mkdir(dataDir, { recursive: true })
  child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)

  const db = new NextDbClient({ endpoint, userId: "alice" })
  const existing = await openRealtimeSocket({
    userId: "alice",
    sessionId: "drain-existing",
  })
  sockets.push(existing)

  const drain = await db.setRuntimeDraining(true, "runtime drain connection smoke")
  assert.equal(drain.draining, true)

  const health = await db.health()
  assert.equal(health.draining, true)
  assert.equal(health.acceptingWrites, false)

  await expectSocketRejected({
    userId: "alice",
    sessionId: "drain-new",
  }, "new WebSocket while draining")

  const jsonl = await postJsonlStatus("/v1/connect/jsonl?userId=alice&sessionId=drain-jsonl")
  assert.equal(jsonl.status, 503)
  assert.equal(jsonl.body.draining, true)
  assert.match(jsonl.body.error, /node is draining/)

  await assert.rejects(
    () => db.room("drain-room").messages.send("blocked while draining", {
      clientMutationId: "drain-write-blocked",
    }),
    (error) => error?.status === 503 && error?.payload?.draining === true,
  )

  existing.send({
    type: "updateConnectionMetadata",
    metadata: { phase: "draining", stillConnected: true },
  })
  const metadataUpdated = await existing.waitForFrame(
    (frame) =>
      frame.type === "connectionMetadataUpdated" &&
      frame.session?.sessionId === "drain-existing",
    "existing connection metadata update during drain",
  )
  assert.equal(metadataUpdated.session.metadata.phase, "draining")
  assert.equal(metadataUpdated.session.metadata.stillConnected, true)

  await db.setRuntimeDraining(false, "runtime drain connection smoke complete")
  const recovered = await db.health()
  assert.equal(recovered.draining, false)
  assert.equal(recovered.acceptingWrites, true)

  db.close()
  console.log("runtime drain connection smoke ok")
} finally {
  for (const socket of sockets.reverse()) {
    socket.close()
  }
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function openRealtimeSocket(params) {
  const url = connectionUrl(params)
  const ws = new WebSocket(url)
  const frames = []
  const waiters = []

  ws.addEventListener("message", async (event) => {
    const frame = JSON.parse(await frameText(event.data))
    frames.push(frame)
    for (const waiter of [...waiters]) {
      if (waiter.predicate(frame)) {
        waiter.resolve(frame)
        waiters.splice(waiters.indexOf(waiter), 1)
      }
    }
  })

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.close()
      reject(new Error(`timed out opening ${url}`))
    }, 3_000)
    ws.addEventListener("open", () => {
      clearTimeout(timer)
      resolve({
        frames,
        send: (frame) => ws.send(JSON.stringify(frame)),
        close: () => ws.close(),
        waitForFrame: (predicate, label) =>
          waitForFrame(frames, waiters, predicate, label),
      })
    }, { once: true })
    ws.addEventListener("error", () => {
      clearTimeout(timer)
      reject(new Error(`failed opening ${url}`))
    }, { once: true })
  })
}

function expectSocketRejected(params, label) {
  const url = connectionUrl(params)
  const ws = new WebSocket(url)

  return new Promise((resolve, reject) => {
    let settled = false
    const finish = (fn) => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      fn()
    }
    const timer = setTimeout(() => {
      ws.close()
      finish(() => reject(new Error(`${label} did not reject in time`)))
    }, 3_000)
    ws.addEventListener("open", () => {
      ws.close()
      finish(() => reject(new Error(`${label} unexpectedly opened`)))
    }, { once: true })
    ws.addEventListener("error", () => {
      finish(resolve)
    }, { once: true })
    ws.addEventListener("close", () => {
      finish(resolve)
    }, { once: true })
  })
}

function connectionUrl(params = {}) {
  const url = new URL("/v1/connect", endpoint)
  url.protocol = "ws:"
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      url.searchParams.set(key, value)
    }
  }
  return url
}

async function waitForFrame(frames, waiters, predicate, label) {
  const existing = frames.find(predicate)
  if (existing) {
    return existing
  }
  return new Promise((resolve, reject) => {
    const waiter = { predicate, resolve }
    waiters.push(waiter)
    setTimeout(() => {
      const index = waiters.indexOf(waiter)
      if (index >= 0) {
        waiters.splice(index, 1)
      }
      reject(new Error(`timed out waiting for ${label}`))
    }, 3_000)
  })
}

async function frameText(data) {
  if (typeof data === "string") {
    return data
  }
  if (data instanceof Blob) {
    return data.text()
  }
  if (data instanceof ArrayBuffer) {
    return Buffer.from(data).toString("utf8")
  }
  return Buffer.from(data).toString("utf8")
}

async function postJsonlStatus(path) {
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/x-ndjson",
    },
    body: "",
  })
  return { status: response.status, body: await response.json().catch(() => ({})) }
}

async function waitForHealth(baseUrl) {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/v1/health`)
      if (response.ok) {
        return
      }
    } catch {
      // Retry until the server opens the port.
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error("server did not become healthy")
}

async function stopNode(process) {
  if (process.exitCode !== null) {
    return
  }
  process.kill("SIGTERM")
  const result = await Promise.race([
    once(process, "exit"),
    new Promise((resolve) => setTimeout(() => resolve(null), 5_000)),
  ])
  if (result === null && process.exitCode === null) {
    process.kill("SIGKILL")
    await once(process, "exit")
  }
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
