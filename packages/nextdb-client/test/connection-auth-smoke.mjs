import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-connection-auth-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const adminToken = "admin-secret"
const clientToken = "client-secret"
const aliceToken = "alice-secret"
const bobToken = "bob-secret"
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
      NEXTDB_ADMIN_TOKEN: adminToken,
      NEXTDB_CLIENT_TOKEN: clientToken,
      NEXTDB_CLIENT_USER_TOKENS: `alice=${aliceToken},bob=${bobToken}`,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)

  await expectSocketRejected({ userId: "alice" }, "missing WebSocket token")
  await expectSocketRejected(
    { userId: "alice", authToken: bobToken },
    "wrong user WebSocket token",
  )
  await expectSocketRejected(
    { authToken: aliceToken },
    "anonymous WebSocket with user token",
  )
  await expectSocketRejected(
    { userId: "bob", authToken: clientToken },
    "global client token cannot impersonate user WebSocket",
  )

  const jsonlNoToken = await postJsonlStatus("/v1/connect/jsonl")
  assert.equal(jsonlNoToken.status, 401)
  assert.match(jsonlNoToken.body.error, /client token is required/)
  const jsonlAnonymousUserToken = await postJsonlStatus(
    `/v1/connect/jsonl?authToken=${encodeURIComponent(aliceToken)}`,
  )
  assert.equal(jsonlAnonymousUserToken.status, 401)
  assert.match(jsonlAnonymousUserToken.body.error, /client token is required/)

  const alice = await openRealtimeSocket({
    userId: "alice",
    sessionId: "alice-ws-auth",
    authToken: aliceToken,
  })
  sockets.push(alice)

  alice.send({ type: "subscribeConnectionEvents" })
  const aliceAdminError = await alice.waitForFrame(
    (frame) =>
      frame.type === "error" &&
      /subscribeConnectionEvents requires admin token/.test(frame.message ?? ""),
    "non-admin connection event subscription rejection",
  )
  assert.equal(aliceAdminError.type, "error")

  const admin = await openRealtimeSocket({
    userId: "bob",
    sessionId: "admin-bob-ws",
    adminToken,
  })
  sockets.push(admin)

  admin.send({ type: "subscribeConnectionEvents" })
  await admin.waitForFrame(
    (frame) => frame.type === "connectionEventsSubscribed",
    "admin connection events subscribed",
  )

  const connections = await getJson(
    `/v1/admin/connections?userId=alice`,
    { authorization: `Bearer ${adminToken}` },
  )
  assert.equal(connections.sessions.some((session) => session.sessionId === "alice-ws-auth"), true)

  console.log("connection auth smoke ok")
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

async function getJson(path, headers = {}) {
  const response = await fetch(`${endpoint}${path}`, { headers })
  assert.equal(response.status, 200)
  return response.json()
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
