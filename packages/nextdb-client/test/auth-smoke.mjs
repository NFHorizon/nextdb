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
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-auth-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
const adminToken = "admin-secret"
const clientToken = "client-secret"
const aliceToken = "alice-secret"
const bobToken = "bob-secret"
let child

try {
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

  const health = await getJson("/v1/health")
  assert.equal(health.adminAuthEnabled, true)
  assert.equal(health.clientAuthEnabled, true)
  assert.equal(health.clientUserAuthEnabled, true)

  const noAdmin = await getStatus("/v1/admin/export/manifest")
  assert.equal(noAdmin.status, 401)
  assert.match(noAdmin.body.error, /admin token is required/)
  const clientAsAdmin = await getStatus("/v1/admin/export/manifest", {
    authorization: `Bearer ${clientToken}`,
  })
  assert.equal(clientAsAdmin.status, 401)
  const admin = new NextDbClient({ endpoint, adminToken })
  assert.equal((await admin.exportManifest()).wal.records, 0)

  const roomKey = `auth-room-${Date.now()}`
  const userTokenRecord = await postJsonStatus(
    `/v1/records/rooms/${encodeURIComponent(roomKey)}-user-token`,
    { value: { id: `${roomKey}-user-token`, title: "User Token Record" } },
    clientHeaders(aliceToken),
  )
  assert.equal(userTokenRecord.status, 401)
  assert.match(userTokenRecord.body.error, /client token is required/)

  const clientRecord = await postJsonStatus(
    `/v1/records/rooms/${encodeURIComponent(roomKey)}`,
    { value: { id: roomKey, title: "Client Token Record" }, clientMutationId: `${roomKey}-upsert` },
    clientHeaders(clientToken),
  )
  assert.equal(clientRecord.status, 200)
  assert.equal(clientRecord.body.record.key, roomKey)

  const userTokenObject = await putObjectStatus(`${roomKey}-object-user-token`, "blocked", clientHeaders(aliceToken))
  assert.equal(userTokenObject.status, 401)
  assert.match(userTokenObject.body.error, /client token is required/)
  const clientObject = await putObjectStatus(`${roomKey}-object-client-token`, "allowed", clientHeaders(clientToken))
  assert.equal(clientObject.status, 200)
  assert.equal(clientObject.body.id, `${roomKey}-object-client-token`)

  const aliceRoomId = `${roomKey}-alice`
  const noClientMessage = await sendMessageStatus(aliceRoomId, "alice", "missing token")
  assert.equal(noClientMessage.status, 401)
  assert.match(noClientMessage.body.error, /client token is required/)
  const globalTokenMessage = await sendMessageStatus(aliceRoomId, "alice", "global token cannot impersonate user", clientHeaders(clientToken))
  assert.equal(globalTokenMessage.status, 401)
  assert.match(globalTokenMessage.body.error, /not authorized for userId/)
  const bobAsAlice = await sendMessageStatus(aliceRoomId, "alice", "bob cannot impersonate alice", clientHeaders(bobToken))
  assert.equal(bobAsAlice.status, 401)
  assert.match(bobAsAlice.body.error, /not authorized for userId/)

  const alice = new NextDbClient({ endpoint, userId: "alice", authToken: aliceToken })
  const aliceMessage = await alice.room(aliceRoomId).messages.send("alice token ok", {
    clientMutationId: `${roomKey}-alice-message`,
  })
  assert.equal(aliceMessage.senderId, "alice")

  const adminBob = new NextDbClient({ endpoint, userId: "bob", adminToken })
  const bobMessage = await adminBob.room(`${roomKey}-admin-bob`).messages.send("admin can act as bob", {
    clientMutationId: `${roomKey}-admin-bob-message`,
  })
  assert.equal(bobMessage.senderId, "bob")

  const userTokenVolatile = await postJsonStatus("/v1/mutate", {
    type: "publishVolatile",
    roomId: `${roomKey}-volatile`,
    name: "presence.ping",
    payload: { at: Date.now() },
  }, clientHeaders(aliceToken))
  assert.equal(userTokenVolatile.status, 401)
  assert.match(userTokenVolatile.body.error, /client token is required/)
  const clientVolatile = await postJsonStatus("/v1/mutate", {
    type: "publishVolatile",
    roomId: `${roomKey}-volatile`,
    name: "presence.ping",
    payload: { at: Date.now() },
  }, clientHeaders(clientToken))
  assert.equal(clientVolatile.status, 200)

  const bobJoinAlice = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(roomKey)}/join`, {
    userId: "alice",
    metadata: { auth: "wrong user" },
  }, clientHeaders(bobToken))
  assert.equal(bobJoinAlice.status, 401)
  assert.match(bobJoinAlice.body.error, /not authorized for userId/)
  const aliceJoin = await postJsonStatus(`/v1/realtime/channels/${encodeURIComponent(roomKey)}/join`, {
    userId: "alice",
    metadata: { auth: "ok" },
  }, clientHeaders(aliceToken))
  assert.equal(aliceJoin.status, 200)
  assert.equal(aliceJoin.body.member.userId, "alice")

  admin.close()
  alice.close()
  adminBob.close()
  console.log("auth smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function clientHeaders(token) {
  return {
    authorization: `Bearer ${token}`,
    "x-nextdb-client-token": token,
  }
}

async function getJson(path) {
  const response = await fetch(`${endpoint}${path}`)
  assert.equal(response.status, 200)
  return response.json()
}

async function getStatus(path, headers = {}) {
  const response = await fetch(`${endpoint}${path}`, { headers })
  return { status: response.status, body: await response.json().catch(() => ({})) }
}

async function postJsonStatus(path, body, headers = {}) {
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: JSON.stringify(body),
  })
  return { status: response.status, body: await response.json().catch(() => ({})) }
}

async function putObjectStatus(objectId, body, headers = {}) {
  const response = await fetch(`${endpoint}/v1/objects?${new URLSearchParams({
    objectId,
    contentType: "text/plain",
  })}`, {
    method: "POST",
    headers: {
      "content-type": "text/plain",
      ...headers,
    },
    body,
  })
  return { status: response.status, body: await response.json().catch(() => ({})) }
}

async function sendMessageStatus(roomId, userId, body, headers = {}) {
  return postJsonStatus("/v1/mutate", {
    type: "sendMessage",
    roomId,
    userId,
    body,
    durability: "strict",
    clientMutationId: `${roomId}-${userId}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  }, headers)
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
    const response = await fetch(`${url}/v1/health`).catch(() => undefined)
    return response?.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 15_000) {
  const started = Date.now()
  while (Date.now() - started < timeoutMs) {
    if (await check()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
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
