import assert from "node:assert/strict"
import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const behaviorExample = resolve(root, "examples/behaviors/echo")
const behaviorWasm = resolve(root, "target/wasm32-unknown-unknown/release/nextdb_echo_behavior.wasm")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-behavior-rust-wasm-"))
const dataDir = join(tempRoot, "data")
const behaviorOut = join(dataDir, "behaviors", "echo")
const node = {
  url: "http://127.0.0.1:3397",
  addr: "127.0.0.1:3397",
  dataDir,
}
const children = []
let db

try {
  await run("cargo", [
    "build",
    "--manifest-path",
    join(behaviorExample, "Cargo.toml"),
    "--target",
    "wasm32-unknown-unknown",
    "--release",
  ])
  await mkdir(behaviorOut, { recursive: true })
  await copyFile(join(behaviorExample, "nextdb.behavior.json"), join(behaviorOut, "nextdb.behavior.json"))
  await copyFile(behaviorWasm, join(behaviorOut, "echo.wasm"))

  children.push(startNode(node))
  await waitForHealth(node.url)
  db = new NextDbClient({
    endpoint: node.url,
    userId: "alice",
    sessionId: "alice-rust-behavior-session",
    connectionMetadata: { device: "rust-smoke", capabilities: ["behavior"] },
  })

  const behaviors = await db.listBehaviors()
  assert(behaviors.some((behavior) => behavior.name === "echo" && behavior.mutations.includes("echo.send")))
  const echoBehavior = behaviors.find((behavior) => behavior.name === "echo")
  assert.deepEqual(echoBehavior?.reads, [
    "records",
    "nestedRecords",
    "objectBodies",
    "realtimeChannelMembers",
    "realtimeChannelStates",
    "connectionSessions",
  ])
  assert.deepEqual(echoBehavior?.recordScopes, {
    read: ["rooms"],
    write: ["rooms"],
    nestedRead: ["rooms.messages"],
    nestedWrite: ["rooms.messages"],
  })
  assert.deepEqual(echoBehavior?.objectScopes, {
    read: ["rust-behavior-object-*"],
    write: ["rust-behavior-output-*"],
  })
  assert.deepEqual(echoBehavior?.realtimeScopes, {
    read: ["rust-behavior-room-*"],
  })
  assert.deepEqual(echoBehavior?.connectionScopes, {
    read: ["alice"],
  })

  const roomId = `rust-behavior-room-${Date.now()}`
  const seedNestedId = "seed-nested"
  const objectId = `rust-behavior-object-${Date.now()}`
  const objectBody = "read plan object body"

  await db.table("rooms").upsert(roomId, { id: roomId, title: "Read Plan Room" })
  await postJson(`${node.url}/v1/records/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent(seedNestedId)}`, {
    value: {
      id: seedNestedId,
      roomId,
      senderId: "alice",
      body: "seed nested body",
      attachments: [],
      createdAtMs: 1,
      path: `tables/rooms/${roomId}/messages/${seedNestedId}`,
    },
    durability: "strict",
    clientMutationId: `${roomId}-seed-nested`,
  })
  await db.putObject(objectBody, {
    contentType: "text/plain",
    objectId,
    clientMutationId: `${objectId}-put`,
  })
  await db.realtimeChannel(roomId).join({ role: "rust-behavior-smoke" })
  await db.realtimeChannel(roomId).updateState({ label: "seeded", roomId }, { expectedVersion: 0 })

  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoManifest({ reads: ["records"] }), null, 2)}\n`,
  )
  const restrictedReload = await db.reloadBehaviors()
  assert.equal(restrictedReload.loaded, 1)
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo",
      mutation: "echo.send",
      input: {
        roomId,
        body: "blocked read plan",
      },
      read: {
        objectBodies: [{ objectId }],
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to use read plan"),
  )
  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoManifest({
      recordScopes: {
        read: ["users"],
        write: ["rooms"],
        nestedRead: ["rooms.messages"],
        nestedWrite: ["rooms.messages"],
      },
    }), null, 2)}\n`,
  )
  const scopedReload = await db.reloadBehaviors()
  assert.equal(scopedReload.loaded, 1)
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo",
      mutation: "echo.send",
      input: {
        roomId,
        body: "blocked read scope",
      },
      read: {
        records: [{ table: "rooms", key: roomId }],
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to read table 'rooms'"),
  )
  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoManifest({
      objectScopes: {
        read: ["other-object-*"],
        write: ["rust-behavior-output-*"],
      },
    }), null, 2)}\n`,
  )
  const objectScopedReload = await db.reloadBehaviors()
  assert.equal(objectScopedReload.loaded, 1)
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo",
      mutation: "echo.send",
      input: {
        roomId,
        body: "blocked object read scope",
      },
      read: {
        objectBodies: [{ objectId }],
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to read object"),
  )
  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoManifest({
      realtimeScopes: {
        read: ["other-channel-*"],
      },
    }), null, 2)}\n`,
  )
  const realtimeScopedReload = await db.reloadBehaviors()
  assert.equal(realtimeScopedReload.loaded, 1)
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo",
      mutation: "echo.send",
      input: {
        roomId,
        body: "blocked realtime read scope",
      },
      read: {
        realtimeChannelMembers: [{ channelId: roomId }],
        realtimeChannelStates: [{ channelId: roomId }],
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to read realtime channel"),
  )
  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoManifest({
      connectionScopes: {
        read: ["bob"],
      },
    }), null, 2)}\n`,
  )
  const connectionScopedReload = await db.reloadBehaviors()
  assert.equal(connectionScopedReload.loaded, 1)
  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo",
      mutation: "echo.send",
      input: {
        roomId,
        body: "blocked connection read scope",
      },
      read: {
        connectionSessions: [{ userId: "alice" }],
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to read connection sessions for user"),
  )
  await writeFile(
    join(behaviorOut, "nextdb.behavior.json"),
    `${JSON.stringify(echoManifest(), null, 2)}\n`,
  )
  const restoredReload = await db.reloadBehaviors()
  assert.equal(restoredReload.loaded, 1)

  await db.table("rooms").upsert(roomId, { id: roomId, title: "Read Plan Room Hot" }, {
    durability: "volatile",
    clientMutationId: `${roomId}-hot-title`,
  })

  const response = await db.invokeBehavior({
    behavior: "echo",
    mutation: "echo.send",
    clientMutationId: `${roomId}-invoke`,
    input: {
      roomId,
      body: "hello rust wasm",
    },
    read: {
      records: [{ table: "rooms", key: roomId }],
      nestedRecords: [{ table: "rooms", parentKey: roomId, nested: "messages", nestedKey: seedNestedId }],
      objectBodies: [{ objectId }],
      realtimeChannelMembers: [{ channelId: roomId }],
      realtimeChannelStates: [{ channelId: roomId }],
      connectionSessions: [{ userId: "alice", sessionId: "alice-rust-behavior-session", transport: "webSocket" }],
    },
  })

  assert.equal(response.output.result.handledBy, "nextdb-echo-behavior")
  assert.equal(response.output.result.userId, "alice")
  assert.equal(response.output.result.clientMutationId, `${roomId}-invoke`)
  assert.equal(response.output.result.existingTitle, "Read Plan Room Hot")
  assert.equal(response.output.result.nestedBody, "seed nested body")
  assert.equal(response.output.result.objectBodyReads, 1)
  assert.equal(response.output.result.realtimeMemberCount, 1)
  assert.equal(response.output.result.realtimeStateVersion, 1)
  assert.equal(response.output.result.realtimeStateLabel, "seeded")
  assert.equal(response.output.result.connectionSessionCount, 1)
  assert.equal(response.output.result.connectionMetadataDevice, "rust-smoke")
  assert.equal(typeof response.output.result.runtimeTimestampMs, "number")
  assert.equal(response.output.result.runtimeSenderKind, "user")
  assert.equal(typeof response.output.result.runtimeRngSeed, "string")
  assert(response.output.result.runtimeRngSeed.length > 0)
  assert.deepEqual(response.committed.map((entry) => entry.type), [
    "recordUpserted",
    "recordTransactionCommitted",
    "objectCommitted",
    "messageCreated",
  ])

  const room = await db.table("rooms").get(roomId)
  assert.equal(room.value.title, "Read Plan Room Hot")

  const behaviorNested = await getJson(`${node.url}/v1/records/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent("behavior-echo.send")}`)
  assert.equal(behaviorNested.record.value.body, "[echo:echo.send] hello rust wasm")

  const objectCommit = response.committed.find((entry) => entry.type === "objectCommitted")
  assert(objectCommit?.object?.id)
  const committedObjectBody = await db.getObjectBody(objectCommit.object.id)
  assert.equal(await committedObjectBody.text(), "[echo:echo.send] hello rust wasm")

  const messages = await db.room(roomId).messages.latest({ limit: 10 })
  assert.equal(messages.messages.length, 1)
  assert.equal(messages.messages[0].body, "[echo:echo.send] hello rust wasm")
  assert.equal(messages.messages[0].senderId, "alice")

  const retry = await db.invokeBehavior({
    behavior: "echo",
    mutation: "echo.send",
    clientMutationId: `${roomId}-invoke`,
    input: {
      roomId,
      body: "changed body",
    },
    read: {
      records: [{ table: "rooms", key: roomId }],
      nestedRecords: [{ table: "rooms", parentKey: roomId, nested: "messages", nestedKey: seedNestedId }],
      objectBodies: [{ objectId }],
      realtimeChannelMembers: [{ channelId: roomId }],
      realtimeChannelStates: [{ channelId: roomId }],
      connectionSessions: [{ userId: "alice", sessionId: "alice-rust-behavior-session", transport: "webSocket" }],
    },
  })
  assert.deepEqual(retry.committed.map((entry) => entry.type), [
    "recordUpserted",
    "recordTransactionCommitted",
    "objectCommitted",
    "messageCreated",
  ])
  assert.equal(retry.output.result.clientMutationId, `${roomId}-invoke`)
  const afterRetryMessages = await db.room(roomId).messages.latest({ limit: 10 })
  assert.equal(afterRetryMessages.messages.length, 1)
  assert.equal(afterRetryMessages.messages[0].body, "[echo:echo.send] hello rust wasm")

  const audit = await getJson(`${node.url}/v1/audit/wal?clientMutationId=${encodeURIComponent(`${roomId}-invoke:003:sendMessage`)}`)
  assert.equal(audit.records.length, 1)

  const evictedRuntimeRoom = await db.evictRuntimeRoom({ roomId })
  assert.equal(evictedRuntimeRoom.evicted, true)
  const runtimeActivationResponse = await db.invokeBehavior({
    behavior: "echo",
    mutation: "echo.send",
    input: {
      roomId,
      body: "runtime activation from rust wasm",
      runtimeActivate: true,
    },
  })
  assert.deepEqual(runtimeActivationResponse.committed.map((entry) => entry.type), [
    "recordUpserted",
    "recordTransactionCommitted",
    "objectCommitted",
    "runtimeRecordsActivated",
    "runtimeRoomActivated",
    "messageCreated",
  ])
  assert.equal(runtimeActivationResponse.committed[3]?.response?.table, "rooms")
  assert.equal(runtimeActivationResponse.committed[3]?.response?.found, 1)
  assert.equal(runtimeActivationResponse.committed[4]?.response?.roomId, roomId)
  assert(runtimeActivationResponse.committed[4]?.response?.found >= 1)

  console.log("behavior rust wasm smoke ok")
} finally {
  db?.close()
  await Promise.all(children.map((child) => stopNode(child)))
  await rm(tempRoot, { recursive: true, force: true })
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
    if (process.env.NEXTDB_BEHAVIOR_RUST_WASM_SMOKE_LOGS === "1") {
      process.stdout.write(`[rust-behavior] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_BEHAVIOR_RUST_WASM_SMOKE_LOGS === "1") {
      process.stderr.write(`[rust-behavior] ${chunk}`)
    }
  })
  return child
}

function echoManifest({
  reads = ["records", "nestedRecords", "objectBodies", "realtimeChannelMembers", "realtimeChannelStates", "connectionSessions"],
  recordScopes = {
    read: ["rooms"],
    write: ["rooms"],
    nestedRead: ["rooms.messages"],
    nestedWrite: ["rooms.messages"],
  },
  objectScopes = {
    read: ["rust-behavior-object-*"],
    write: ["rust-behavior-output-*"],
  },
  realtimeScopes = {
    read: ["rust-behavior-room-*"],
  },
  connectionScopes = {
    read: ["alice"],
  },
} = {}) {
  return {
    name: "echo",
    version: "0.1.0",
    modulePath: "echo.wasm",
    abiEncoding: "postcardTypedSchema",
    mutations: ["echo.send"],
    inputs: {
      "echo.send": {
        type: {
          kind: "object",
          fields: {
            roomId: { type: { kind: "id", entity: "Room" } },
            body: { type: { kind: "string" } },
          },
        },
      },
    },
    reads,
    recordScopes,
    objectScopes,
    realtimeScopes,
    connectionScopes,
    commands: ["upsertRecord", "recordTransaction", "putObject", "activateRuntimeRecords", "activateRuntimeRoom", "sendMessage"],
    maxFuel: 1_000_000,
  }
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
    const health = await getJson(`${url}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${url}`)
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
    await new Promise((resolve) => setTimeout(resolve, 100))
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

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST ${url} ${response.status}: ${text}`)
  }
  return text ? JSON.parse(text) : undefined
}

async function run(command, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: root,
      stdio: process.env.NEXTDB_BEHAVIOR_RUST_WASM_SMOKE_LOGS === "1" ? "inherit" : "pipe",
    })
    let stderr = ""
    child.stderr?.on("data", (chunk) => {
      stderr += chunk
    })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolve()
      } else {
        reject(new Error(`${command} ${args.join(" ")} failed with ${code}: ${stderr}`))
      }
    })
  })
}
