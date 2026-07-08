import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-runtime-limits-"))
const port = await availablePort()
const node = {
  url: `http://127.0.0.1:${port}`,
  addr: `127.0.0.1:${port}`,
  dataDir: join(tempRoot, "data"),
}
let child

try {
  await mkdir(node.dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const db = new NextDbClient({ endpoint: node.url, userId: "alice" })
  const health = await db.health()
  assert.equal(health.limits.maxObjectBytes, 8)
  assert.equal(health.limits.maxMessageBytes, 4)
  assert.equal(health.limits.maxUserEventBytes, 8)
  assert.equal(health.limits.maxRecordValueBytes, 8)
  assert.equal(health.limits.maxLiveQueriesPerConnection, 3)
  assert.equal(health.limits.maxLiveQueriesPerTablePerConnection, 2)
  assert.equal(health.limits.maxLiveQueriesPerUser, 4)
  assert.equal(health.connectionLayer.protocol, "nextdb.realtime.v1")
  assert.equal(health.connectionLayer.frameEncoding, "json")
  assert.equal(health.connectionLayer.connectPath, "/v1/connect")
  assert.deepEqual(health.connectionLayer.supportedTransports, ["webSocket", "custom"])
  assert.equal(health.connectionLayer.defaultTransport, "webSocket")
  assert.equal(health.connectionLayer.webSocket.supported, true)
  assert.equal(health.connectionLayer.webTransport.supported, false)
  assert.equal(health.connectionLayer.custom.supported, true)
  assert.equal(health.connectionLayer.custom.connectPath, "/v1/connect/jsonl")
  assert.deepEqual(await db.realtimeTransportCompatibility(), {
    requestedKind: "websocket",
    requestedTransport: "webSocket",
    supported: true,
    status: "supported",
    supportedTransports: ["webSocket", "custom"],
    defaultTransport: "webSocket",
  })
  assert.equal((await db.realtimeTransportCompatibility("webtransport")).supported, false)
  assert.equal((await db.realtimeTransportCompatibility("webtransport")).fallbackTransport, "webSocket")
  assert.deepEqual(await db.realtimeTransportCompatibility("jsonl"), {
    requestedKind: "jsonl",
    requestedTransport: "custom",
    supported: true,
    status: "supported",
    supportedTransports: ["webSocket", "custom"],
    defaultTransport: "webSocket",
  })
  const jsonlResponse = await fetch(`${node.url}/v1/connect/jsonl?userId=alice&sessionId=jsonl-smoke`, {
    method: "POST",
    headers: { "content-type": "application/x-ndjson" },
    body: '{"type":"subscribeObjects","afterLsn":0,"catchUpLimit":1}\n',
  })
  assert.equal(jsonlResponse.status, 200)
  assert.equal(jsonlResponse.headers.get("content-type"), "application/x-ndjson")
  const jsonlFrames = (await jsonlResponse.text())
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line))
  assert.deepEqual(jsonlFrames[0], { type: "hello", userId: "alice", sessionId: "jsonl-smoke" })
  assert.ok(jsonlFrames.some((frame) => frame.type === "objectsSubscribed"))
  const jsonlFallbackClient = new NextDbClient({
    endpoint: node.url,
    userId: "alice",
    sessionId: "jsonl-compatible-smoke",
    realtimeTransportKind: "webtransport",
  })
  const jsonlFallback = await jsonlFallbackClient.connectCompatibleRealtime({ fallbackTo: "jsonl" })
  assert.equal(jsonlFallback.requestedKind, "webtransport")
  assert.equal(jsonlFallback.requestedTransport, "webTransport")
  assert.equal(jsonlFallback.supported, false)
  assert.equal(jsonlFallback.fallbackTransport, "custom")
  assert.equal(jsonlFallback.fallbackApplied, true)
  assert.equal(jsonlFallback.connected, true)
  assert.equal(jsonlFallback.activeKind, "jsonl")
  assert.equal(jsonlFallback.activeTransport, "custom")
  jsonlFallbackClient.close()

  const metrics = await db.metrics()
  assert.match(metrics, /^nextdb_limit_max_object_bytes 8$/m)
  assert.match(metrics, /^nextdb_limit_max_message_bytes 4$/m)
  assert.match(metrics, /^nextdb_limit_max_user_event_bytes 8$/m)
  assert.match(metrics, /^nextdb_limit_max_record_value_bytes 8$/m)
  assert.match(metrics, /^nextdb_limit_max_live_queries_per_connection 3$/m)
  assert.match(metrics, /^nextdb_limit_max_live_queries_per_table_per_connection 2$/m)
  assert.match(metrics, /^nextdb_limit_max_live_queries_per_user 4$/m)
  assert.match(metrics, /^nextdb_wal_repair_controller_interval_ms 0$/m)
  assert.match(metrics, /^nextdb_wal_repair_controller_repaired_replicas 0$/m)
  assert.match(metrics, /^nextdb_wal_repair_controller_satisfied 1$/m)
  assert.match(metrics, /^nextdb_wal_repair_controller_last_error 0$/m)
  assert.match(metrics, /^nextdb_object_repair_controller_interval_ms 0$/m)
  assert.match(metrics, /^nextdb_object_repair_controller_repaired_replicas 0$/m)
  assert.match(metrics, /^nextdb_object_repair_controller_satisfied 1$/m)
  assert.match(metrics, /^nextdb_object_repair_controller_last_error 0$/m)

  const objectId = `limit-object-${Date.now()}`
  const smallObject = await db.putObject("12345678", {
    objectId,
    contentType: "text/plain",
    clientMutationId: `${objectId}-put`,
  })
  assert.equal(smallObject.byteSize, 8)
  await assertLimit(
    putObject(node.url, `limit-object-big-${Date.now()}`, "123456789"),
    "object body exceeds limit",
  )

  await db.sendMessage(`limit-room-${Date.now()}`, "hey", {
    userId: "alice",
    clientMutationId: `limit-message-ok-${Date.now()}`,
  })
  await assertLimit(
    postJson(`${node.url}/v1/mutate`, {
      type: "sendMessage",
      roomId: `limit-room-${Date.now()}`,
      userId: "alice",
      body: "hello",
      durability: "strict",
      clientMutationId: `limit-message-big-${Date.now()}`,
    }),
    "message body exceeds limit",
  )

  await assertLimit(
    postJson(`${node.url}/v1/mutate`, {
      type: "publishUserEvent",
      userId: "alice",
      name: "limit.event",
      payload: { value: "123456789" },
      durability: "strict",
    }),
    "user event payload exceeds limit",
  )

  const realtimeLimitChannel = `limit-channel-${Date.now()}`
  const realtimeJoin = await postJson(`${node.url}/v1/realtime/channels/${encodeURIComponent(realtimeLimitChannel)}/join`, {
    userId: "alice",
    metadata: {},
  })
  assert.equal(realtimeJoin.status, 200)
  const beforeRealtimeSequence = await realtimeChannelSequence(node.url, realtimeLimitChannel)
  await assertLimit(
    postJson(`${node.url}/v1/realtime/channels/${encodeURIComponent(realtimeLimitChannel)}/broadcast`, {
      fromUserId: "alice",
      kind: "gameInput",
      payload: { value: "123456789" },
      includeSelf: true,
    }),
    "volatile user event payload exceeds limit",
  )
  assert.equal(await realtimeChannelSequence(node.url, realtimeLimitChannel), beforeRealtimeSequence)
  await assertLiveQueryBudget(node.url)

  await assertLimit(
    postJson(`${node.url}/v1/records/rooms/${encodeURIComponent(`limit-room-record-${Date.now()}`)}`, {
      value: { id: "limit-room-record", title: "too large" },
      durability: "strict",
    }),
    "record value exceeds limit",
  )

  db.close()
  console.log("runtime limits smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function assertLimit(promise, message) {
  const response = await promise
  assert.equal(response.status, 413)
  const body = await response.json()
  assert.match(body.error, new RegExp(message))
}

async function realtimeChannelSequence(baseUrl, channelId) {
  const realtimeChannels = await (await fetch(`${baseUrl}/v1/realtime/channels`)).json()
  const channel = realtimeChannels.channels.find((summary) => summary.channelId === channelId)
  assert(Number.isInteger(channel?.sequence))
  return channel.sequence
}

async function putObject(baseUrl, objectId, body) {
  const url = new URL("/v1/objects", baseUrl)
  url.searchParams.set("objectId", objectId)
  url.searchParams.set("contentType", "text/plain")
  return fetch(url, {
    method: "POST",
    body,
  })
}

async function postJson(url, body) {
  return fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
}

function startNode(node) {
  return spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: node.addr,
      NEXTDB_DATA_DIR: node.dataDir,
      NEXTDB_MAX_OBJECT_BYTES: "8",
      NEXTDB_MAX_MESSAGE_BYTES: "4",
      NEXTDB_MAX_USER_EVENT_BYTES: "8",
      NEXTDB_MAX_RECORD_VALUE_BYTES: "8",
      NEXTDB_MAX_LIVE_QUERIES_PER_CONNECTION: "3",
      NEXTDB_MAX_LIVE_QUERIES_PER_TABLE_PER_CONNECTION: "2",
      NEXTDB_MAX_LIVE_QUERIES_PER_USER: "4",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
}

async function assertLiveQueryBudget(baseUrl) {
  const wsBase = baseUrl.replace(/^http:/, "ws:").replace(/^https:/, "wss:")
  const frames = []
  const ws = new WebSocket(`${wsBase}/v1/connect?userId=alice&sessionId=live-query-budget`)
  const secondFrames = []
  const secondWs = new WebSocket(`${wsBase}/v1/connect?userId=alice&sessionId=live-query-budget-2`)
  ws.addEventListener("message", (event) => frames.push(JSON.parse(event.data)))
  secondWs.addEventListener("message", (event) => secondFrames.push(JSON.parse(event.data)))

  try {
    await waitForFrame(frames, (frame) => frame.type === "hello", "live query budget hello")
    await waitForFrame(secondFrames, (frame) => frame.type === "hello", "second live query budget hello")
    ws.send(JSON.stringify({ type: "subscribeQuery", queryId: "budget-rooms-1", table: "rooms", limit: 1 }))
    await waitForFrame(frames, (frame) => frame.type === "querySubscribed" && frame.queryId === "budget-rooms-1", "first query subscription")
    ws.send(JSON.stringify({ type: "subscribeQuery", queryId: "budget-rooms-2", table: "rooms", limit: 1 }))
    await waitForFrame(frames, (frame) => frame.type === "querySubscribed" && frame.queryId === "budget-rooms-2", "second query subscription")
    ws.send(JSON.stringify({ type: "subscribeQuery", queryId: "budget-rooms-3", table: "rooms", limit: 1 }))
    const tableLimit = await waitForFrame(
      frames,
      (frame) => frame.type === "error" && /maxLiveQueriesPerTablePerConnection=2/.test(frame.message),
      "table query budget rejection",
    )
    assert.match(tableLimit.message, /table=rooms/)

    ws.send(JSON.stringify({
      type: "subscribeQuery",
      queryId: "budget-messages-1",
      table: "rooms",
      parentKey: "budget-room",
      nested: "messages",
      limit: 1,
    }))
    await waitForFrame(frames, (frame) => frame.type === "querySubscribed" && frame.queryId === "budget-messages-1", "nested query subscription")
    ws.send(JSON.stringify({
      type: "subscribeQuery",
      queryId: "budget-messages-2",
      table: "rooms",
      parentKey: "budget-room",
      nested: "messages",
      limit: 1,
    }))
    await waitForFrame(
      frames,
      (frame) => frame.type === "error" && /maxLiveQueriesPerConnection=3/.test(frame.message),
      "connection query budget rejection",
    )

    const connections = await (await fetch(`${baseUrl}/v1/admin/connections?userId=alice`)).json()
    const session = connections.sessions.find((candidate) => candidate.sessionId === "live-query-budget")
    assert.deepEqual(session.subscribedQueries.sort(), ["budget-messages-1", "budget-rooms-1", "budget-rooms-2"])
    assert.deepEqual(session.subscribedQueryTables, { rooms: 2, "rooms.messages": 1 })
    const summary = connections.userSummaries.find((candidate) => candidate.userId === "alice")
    assert.deepEqual(summary.subscribedQueryTables, { rooms: 2, "rooms.messages": 1 })

    secondWs.send(JSON.stringify({ type: "subscribeQuery", queryId: "budget-second-rooms-1", table: "rooms", limit: 1 }))
    await waitForFrame(
      secondFrames,
      (frame) => frame.type === "querySubscribed" && frame.queryId === "budget-second-rooms-1",
      "second session first query subscription",
    )
    secondWs.send(JSON.stringify({ type: "subscribeQuery", queryId: "budget-second-rooms-2", table: "rooms", limit: 1 }))
    await waitForFrame(
      secondFrames,
      (frame) => frame.type === "error" && /maxLiveQueriesPerUser=4/.test(frame.message),
      "user query budget rejection",
    )
  } finally {
    ws.close()
    secondWs.close()
  }
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
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}`)
}

async function waitForFrame(frames, predicate, label) {
  let found
  await waitFor(() => {
    found = frames.find(predicate)
    return found !== undefined
  }, label)
  return found
}

function availablePort() {
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
        if (typeof port !== "number") {
          reject(new Error("failed to allocate runtime limits smoke port"))
          return
        }
        resolve(port)
      })
    })
  })
}
