import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const wsEndpoint = endpoint.replace(/^http:/, "ws:").replace(/^https:/, "wss:")
const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
const roomA = `nested-sub-room-a-${suffix}`
const roomB = `nested-sub-room-b-${suffix}`
const db = new NextDbClient({ endpoint, userId: `nested-sub-user-${suffix}` })

try {
  const historicalA = await db.nestedTable("rooms", roomA, "messages").upsert(
    `historical-a-${suffix}`,
    messageValue(roomA, `historical-a-${suffix}`),
    { clientMutationId: `historical-a-${suffix}` },
  )
  await db.nestedTable("rooms", roomB, "messages").upsert(
    `historical-b-${suffix}`,
    messageValue(roomB, `historical-b-${suffix}`),
    { clientMutationId: `historical-b-${suffix}` },
  )

  const catchUp = await openSocket(`/v1/connect?userId=${encodeURIComponent(`nested-catchup-${suffix}`)}`)
  catchUp.socket.send(JSON.stringify({
    type: "subscribeNestedTable",
    table: "rooms",
    parentKey: roomA,
    nested: "messages",
    afterLsn: 0,
    catchUpLimit: 20,
  }))
  await waitForFrame(catchUp.frames, (frame) => frame.type === "tableSubscribed" && frame.table === "rooms.messages", "nested catch-up subscription")
  await waitForFrame(
    catchUp.frames,
    (frame) => frame.type === "event" && frame.event?.type === "recordUpserted" && frame.event.key === `${roomA}:historical-a-${suffix}`,
    "nested catch-up target event",
  )
  const catchUpComplete = await waitForFrame(catchUp.frames, (frame) => frame.type === "subscriptionCatchUp", "nested catch-up complete")
  assert.deepEqual(catchUpComplete.nestedTables, [{ table: "rooms", parentKey: roomA, nested: "messages" }])
  assert.equal(
    catchUp.frames.some((frame) => frame.type === "event" && frame.event?.key === `${roomB}:historical-b-${suffix}`),
    false,
    "nested catch-up must not replay other parent partitions",
  )
  catchUp.socket.close()

  const nestedSyncParams = new URLSearchParams({
    afterLsn: "0",
    nestedTables: `rooms:${roomA}:messages`,
    limit: "50",
  })
  const nestedSync = await getJson(`/v1/sync/pull?${nestedSyncParams}`)
  assert(
    nestedSync.events.some((event) => event.type === "recordUpserted" && event.key === `${roomA}:historical-a-${suffix}`),
    "nested sync must include target parent partition event",
  )
  assert.equal(
    nestedSync.events.some((event) => event.type === "recordUpserted" && event.key === `${roomB}:historical-b-${suffix}`),
    false,
    "nested sync must not include other parent partitions",
  )

  const live = await openSocket(`/v1/connect?userId=${encodeURIComponent(`nested-live-${suffix}`)}`)
  live.socket.send(JSON.stringify({
    type: "subscribeNestedTable",
    table: "rooms",
    parentKey: roomA,
    nested: "messages",
  }))
  await waitForFrame(live.frames, (frame) => frame.type === "tableSubscribed" && frame.table === "rooms.messages", "nested live subscription")

  await db.nestedTable("rooms", roomB, "messages").upsert(
    `live-b-${suffix}`,
    messageValue(roomB, `live-b-${suffix}`),
    { clientMutationId: `live-b-${suffix}` },
  )
  await waitForStableEventCount(live.frames, 0, 250, "nested live subscription must ignore other parent partitions")

  const liveA = await db.nestedTable("rooms", roomA, "messages").upsert(
    `live-a-${suffix}`,
    messageValue(roomA, `live-a-${suffix}`),
    { clientMutationId: `live-a-${suffix}` },
  )
  await waitForFrame(
    live.frames,
    (frame) => frame.type === "event" &&
      frame.event?.type === "recordUpserted" &&
      frame.event.key === `${roomA}:live-a-${suffix}` &&
      frame.event.record?.lsn === liveA.lsn,
    "nested live target event",
  )

  const connections = await getJson(`/v1/admin/connections?userId=${encodeURIComponent(`nested-live-${suffix}`)}`)
  assert.equal(connections.total, 1)
  assert.deepEqual(connections.sessions[0].subscribedTables, [])
  assert.deepEqual(connections.sessions[0].subscribedNestedTables, [`rooms/${roomA}/messages`])

  live.socket.send(JSON.stringify({
    type: "unsubscribeNestedTable",
    table: "rooms",
    parentKey: roomA,
    nested: "messages",
  }))
  await waitForFrame(live.frames, (frame) => frame.type === "tableUnsubscribed" && frame.table === "rooms.messages", "nested unsubscribe")
  live.socket.close()

  console.log("nested subscription smoke ok")
} finally {
  db.close()
}

function messageValue(roomId, id) {
  return {
    id,
    roomId,
    senderId: `nested-sub-user-${suffix}`,
    body: id,
    attachments: [],
    createdAtMs: Date.now(),
    path: `rooms/${roomId}/messages/${id}`,
  }
}

async function getJson(path) {
  const response = await fetch(new URL(path, endpoint))
  if (!response.ok) {
    assert.fail(`${path} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function openSocket(path) {
  const url = new URL(path, wsEndpoint)
  const socket = new WebSocket(url)
  const frames = []
  socket.addEventListener("message", (event) => frames.push(JSON.parse(event.data)))
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true })
    socket.addEventListener("error", reject, { once: true })
  })
  await waitForFrame(frames, (frame) => frame.type === "hello", "websocket hello")
  return { socket, frames }
}

async function waitForFrame(frames, predicate, label, timeoutMs = 2_500) {
  const started = Date.now()
  while (Date.now() - started < timeoutMs) {
    const frame = frames.find(predicate)
    if (frame) {
      return frame
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  assert.fail(`timed out waiting for ${label}`)
}

async function waitForStableEventCount(frames, expected, stableMs, label) {
  const started = Date.now()
  while (Date.now() - started < stableMs) {
    assert.equal(frames.filter((frame) => frame.type === "event").length, expected, label)
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}
