import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
const roomId = `volatile-message-${suffix}`
const alice = new NextDbClient({
  endpoint,
  userId: `alice-${suffix}`,
  sessionId: `alice-${suffix}-session`,
})
const bob = new NextDbClient({
  endpoint,
  userId: `bob-${suffix}`,
  sessionId: `bob-${suffix}-session`,
})

const bobEvents = []
const stopBob = bob.room(roomId).messages.subscribe((event) => {
  bobEvents.push(event)
})

try {
  await waitFor(async () => {
    const connections = await bob.listConnections(`bob-${suffix}`)
    return connections.sessions.some((session) => session.subscribedRooms.includes(roomId))
  }, "bob room subscription")

  const body = `volatile hello ${suffix}`
  const message = await alice.room(roomId).messages.send(body, {
    durability: "volatile",
    clientMutationId: `volatile-${suffix}`,
  })

  assert.equal(message.lsn, 0)
  assert.equal(message.body, body)
  assert.match(message.path, /^volatile\/rooms\//)

  await waitFor(() =>
    bobEvents.some((event) =>
      event.type === "messageCreated" &&
      event.message.id === message.id &&
      event.message.lsn === 0 &&
      event.message.path.startsWith("volatile/"),
    ), "bob receives volatile message")

  const latest = await alice.room(roomId).messages.latest(10)
  assert.equal(latest.source, "live")
  assert(latest.messages.some((entry) => entry.id === message.id && entry.lsn === 0))

  const cached = await alice.room(roomId).messages.cached({ limit: 10 })
  assert.equal(cached.messages.some((entry) => entry.id === message.id), false)

  const audit = await alice.auditWal({
    payloadType: "messageCreated",
    roomId,
    clientMutationId: `volatile-${suffix}`,
  })
  assert.equal(audit.records.length, 0)

  const sync = await alice.syncPull({ rooms: [roomId], afterLsn: 0, limit: 50 })
  assert.equal(sync.events.some((event) => event.type === "messageCreated" && event.message.id === message.id), false)

  console.log("volatile message smoke ok")
} finally {
  stopBob()
  alice.close()
  bob.close()
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
