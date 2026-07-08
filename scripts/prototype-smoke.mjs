import assert from "node:assert/strict"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`

const ready = await getJson("/v1/ready")
assert.equal(ready.ok, true)
assert.equal(ready.readReady, true)
assert.equal(ready.writeReady, true)
assert.equal(ready.realtimeReady, true)
assert.equal(ready.checks.some((check) => check.name === "wal" && check.ok), true)
assert.equal(ready.checks.some((check) => check.name === "connectionLayer" && check.ok), true)

const health = await getJson("/v1/health")
assert.equal(health.ok, true)
assert.equal(health.acceptingWrites, true)
assert.equal(health.connectionLayer.protocol, "nextdb.realtime.v1")
assert.equal(health.connectionLayer.supportedTransports.includes("webSocket"), true)
assert.equal(health.connectionLayer.supportedTransports.includes("custom"), true)
assert.equal(health.objectStore, "enabled")
assert.equal(typeof health.recordHotCache.tableCount, "number")

const schema = await getJson("/v1/schema")
assert.equal(schema.name, "nextdb")
assert(schema.objects.Object)
assert(schema.tables.rooms)
assert.equal(schema.tables.rooms.nested.messages.storage.kind, "chatLog")
assert.deepEqual(schema.tables.rooms.nested.messages.fields.attachments.type, {
  kind: "list",
  item: { kind: "objectRef", object: "Object" },
})
assert(schema.behaviors.echo)
assert(schema.behaviors.echo.mutations["echo.send"])
assert(schema.events["notification.created"])
assert(schema.events["realtime.channel.event"])

const behaviors = await getJson("/v1/behaviors")
assert(behaviors.some((behavior) =>
  behavior.name === "echo-ts" &&
  behavior.mutations.includes("echo.send") &&
  behavior.commands.includes("sendMessage")
))

const roomId = `prototype-room-${suffix}`
const objectId = `prototype-object-${suffix}`
const objectBody = `prototype object body ${suffix}`
const object = await postBytes(`/v1/objects?objectId=${encodeURIComponent(objectId)}&contentType=text/plain`, objectBody, {
  "content-type": "text/plain",
})
assert.equal(object.id, objectId)
assert.equal(object.byteSize, objectBody.length)
assert.equal(await getText(`/v1/objects/${encodeURIComponent(objectId)}/body`), objectBody)

const room = await postJson(`/v1/records/rooms/${encodeURIComponent(roomId)}`, {
  value: { id: roomId, title: "Prototype Acceptance" },
  durability: "strict",
  clientMutationId: `${roomId}-record`,
})
assert.equal(room.record.key, roomId)
assert(room.record.lsn > 0)

const message = await postJson("/v1/mutate", {
  type: "sendMessage",
  roomId,
  userId: "prototype-user",
  body: "prototype durable message",
  attachments: [objectId],
  durability: "strict",
  clientMutationId: `${roomId}-message`,
})
assert.equal(message.type, "messageCreated")
assert(message.message.lsn > room.record.lsn)

const latest = await getJson(`/v1/rooms/${encodeURIComponent(roomId)}/messages/latest?limit=5`)
assert.equal(latest.messages[0].body, "prototype durable message")
assert.equal(latest.messages[0].attachments[0].id, objectId)

const channelId = `prototype-channel-${suffix}`
const join = await postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/join`, {
  userId: "prototype-user",
  metadata: { role: "acceptance" },
})
assert.equal(join.member.userId, "prototype-user")
const state = await postJson(`/v1/realtime/channels/${encodeURIComponent(channelId)}/state`, {
  fromUserId: "prototype-user",
  state: { phase: "acceptance", suffix },
})
assert.equal(state.state.version, 1)
assert.equal(state.state.state.phase, "acceptance")
const channels = await getJson("/v1/realtime/channels")
assert(channels.channels.some((channel) => channel.channelId === channelId && channel.stateVersion === 1))

const manifest = await getJson("/v1/admin/export/manifest")
assert.equal(manifest.format, "nextdb.logical-export-manifest.v1")
assert(manifest.currentLsn >= message.message.lsn)
assert(manifest.wal.records >= 2)
assert.equal(manifest.wal.checksumMismatch, 0)
assert(manifest.objects.live >= 1)

const audit = await getJson(`/v1/audit/wal?payloadType=messageCreated&roomId=${encodeURIComponent(roomId)}&limit=10`)
assert(audit.records.some((record) => record.payload?.message?.body === "prototype durable message"))

console.log("prototype smoke ok")

async function getJson(path) {
  const response = await fetch(`${endpoint}${path}`)
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return JSON.parse(text)
}

async function getText(path) {
  const response = await fetch(`${endpoint}${path}`)
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return text
}

async function postJson(path, body) {
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return JSON.parse(text)
}

async function postBytes(path, body, headers) {
  const response = await fetch(`${endpoint}${path}`, {
    method: "POST",
    headers,
    body,
  })
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return JSON.parse(text)
}
