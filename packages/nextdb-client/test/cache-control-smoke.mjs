import assert from "node:assert/strict"

const baseUrl = process.env.NEXTDB_BASE_URL ?? "http://127.0.0.1:3188"
const testId = `cache-control-${Date.now()}-${Math.random().toString(36).slice(2)}`
const userId = `user-${testId}`
const clientId = `client-${testId}`

const health = await getJson("/v1/health")
assert.equal(health.ok, true, `server at ${baseUrl} is not healthy`)

const ws = await openSocket(`/v1/connect?userId=${encodeURIComponent(userId)}`)
await waitForFrame(ws.frames, (frame) => frame.type === "hello", "websocket hello")

const entry = await postJson("/v1/admin/cache/invalidate", {
  scope: "user",
  key: userId,
  minValidLsn: 7,
  reason: "cache control smoke user inbox reset",
})

assert.equal(entry.entry.scope, "user")
assert.equal(entry.entry.key, userId)
assert.equal(entry.entry.minValidLsn, 7)

const pushed = await waitForFrame(
  ws.frames,
  (frame) => frame.type === "cacheInvalidated" && frame.invalidation?.id === entry.entry.id,
  "cache invalidation push",
)
assert.deepEqual(pushed.invalidation, entry.entry)

const profile = await getJson(`/v1/cache/profile?clientId=${encodeURIComponent(clientId)}&afterInvalidationGeneration=${entry.entry.generation - 1}`)
const invalidation = profile.invalidations.find((candidate) => candidate.id === entry.entry.id)

assert.equal(invalidation.scope, "user")
assert.equal(invalidation.key, userId)
assert.equal(invalidation.minValidLsn, 7)

const nestedEntry = await postJson("/v1/admin/cache/invalidate", {
  scope: "nestedTable",
  table: "rooms",
  parentKey: "cache-control-room",
  nested: "messages",
  minValidLsn: 11,
  reason: "cache control smoke nested partition reset",
})

assert.equal(nestedEntry.entry.scope, "nestedTable")
assert.equal(nestedEntry.entry.key, "rooms.messages:cache-control-room")
assert.equal(nestedEntry.entry.table, "rooms")
assert.equal(nestedEntry.entry.parentKey, "cache-control-room")
assert.equal(nestedEntry.entry.nested, "messages")
assert.equal(nestedEntry.entry.minValidLsn, 11)

const nestedPushed = await waitForFrame(
  ws.frames,
  (frame) => frame.type === "cacheInvalidated" && frame.invalidation?.id === nestedEntry.entry.id,
  "nested cache invalidation push",
)
assert.deepEqual(nestedPushed.invalidation, nestedEntry.entry)

const nestedProfile = await getJson(`/v1/cache/profile?clientId=${encodeURIComponent(clientId)}&afterInvalidationGeneration=${nestedEntry.entry.generation - 1}`)
const nestedInvalidation = nestedProfile.invalidations.find((candidate) => candidate.id === nestedEntry.entry.id)

assert.deepEqual(nestedInvalidation, nestedEntry.entry)

const conflictProfileUpdate = await fetch(new URL("/v1/admin/cache/profile", baseUrl), {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    expectedVersion: nestedProfile.profile.version + 100,
    maxNestedPartitions: (nestedProfile.profile.maxNestedPartitions ?? 0) + 1,
  }),
})
assert.equal(conflictProfileUpdate.status, 409)

const updatedProfile = await postJson("/v1/admin/cache/profile", {
  expectedVersion: nestedProfile.profile.version,
  maxNestedPartitions: (nestedProfile.profile.maxNestedPartitions ?? 0) + 1,
  reason: "cache control smoke profile update",
})
assert.equal(updatedProfile.profile.version, nestedProfile.profile.version + 1)
assert.equal(updatedProfile.profile.maxNestedPartitions, (nestedProfile.profile.maxNestedPartitions ?? 0) + 1)
assert.equal(updatedProfile.invalidation.scope, "profile")

const profilePushed = await waitForFrame(
  ws.frames,
  (frame) => frame.type === "cacheInvalidated" && frame.invalidation?.id === updatedProfile.invalidation.id,
  "cache profile invalidation push",
)
assert.deepEqual(profilePushed.invalidation, updatedProfile.invalidation)

const refreshedProfile = await getJson(`/v1/cache/profile?clientId=${encodeURIComponent(clientId)}&afterInvalidationGeneration=${updatedProfile.invalidation.generation - 1}`)
assert.equal(refreshedProfile.profile.version, updatedProfile.profile.version)
assert.equal(refreshedProfile.profile.maxNestedPartitions, updatedProfile.profile.maxNestedPartitions)
assert.deepEqual(refreshedProfile.invalidations.find((candidate) => candidate.id === updatedProfile.invalidation.id), updatedProfile.invalidation)

const restoredProfile = await postJson("/v1/admin/cache/profile", {
  expectedVersion: updatedProfile.profile.version,
  maxNestedPartitions: nestedProfile.profile.maxNestedPartitions ?? 0,
  reason: "cache control smoke profile restore",
})
assert.equal(restoredProfile.profile.version, updatedProfile.profile.version + 1)
assert.equal(restoredProfile.profile.maxNestedPartitions, nestedProfile.profile.maxNestedPartitions ?? 0)
ws.close()

const missingKey = await fetch(new URL("/v1/admin/cache/invalidate", baseUrl), {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ scope: "user" }),
})
assert.equal(missingKey.status, 400)

const missingNestedField = await fetch(new URL("/v1/admin/cache/invalidate", baseUrl), {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ scope: "nestedTable", table: "rooms", parentKey: "cache-control-room" }),
})
assert.equal(missingNestedField.status, 400)

console.log("cache control smoke ok")

async function getJson(path) {
  const response = await fetch(new URL(path, baseUrl))
  if (!response.ok) {
    assert.fail(`${path} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function postJson(path, body) {
  const response = await fetch(new URL(path, baseUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    assert.fail(`${path} failed with ${response.status}: ${await response.text()}`)
  }
  return response.json()
}

async function openSocket(path) {
  const url = new URL(path, baseUrl)
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:"
  const socket = new WebSocket(url)
  const frames = []
  socket.addEventListener("message", (event) => frames.push(JSON.parse(event.data)))
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`websocket open timed out: ${url}`)), 5_000)
    socket.addEventListener("open", () => {
      clearTimeout(timeout)
      resolve()
    }, { once: true })
    socket.addEventListener("error", () => {
      clearTimeout(timeout)
      reject(new Error(`websocket open failed: ${url}`))
    }, { once: true })
  })
  return {
    frames,
    close: () => socket.close(),
  }
}

async function waitForFrame(frames, predicate, label) {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    const frame = frames.find(predicate)
    if (frame) {
      return frame
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  assert.fail(`timed out waiting for ${label}; frames=${JSON.stringify(frames)}`)
}
