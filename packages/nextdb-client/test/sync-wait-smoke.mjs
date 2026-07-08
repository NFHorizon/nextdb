import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const db = new NextDbClient({ endpoint })

const health = await getJson("/v1/health")
assert.equal(health.ok, true, `server at ${endpoint} is not healthy`)

const current = await db.waitForLsn(health.currentLsn, { timeoutMs: 100 })
assert.equal(current.caughtUp, true)
assert(current.currentLsn >= health.currentLsn)

const future = await db.waitForLsn(current.currentLsn + 1, { timeoutMs: 20 })
assert.equal(future.caughtUp, false)
assert(future.currentLsn < current.currentLsn + 1)

const key = `sync-wait-${Date.now()}-${Math.random().toString(36).slice(2)}`
const record = await db.table("rooms").upsert(key, {
  id: key,
  title: "Sync wait smoke",
}, {
  durability: "strict",
  clientMutationId: `${key}-upsert`,
})
assert(record.lsn >= current.currentLsn + 1)

const caughtUp = await db.waitForLsn(record.lsn, { timeoutMs: 1_000 })
assert.equal(caughtUp.caughtUp, true)
assert(caughtUp.currentLsn >= record.lsn)

const quorumCaughtUp = await db.waitForLsn(record.lsn, {
  timeoutMs: 1_000,
  consistency: "quorum",
  shardKey: `rooms:${key}`,
})
assert.equal(quorumCaughtUp.caughtUp, true)
assert.equal(quorumCaughtUp.consistency, "quorum")
assert.equal(quorumCaughtUp.remoteCaughtUp, true)
assert.equal(quorumCaughtUp.remoteRequiredAcks, 0)
assert.equal(quorumCaughtUp.remoteAcked, 0)

const rawUpdate = await postJson(`/v1/records/rooms/${encodeURIComponent(key)}`, {
  value: { id: key, title: "Sync wait smoke fresh" },
  durability: "strict",
  clientMutationId: `${key}-fresh-upsert`,
})
assert(rawUpdate.record.lsn > record.lsn)

const cached = await db.table("rooms").get(key)
assert.equal(cached.value.title, "Sync wait smoke")

const fresh = await db.table("rooms").get(key, { minLsn: rawUpdate.record.lsn, timeoutMs: 1_000 })
assert.equal(fresh.value.title, "Sync wait smoke fresh")

const freshQuorum = await db.table("rooms").get(key, {
  minLsn: rawUpdate.record.lsn,
  timeoutMs: 1_000,
  consistency: "quorum",
})
assert.equal(freshQuorum.value.title, "Sync wait smoke fresh")

const freshQuorumList = await db.table("rooms").list({
  limit: 50,
  minLsn: rawUpdate.record.lsn,
  timeoutMs: 1_000,
  consistency: "quorum",
})
assert(freshQuorumList.records.some((row) => row.key === key && row.value.title === "Sync wait smoke fresh"))

const freshList = await db.table("rooms").list({ limit: 50, minLsn: rawUpdate.record.lsn, timeoutMs: 1_000 })
assert(freshList.records.some((row) => row.key === key && row.value.title === "Sync wait smoke fresh"))

console.log("sync wait smoke ok")

async function getJson(path) {
  const response = await fetch(new URL(path, endpoint))
  const text = await response.text()
  assert(response.ok, `${path} failed with ${response.status}: ${text}`)
  return JSON.parse(text)
}

async function postJson(path, body) {
  const response = await fetch(new URL(path, endpoint), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  assert(response.ok, `${path} failed with ${response.status}: ${text}`)
  return JSON.parse(text)
}
