import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const db = new NextDbClient({ endpoint, userId: "behavior-idempotency-smoke" })

const health = await db.health()
assert.equal(health.ok, true, `server at ${endpoint} is not healthy`)

await db.reloadBehaviors()
const behaviors = await db.listBehaviors()
assert(
  behaviors.some((behavior) => behavior.name === "echo-ts" && behavior.mutations.includes("echo.send")),
  "echo-ts behavior must be loaded",
)

const mutationId = `behavior-idempotency-${Date.now()}-${Math.random().toString(36).slice(2)}`
const roomId = `behavior-idempotency-room-${Date.now()}`
const request = {
  behavior: "echo-ts",
  mutation: "echo.send",
  userId: "behavior-idempotency-smoke",
  clientMutationId: mutationId,
  input: {
    roomId,
    body: "retry safe behavior",
    title: "Behavior Idempotency",
  },
}

const before = await db.health()
const first = await db.invokeBehavior(request)
const afterFirst = await db.health()
const second = await db.invokeBehavior(request)
const afterSecond = await db.health()

const firstLsns = committedLsns(first)
const secondLsns = committedLsns(second)
assert.deepEqual(secondLsns, firstLsns)
assert.equal(afterSecond.currentLsn, afterFirst.currentLsn)
assert(afterFirst.currentLsn >= before.currentLsn + 3)

const audit = await db.auditWal({ clientMutationId: `${mutationId}:000:upsertRecord`, limit: 10 })
assert.equal(audit.records.length, 1)
assert.equal(audit.records[0].payload.type, "recordUpserted")

const userId = `user-idempotency-${Date.now()}-${Math.random().toString(36).slice(2)}`
const userMutationId = `${userId}-profile`
const firstUser = await db.upsertUser(userId, {
  displayName: "First User",
  metadata: { attempt: 1 },
  clientMutationId: userMutationId,
})
const afterFirstUser = await db.health()
const secondUser = await db.upsertUser(userId, {
  displayName: "Second User",
  metadata: { attempt: 2 },
  clientMutationId: userMutationId,
})
const afterSecondUser = await db.health()
assert.deepEqual(secondUser, firstUser)
assert.equal(afterSecondUser.currentLsn, afterFirstUser.currentLsn)
const userAudit = await db.auditWal({ clientMutationId: userMutationId, limit: 10 })
assert.equal(userAudit.records.length, 1)
assert.equal(userAudit.records[0].payload.type, "userUpserted")

const eventMutationId = `${userId}-event`
const firstEvent = await db.publishUserEvent(userId, "notification.created", { text: "first event" }, {
  clientMutationId: eventMutationId,
})
const afterFirstEvent = await db.health()
const secondEvent = await db.publishUserEvent(userId, "notification.created", { text: "second event" }, {
  clientMutationId: eventMutationId,
})
const afterSecondEvent = await db.health()
assert.deepEqual(secondEvent, firstEvent)
assert.equal(afterSecondEvent.currentLsn, afterFirstEvent.currentLsn)
const eventAudit = await db.auditWal({ clientMutationId: eventMutationId, limit: 10 })
assert.equal(eventAudit.records.length, 1)
assert.equal(eventAudit.records[0].payload.type, "userEventPublished")

console.log("behavior idempotency smoke ok")

function committedLsns(response) {
  return response.committed
    .map((entry) => {
      if (entry.type === "recordUpserted") return entry.record.lsn
      if (entry.type === "objectCommitted") return undefined
      if (entry.type === "messageCreated") return entry.message.lsn
      if (entry.type === "recordTransactionCommitted") return entry.lsn
      if (entry.type === "recordDeleted" || entry.type === "objectDeleted") return entry.lsn
      return undefined
    })
    .filter((value) => value !== undefined)
}
