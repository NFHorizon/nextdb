import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const db = new NextDbClient({ endpoint })

try {
  const before = await db.health()
  assert.equal(before.ok, true, `server at ${endpoint} is not healthy`)
  assert.equal(typeof before.runtimeWrites.inFlight, "number")
  assert.equal(before.runtimeWrites.inFlight, 0)
  const readyBefore = await db.readiness()
  assert.equal(readyBefore.ok, true)
  assert.equal(readyBefore.readReady, true)
  assert.equal(readyBefore.writeReady, true)
  assert.equal(readyBefore.realtimeReady, true)
  assert.equal(readyBefore.acceptingWrites, true)
  assert.equal(readyBefore.draining, false)
  assert.equal(readyBefore.currentLsn, before.currentLsn)
  assert.equal(readyBefore.checks.some((check) => check.name === "wal" && check.ok), true)
  assert.equal(readyBefore.checks.some((check) => check.name === "connectionLayer" && check.ok), true)

  const prepared = await db.prepareRestart({
    reason: "runtime prepare smoke",
    snapshot: true,
    compactWal: false,
    waitForWritesMs: 1_000,
  })

  assert.equal(prepared.drain.draining, true)
  assert.equal(prepared.writesQuiesced, true)
  assert.equal(prepared.writeWaitTimedOut, false)
  assert.equal(prepared.readyForRestart, true)
  assert.equal(prepared.runtimeWrites.inFlight, 0)
  assert(prepared.waitedForWritesMs >= 0)
  assert(prepared.snapshot, "prepareRestart should snapshot after writes quiesce")
  assert(prepared.snapshot.lsn <= prepared.currentLsn)

  const duringDrain = await db.health()
  assert.equal(duringDrain.draining, true)
  assert.equal(duringDrain.acceptingWrites, false)
  assert.equal(duringDrain.runtimeWrites.inFlight, 0)
  const readyDuringDrain = await db.readiness()
  assert.equal(readyDuringDrain.ok, false)
  assert.equal(readyDuringDrain.readReady, true)
  assert.equal(readyDuringDrain.writeReady, false)
  assert.equal(readyDuringDrain.realtimeReady, false)
  assert.equal(readyDuringDrain.acceptingWrites, false)
  assert.equal(readyDuringDrain.draining, true)
  assert.equal(
    readyDuringDrain.checks.some((check) =>
      check.name === "runtimeDrain" &&
      check.ok === false &&
      check.detail.includes("runtime prepare smoke")
    ),
    true,
  )
} finally {
  await db.setRuntimeDraining(false, "runtime prepare smoke complete")
}

const after = await db.health()
assert.equal(after.draining, false)
assert.equal(after.acceptingWrites, true)
const readyAfter = await db.readiness()
assert.equal(readyAfter.ok, true)
assert.equal(readyAfter.writeReady, true)
assert.equal(readyAfter.realtimeReady, true)

console.log("runtime prepare smoke ok")
