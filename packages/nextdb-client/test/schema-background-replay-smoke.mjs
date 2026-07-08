import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient, NextDbHttpError } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-background-replay-"))
const node = {
  url: "http://127.0.0.1:3419",
  addr: "127.0.0.1:3419",
  dataDir: join(tempRoot, "data"),
}
let child

try {
  await mkdir(node.dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const db = new NextDbClient({ endpoint: node.url })
  const current = await db.getSchema()
  const initialStatus = await db.schemaReplayApplyStatus()
  assert.equal(initialStatus.phase, "idle")
  assert.equal(initialStatus.resumeEligible, false)

  const withBackgroundField = structuredClone(current)
  withBackgroundField.version = current.version + 1
  withBackgroundField.tables.rooms.fields.backgroundReplayField = {
    type: { kind: "string" },
    optional: true,
  }

  await assert.rejects(
    db.applySchema(withBackgroundField, {
      expectedVersion: current.version,
      backgroundReplay: true,
    }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 400)
      assert.match(error.message, /backgroundReplay requires/)
      return true
    },
  )

  const added = await db.applySchema(withBackgroundField, {
    expectedVersion: current.version,
  })
  assert.equal(added.applied, true)
  assert.equal(added.replayRebuild, false)

  const key = `schema-background-replay-${Date.now()}`
  await db.table("rooms").upsert(key, {
    id: key,
    title: "Background replay field removal",
    backgroundReplayField: "field to remove",
  }, {
    clientMutationId: `${key}-upsert`,
  })

  const withoutBackgroundField = structuredClone(withBackgroundField)
  withoutBackgroundField.version = withBackgroundField.version + 1
  delete withoutBackgroundField.tables.rooms.fields.backgroundReplayField

  db.close()
  await stopNode(child)
  await mkdir(join(node.dataDir, "schema"), { recursive: true })
  await writeFile(join(node.dataDir, "schema", "schema-replay-status.json"), JSON.stringify({
    phase: "running",
    runId: "interrupted-schema-replay",
    targetVersion: withoutBackgroundField.version,
    expectedVersion: withBackgroundField.version,
    schema: withoutBackgroundField,
    allowBreakingReplay: true,
    replayRebuild: true,
    projectionRebuild: true,
    startedAtMs: Date.now(),
  }))
  child = startNode(node)
  await waitForHealth(node.url)
  const retryClient = new NextDbClient({ endpoint: node.url })
  const interrupted = await retryClient.schemaReplayApplyStatus()
  assert.equal(interrupted.phase, "failed")
  assert.equal(interrupted.runId, "interrupted-schema-replay")
  assert.equal(interrupted.targetVersion, withoutBackgroundField.version)
  assert.equal(interrupted.expectedVersion, withBackgroundField.version)
  assert.equal(interrupted.schema.version, withoutBackgroundField.version)
  assert.match(interrupted.error, /previous shutdown/)
  assert.equal(interrupted.resumeEligible, true)
  assert.match(interrupted.resumeReason, /resumeSchemaReplayApply/)

  const started = await retryClient.resumeSchemaReplayApply()
  assert.equal(started.applied, false)
  assert.equal(started.persisted, false)
  assert.equal(started.replayRebuild, true)
  assert.equal(started.projectionRebuilt, false)
  assert.equal(started.backgroundReplayPhase, "running")
  assert.equal(typeof started.backgroundReplayRunId, "string")

  const completed = await waitForSchemaReplay(retryClient, started.backgroundReplayRunId)
  assert.equal(completed.phase, "succeeded")
  assert.equal(completed.resumedFromRunId, "interrupted-schema-replay")
  assert.equal(completed.targetVersion, withoutBackgroundField.version)
  assert.equal(completed.expectedVersion, withBackgroundField.version)
  assert.equal(completed.schema.version, withoutBackgroundField.version)
  assert.equal(completed.allowBreakingReplay, true)
  assert.equal(completed.replayRebuild, true)
  assert.equal(completed.projectionRebuild, true)
  assert.equal(completed.resumeEligible, false)
  assert.equal(completed.resumeReason, undefined)
  assert(Number.isInteger(completed.schemaAuditLsn))
  assert(completed.projectionStatus.records >= 1)

  await assert.rejects(
    retryClient.retrySchemaReplayApply(),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.match(error.message, /requires a failed replay status/)
      return true
    },
  )

  await assert.rejects(
    retryClient.cancelSchemaReplayApply(),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.match(error.message, /requires a running replay status/)
      return true
    },
  )

  const latest = await retryClient.getSchema()
  assert.equal(latest.version, withoutBackgroundField.version)
  assert.equal(latest.tables.rooms.fields.backgroundReplayField, undefined)

  const record = await retryClient.table("rooms").get(key)
  assert.equal(record.value.id, key)
  assert.equal(record.value.title, "Background replay field removal")

  const schemaAudit = await retryClient.auditWal({ payloadType: "schemaApplied", limit: 20 })
  const removalAudit = schemaAudit.records.find((record) => record.lsn === completed.schemaAuditLsn)
  assert(removalAudit)
  assert.equal(removalAudit.payload.schema.version, withoutBackgroundField.version)
  assert.equal(removalAudit.payload.migration.requiresReplayRebuild, true)
  assert.deepEqual(removalAudit.payload.migration.projectionRebuildReasons, [
    "tables.rooms.fields.backgroundReplayField removed",
  ])
  retryClient.close()

  await stopNode(child)
  await writeFile(join(node.dataDir, "schema", "schema-replay-status.json"), JSON.stringify({
    phase: "committing",
    runId: "committing-schema-replay",
    targetVersion: withoutBackgroundField.version,
    expectedVersion: withBackgroundField.version,
    schema: withoutBackgroundField,
    allowBreakingReplay: true,
    replayRebuild: true,
    projectionRebuild: true,
    startedAtMs: Date.now(),
  }))
  child = startNode(node)
  await waitForHealth(node.url)
  const restarted = new NextDbClient({ endpoint: node.url })
  const restartedStatus = await restarted.schemaReplayApplyStatus()
  assert.equal(restartedStatus.phase, "succeeded")
  assert.equal(restartedStatus.runId, "committing-schema-replay")
  assert.equal(restartedStatus.schemaAuditLsn, completed.schemaAuditLsn)
  assert.equal(restartedStatus.targetVersion, withoutBackgroundField.version)
  assert.equal(restartedStatus.schema.version, withoutBackgroundField.version)
  assert.equal(restartedStatus.error, undefined)
  assert.equal(restartedStatus.resumeEligible, false)
  assert.equal(restartedStatus.resumeReason, undefined)
  await assert.rejects(
    restarted.resumeSchemaReplayApply(),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.match(error.message, /requires a failed replay status/)
      return true
    },
  )
  restarted.close()

  console.log("schema background replay smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node) {
  return spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: node.addr,
      NEXTDB_DATA_DIR: node.dataDir,
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
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

async function waitForSchemaReplay(db, runId) {
  let last
  await waitFor(async () => {
    last = await db.schemaReplayApplyStatus()
    assert.equal(last.runId, runId)
    return last.phase !== "running" && last.phase !== "committing"
  }, `schema replay apply ${runId}`)
  return last
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}`)
}
