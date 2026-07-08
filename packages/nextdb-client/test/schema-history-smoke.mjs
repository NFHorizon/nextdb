import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient, NextDbHttpError } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-history-"))
const dataDir = join(tempRoot, "data")
const node = {
  url: "http://127.0.0.1:3404",
  addr: "127.0.0.1:3404",
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

	  const db = new NextDbClient({ endpoint: node.url })
	  const current = await db.getSchema()
	  const initialHistory = await db.schemaHistory()
	  assert.deepEqual(initialHistory.entries.map((entry) => entry.version), [current.version])
	  assert.equal(initialHistory.entries[0].current, true)

	  const objectId = `schema-history-object-${Date.now()}`
	  const object = await db.putObject("schema history object", {
	    contentType: "text/plain",
	    objectId,
	    clientMutationId: `${objectId}-put`,
	  })
	  const validCoverKey = `schema-history-valid-cover-${Date.now()}`
	  await db.table("rooms").upsert(validCoverKey, {
	    id: validCoverKey,
	    title: "Valid existing ObjectRef",
	    cover: object,
	  }, {
	    clientMutationId: `${validCoverKey}-upsert`,
	  })
	  const invalidCoverKey = `schema-history-invalid-cover-${Date.now()}`
	  await db.table("rooms").upsert(invalidCoverKey, {
	    id: invalidCoverKey,
	    title: "Invalid existing ObjectRef",
	    cover: {
	      ...object,
	      id: `${objectId}-missing`,
	      path: `objects/${objectId}-missing`,
	    },
	  }, {
	    clientMutationId: `${invalidCoverKey}-upsert`,
	  })

	  const candidate = structuredClone(current)
	  candidate.version = current.version + 1
	  candidate.tables.rooms.fields.topic = { type: { kind: "string" }, optional: true }
	  candidate.tables.rooms.fields.cover = { type: { kind: "objectRef", object: "Object" }, optional: true }
	  await assert.rejects(
	    db.applySchema(candidate, { dryRun: true, expectedVersion: current.version }),
	    (error) => {
	      assert(error instanceof NextDbHttpError)
	      assert.equal(error.status, 404)
	      assert.match(error.message, /object ref not found/)
	      return true
	    },
	  )
	  await db.table("rooms").delete(invalidCoverKey, {
	    clientMutationId: `${invalidCoverKey}-delete`,
	  })
	  const applied = await db.applySchema(candidate, { expectedVersion: current.version })
	  assert.equal(applied.applied, true)
  assert.equal(applied.persisted, true)
  assert.equal(applied.version, candidate.version)
  assert(Number.isInteger(applied.schemaAuditLsn))

  const staleCandidate = structuredClone(candidate)
  staleCandidate.version = candidate.version + 1
  staleCandidate.tables.rooms.fields.staleExpectedVersion = { type: { kind: "string" }, optional: true }
  await assert.rejects(
    db.applySchema(staleCandidate, { expectedVersion: current.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.equal(error.payload?.schemaVersionConflict, true)
      assert.equal(error.payload?.expectedVersion, current.version)
      assert.equal(error.payload?.activeVersion, candidate.version)
      return true
    },
  )

  const schemaAudit = await db.auditWal({ payloadType: "schemaApplied", limit: 10 })
  assert.equal(schemaAudit.records.length, 1)
  assert.equal(schemaAudit.records[0].lsn, applied.schemaAuditLsn)
  assert.equal(schemaAudit.records[0].payload.schema.version, candidate.version)
  assert.equal(schemaAudit.records[0].payload.migration.fromVersion, current.version)
  assert.equal(schemaAudit.records[0].payload.migration.toVersion, candidate.version)

  const schemaPathAudit = await db.auditWal({
    payloadType: "schemaApplied",
    path: `schema/versions/${candidate.version}`,
    limit: 10,
  })
  assert.equal(schemaPathAudit.records.length, 1)

  const history = await db.schemaHistory()
  assert.deepEqual(history.entries.map((entry) => entry.version), [current.version, candidate.version])
  assert.equal(history.entries.find((entry) => entry.version === current.version)?.current, false)
  assert.equal(history.entries.find((entry) => entry.version === candidate.version)?.current, true)

	  const v1 = await db.getSchemaVersion(current.version)
	  const v2 = await db.getSchemaVersion(candidate.version)
	  assert.equal(v1.tables.rooms.fields.topic, undefined)
	  assert.equal(v2.tables.rooms.fields.topic.optional, true)
	  assert.equal(v2.tables.rooms.fields.cover.optional, true)
	  assert.equal(v2.tables.rooms.fields.staleExpectedVersion, undefined)
	  const coverRefs = await db.getObjectReferences(objectId)
	  assert.equal(coverRefs.sources.includes(`tables/rooms/${validCoverKey}`), true)

  const missing = await fetch(`${node.url}/v1/schema/history/${candidate.version + 100}`)
  assert.equal(missing.status, 404)
  db.close()

  await stopNode(child)
  await rm(join(dataDir, "schema"), { recursive: true, force: true })
  child = startNode(node)
  await waitForHealth(node.url)
  const restarted = new NextDbClient({ endpoint: node.url })
  const restartedHealth = await restarted.health()
  assert.equal(restartedHealth.startupRecovery.schemaWalRecovery.recovered, true)
  assert.equal(restartedHealth.startupRecovery.schemaWalRecovery.latestLsn, applied.schemaAuditLsn)
  assert.equal(restartedHealth.startupRecovery.schemaWalRecovery.latestVersion, candidate.version)
  assert.deepEqual(restartedHealth.startupRecovery.schemaWalRecovery.historyVersions, [candidate.version])
  const restartedCurrentSchema = await restarted.getSchema()
  assert.equal(restartedCurrentSchema.version, candidate.version)
  const restartedHistory = await restarted.schemaHistory()
  assert.deepEqual(restartedHistory.entries.map((entry) => entry.version), [current.version, candidate.version])
  assert.equal((await restarted.getSchemaVersion(current.version)).version, current.version)
  assert.equal((await restarted.getSchemaVersion(candidate.version)).version, candidate.version)
  restarted.close()

  console.log("schema history smoke ok")
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

async function waitFor(predicate, label) {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}`)
}
