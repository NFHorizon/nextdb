import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient, NextDbHttpError } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-proposal-"))
const node = {
  url: "http://127.0.0.1:3407",
  addr: "127.0.0.1:3407",
  dataDir: join(tempRoot, "data"),
}
let child

try {
  await mkdir(node.dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const db = new NextDbClient({ endpoint: node.url })
  const current = await db.getSchema()

  const abortCandidate = structuredClone(current)
  abortCandidate.version = current.version + 1
  abortCandidate.tables.rooms.fields.abortedProposal = { type: { kind: "string" }, optional: true }
  const abortPrepared = await db.startSchemaProposal(abortCandidate, {
    expectedVersion: current.version,
    reason: "schema proposal abort smoke",
  })
  assert.equal(abortPrepared.proposal.phase, "prepared")
  assert.equal(abortPrepared.proposal.expectedVersion, current.version)
  assert.equal(abortPrepared.proposal.migration.toVersion, abortCandidate.version)
  assert.equal(abortPrepared.proposal.requiredAcks, 1)
  assert.equal(abortPrepared.proposal.prepareAcks.length, 1)
  assert.equal(abortPrepared.proposal.prepareAcks[0].applied, true)
  const aborted = await db.abortSchemaProposal(abortPrepared.proposal.id)
  assert.equal(aborted.proposal.phase, "aborted")
  await assert.rejects(
    db.commitSchemaProposal(abortPrepared.proposal.id),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.match(error.message, /aborted schema proposal/)
      return true
    },
  )
  const afterAbort = await db.getSchema()
  assert.equal(afterAbort.version, current.version)
  assert.equal(afterAbort.tables.rooms.fields.abortedProposal, undefined)

  const commitCandidate = structuredClone(current)
  commitCandidate.version = current.version + 1
  commitCandidate.tables.rooms.fields.proposedTopic = { type: { kind: "string" }, optional: true }
  const prepared = await db.startSchemaProposal(commitCandidate, {
    expectedVersion: current.version,
    reason: "schema proposal commit smoke",
  })
  assert.equal(prepared.proposal.phase, "prepared")
  assert.equal(prepared.proposal.report.ok, true)
  assert.equal(prepared.proposal.projectionRebuilt, false)
  assert.equal(prepared.proposal.projectionStatus.records, 0)
  assert.equal(prepared.proposal.requiredAcks, 1)
  const committed = await db.commitSchemaProposal(prepared.proposal.id)
  assert.equal(committed.proposal.phase, "committed")
  assert.equal(committed.proposal.schema.version, commitCandidate.version)
  assert.equal(committed.proposal.projectionRebuilt, false)
  assert(Number.isInteger(committed.proposal.schemaAuditLsn))
  assert.equal(committed.proposal.commitAcks.length, 1)
  assert.equal(committed.proposal.commitAcks[0].applied, true)
  const afterCommit = await db.getSchema()
  assert.equal(afterCommit.version, commitCandidate.version)
  assert.equal(afterCommit.tables.rooms.fields.proposedTopic.optional, true)

  const proposals = await db.schemaProposals()
  const phases = new Map(proposals.proposals.map((proposal) => [proposal.id, proposal.phase]))
  assert.equal(phases.get(abortPrepared.proposal.id), "aborted")
  assert.equal(phases.get(prepared.proposal.id), "committed")

  const staleCandidate = structuredClone(afterCommit)
  staleCandidate.version = afterCommit.version + 1
  staleCandidate.tables.rooms.fields.staleProposal = { type: { kind: "string" }, optional: true }
  await assert.rejects(
    db.startSchemaProposal(staleCandidate, { expectedVersion: current.version }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.equal(error.payload?.schemaVersionConflict, true)
      assert.equal(error.payload?.expectedVersion, current.version)
      assert.equal(error.payload?.activeVersion, afterCommit.version)
      return true
    },
  )

  const schemaAudit = await db.auditWal({ payloadType: "schemaApplied", limit: 10 })
  assert.equal(schemaAudit.records.length, 1)
  assert.equal(schemaAudit.records[0].lsn, committed.proposal.schemaAuditLsn)
  db.close()

  console.log("schema proposal smoke ok")
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
