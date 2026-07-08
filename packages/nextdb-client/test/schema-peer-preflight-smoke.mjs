import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient, NextDbHttpError } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-peer-preflight-"))
const node = {
  url: "http://127.0.0.1:3405",
  addr: "127.0.0.1:3405",
  dataDir: join(tempRoot, "data"),
}
let child

try {
  await mkdir(node.dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const db = new NextDbClient({ endpoint: node.url })
  const current = await db.getSchema()
  const candidate = structuredClone(current)
  candidate.version = current.version + 1
  candidate.tables.rooms.fields.preflightBlocked = { type: { kind: "string" }, optional: true }

  await assert.rejects(
    db.applySchema(candidate, { dryRun: false }),
    (error) => {
      assert(error instanceof NextDbHttpError)
      assert.equal(error.status, 409)
      assert.match(error.message, /schema peer preflight requires 1 acks, got 0/)
      assert.equal(error.payload?.peerPreflight?.requiredAcks, 1)
      assert.equal(error.payload?.peerPreflight?.acked, 0)
      assert.equal(error.payload?.peerPreflight?.replicas?.length, 1)
      assert.equal(error.payload?.peerPreflight?.replicas?.[0]?.ok, false)
      return true
    },
  )

  const afterRejected = await db.getSchema()
  assert.equal(afterRejected.version, current.version)
  assert.equal(afterRejected.tables.rooms.fields.preflightBlocked, undefined)
  const schemaAudit = await db.auditWal({ payloadType: "schemaApplied", limit: 10 })
  assert.equal(schemaAudit.records.length, 0)
  db.close()

  console.log("schema peer preflight smoke ok")
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
      NEXTDB_WAL_REMOTE_REPLICAS: "http://127.0.0.1:9",
      NEXTDB_WAL_REMOTE_ACKS: "all",
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
