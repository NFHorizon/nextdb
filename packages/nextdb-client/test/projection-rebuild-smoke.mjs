import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-projection-rebuild-"))
const node = {
  url: "http://127.0.0.1:3418",
  addr: "127.0.0.1:3418",
  dataDir: join(tempRoot, "data"),
}
let child

try {
  await mkdir(node.dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const db = new NextDbClient({ endpoint: node.url })
  const initialStatus = await db.projectionRebuildStatus()
  assert.equal(initialStatus.phase, "idle")

  const key = `projection-rebuild-${Date.now()}`
  await db.table("rooms").upsert(key, {
    id: key,
    title: "Projection Rebuild Smoke",
  }, {
    clientMutationId: `${key}-upsert`,
  })

  const background = await db.rebuildProjections({ background: true })
  assert.equal(background.phase, "running")
  assert.equal(background.background, true)
  assert.equal(typeof background.runId, "string")
  assert.equal(background.messages, 0)
  assert.equal(background.records, 0)

  const completed = await waitForRebuild(node.url, background.runId)
  assert.equal(completed.phase, "succeeded")
  assert.equal(completed.background, true)
  assert.equal(completed.error, undefined)
  assert(completed.records >= 1)

  const sync = await db.rebuildProjections()
  assert.equal(sync.phase, "succeeded")
  assert.equal(sync.background, false)
  assert.equal(sync.error, undefined)
  assert(sync.records >= 1)

  const finalStatus = await db.projectionRebuildStatus()
  assert.equal(finalStatus.phase, "succeeded")
  assert.equal(finalStatus.background, false)
  assert.equal(finalStatus.runId, sync.runId)
  assert.equal(finalStatus.records, sync.records)
  db.close()

  console.log("projection rebuild smoke ok")
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

async function waitForRebuild(baseUrl, runId) {
  let last
  await waitFor(async () => {
    const response = await fetch(`${baseUrl}/v1/admin/projections/rebuild/status`)
    assert.equal(response.ok, true)
    last = await response.json()
    assert.equal(last.runId, runId)
    return last.phase !== "running"
  }, `projection rebuild ${runId}`)
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
