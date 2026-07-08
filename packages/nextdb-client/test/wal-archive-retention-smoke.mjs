import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"
import { walFileContainsString } from "./wal-frame-helpers.mjs"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-wal-archive-retention-"))
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const dataDir = join(tempRoot, "data")
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = spawn(serverBin, {
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "ignore", "inherit"],
  })
  await waitForHealth(endpoint)

  const db = new NextDbClient({ endpoint, userId: "wal-retention-user" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const roomId = `wal-retention-room-${suffix}`

  const room = await db.table("rooms").upsert(roomId, {
    id: roomId,
    title: "WAL Retention Room",
  }, {
    clientMutationId: `${roomId}-upsert`,
  })
  const message = await db.room(roomId).messages.send("before compact", {
    clientMutationId: `${roomId}-message`,
  })
  assert(message.lsn > room.lsn)

  const snapshot = await db.createSnapshot()
  assert.equal(snapshot.lsn, message.lsn)
  const compact = await db.compactWal()
  assert.equal(compact.lastSnapshotLsn, snapshot.lsn)
  assert.equal(compact.archived, 2)
  assert.equal(compact.retained, 0)
  assert(compact.reports[0].archivePath)

  const activeWalPath = join(dataDir, "wal", "shard-0000.jsonl")
  const activeWal = await readFile(activeWalPath, "utf8")
  assert.equal(activeWal.trim(), "")
  assert.equal(await walFileContainsString(compact.reports[0].archivePath, roomId), true)

  const archivedSync = await db.syncPull({ rooms: [roomId], afterLsn: room.lsn, limit: 5 })
  assert.deepEqual(
    archivedSync.events
      .filter((event) => event.type === "messageCreated")
      .map((event) => event.message.id),
    [message.id],
  )
  assert.equal(archivedSync.nextAfterLsn, message.lsn)

  const coveredArchiveSync = await db.syncPull({ rooms: [roomId], afterLsn: message.lsn, limit: 5 })
  assert.deepEqual(coveredArchiveSync.events, [])
  assert.equal(coveredArchiveSync.nextAfterLsn, message.lsn)

  const dryRetain = await db.retainWalArchives({ dryRun: true, beforeLsn: message.lsn })
  assert.equal(dryRetain.dryRun, true)
  assert.equal(dryRetain.candidates, 0)
  assert.equal(dryRetain.deleted, 0)
  assert.equal(dryRetain.retained, 1)
  assert.equal(dryRetain.reports[0].action, "retain")

  const dryDelete = await db.retainWalArchives({ dryRun: true, beforeLsn: message.lsn + 1 })
  assert.equal(dryDelete.dryRun, true)
  assert.equal(dryDelete.candidates, 1)
  assert.equal(dryDelete.deleted, 0)
  assert.equal(dryDelete.reports[0].action, "delete")

  const applyDelete = await db.retainWalArchives({ dryRun: false, beforeLsn: message.lsn + 1 })
  assert.equal(applyDelete.dryRun, false)
  assert.equal(applyDelete.candidates, 1)
  assert.equal(applyDelete.deleted, 1)
  assert.equal(applyDelete.reports[0].action, "deleted")
  await assert.rejects(readFile(compact.reports[0].archivePath, "utf8"))
  await readFile(activeWalPath, "utf8")

  const latest = await db.room(roomId).messages.latest({ limit: 5 })
  assert.deepEqual(latest.messages.map((entry) => entry.id), [message.id])
  const integrity = await db.walIntegrity()
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, 0)
  const postRetentionManifest = await db.exportManifest()
  assert.equal(postRetentionManifest.wal.records, 0)
  assert.deepEqual(postRetentionManifest.rooms, {})

  db.close()
  console.log("wal archive retention smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function stopNode(child, signal = "SIGTERM") {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill(signal)
  await Promise.race([
    once(child, "exit"),
    new Promise((resolve) => setTimeout(resolve, 5_000)).then(() => {
      child.kill("SIGKILL")
      return once(child, "exit").catch(() => {})
    }),
  ])
}

async function waitForHealth(url) {
  await waitFor(async () => {
    const response = await fetch(`${url}/v1/health`).catch(() => undefined)
    return response?.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 15_000) {
  const started = Date.now()
  while (Date.now() - started < timeoutMs) {
    if (await check()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}`)
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      const port = typeof address === "object" && address ? address.port : undefined
      server.close((error) => {
        if (error) {
          reject(error)
          return
        }
        if (port === undefined) {
          reject(new Error("failed to allocate local port"))
          return
        }
        resolve(port)
      })
    })
  })
}
