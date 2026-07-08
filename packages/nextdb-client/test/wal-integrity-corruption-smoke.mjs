import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"
import { corruptWalPayloadString, walFileContainsString } from "./wal-frame-helpers.mjs"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-wal-integrity-corruption-"))
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

  const db = new NextDbClient({ endpoint, userId: "wal-corruption-user" })
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const roomId = `wal-corruption-room-${suffix}`
  const originalBody = `wal integrity original ${suffix}`
  const corruptedBody = `wal integrity modified ${suffix}`
  const message = await db.room(roomId).messages.send(originalBody, {
    durability: "strict",
    clientMutationId: `${roomId}-message`,
  })
  assert(message.lsn > 0)

  const clean = await db.walIntegrity()
  assert.equal(clean.ok, true)
  assert.equal(clean.checksumMismatchCount, 0)

  const activeWalPath = join(dataDir, "wal", "shard-0000.jsonl")
  assert.equal(await walFileContainsString(activeWalPath, originalBody), true)
  assert.equal(await walFileContainsString(activeWalPath, corruptedBody), false)
  await corruptWalPayloadString(activeWalPath, originalBody, corruptedBody)

  const corrupted = await db.walIntegrity()
  assert.equal(corrupted.ok, false)
  assert.equal(corrupted.checksumMismatchCount, 1)
  assert(corrupted.issues.some((issue) =>
    issue.severity === "error" &&
    issue.code === "checksumMismatch" &&
    issue.lsn === message.lsn &&
    issue.path === activeWalPath
  ))

  db.close()
  console.log("wal integrity corruption smoke ok")
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
