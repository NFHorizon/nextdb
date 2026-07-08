import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-schema-version-"))
const dataDir = join(tempRoot, "data")
const node = {
  url: "http://127.0.0.1:3400",
  addr: "127.0.0.1:3400",
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const schema = await getJson(`${node.url}/v1/schema`)
  const currentVersion = schema.version
  const staleVersion = currentVersion + 10_000

  const currentClient = new NextDbClient({
    endpoint: node.url,
    userId: "schema-version-smoke",
    schemaVersion: currentVersion,
  })
  const staleClient = new NextDbClient({
    endpoint: node.url,
    userId: "schema-version-smoke",
    schemaVersion: staleVersion,
  })

  const key = `schema-version-${Date.now()}`
  const ok = await currentClient.table("rooms").upsert(key, {
    id: key,
    title: "Schema Version Smoke",
  })
  assert.equal(ok.key, key)

  await assert.rejects(
    () => staleClient.table("rooms").upsert(`${key}-stale`, {
      id: `${key}-stale`,
      title: "Stale Schema Version",
    }),
    (error) => {
      assert.equal(error.status, 409)
      assert.equal(error.payload.schemaVersionMismatch, true)
      assert.equal(error.payload.clientSchemaVersion, staleVersion)
      assert.equal(error.payload.serverSchemaVersion, currentVersion)
      return true
    },
  )

  const staleConnect = await fetch(`${node.url}/v1/connect?schemaVersion=${staleVersion}`)
  assert.equal(staleConnect.status, 409)
  const staleConnectBody = await staleConnect.json()
  assert.equal(staleConnectBody.schemaVersionMismatch, true)

  const currentConnect = await fetch(`${node.url}/v1/connect?schemaVersion=${currentVersion}`)
  assert.notEqual(currentConnect.status, 409)

  currentClient.close()
  staleClient.close()
  console.log("schema version smoke ok")
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

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  assert.equal(response.status, 200, text)
  return JSON.parse(text)
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
