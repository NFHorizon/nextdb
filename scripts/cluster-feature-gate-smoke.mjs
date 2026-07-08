import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { createServer } from "node:net"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const serverBin = resolve(root, process.env.NEXTDB_SERVER_BIN ?? "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-cluster-gate-"))
const dataDir = join(tempRoot, "data")
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const verbose = process.env.NEXTDB_CLUSTER_GATE_LOGS === "1"
let child

const disabledRoutes = [
  "/v1/cluster/topology",
  "/v1/cluster/route?key=rooms/example",
  "/v1/admin/cluster/topology/overrides",
  "/v1/admin/cluster/topology/log",
  "/v1/admin/cluster/topology/proposals",
  "/v1/admin/cluster/topology/lease/cleanup",
  "/v1/admin/cluster/shards/0/freeze",
  "/v1/admin/cluster/handoff/plan",
  "/v1/admin/cluster/failover/plan",
  "/v1/admin/cluster/handoff/workflows",
  "/v1/admin/objects/replicate",
  "/v1/admin/objects/repair",
  "/v1/admin/wal/replicate",
  "/v1/admin/wal/replicate/repair",
]

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode()
  await waitForHealth()

  const health = await getJson("/v1/health")
  assert.equal(health.ok, true)
  const wal = await getJson("/v1/admin/wal/integrity")
  assert.equal(wal.ok, true)

  for (const route of disabledRoutes) {
    const response = await fetch(`${endpoint}${route}`, { method: route.includes("/replicate") || route.includes("/repair") || route.includes("/plan") || route.includes("/freeze") || route.includes("/cleanup") ? "POST" : "GET" })
    assert.equal(response.status, 404, `${route} should be absent when cluster feature is disabled`)
  }

  console.log("cluster feature gate smoke ok")
  console.log(JSON.stringify({
    ok: true,
    endpoint,
    disabledRoutes: disabledRoutes.length,
  }, null, 2))
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode() {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (verbose) {
      process.stdout.write(`[cluster-gate] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (verbose) {
      process.stderr.write(`[cluster-gate] ${chunk}`)
    }
  })
  return child
}

async function stopNode(child) {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill("SIGTERM")
  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL")
      resolve()
    }, 5_000)
    child.once("exit", () => {
      clearTimeout(timeout)
      resolve()
    })
  })
}

async function waitForHealth() {
  await waitFor(async () => {
    const health = await getJson("/v1/health").catch(() => undefined)
    return health?.ok === true
  }, `health at ${endpoint}`)
}

async function waitFor(check, label, timeoutMs = 15_000) {
  const started = Date.now()
  let lastError
  while (Date.now() - started < timeoutMs) {
    try {
      if (await check()) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}

async function getJson(path) {
  const response = await fetch(`${endpoint}${path}`)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${path} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}

async function freePort() {
  const server = createServer()
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  const port = address.port
  server.close()
  await once(server, "close")
  return port
}
