import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:net"
import { spawn } from "node:child_process"
import { cp, mkdir, mkdtemp, readFile, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8"))
const version = packageJson.version ?? "0.1.0"
const targetTriple = `${process.platform}-${process.arch}`
const releaseRoot = resolve(process.env.NEXTDB_RELEASE_DIR ?? join(root, "dist", "release"))
const bundleDir = resolve(process.env.NEXTDB_RELEASE_BUNDLE_DIR ?? join(releaseRoot, `nextdb-${version}-${targetTriple}`))
const serverBin = join(bundleDir, "bin", executableName("nextdb-server"))
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-release-smoke-"))
const dataDir = join(tempRoot, "data")
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
let child

try {
  const manifest = JSON.parse(await readFile(join(bundleDir, "manifest.json"), "utf8"))
  assert.equal(manifest.format, "nextdb.release-bundle.v1")
  assert.equal(manifest.server.path, `bin/${executableName("nextdb-server")}`)
  await assertFile(serverBin)
  await assertFile(join(bundleDir, "admin", "index.html"))
  await assertFile(join(bundleDir, "README_RELEASE.md"))
  assert(manifest.files.some((file) => file.path === "admin/index.html"))
  assert(manifest.files.some((file) => file.path === `bin/${executableName("nextdb-server")}`))

  await mkdir(dataDir, { recursive: true })
  await copyIfExists(join(bundleDir, "data", "behaviors"), join(dataDir, "behaviors"))
  await copyIfExists(join(bundleDir, "data", "schema"), join(dataDir, "schema"))

  child = spawn(serverBin, {
    cwd: bundleDir,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_RELEASE_SMOKE_LOGS === "1") {
      process.stdout.write(`[release] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_RELEASE_SMOKE_LOGS === "1") {
      process.stderr.write(`[release] ${chunk}`)
    }
  })

  await waitForHealth(endpoint)
  const ready = await getJson(`${endpoint}/v1/ready`)
  assert.equal(ready.ok, true)
  assert.equal(ready.writeReady, true)
  const health = await getJson(`${endpoint}/v1/health`)
  assert.equal(health.ok, true)
  const behaviors = await getJson(`${endpoint}/v1/behaviors`)
  assert(Array.isArray(behaviors))
  assert(behaviors.some((behavior) => behavior.name === "echo-ts" || behavior.name === "echo"))

  const key = `release-smoke-${Date.now()}`
  const upsert = await postJson(`${endpoint}/v1/records/rooms/${encodeURIComponent(key)}`, {
    value: { id: key, title: "Release Smoke" },
    durability: "strict",
    clientMutationId: `${key}-upsert`,
  })
  assert.equal(upsert.record.key, key)
  assert(upsert.record.lsn > 0)
  const fetched = await getJson(`${endpoint}/v1/records/rooms/${encodeURIComponent(key)}?minLsn=${upsert.record.lsn}`)
  assert.equal(fetched.record.value.title, "Release Smoke")
  const integrity = await getJson(`${endpoint}/v1/admin/wal/integrity`)
  assert.equal(integrity.ok, true)
  assert.equal(integrity.recordCount, upsert.record.lsn)

  console.log("nextdb release smoke ok")
  console.log(JSON.stringify({
    bundleDir,
    endpoint,
    lsn: upsert.record.lsn,
    behaviorCount: behaviors.length,
  }, null, 2))
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function executableName(name) {
  return process.platform === "win32" ? `${name}.exe` : name
}

async function assertFile(path) {
  const entry = await stat(path).catch(() => undefined)
  if (!entry?.isFile()) {
    throw new Error(`expected file is missing: ${path}`)
  }
}

async function copyIfExists(source, target) {
  const entry = await stat(source).catch(() => undefined)
  if (!entry) {
    return
  }
  await mkdir(dirname(target), { recursive: true })
  await cp(source, target, { recursive: entry.isDirectory() })
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
    const health = await getJson(`${url}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${url}`)
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
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`)
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
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
