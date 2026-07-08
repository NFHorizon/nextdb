import assert from "node:assert/strict"
import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const behaviorCli = resolve(root, "packages/nextdb-behavior-sdk/dist/cli.js")
const behaviorExample = resolve(root, "examples/behaviors/echo-ts")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-behavior-hot-reload-"))
const dataDir = join(tempRoot, "data")
const behaviorOut = join(dataDir, "behaviors", "echo-ts")
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
const addr = `127.0.0.1:${port}`

let child
let db
let writer

try {
  await mkdir(behaviorOut, { recursive: true })
  await run(process.execPath, [
    behaviorCli,
    "compile",
    "--manifest",
    join(behaviorExample, "nextdb.behavior.json"),
    "--entry",
    join(behaviorExample, "src/index.ts"),
    "--out",
    behaviorOut,
  ])

  child = startNode()
  await waitForHealth(endpoint)
  db = new NextDbClient({ endpoint, userId: "alice" })
  writer = new NextDbClient({ endpoint, userId: "behavior-hot-reload-writer" })

  const initialHealth = await db.health()
  const initialEpoch = initialHealth.behaviorRuntime.epoch

  const roomId = `behavior-watch-${Date.now()}`
  const tableEvents = []
  const stopTable = db.table("rooms").subscribe((event) => tableEvents.push(event), {
    catchUp: false,
  })
  await waitFor(async () => {
    const connections = await db.listConnections("alice")
    return connections.sessions.some((session) => session.subscribedTables.includes("rooms"))
  }, "rooms table subscription")

  const subscriptionProbeKey = `${roomId}-subscription-probe`
  await writer.table("rooms").upsert(subscriptionProbeKey, {
    id: subscriptionProbeKey,
    title: "Behavior Hot Reload Subscription Probe",
  })
  await waitFor(() =>
    tableEvents.some((event) =>
      event.type === "recordUpserted" &&
      event.key === subscriptionProbeKey,
    ), "subscription receives pre-reload record")

  const first = await db.invokeBehavior({
    behavior: "echo-ts",
    mutation: "echo.send",
    input: {
      roomId,
      body: "before hot reload",
    },
  })
  assert.deepEqual(first.committed.map((entry) => entry.type), [
    "recordUpserted",
    "objectCommitted",
    "messageCreated",
  ])
  assert.equal(first.metadata.epoch, initialEpoch)

  const manifestPath = join(behaviorOut, "nextdb.behavior.json")
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"))
  manifest.version = "0.1.1"
  manifest.commands = ["sendMessage"]
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`)

  const loadKeys = Array.from({ length: 6 }, (_, index) => `${roomId}-reload-load-${index}`)
  await Promise.all(loadKeys.map((key) =>
    writer.table("rooms").upsert(key, {
      id: key,
      title: `Behavior Hot Reload Load ${key}`,
    })))

  const reloadedHealth = await waitForBehaviorEpoch(db, initialEpoch)
  assert(reloadedHealth.behaviorRuntime.epoch > initialEpoch)
  await waitFor(() =>
    loadKeys.every((key) =>
      tableEvents.some((event) =>
        event.type === "recordUpserted" &&
        event.key === key,
      )),
    () => {
      const seen = new Set(tableEvents
        .filter((event) => event.type === "recordUpserted")
        .map((event) => event.key))
      const missing = loadKeys.filter((key) => !seen.has(key))
      return `subscription receives all records written during hot reload; missing=${missing.join(",")}; seen=${[...seen].join(",")}`
    },
  )
  const published = await db.auditWal({ payloadType: "behaviorPublished", limit: 10 })
  assert(
    published.records.some((record) => record.payload.publish.epoch === reloadedHealth.behaviorRuntime.epoch),
    "hot reload must publish a WAL behaviorPublished fact for the active epoch",
  )

  await assert.rejects(
    () => db.invokeBehavior({
      behavior: "echo-ts",
      mutation: "echo.send",
      input: {
        roomId: `${roomId}-restricted`,
        body: "after hot reload",
      },
    }),
    (error) => error?.status === 400 && error.message.includes("not allowed to return host command"),
  )

  const stillAlive = await db.health()
  assert.equal(stillAlive.ok, true)
  assert.equal(stillAlive.behaviorRuntime.epoch, reloadedHealth.behaviorRuntime.epoch)

  const afterReloadKey = `${roomId}-after-reload`
  await writer.table("rooms").upsert(afterReloadKey, {
    id: afterReloadKey,
    title: "Behavior Hot Reload Connection Still Alive",
  })
  await waitFor(() =>
    tableEvents.some((event) =>
      event.type === "recordUpserted" &&
      event.key === afterReloadKey,
    ), "same subscription receives post-reload record")
  stopTable()

  console.log("behavior hot reload smoke ok")
} finally {
  writer?.close()
  db?.close()
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode() {
  const spawned = spawn(serverBin, ["dev", "--watch"], {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_ADDR: addr,
      NEXTDB_BEHAVIOR_WATCH_INTERVAL_MS: "50",
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  spawned.once("error", (error) => {
    throw error
  })
  spawned.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_BEHAVIOR_HOT_RELOAD_SMOKE_LOGS === "1") {
      process.stdout.write(`[behavior-hot-reload] ${chunk}`)
    }
  })
  spawned.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_BEHAVIOR_HOT_RELOAD_SMOKE_LOGS === "1") {
      process.stderr.write(`[behavior-hot-reload] ${chunk}`)
    }
  })
  return spawned
}

async function waitForBehaviorEpoch(client, previousEpoch) {
  const deadline = Date.now() + 10_000
  let lastError
  while (Date.now() < deadline) {
    try {
      const health = await client.health()
      if (health.behaviorRuntime.epoch > previousEpoch) {
        return health
      }
    } catch (error) {
      lastError = error
    }
    await delay(100)
  }
  throw new Error(`behavior hot reload did not advance epoch; lastError=${lastError?.message ?? "none"}`)
}

async function waitForHealth(baseUrl) {
  const deadline = Date.now() + 10_000
  let lastError
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/v1/health`)
      if (response.ok) {
        const health = await response.json()
        if (health.ok) {
          return health
        }
      }
    } catch (error) {
      lastError = error
    }
    await delay(100)
  }
  throw new Error(`server did not become healthy; lastError=${lastError?.message ?? "none"}`)
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 10_000
  let lastError
  while (Date.now() < deadline) {
    try {
      if (await predicate()) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await delay(50)
  }
  const resolvedLabel = typeof label === "function" ? label() : label
  throw new Error(`${resolvedLabel} timed out; lastError=${lastError?.message ?? "none"}`)
}

function run(cmd, args) {
  return new Promise((resolvePromise, reject) => {
    const spawned = spawn(cmd, args, {
      cwd: root,
      stdio: "inherit",
    })
    spawned.on("error", reject)
    spawned.on("exit", (code) => {
      if (code === 0) {
        resolvePromise()
      } else {
        reject(new Error(`${cmd} ${args.join(" ")} failed with exit code ${code}`))
      }
    })
  })
}

async function stopNode(spawned) {
  if (spawned.exitCode !== null) {
    return
  }
  spawned.kill("SIGTERM")
  await new Promise((resolvePromise) => {
    const timeout = setTimeout(() => {
      spawned.kill("SIGKILL")
      resolvePromise()
    }, 5_000)
    spawned.once("exit", () => {
      clearTimeout(timeout)
      resolvePromise()
    })
  })
}

function delay(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms))
}

async function freePort() {
  const server = createServer()
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  assert(address && typeof address === "object")
  const free = address.port
  server.close()
  await once(server, "close")
  return free
}
