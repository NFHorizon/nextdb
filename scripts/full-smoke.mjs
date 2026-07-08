import { once } from "node:events"
import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { cp, mkdtemp, rm } from "node:fs/promises"
import { existsSync } from "node:fs"
import { join } from "node:path"
import { tmpdir } from "node:os"

const preManagedServerSteps = [
  ["npm", ["run", "p0:safety-net"]],
  ["cargo", ["fmt", "--check"]],
  ["cargo", ["check", "-p", "nextdb-server"]],
  ["cargo", ["build", "-p", "nextdb-server"]],
  ["npm", ["run", "build"]],
  ["npm", ["run", "typecheck:behavior-ts"]],
  ["npm", ["run", "compile:behavior-ts"]],
  ["npm", ["run", "test:cache", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:auth", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:connection-auth", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:transport", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:cache-profile", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:runtime-limits", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:object-range", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:message-batch", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:record-batch", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:write-throughput", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:runtime-chaos", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:wal-archive-retention", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:wal-integrity-corruption", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:wal-startup-corruption", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:wal-export-corruption", "-w", "@nextdb/client"]],
]

const managedServerSteps = [
  ["npm", ["run", "test:cache-control", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:prototype"]],
  ["npm", ["run", "test:nested-subscription", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:live-query", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:record-predicate", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:lru-record-hot", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:actor-window", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:volatile-message", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:volatile-record", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:volatile-overlay-restart", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:room-volatile", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:schema-version", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:schema-history", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:schema-proposal", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:schema-peer-preflight", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:schema-actor-policy", "-w", "@nextdb/client"]],
  ["node", ["scripts/admin-ui-smoke.mjs"]],
  ["npm", ["run", "test:realtime-channel", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:realtime-channel-sdk", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:sync-wait", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:runtime-prepare", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:runtime-drain-connection", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:runtime-restart", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:export-import", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:audit-trace", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:behavior-wasm", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:behavior-hot-reload", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:behavior-rust-wasm", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:behavior-idempotency", "-w", "@nextdb/client"]],
  ["cargo", ["build", "-p", "nextdb-server", "--features", "cluster"]],
  ["npm", ["run", "test:cluster-handoff", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:cluster-handoff", "-w", "@nextdb/client"], { NEXTDB_CLUSTER_HANDOFF_MODE: "controller" }],
  ["npm", ["run", "test:cluster-handoff", "-w", "@nextdb/client"], { NEXTDB_CLUSTER_HANDOFF_MODE: "failover-plan" }],
  ["npm", ["run", "test:cluster-handoff", "-w", "@nextdb/client"], { NEXTDB_CLUSTER_HANDOFF_MODE: "failover-controller" }],
  ["npm", ["run", "test:cluster-failover-election", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:cluster-read-quorum", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:cluster-wal-repair", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:cluster-wal-repair", "-w", "@nextdb/client"], { NEXTDB_CLUSTER_WAL_REPAIR_MODE: "controller" }],
  ["npm", ["run", "test:cluster-object-repair", "-w", "@nextdb/client"]],
  ["npm", ["run", "test:cluster-object-repair", "-w", "@nextdb/client"], { NEXTDB_CLUSTER_OBJECT_REPAIR_MODE: "controller" }],
]

const activeServers = new Set()

process.once("SIGINT", () => {
  void stopActiveServers()
  process.exit(130)
})
process.once("SIGTERM", () => {
  void stopActiveServers()
  process.exit(143)
})

async function main() {
  for (const [cmd, args, env] of preManagedServerSteps) {
    await run(cmd, args, env)
  }

  let managedServer
  let managedEnv = {}
  if (process.env.NEXTDB_ENDPOINT) {
    managedEnv = {
      NEXTDB_ENDPOINT: process.env.NEXTDB_ENDPOINT,
      NEXTDB_BASE_URL: process.env.NEXTDB_BASE_URL ?? process.env.NEXTDB_ENDPOINT,
    }
  } else {
    managedServer = await startTempServer("nextdb-full-main-")
    await copyBehaviorArtifacts(managedServer.dataDir)
    await waitForHealth(managedServer.endpoint, () => managedServer.exit)
    managedEnv = {
      NEXTDB_ENDPOINT: managedServer.endpoint,
      NEXTDB_BASE_URL: managedServer.endpoint,
    }
  }

  try {
    for (const [cmd, args, env] of managedServerSteps) {
      await run(cmd, args, {
        ...managedEnv,
        ...env,
      })
    }
  } finally {
    await managedServer?.stop()
  }

  const codegenEndpoint = process.env.NEXTDB_FULL_CODEGEN_ENDPOINT ?? await localEndpoint()
  const codegenAddr = new URL(codegenEndpoint)
  const codegenListen = `${codegenAddr.hostname}:${codegenAddr.port || "80"}`
  const codegenServer = startServer({
    endpoint: codegenEndpoint,
    dataDir: await mkdtemp(join(tmpdir(), "nextdb-full-codegen-")),
    env: {
      NEXTDB_ADDR: codegenListen,
    },
  })

  try {
    await waitForHealth(codegenEndpoint, () => codegenServer.exit)
    await run("npm", ["run", "test:codegen", "-w", "@nextdb/client"], {
      NEXTDB_ENDPOINT: codegenEndpoint,
    })
  } finally {
    await codegenServer.stop()
  }

  await run("git", ["diff", "--check"])
}

function run(cmd, args, env = {}) {
  console.log(`\n$ ${[cmd, ...args].join(" ")}`)
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      env: {
        ...process.env,
        ...env,
      },
      stdio: "inherit",
    })
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolve()
        return
      }
      reject(new Error(`${cmd} ${args.join(" ")} failed with ${signal ?? code}`))
    })
  })
}

async function startTempServer(prefix) {
  const endpoint = await localEndpoint()
  const addr = new URL(endpoint)
  const dataDir = await mkdtemp(join(tmpdir(), prefix))
  return startServer({
    endpoint,
    dataDir,
    env: {
      NEXTDB_ADDR: `${addr.hostname}:${addr.port || "80"}`,
    },
  })
}

function startServer({ endpoint, dataDir, env }) {
  const child = spawn("target/debug/nextdb-server", {
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: dataDir,
      ...env,
    },
    stdio: ["ignore", "inherit", "inherit"],
  })
  const server = {
    endpoint,
    dataDir,
    exit: undefined,
    stop: async () => {
      activeServers.delete(server)
      if (!child.killed) {
        child.kill("SIGINT")
      }
      await server.exitPromise.catch(() => {})
      if (process.env.NEXTDB_KEEP_FULL_SMOKE_DATA !== "true") {
        await rm(dataDir, { recursive: true, force: true })
      }
    },
    exitPromise: undefined,
  }
  server.exitPromise = once(child, "exit").then(([code, signal]) => {
    server.exit = { code, signal }
  })
  activeServers.add(server)
  return server
}

async function stopActiveServers() {
  await Promise.allSettled([...activeServers].map((server) => server.stop()))
}

async function localEndpoint() {
  const port = await freePort()
  return `http://127.0.0.1:${port}`
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

async function copyBehaviorArtifacts(dataDir) {
  if (!existsSync("data/behaviors")) {
    return
  }
  await cp("data/behaviors", join(dataDir, "behaviors"), {
    recursive: true,
  })
}

async function waitForHealth(endpoint, getServerExit) {
  const healthUrl = new URL("/v1/health", endpoint).toString()
  const startedAt = Date.now()
  let lastError
  while (Date.now() - startedAt < 10_000) {
    const serverExit = getServerExit()
    if (serverExit) {
      throw new Error(`nextdb-server exited before codegen health check: ${serverExit.signal ?? serverExit.code}`)
    }
    try {
      const response = await fetch(healthUrl)
      if (response.ok) {
        return
      }
      lastError = new Error(`health returned ${response.status}`)
    } catch (error) {
      lastError = error
    }
    await sleep(100)
  }
  throw new Error(`Timed out waiting for ${healthUrl}: ${lastError?.message ?? "unknown error"}`)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
