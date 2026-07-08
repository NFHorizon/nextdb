import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:net"
import { spawn } from "node:child_process"
import { mkdir, mkdtemp, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-flamegraph-"))
const dataDir = join(tempRoot, "data")
const output = resolve(process.env.NEXTDB_FLAMEGRAPH_OUT ?? "target/nextdb-server-flamegraph.svg")
const benchmarkOut = resolve(process.env.NEXTDB_FLAMEGRAPH_BENCH_OUT ?? "target/nextdb-flamegraph-benchmark.json")
const walShards = process.env.NEXTDB_WAL_SHARDS ?? "4"
const port = await freePort()
const endpoint = `http://127.0.0.1:${port}`
let profiler

try {
  await mkdir(dataDir, { recursive: true })
  await mkdir(dirname(output), { recursive: true })
  await mkdir(dirname(benchmarkOut), { recursive: true })
  await assertCargoFlamegraph()

  profiler = spawn("cargo", [
    "flamegraph",
    "--release",
    "--bin",
    "nextdb-server",
    "--output",
    output,
  ], {
    cwd: root,
    detached: true,
    env: {
      ...process.env,
      NEXTDB_ADDR: `127.0.0.1:${port}`,
      NEXTDB_DATA_DIR: dataDir,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
      NEXTDB_WAL_SHARDS: walShards,
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  forward(profiler, "flamegraph")
  await waitForHealth(endpoint, profiler)

  await run("node", ["scripts/benchmark-local.mjs"], {
    ...process.env,
    NEXTDB_BENCH_ENDPOINT: endpoint,
    NEXTDB_BENCH_OUT: benchmarkOut,
    NEXTDB_WAL_SHARDS: walShards,
  })

  await stopProfiler(profiler)
  profiler = undefined

  const entry = await stat(output)
  assert(entry.isFile() && entry.size > 0, `missing flamegraph output ${output}`)
  console.log("nextdb flamegraph ok")
  console.log(JSON.stringify({
    endpoint,
    output,
    benchmarkOut,
    bytes: entry.size,
  }, null, 2))
} finally {
  if (profiler) {
    await stopProfiler(profiler, "SIGTERM").catch(() => {})
  }
  if (process.env.NEXTDB_FLAMEGRAPH_KEEP_DATA !== "true") {
    await rm(tempRoot, { recursive: true, force: true })
  } else {
    console.log(`kept flamegraph data at ${dataDir}`)
  }
}

async function assertCargoFlamegraph() {
  const result = await runCapture("cargo", ["flamegraph", "--help"])
  if (result.code !== 0) {
    throw new Error(
      "cargo flamegraph is required. Install cargo-flamegraph and run this on Linux for acceptance profiling.",
    )
  }
}

function forward(child, label) {
  child.stdout.on("data", (chunk) => {
    process.stdout.write(`[${label}] ${chunk}`)
  })
  child.stderr.on("data", (chunk) => {
    process.stderr.write(`[${label}] ${chunk}`)
  })
}

async function run(command, args, env = process.env) {
  const child = spawn(command, args, {
    cwd: root,
    env,
    stdio: "inherit",
  })
  const [code, signal] = await once(child, "exit")
  if (code !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with ${code ?? signal}`)
  }
}

async function runCapture(command, args) {
  const child = spawn(command, args, {
    cwd: root,
    env: process.env,
    stdio: ["ignore", "ignore", "ignore"],
  })
  const [code, signal] = await once(child, "exit")
  return { code, signal }
}

async function stopProfiler(child, signal = "SIGINT") {
  if (!child || child.exitCode !== null) {
    return
  }
  process.kill(-child.pid, signal)
  await Promise.race([
    once(child, "exit"),
    new Promise((resolve) => setTimeout(resolve, 30_000)).then(() => {
      process.kill(-child.pid, "SIGKILL")
      return once(child, "exit").catch(() => {})
    }),
  ])
}

async function waitForHealth(url, child) {
  await waitFor(async () => {
    if (child.exitCode !== null) {
      throw new Error(`profiler exited before server became healthy: ${child.exitCode}`)
    }
    const health = await getJson(`${url}/v1/health`).catch(() => undefined)
    return health?.ok === true
  }, `health at ${url}`, 90_000)
}

async function waitFor(check, label, timeoutMs) {
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
    await new Promise((resolve) => setTimeout(resolve, 250))
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
