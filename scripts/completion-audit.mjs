import assert from "node:assert/strict"
import { readdir, readFile, stat } from "node:fs/promises"
import { join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(new URL("..", import.meta.url).pathname)
const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8"))
const acceptance = await readText("docs/PROTOTYPE_ACCEPTANCE.md")
const readme = await readText("README.md")

const requiredScripts = [
  "build",
  "test:full",
  "test:prototype",
  "test:auth",
  "test:connection-auth",
  "test:transport",
  "test:cache",
  "test:cache-profile",
  "test:cache-control",
  "test:runtime-limits",
  "test:runtime-chaos",
  "test:runtime-restart",
  "test:message-batch",
  "test:record-batch",
  "test:write-throughput",
  "test:wal-integrity-corruption",
  "test:wal-startup-corruption",
  "test:wal-export-corruption",
  "test:export-import",
  "test:audit-trace",
  "test:behavior-wasm",
  "test:behavior-hot-reload",
  "test:behavior-rust-wasm",
  "test:behavior-idempotency",
  "test:realtime-channel",
  "test:realtime-channel-sdk",
  "test:codegen",
  "benchmark:micro",
  "benchmark:local",
  "benchmark:flamegraph",
  "p0:panic-hygiene",
  "p0:wal-fault-injection",
  "p0:safety-net",
  "soak:local",
  "release:package",
  "release:artifact",
  "release:verify",
]
for (const script of requiredScripts) {
  assert(packageJson.scripts?.[script], `missing package script ${script}`)
}

const requiredFiles = [
  "Cargo.toml",
  "Cargo.lock",
  "package-lock.json",
  "crates/nextdb-server/src/main.rs",
  "crates/nextdb-server/src/wal.rs",
  "crates/nextdb-server/src/actor.rs",
  "crates/nextdb-server/src/behavior.rs",
  "crates/nextdb-server/src/realtime.rs",
  "crates/nextdb-server/src/object_store.rs",
  "crates/nextdb-server/src/schema.rs",
  "packages/nextdb-client/src/index.ts",
  "packages/nextdb-admin/src/main.tsx",
  "scripts/full-smoke.mjs",
  "scripts/prototype-smoke.mjs",
  "scripts/benchmark-local.mjs",
  "scripts/flamegraph-local.mjs",
  "scripts/panic-hygiene.mjs",
  "scripts/soak-local.mjs",
  "scripts/package-release.mjs",
  "scripts/release-artifact-verify.mjs",
  "scripts/release-smoke.mjs",
  "docs/ARCHITECTURE.md",
  "docs/PROTOTYPE_ACCEPTANCE.md",
]
for (const path of requiredFiles) {
  await assertFile(path)
}

const originalGoals = [
  "Data and behavior separation",
  "Erlang ideas, Elixir-like syntax, Rust performance",
  "Runtime restart while serving",
  "Type system strongly bound to database fields",
  "Virtual actor tables with resident/LRU/disk behavior",
  "WAL persistence plus event sourcing, audit, and tracing",
  "Built-in object storage",
  "Borrow from Convex and rustfs",
  "Realtime database syncing changes to clients",
  "Client SDK owns local cache management",
  "Polished management UI",
  "Realtime channels for voice/video/game",
]
for (const goal of originalGoals) {
  const row = acceptance.split("\n").find((line) => line.includes(`| ${goal} |`))
  assert(row, `acceptance matrix missing original goal: ${goal}`)
  assert(row.includes("test:"), `acceptance row lacks smoke evidence: ${goal}`)
}

const guaranteeRows = [
  "Readiness and operator drain",
  "Backup and restore",
  "Cluster control",
  "Runtime limits and short chaos",
  "Local benchmark harness",
  "Local soak harness",
  "Release packaging",
]
for (const guarantee of guaranteeRows) {
  assert(acceptance.includes(`| ${guarantee} |`), `acceptance matrix missing guarantee: ${guarantee}`)
}

for (const command of [
  "npm run test:full",
  "npm run benchmark:local",
  "npm run soak:local",
  "npm run release:verify",
]) {
  assert(acceptance.includes(command), `acceptance doc missing command ${command}`)
  assert(readme.includes(command), `README missing command ${command}`)
}

const clientTestFiles = await listFiles("packages/nextdb-client/test")
const smokeFiles = clientTestFiles.filter((path) => path.endsWith("-smoke.mjs"))
assert(smokeFiles.length >= 40, `expected broad smoke coverage, found ${smokeFiles.length}`)
for (const expected of [
  "runtime-chaos-smoke.mjs",
  "runtime-restart-recovery-smoke.mjs",
  "message-batch-smoke.mjs",
  "record-batch-smoke.mjs",
  "write-throughput-smoke.mjs",
  "wal-integrity-corruption-smoke.mjs",
  "audit-trace-smoke.mjs",
  "behavior-wasm-smoke.mjs",
  "behavior-hot-reload-smoke.mjs",
  "behavior-rust-wasm-smoke.mjs",
  "realtime-channel-sdk-smoke.mjs",
  "cluster-failover-election-smoke.mjs",
  "cluster-wal-repair-smoke.mjs",
  "cluster-object-repair-smoke.mjs",
  "object-range-smoke.mjs",
]) {
  assert(smokeFiles.some((path) => path.endsWith(expected)), `missing smoke test ${expected}`)
}

const releaseBundleDir = await findReleaseBundleDir()
const manifest = JSON.parse(await readFile(join(releaseBundleDir, "manifest.json"), "utf8"))
assert.equal(manifest.format, "nextdb.release-bundle.v1")
assert.equal(manifest.sbom?.format, "nextdb.sbom.v1")
assert(Array.isArray(manifest.files))
assert(manifest.files.length >= 14, "release manifest should include server, admin, behaviors, schema, docs, and SBOM")
for (const requiredPath of [
  manifest.server.path,
  manifest.admin.entry,
  manifest.data.schemaPath,
  manifest.sbom.path,
  "README_RELEASE.md",
]) {
  assert(manifest.files.some((file) => file.path === requiredPath), `release manifest missing ${requiredPath}`)
}

const sbom = JSON.parse(await readFile(join(releaseBundleDir, manifest.sbom.path), "utf8"))
assert.equal(sbom.format, "nextdb.sbom.v1")
assert(sbom.components.rust.length >= 1, "SBOM rust components missing")
assert(sbom.components.npm.length >= 1, "SBOM npm components missing")
assert(sbom.components.rust.some((component) => component.name === "nextdb-server"))
assert(sbom.components.npm.some((component) => component.name === "@nextdb/client"))

const knownScope = [
  "Native WebTransport/HTTP3 server listener",
  "Production-grade distributed consensus",
  "Production benchmark and soak certification",
  "Hardened multi-platform release workflow",
]
for (const item of knownScope) {
  assert(acceptance.includes(item), `known non-production scope missing: ${item}`)
}

console.log("nextdb completion audit ok")
console.log(JSON.stringify({
  originalGoalCount: originalGoals.length,
  requiredScriptCount: requiredScripts.length,
  smokeFileCount: smokeFiles.length,
  releaseBundleDir,
  releaseFileCount: manifest.files.length,
  rustComponentCount: sbom.components.rust.length,
  npmComponentCount: sbom.components.npm.length,
}, null, 2))

async function readText(path) {
  return readFile(join(root, path), "utf8")
}

async function assertFile(path) {
  const entry = await stat(join(root, path)).catch(() => undefined)
  assert(entry?.isFile(), `missing required file ${path}`)
}

async function listFiles(path) {
  const dir = join(root, path)
  const entries = await readdir(dir, { withFileTypes: true })
  const files = []
  for (const entry of entries) {
    const fullPath = join(dir, entry.name)
    const relativePath = `${path}/${entry.name}`
    if (entry.isDirectory()) {
      files.push(...await listFiles(relativePath))
    } else if (entry.isFile()) {
      files.push(fullPath)
    }
  }
  return files
}

async function findReleaseBundleDir() {
  const releaseRoot = join(root, "dist", "release")
  const entries = await readdir(releaseRoot, { withFileTypes: true }).catch(() => [])
  const dirs = entries
    .filter((entry) => entry.isDirectory() && entry.name.startsWith("nextdb-"))
    .map((entry) => join(releaseRoot, entry.name))
    .sort()
  assert(dirs.length > 0, "release bundle directory missing; run npm run release:verify first")
  return dirs.at(-1)
}
