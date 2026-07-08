import { createHash } from "node:crypto"
import { spawn } from "node:child_process"
import { cp, mkdir, readdir, readFile, rm, stat, writeFile } from "node:fs/promises"
import { constants } from "node:fs"
import { basename, dirname, join, relative, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8"))
const version = packageJson.version ?? "0.1.0"
const targetTriple = `${process.platform}-${process.arch}`
const releaseRoot = resolve(process.env.NEXTDB_RELEASE_DIR ?? join(root, "dist", "release"))
const bundleName = `nextdb-${version}-${targetTriple}`
const bundleDir = join(releaseRoot, bundleName)
const archivePath = `${bundleDir}.tar.gz`

await assertFile(join(root, "target", "release", executableName("nextdb-server")))
await assertFile(join(root, "packages", "nextdb-admin", "dist", "index.html"))

await rm(bundleDir, { recursive: true, force: true })
await rm(archivePath, { force: true })
await mkdir(join(bundleDir, "bin"), { recursive: true })
await mkdir(join(bundleDir, "admin"), { recursive: true })
await mkdir(join(bundleDir, "data"), { recursive: true })
await mkdir(join(bundleDir, "docs"), { recursive: true })

const serverSource = join(root, "target", "release", executableName("nextdb-server"))
const serverTarget = join(bundleDir, "bin", executableName("nextdb-server"))
await cp(serverSource, serverTarget)
await chmodExecutable(serverTarget)
await cp(join(root, "packages", "nextdb-admin", "dist"), join(bundleDir, "admin"), { recursive: true })
await copyIfExists(join(root, "data", "behaviors"), join(bundleDir, "data", "behaviors"))
await copyIfExists(join(root, "data", "schema", "nextdb.schema.json"), join(bundleDir, "data", "schema", "nextdb.schema.json"))
await cp(join(root, "README.md"), join(bundleDir, "docs", "README.md"))
await cp(join(root, "docs", "PROTOTYPE_ACCEPTANCE.md"), join(bundleDir, "docs", "PROTOTYPE_ACCEPTANCE.md"))

await writeFile(join(bundleDir, "README_RELEASE.md"), releaseReadme(bundleName))
await writeFile(join(bundleDir, "sbom.json"), `${JSON.stringify(await createSbom(), null, 2)}\n`)
const files = await listFiles(bundleDir)
const manifest = {
  format: "nextdb.release-bundle.v1",
  name: "nextdb",
  version,
  target: targetTriple,
  createdAtMs: Date.now(),
  server: {
    path: `bin/${executableName("nextdb-server")}`,
  },
  admin: {
    path: "admin",
    entry: "admin/index.html",
  },
  data: {
    behaviorsPath: "data/behaviors",
    schemaPath: "data/schema/nextdb.schema.json",
  },
  sbom: {
    path: "sbom.json",
    format: "nextdb.sbom.v1",
  },
  files,
}
await writeFile(join(bundleDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`)

await run("tar", ["-czf", archivePath, "-C", releaseRoot, bundleName])
const archiveSha256 = await sha256File(archivePath)
await writeFile(
  `${archivePath}.sha256`,
  `${archiveSha256}  ${basename(archivePath)}\n`,
)

console.log("nextdb release package ok")
console.log(JSON.stringify({
  bundleDir,
  archivePath,
  archiveSha256,
  fileCount: files.length,
}, null, 2))

function executableName(name) {
  return process.platform === "win32" ? `${name}.exe` : name
}

async function assertFile(path) {
  const entry = await stat(path).catch(() => undefined)
  if (!entry?.isFile()) {
    throw new Error(`required release input is missing: ${path}`)
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

async function chmodExecutable(path) {
  if (process.platform !== "win32") {
    await import("node:fs/promises").then((fs) => fs.chmod(path, constants.S_IRWXU | constants.S_IRGRP | constants.S_IXGRP | constants.S_IROTH | constants.S_IXOTH))
  }
}

async function listFiles(baseDir, dir = baseDir) {
  const entries = await readdir(dir, { withFileTypes: true })
  const files = []
  for (const entry of entries) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) {
      files.push(...await listFiles(baseDir, path))
    } else if (entry.isFile()) {
      const fileStat = await stat(path)
      files.push({
        path: relative(baseDir, path),
        bytes: fileStat.size,
        sha256: await sha256File(path),
      })
    }
  }
  return files.sort((left, right) => left.path.localeCompare(right.path))
}

async function sha256File(path) {
  const hash = createHash("sha256")
  hash.update(await readFile(path))
  return hash.digest("hex")
}

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: root,
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

function releaseReadme(bundleName) {
  return `# ${bundleName}

This is a local NextDB prototype release bundle.

## Run

\`\`\`sh
NEXTDB_DATA_DIR=./data NEXTDB_ADDR=127.0.0.1:3188 ./bin/${executableName("nextdb-server")}
\`\`\`

The server exposes:

- http://127.0.0.1:3188/v1/ready
- http://127.0.0.1:3188/v1/health
- http://127.0.0.1:3188/v1/metrics

The Admin UI static files are in \`admin/\`. Serve them with any static file
server and point the UI at the NextDB endpoint.

The bundle includes compiled behavior modules under \`data/behaviors/\` when
they were present during packaging. Use a writable runtime data directory in
production-like runs; do not write into a read-only unpacked release.
`
}

async function createSbom() {
  return {
    format: "nextdb.sbom.v1",
    name: "nextdb",
    version,
    target: targetTriple,
    generatedAtMs: Date.now(),
    sourceLocks: {
      cargo: "Cargo.lock",
      npm: "package-lock.json",
    },
    components: {
      rust: await parseCargoLock(join(root, "Cargo.lock")),
      npm: await parsePackageLock(join(root, "package-lock.json")),
    },
  }
}

async function parseCargoLock(path) {
  const text = await readFile(path, "utf8")
  return text
    .split(/\n\[\[package\]\]\n/g)
    .slice(1)
    .map((block) => ({
      name: cargoField(block, "name"),
      version: cargoField(block, "version"),
      source: cargoField(block, "source"),
      checksum: cargoField(block, "checksum"),
    }))
    .filter((component) => component.name && component.version)
    .sort(componentSort)
}

function cargoField(block, name) {
  const match = block.match(new RegExp(`^${name} = "([^"]+)"`, "m"))
  return match?.[1]
}

async function parsePackageLock(path) {
  const lock = JSON.parse(await readFile(path, "utf8"))
  return Object.entries(lock.packages ?? {})
    .map(([path, entry]) => ({
      path,
      name: entry.name ?? packageNameFromLockPath(path),
      version: entry.version,
      resolved: entry.resolved,
      integrity: entry.integrity,
      license: entry.license,
    }))
    .filter((component) => component.name && component.version)
    .sort(componentSort)
}

function packageNameFromLockPath(path) {
  if (!path.startsWith("node_modules/")) {
    return undefined
  }
  return path.slice("node_modules/".length)
}

function componentSort(left, right) {
  return `${left.name}@${left.version}`.localeCompare(`${right.name}@${right.version}`)
}
