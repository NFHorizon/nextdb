import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import { spawn } from "node:child_process"
import { readFile, stat } from "node:fs/promises"
import { basename, dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8"))
const version = packageJson.version ?? "0.1.0"
const targetTriple = `${process.platform}-${process.arch}`
const releaseRoot = resolve(process.env.NEXTDB_RELEASE_DIR ?? join(root, "dist", "release"))
const bundleName = `nextdb-${version}-${targetTriple}`
const bundleDir = resolve(process.env.NEXTDB_RELEASE_BUNDLE_DIR ?? join(releaseRoot, bundleName))
const archivePath = process.env.NEXTDB_RELEASE_ARCHIVE ?? `${bundleDir}.tar.gz`
const archiveShaPath = `${archivePath}.sha256`

const manifest = JSON.parse(await readFile(join(bundleDir, "manifest.json"), "utf8"))
assert.equal(manifest.format, "nextdb.release-bundle.v1")
assert.equal(manifest.sbom?.format, "nextdb.sbom.v1")
assert.equal(manifest.sbom?.path, "sbom.json")
assert(Array.isArray(manifest.files))

const seen = new Set()
for (const file of manifest.files) {
  assertSafeRelativePath(file.path)
  assert(!seen.has(file.path), `duplicate manifest path ${file.path}`)
  seen.add(file.path)
  const path = join(bundleDir, file.path)
  const entry = await stat(path)
  assert(entry.isFile(), `manifest path is not a file: ${file.path}`)
  assert.equal(entry.size, file.bytes, `size mismatch for ${file.path}`)
  assert.equal(await sha256File(path), file.sha256, `sha256 mismatch for ${file.path}`)
}

for (const required of [
  manifest.server.path,
  manifest.admin.entry,
  manifest.data.behaviorsPath,
  manifest.data.schemaPath,
  manifest.sbom.path,
  "README_RELEASE.md",
]) {
  assertSafeRelativePath(required)
}
assert(seen.has(manifest.server.path), "server binary missing from manifest files")
assert(seen.has(manifest.admin.entry), "admin entry missing from manifest files")
assert(seen.has(manifest.data.schemaPath), "schema seed missing from manifest files")
assert(seen.has(manifest.sbom.path), "SBOM missing from manifest files")

const sbom = JSON.parse(await readFile(join(bundleDir, manifest.sbom.path), "utf8"))
assert.equal(sbom.format, "nextdb.sbom.v1")
assert.equal(sbom.name, "nextdb")
assert.equal(sbom.version, version)
assert(Array.isArray(sbom.components?.rust), "SBOM rust components missing")
assert(Array.isArray(sbom.components?.npm), "SBOM npm components missing")
assert(sbom.components.rust.some((component) => component.name === "nextdb-server"))
assert(sbom.components.npm.some((component) => component.name === "@nextdb/client"))

const expectedArchiveSha = (await readFile(archiveShaPath, "utf8")).trim().split(/\s+/)[0]
assert.match(expectedArchiveSha, /^[a-f0-9]{64}$/)
assert.equal(await sha256File(archivePath), expectedArchiveSha, "archive sha256 sidecar mismatch")

const tarEntries = (await runCapture("tar", ["-tzf", archivePath]))
  .split("\n")
  .map((line) => line.trim())
  .filter(Boolean)
for (const entry of tarEntries) {
  assert(entry.startsWith(`${bundleName}/`), `archive entry escapes bundle root: ${entry}`)
  assert(!entry.includes("/../"), `archive entry contains traversal: ${entry}`)
  assert(!entry.startsWith("/"), `archive entry is absolute: ${entry}`)
}
for (const file of manifest.files) {
  assert(
    tarEntries.includes(`${bundleName}/${file.path}`),
    `archive missing manifest file ${file.path}`,
  )
}

console.log("nextdb release artifact verify ok")
console.log(JSON.stringify({
  bundleDir,
  archivePath,
  archiveSha256: expectedArchiveSha,
  fileCount: manifest.files.length,
  rustComponentCount: sbom.components.rust.length,
  npmComponentCount: sbom.components.npm.length,
}, null, 2))

function assertSafeRelativePath(path) {
  assert.equal(typeof path, "string")
  assert(path.length > 0, "path must not be empty")
  assert(!path.startsWith("/"), `absolute path is not allowed: ${path}`)
  assert(!path.split(/[\\/]/).includes(".."), `path traversal is not allowed: ${path}`)
  assert(!path.includes("\0"), "NUL byte is not allowed in path")
}

async function sha256File(path) {
  const hash = createHash("sha256")
  hash.update(await readFile(path))
  return hash.digest("hex")
}

function runCapture(cmd, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: root,
      stdio: ["ignore", "pipe", "pipe"],
    })
    let stdout = ""
    let stderr = ""
    child.stdout.on("data", (chunk) => {
      stdout += chunk
    })
    child.stderr.on("data", (chunk) => {
      stderr += chunk
    })
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolve(stdout)
        return
      }
      reject(new Error(`${cmd} ${args.join(" ")} failed with ${signal ?? code}: ${stderr}`))
    })
  })
}
