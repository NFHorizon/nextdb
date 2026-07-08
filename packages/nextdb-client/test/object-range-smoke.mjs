import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { MemoryLocalCache, NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-object-range-"))
const dataDir = join(tempRoot, "data")
const port = await availablePort()
const node = {
  url: `http://127.0.0.1:${port}`,
  addr: `127.0.0.1:${port}`,
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
	  await waitForHealth(node.url)
	
	  const db = new NextDbClient({ endpoint: node.url, userId: "alice" })
	  const schema = await db.getSchema()
	  schema.version += 1
	  schema.tables.rooms.fields.cover = {
	    type: { kind: "objectRef", object: "Object" },
	    optional: true,
	  }
	  const schemaApply = await db.applySchema(schema, { expectedVersion: schema.version - 1 })
	  assert.equal(schemaApply.applied, true)

	  const objectId = `object-range-${Date.now()}`
	  const body = "0123456789abcdefghijklmnopqrstuvwxyz"
	  const metadata = await db.putObject(body, {
    contentType: "text/plain",
    objectId,
    clientMutationId: `${objectId}-put`,
  })
  assert.equal(metadata.byteSize, body.length)

  const fullResponse = await fetch(`${node.url}/v1/objects/${encodeURIComponent(objectId)}/body`)
  assert.equal(fullResponse.status, 200)
  assert.equal(fullResponse.headers.get("accept-ranges"), "bytes")
  assert.equal(fullResponse.headers.get("content-length"), String(body.length))
  assert.equal(await fullResponse.text(), body)

  const first = await rangeFetch(objectId, "bytes=0-9")
  assert.equal(first.status, 206)
  assert.equal(first.contentRange, `bytes 0-9/${body.length}`)
  assert.equal(first.text, "0123456789")

  const middle = await rangeFetch(objectId, "bytes=10-15")
  assert.equal(middle.status, 206)
  assert.equal(middle.contentRange, `bytes 10-15/${body.length}`)
  assert.equal(middle.text, "abcdef")

  const suffix = await rangeFetch(objectId, "bytes=-4")
  assert.equal(suffix.status, 206)
  assert.equal(suffix.contentRange, `bytes ${body.length - 4}-${body.length - 1}/${body.length}`)
  assert.equal(suffix.text, "wxyz")

  const openEnded = await db.getObjectBodyRange(objectId, { start: 30 })
  assert.equal(openEnded.contentRange, `bytes 30-35/${body.length}`)
  assert.equal(openEnded.start, 30)
  assert.equal(openEnded.end, 35)
  assert.equal(openEnded.byteSize, body.length)
  assert.equal(openEnded.contentType, "text/plain")
  assert.equal(await openEnded.body.text(), "uvwxyz")

  const sdkSuffix = await db.objectStore("Object").getBodyRange(objectId, { suffixLength: 3 })
  assert.equal(sdkSuffix.contentRange, `bytes ${body.length - 3}-${body.length - 1}/${body.length}`)
  assert.equal(await sdkSuffix.body.text(), "xyz")

  const rangeCache = new MemoryLocalCache()
  await rangeCache.setMetadata({
    clientId: "object-range-cache-client",
    sessionId: "object-range-cache-session",
    profileVersion: 1,
    schemaVersion: schema.version,
    invalidationGeneration: 0,
    leaseExpiresAtMs: Date.now() + 60_000,
    lastValidatedAtMs: Date.now(),
  })
  await rangeCache.putObject(metadata)
  const rangeCachingClient = new NextDbClient({ endpoint: node.url, userId: "alice", cache: rangeCache })
  const cachedFromNetwork = await rangeCachingClient.getObjectBodyRange(objectId, { start: 0, end: 15 })
  assert.equal(await cachedFromNetwork.body.text(), "0123456789abcdef")
  rangeCachingClient.close()
  const rangeOfflineClient = new NextDbClient({ endpoint: "http://127.0.0.1:9", userId: "alice", cache: rangeCache })
  const cachedSubrange = await rangeOfflineClient.getObjectBodyRange(objectId, { start: 10, end: 15 })
  assert.equal(cachedSubrange.contentRange, `bytes 10-15/${body.length}`)
  assert.equal(await cachedSubrange.body.text(), "abcdef")
  assert.equal(await rangeOfflineClient.getCachedObjectBody(objectId), undefined)
  rangeOfflineClient.close()

  const invalid = await fetch(`${node.url}/v1/objects/${encodeURIComponent(objectId)}/body`, {
    headers: { range: `bytes=${body.length + 10}-` },
  })
  assert.equal(invalid.status, 416)
  const invalidBody = await invalid.json()
  assert.equal(invalidBody.contentRange, `bytes */${body.length}`)

  const missingObjectId = `${objectId}-missing`
  const missingRef = {
    ...metadata,
    id: missingObjectId,
    path: `objects/${missingObjectId}`,
  }
  const missingRecordKey = `object-ref-missing-${Date.now()}`
  await assert.rejects(
    () => db.table("rooms").upsert(
      missingRecordKey,
      { id: missingRecordKey, title: "Missing ObjectRef", cover: missingRef },
      { clientMutationId: `${objectId}-missing-ref` },
    ),
    (error) => error?.status === 404 && error.message.includes("object ref not found"),
  )

  const mismatchedRecordKey = `object-ref-mismatch-${Date.now()}`
  await assert.rejects(
    () => db.table("rooms").upsert(
      mismatchedRecordKey,
      {
        id: mismatchedRecordKey,
        title: "Mismatched ObjectRef",
        cover: { ...metadata, sha256: "not-the-object-sha" },
      },
      { clientMutationId: `${objectId}-mismatched-ref` },
    ),
    (error) => error?.status === 400 && error.message.includes("object ref metadata does not match"),
  )

  const recordKey = `object-ref-record-${Date.now()}`
  const coverRecord = await db.table("rooms").upsert(recordKey, {
    id: recordKey,
    title: "ObjectRef Record",
    cover: metadata,
  }, {
    clientMutationId: `${objectId}-record-ref`,
  })
  assert.equal(coverRecord.value.cover.id, objectId)

  const missingTxRecordKey = `object-ref-transaction-missing-${Date.now()}`
  await assert.rejects(
    () => db.recordTransaction([
      {
        type: "upsert",
        table: "rooms",
        key: missingTxRecordKey,
        value: {
          id: missingTxRecordKey,
          title: "Missing Tx ObjectRef",
          cover: missingRef,
        },
      },
    ], { clientMutationId: `${objectId}-missing-ref-transaction` }),
    (error) => error?.status === 404 && error.message.includes("object ref not found"),
  )

  const txRecordKey = `object-ref-transaction-${Date.now()}`
  const tx = await db.recordTransaction([
    {
      type: "upsert",
      table: "rooms",
      key: txRecordKey,
      value: { id: txRecordKey, title: "ObjectRef Transaction", cover: metadata },
    },
  ], { clientMutationId: `${objectId}-record-ref-transaction` })
  assert.equal(tx.operations.some((operation) =>
    operation.type === "recordUpserted" &&
    operation.record.key === txRecordKey &&
    operation.record.value.cover.id === objectId,
  ), true)

  const recordRefs = await db.getObjectReferences(objectId)
  assert.equal(recordRefs.objectExists, true)
  assert.equal(recordRefs.dangling, false)
  assert.equal(recordRefs.refCount, 2)
  assert.deepEqual(sorted(recordRefs.sources), sorted([coverRecord.path, `tables/rooms/${txRecordKey}`]))

  const roomId = `object-ref-room-${Date.now()}`
  const referenced = await db.room(roomId).messages.send("object reference smoke", {
    attachments: [objectId],
    clientMutationId: `${objectId}-referenced-message`,
  })
	  const refsBeforeDelete = await db.getObjectReferences(objectId)
	  assert.equal(refsBeforeDelete.objectExists, true)
	  assert.equal(refsBeforeDelete.dangling, false)
  assert.equal(refsBeforeDelete.refCount, 3)
  assert.deepEqual(sorted(refsBeforeDelete.sources), sorted([coverRecord.path, `tables/rooms/${txRecordKey}`, referenced.path]))

  const protectedDelete = await fetch(`${node.url}/v1/objects/${encodeURIComponent(objectId)}`, {
    method: "DELETE",
  })
  assert.equal(protectedDelete.status, 409)

  const deleted = await db.deleteObject(objectId, {
    force: true,
    clientMutationId: `${objectId}-force-delete`,
  })
  assert.equal(deleted.deleted, true)

	  const refsAfterForceDelete = await db.getObjectReferences(objectId)
	  assert.equal(refsAfterForceDelete.objectExists, false)
  assert.equal(refsAfterForceDelete.dangling, true)
  assert.equal(refsAfterForceDelete.refCount, 3)
  assert.deepEqual(sorted(refsAfterForceDelete.sources), sorted([coverRecord.path, `tables/rooms/${txRecordKey}`, referenced.path]))

  const audit = await getJson(`${node.url}/v1/audit/wal?objectId=${encodeURIComponent(objectId)}&payloadType=objectDeleted`)
  assert.equal(audit.records.length, 1)
  assert.equal(audit.records[0].payload.force, true)

  const restored = await db.putObject("replacement body", {
    contentType: "text/plain",
    objectId,
    clientMutationId: `${objectId}-restore`,
  })
  assert.equal(restored.id, objectId)
  const refsAfterRestore = await db.getObjectReferences(objectId)
  assert.equal(refsAfterRestore.objectExists, true)
  assert.equal(refsAfterRestore.dangling, false)
  assert.equal(refsAfterRestore.refCount, 3)
  assert.deepEqual(sorted(refsAfterRestore.sources), sorted([coverRecord.path, `tables/rooms/${txRecordKey}`, referenced.path]))

  db.close()
  console.log("object range smoke ok")
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

async function rangeFetch(objectId, range) {
  const response = await fetch(`${node.url}/v1/objects/${encodeURIComponent(objectId)}/body`, {
    headers: { range },
  })
  return {
    status: response.status,
    contentRange: response.headers.get("content-range"),
    text: await response.text(),
  }
}

function sorted(values) {
  return [...values].sort()
}

async function getJson(url) {
  const response = await fetch(url)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`GET ${url} ${response.status}: ${text}`)
  }
  return JSON.parse(text)
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

async function waitFor(predicate, label) {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`timed out waiting for ${label}`)
}

function availablePort() {
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
        if (typeof port !== "number") {
          reject(new Error("failed to allocate object range smoke port"))
          return
        }
        resolve(port)
      })
    })
  })
}
