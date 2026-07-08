import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawn } from "node:child_process"

import { NextDbClient } from "../dist/index.js"

const root = resolve(new URL("../../..", import.meta.url).pathname)
const serverBin = resolve(root, "target/debug/nextdb-server")
const tempRoot = await mkdtemp(join(tmpdir(), "nextdb-record-predicate-"))
const dataDir = join(tempRoot, "data")
const node = {
  url: "http://127.0.0.1:3396",
  addr: "127.0.0.1:3396",
  dataDir,
}
let child

try {
  await mkdir(dataDir, { recursive: true })
  child = startNode(node)
  await waitForHealth(node.url)

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
  const db = new NextDbClient({
    endpoint: node.url,
    userId: `predicate-user-${suffix}`,
    sessionId: `predicate-session-${suffix}`,
  })
  const rooms = db.table("rooms")

  try {
    const schema = await db.getSchema()
    schema.version += 1
    schema.tables.rooms.fields.status = { type: { kind: "string" } }
    schema.tables.rooms.fields.score = { type: { kind: "int64" } }
    schema.tables.rooms.fields.tags = { type: { kind: "list", item: { kind: "string" } } }
    schema.tables.rooms.indexes.byScore = { fields: ["score"], unique: false }
    await db.applySchema(schema, { dryRun: false })

    const records = [
      ["predicate-a", { title: "Shared", status: "open", score: 12, tags: ["urgent", "edge"] }],
      ["predicate-b", { title: "Shared", status: "closed", score: 15, tags: ["urgent"] }],
      ["predicate-c", { title: "Unique", status: "open", score: 7, tags: ["urgent"] }],
      ["predicate-d", { title: "Shared", status: "open", score: 20, tags: ["later"] }],
      ["predicate-e", { title: "Other", status: "open", score: 30, tags: ["urgent"] }],
    ].map(([name, value]) => [`${name}-${suffix}`, value])

    for (const [key, value] of records) {
      await rooms.upsert(key, { id: key, ...value }, { clientMutationId: `${key}-upsert` })
    }

    const predicate = {
      all: [
        { field: "status", op: "eq", value: "open" },
        { field: "score", op: "gte", value: 10 },
        { field: "tags", op: "contains", value: "urgent" },
      ],
    }
    const firstPage = await rooms.list({ limit: 2, predicate })
    assert.deepEqual(firstPage.records.map((record) => record.key), [
      `predicate-a-${suffix}`,
      `predicate-e-${suffix}`,
    ])
    assert.equal(firstPage.hasMore, false)

    const indexed = await rooms.index("byTitle", {
      value: "Shared",
      predicate: {
        all: [
          { field: "status", op: "eq", value: "open" },
          { field: "score", op: "gt", value: 10 },
        ],
      },
    })
    assert.deepEqual(indexed.records.map((record) => record.key), [
      `predicate-a-${suffix}`,
      `predicate-d-${suffix}`,
    ])

    const plainRangeFirst = await rooms.index("byScore", {
      lower: 10,
      upper: 30,
      limit: 2,
    })
    assert.deepEqual(plainRangeFirst.records.map((record) => record.key), [
      `predicate-a-${suffix}`,
      `predicate-b-${suffix}`,
    ])
    assert.equal(plainRangeFirst.hasMore, true)
    assert.equal(typeof plainRangeFirst.nextCursor, "string")

    const plainRangeSecond = await rooms.index("byScore", {
      lower: 10,
      upper: 30,
      limit: 2,
      afterCursor: plainRangeFirst.nextCursor,
    })
    assert.deepEqual(plainRangeSecond.records.map((record) => record.key), [
      `predicate-d-${suffix}`,
      `predicate-e-${suffix}`,
    ])
    assert.equal(plainRangeSecond.hasMore, false)
    assert.equal(plainRangeSecond.nextCursor, undefined)

    const rangeFirst = await rooms.index("byScore", {
      lower: 10,
      upper: 30,
      limit: 2,
      predicate: { all: [{ field: "status", op: "eq", value: "open" }] },
    })
    assert.deepEqual(rangeFirst.records.map((record) => record.key), [
      `predicate-a-${suffix}`,
      `predicate-d-${suffix}`,
    ])
    assert.equal(rangeFirst.hasMore, true)
    assert.equal(typeof rangeFirst.nextCursor, "string")

    const rangeSecond = await rooms.index("byScore", {
      lower: 10,
      upper: 30,
      limit: 2,
      afterCursor: rangeFirst.nextCursor,
      predicate: { all: [{ field: "status", op: "eq", value: "open" }] },
    })
    assert.deepEqual(rangeSecond.records.map((record) => record.key), [
      `predicate-e-${suffix}`,
    ])
    assert.equal(rangeSecond.hasMore, false)
    assert.equal(rangeSecond.nextCursor, null)

    const liveResults = []
    const livePredicate = { all: [{ field: "status", op: "eq", value: "live" }] }
    const stop = rooms.subscribeQuery((result) => liveResults.push(result), {
      queryId: `predicate-live-${suffix}`,
      limit: 20,
      predicate: livePredicate,
    })
    try {
      await waitFor(() => liveResults.length === 1, "initial predicate live query")
      assert.deepEqual(liveResults.at(-1).response.records.map((record) => record.key), [])

      const beforeNonMatch = liveResults.length
      const nonMatchKey = `predicate-live-archived-${suffix}`
      await rooms.upsert(nonMatchKey, {
        id: nonMatchKey,
        title: "Live Candidate",
        status: "archived",
        score: 1,
        tags: [],
      }, { clientMutationId: `${nonMatchKey}-upsert` })
      await waitForStableCount(() => liveResults.length, beforeNonMatch, 300, "non-matching predicate write should not refresh")

      const matchKey = `predicate-live-match-${suffix}`
      const match = await rooms.upsert(matchKey, {
        id: matchKey,
        title: "Live Candidate",
        status: "live",
        score: 2,
        tags: [],
      }, { clientMutationId: `${matchKey}-upsert` })
      await waitFor(
        () => liveResults.length > beforeNonMatch
          && liveResults.at(-1).response.records.some((record) => record.key === matchKey),
        "matching predicate live query refresh",
      )
      assert(liveResults.at(-1).currentLsn >= match.lsn)
    } finally {
      stop()
    }

    console.log("record predicate smoke ok")
  } finally {
    db.close()
  }
} finally {
  if (child) {
    await stopNode(child)
  }
  await rm(tempRoot, { recursive: true, force: true })
}

function startNode(node) {
  const child = spawn(serverBin, {
    cwd: root,
    env: {
      ...process.env,
      NEXTDB_DATA_DIR: node.dataDir,
      NEXTDB_ADDR: node.addr,
      NEXTDB_CHECKPOINT_EVERY_LSN: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  child.once("error", (error) => {
    throw error
  })
  child.stdout.on("data", (chunk) => {
    if (process.env.NEXTDB_RECORD_PREDICATE_SMOKE_LOGS === "1") {
      process.stdout.write(`[record-predicate] ${chunk}`)
    }
  })
  child.stderr.on("data", (chunk) => {
    if (process.env.NEXTDB_RECORD_PREDICATE_SMOKE_LOGS === "1") {
      process.stderr.write(`[record-predicate] ${chunk}`)
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

async function waitForHealth(url) {
  await waitFor(async () => {
    const response = await fetch(`${url}/v1/health`).catch(() => undefined)
    if (!response?.ok) {
      return false
    }
    const health = await response.json()
    return health.ok === true
  }, `health at ${url}`)
}

async function waitFor(check, label, timeoutMs = 5_000) {
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

async function waitForStableCount(readCount, expectedCount, durationMs, label) {
  const deadline = Date.now() + durationMs
  while (Date.now() < deadline) {
    assert.equal(readCount(), expectedCount, label)
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}
