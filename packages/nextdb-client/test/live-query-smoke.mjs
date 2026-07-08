import assert from "node:assert/strict"

import { NextDbClient } from "../dist/index.js"

const endpoint = process.env.NEXTDB_ENDPOINT ?? "http://127.0.0.1:3188"
const wsEndpoint = endpoint.replace(/^http:/, "ws:").replace(/^https:/, "wss:")
const frames = []
const ws = new WebSocket(`${wsEndpoint}/v1/connect?userId=live-query-smoke`)
ws.addEventListener("message", (event) => frames.push(JSON.parse(event.data)))

await waitFor(() => frames.some((frame) => frame.type === "hello"), "hello")

const queryId = `live-query-${Date.now()}`
const indexQueryId = `${queryId}-by-title`
const diffQueryId = `${queryId}-by-title-diff`
const sdkDiffQueryId = `${queryId}-sdk-diff`
const indexedTitle = `Live Query Indexed ${Date.now()}`
ws.send(JSON.stringify({
  type: "subscribeQuery",
  queryId,
  table: "rooms",
  limit: 300,
}))
ws.send(JSON.stringify({
  type: "subscribeQuery",
  queryId: indexQueryId,
  table: "rooms",
  indexName: "byTitle",
  value: indexedTitle,
  limit: 20,
}))
ws.send(JSON.stringify({
  type: "subscribeQuery",
  queryId: diffQueryId,
  table: "rooms",
  indexName: "byTitle",
  value: indexedTitle,
  limit: 20,
  diff: true,
}))

await waitFor(
  () => frames.some((frame) => frame.type === "queryResult" && frame.queryId === queryId),
  "initial query result",
)
await waitFor(
  () => frames.some((frame) => frame.type === "queryResult" && frame.queryId === indexQueryId),
  "initial indexed query result",
)
await waitFor(
  () => frames.some((frame) => frame.type === "queryResult" && frame.queryId === diffQueryId),
  "initial diff query result",
)
await waitForConnectionQueryTables("live-query-smoke", { rooms: 3 })
let liveMetrics = await liveQueryMetrics()
assert(liveMetrics.health.current >= 3)
assert(liveMetrics.health.eventBatchMax >= 1)
assert(liveMetrics.health.resultFramesTotal >= 3)
assert.match(liveMetrics.metrics, /^nextdb_live_queries_current \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_event_batch_max \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_event_batches_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_batched_events_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_refresh_candidates_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_executions_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_result_frames_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_evaluation_cache_hits_total \d+$/m)
const initialIndexResult = frames
  .filter((frame) => frame.type === "queryResult" && frame.queryId === indexQueryId)
  .at(-1)
assert.match(initialIndexResult.resultId, /^sha256:/)
await assertQueryResumeUnchanged({
  endpoint,
  wsEndpoint,
  queryId: `${indexQueryId}-resume`,
  table: "rooms",
  indexName: "byTitle",
  value: indexedTitle,
  resultId: initialIndexResult.resultId,
})
const beforeCount = queryResultCount(queryId)
const beforeIndexCount = queryResultCount(indexQueryId)
const beforeDiffCount = queryDiffCount(diffQueryId)
const db = new NextDbClient({ endpoint, userId: "live-query-smoke-sdk" })
const sdkEvents = []
const sdkCacheChanges = []
const aggregateEvents = []
const aggregateSumEvents = []
const unsubscribeCacheChanges = db.onCacheChange((event) => sdkCacheChanges.push(event))
const unsubscribeAggregate = db.subscribeAggregateCount("rooms", (event) => aggregateEvents.push(event))
const unsubscribeAggregateSum = db.subscribeAggregateSum("rooms", "score", (event) => aggregateSumEvents.push(event))
const unsubscribeSdkDiff = db.subscribeQuery(
  {
    queryId: sdkDiffQueryId,
    table: "rooms",
    indexName: "byTitle",
    value: indexedTitle,
    limit: 20,
  },
  (event) => sdkEvents.push(event),
)
await waitFor(() => sdkEvents.some((event) => event.queryId === sdkDiffQueryId), "initial SDK diff query result")
await waitFor(() => aggregateEvents.some((event) => event.source === "snapshot"), "initial aggregate count")
await waitFor(() => aggregateSumEvents.some((event) => event.source === "snapshot"), "initial aggregate sum")
const initialAggregateCount = aggregateEvents.at(-1).count
const initialAggregateSum = aggregateSumEvents.at(-1).sum
await waitForConnectionQueryTables("live-query-smoke-sdk", { rooms: 1 })
ws.send(JSON.stringify({
  type: "subscribeTable",
  table: "rooms",
  indexName: "byTitle",
  indexValues: JSON.stringify([indexedTitle]),
}))
await waitFor(
  () => frames.some((frame) => frame.type === "tableSubscribed" && frame.table === "rooms"),
  "table index-prefix subscription",
)
const nonMatchingKey = `live-query-nonmatch-${Date.now()}`
const nonMatchingResponse = await fetch(`${endpoint}/v1/records/rooms/${encodeURIComponent(nonMatchingKey)}`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    value: { id: nonMatchingKey, title: `${indexedTitle} no-match`, score: 2 },
    durability: "strict",
    clientMutationId: `${nonMatchingKey}-upsert`,
  }),
})
const nonMatchingText = await nonMatchingResponse.text()
assert.equal(nonMatchingResponse.status, 200, nonMatchingText)
await waitFor(() => queryResultCount(queryId) > beforeCount, "list query refresh for non-matching write")
await waitFor(
  () => aggregateEvents.some((event) => event.source === "update" && event.count >= initialAggregateCount + 1),
  "aggregate count update for non-matching write",
)
await waitFor(
  () => aggregateSumEvents.some((event) => event.source === "update" && event.sum >= initialAggregateSum + 2),
  "aggregate sum update for non-matching write",
)
await waitForStableCount(indexQueryId, beforeIndexCount, 250, "indexed query should not refresh for non-matching write")
await waitForStableDiffCount(diffQueryId, beforeDiffCount, 250, "diff indexed query should not refresh for non-matching write")
assert.equal(
  frames.some((frame) => frame.type === "event" && frame.event?.type === "recordUpserted" && frame.event.key === nonMatchingKey),
  false,
  "table index-prefix subscription must ignore non-matching writes",
)
const beforeMatchingListCount = queryResultCount(queryId)
const beforeMatchingIndexCount = queryResultCount(indexQueryId)
const beforeMatchingDiffCount = queryDiffCount(diffQueryId)
const beforeMatchingMetrics = await liveQueryMetrics()
const key = `live-query-room-${Date.now()}`
const response = await fetch(`${endpoint}/v1/records/rooms/${encodeURIComponent(key)}`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    value: { id: key, title: indexedTitle, score: 3 },
    durability: "strict",
    clientMutationId: `${key}-upsert`,
  }),
})
const responseText = await response.text()
assert.equal(response.status, 200, responseText)
const upsert = JSON.parse(responseText)
await waitFor(
  () => frames.some((frame) => frame.type === "event" &&
    frame.event?.type === "recordUpserted" &&
    frame.event.key === key &&
    frame.event.record?.lsn === upsert.record.lsn),
  "table index-prefix matching write event",
)
ws.send(JSON.stringify({
  type: "subscribeTable",
  table: "rooms",
  indexName: "byTitle",
  indexValues: JSON.stringify([indexedTitle]),
  snapshotLimit: 50,
}))
await waitFor(
  () => frames.some((frame) => frame.type === "tableSnapshot" &&
    frame.table === "rooms" &&
    frame.indexName === "byTitle" &&
    frame.indexValues === JSON.stringify([indexedTitle])),
  "table index-prefix server snapshot",
)
const indexPrefixSnapshot = frames
  .filter((frame) => frame.type === "tableSnapshot" &&
    frame.table === "rooms" &&
    frame.indexName === "byTitle" &&
    frame.indexValues === JSON.stringify([indexedTitle]))
  .at(-1)
assert(
  indexPrefixSnapshot.response.records.some((record) => record.key === key),
  "table index-prefix snapshot includes matching record",
)
assert.equal(
  indexPrefixSnapshot.response.records.some((record) => record.key === nonMatchingKey),
  false,
  "table index-prefix snapshot excludes non-matching record",
)
assert(
  indexPrefixSnapshot.response.records.every((record) => record.value.title === indexedTitle),
  "table index-prefix snapshot only contains matching index values",
)

await waitFor(() => queryResultCount(queryId) > beforeMatchingListCount, "refreshed query result")
await waitFor(
  () => aggregateEvents.some((event) => event.source === "update" && event.count >= initialAggregateCount + 2),
  "aggregate count update for matching write",
)
await waitFor(
  () => aggregateSumEvents.some((event) => event.source === "update" && event.sum >= initialAggregateSum + 5),
  "aggregate sum update for matching write",
)
await waitFor(() => queryResultCount(indexQueryId) > beforeMatchingIndexCount, "refreshed indexed query result")
await waitFor(() => queryDiffCount(diffQueryId) > beforeMatchingDiffCount, "refreshed diff query result")
liveMetrics = await liveQueryMetrics()
assert(liveMetrics.health.refreshTotal >= 3)
const refreshDelta = liveMetrics.health.refreshTotal - beforeMatchingMetrics.health.refreshTotal
const queryExecutionDelta = liveMetrics.health.queryExecutionsTotal - beforeMatchingMetrics.health.queryExecutionsTotal
assert(
  queryExecutionDelta < refreshDelta,
  `duplicate query shapes should share one execution per event batch; refreshDelta=${refreshDelta} queryExecutionDelta=${queryExecutionDelta}`,
)
assert(liveMetrics.health.eventBatchesTotal >= 1)
assert(liveMetrics.health.batchedEventsTotal >= 1)
assert(liveMetrics.health.refreshCandidatesTotal >= liveMetrics.health.refreshTotal)
assert(liveMetrics.health.diffFramesTotal >= 1)
assert.match(liveMetrics.metrics, /^nextdb_live_query_event_batches_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_batched_events_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_refresh_candidates_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_refresh_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_executions_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_diff_frames_total \d+$/m)
assert.match(liveMetrics.metrics, /^nextdb_live_query_evaluation_cache_hits_total \d+$/m)
await waitFor(
  () => sdkEvents.some((event) => event.diff?.added.some((record) => record.key === key)),
  "SDK diff query result",
)
const latest = frames
  .filter((frame) => frame.type === "queryResult" && frame.queryId === queryId)
  .at(-1)
const latestIndex = frames
  .filter((frame) => frame.type === "queryResult" && frame.queryId === indexQueryId)
  .at(-1)
assert(latest.response.records.some((record) => record.key === key), "query result includes upserted record")
assert(latestIndex.response.records.some((record) => record.key === key), "indexed query result includes upserted record")
assert(
  latestIndex.response.records.every((record) => record.value.title === indexedTitle),
  "indexed query result only includes matching title records",
)
assert.match(latestIndex.resultId, /^sha256:/)
assert.notEqual(latestIndex.resultId, initialIndexResult.resultId)
assert(latest.currentLsn >= upsert.record.lsn)
assert(latestIndex.currentLsn >= upsert.record.lsn)
const latestDiff = frames
  .filter((frame) => frame.type === "queryDiff" && frame.queryId === diffQueryId)
  .at(-1)
assert(latestDiff.diff.added.some((record) => record.key === key), "diff includes added record")
assert(latestDiff.diff.keys.includes(key), "diff ordered keys include added record")
assert.equal(latestDiff.diff.table, "rooms")
const latestSdk = sdkEvents.at(-1)
assert(latestSdk.response.records.some((record) => record.key === key), "SDK reconstructed query result includes added record")
assert(latestSdk.diff.added.some((record) => record.key === key), "SDK exposes query diff")

const rangeSnapshotPrefix = `live-query-range-${Date.now()}`
const rangeSnapshotRecords = [
  { key: `${rangeSnapshotPrefix}-a`, title: `${indexedTitle} range-miss-a`, score: 11 },
  { key: `${rangeSnapshotPrefix}-b`, title: `${indexedTitle} range-miss-b`, score: 12 },
  { key: `${rangeSnapshotPrefix}-c`, title: indexedTitle, score: 13 },
]
for (const record of rangeSnapshotRecords) {
  const rangeResponse = await fetch(`${endpoint}/v1/records/rooms/${encodeURIComponent(record.key)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      value: { id: record.key, title: record.title, score: record.score },
      durability: "strict",
      clientMutationId: `${record.key}-upsert`,
    }),
  })
  const rangeText = await rangeResponse.text()
  assert.equal(rangeResponse.status, 200, rangeText)
}
ws.send(JSON.stringify({
  type: "subscribeTable",
  table: "rooms",
  lowerKey: `${rangeSnapshotPrefix}-a`,
  upperKey: `${rangeSnapshotPrefix}-z`,
  indexName: "byTitle",
  indexValues: JSON.stringify([indexedTitle]),
  snapshotLimit: 1,
}))
await waitFor(
  () => frames.some((frame) => frame.type === "tableSnapshot" &&
    frame.table === "rooms" &&
    frame.lowerKey === `${rangeSnapshotPrefix}-a` &&
    frame.upperKey === `${rangeSnapshotPrefix}-z` &&
    frame.indexName === "byTitle" &&
    frame.indexValues === JSON.stringify([indexedTitle])),
  "table index-prefix range server snapshot",
)
const indexPrefixRangeSnapshot = frames
  .filter((frame) => frame.type === "tableSnapshot" &&
    frame.table === "rooms" &&
    frame.lowerKey === `${rangeSnapshotPrefix}-a` &&
    frame.upperKey === `${rangeSnapshotPrefix}-z` &&
    frame.indexName === "byTitle" &&
    frame.indexValues === JSON.stringify([indexedTitle]))
  .at(-1)
assert.deepEqual(
  indexPrefixRangeSnapshot.response.records.map((record) => record.key),
  [`${rangeSnapshotPrefix}-c`],
  "table index-prefix range snapshot scans past non-matching records before filling the page",
)
assert.equal(indexPrefixRangeSnapshot.response.hasMore, false)

const beforeDeleteDiffCount = queryDiffCount(diffQueryId)
const beforeDeleteAggregateEventCount = aggregateEvents.length
const beforeDeleteAggregateSumEventCount = aggregateSumEvents.length
const deleteResponse = await fetch(`${endpoint}/v1/records/rooms/${encodeURIComponent(key)}?clientMutationId=${encodeURIComponent(`${key}-delete`)}`, {
  method: "DELETE",
})
const deleteText = await deleteResponse.text()
assert.equal(deleteResponse.status, 200, deleteText)
const deleted = JSON.parse(deleteText)
assert.equal(deleted.deleted, true)
await waitFor(
  () => aggregateEvents
    .slice(beforeDeleteAggregateEventCount)
    .some((event) => event.source === "update" && event.count >= initialAggregateCount + 1),
  "aggregate count update for delete",
)
await waitFor(
  () => aggregateSumEvents
    .slice(beforeDeleteAggregateSumEventCount)
    .some((event) => event.source === "update" && event.sum >= initialAggregateSum + 2),
  "aggregate sum update for delete",
)
await waitFor(() => queryDiffCount(diffQueryId) > beforeDeleteDiffCount, "deleted record diff query result")
await waitFor(
  () => sdkEvents.some((event) => event.diff?.removed.some((record) => record.key === key && record.deleted)),
  "SDK deleted diff query result",
)
await waitFor(
  () => sdkCacheChanges.some((event) => event.type === "recordDeleted" && event.table === "rooms" && event.key === key),
  "SDK deleted diff cache change",
)
const deleteDiff = frames
  .filter((frame) => frame.type === "queryDiff" && frame.queryId === diffQueryId)
  .at(-1)
const removed = deleteDiff.diff.removed.find((record) => record.key === key)
assert(removed, "diff includes removed record")
assert.equal(removed.deleted, true)
assert.equal(removed.lsn, deleted.lsn)
assert.equal(removed.deletedAtMs, deleted.deletedAtMs)
assert(!deleteDiff.diff.keys.includes(key), "diff ordered keys no longer include deleted record")
const latestSdkDelete = sdkEvents.at(-1)
assert(!latestSdkDelete.response.records.some((record) => record.key === key), "SDK reconstructed query result removes deleted record")

ws.send(JSON.stringify({ type: "unsubscribeQuery", queryId }))
ws.send(JSON.stringify({ type: "unsubscribeQuery", queryId: indexQueryId }))
ws.send(JSON.stringify({ type: "unsubscribeQuery", queryId: diffQueryId }))
await waitFor(
  () => frames.some((frame) => frame.type === "queryUnsubscribed" && frame.queryId === queryId),
  "query unsubscribe",
)
await waitFor(
  () => frames.some((frame) => frame.type === "queryUnsubscribed" && frame.queryId === indexQueryId),
  "indexed query unsubscribe",
)
await waitFor(
  () => frames.some((frame) => frame.type === "queryUnsubscribed" && frame.queryId === diffQueryId),
  "diff query unsubscribe",
)
await waitForConnectionQueryTables("live-query-smoke", {})
unsubscribeSdkDiff()
unsubscribeAggregate()
unsubscribeAggregateSum()
await waitForConnectionQueryTables("live-query-smoke-sdk", {})
await waitFor(async () => {
  const current = (await liveQueryMetrics()).health.current
  return current === 0
}, "live query current metrics reset")
unsubscribeCacheChanges()
db.close()
ws.close()

console.log("live query smoke ok")

function queryResultCount(queryId) {
  return frames.filter((frame) => frame.type === "queryResult" && frame.queryId === queryId).length
}

function queryDiffCount(queryId) {
  return frames.filter((frame) => frame.type === "queryDiff" && frame.queryId === queryId).length
}

async function waitFor(predicate, label) {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error(`timed out waiting for ${label}; frames=${JSON.stringify(frames)}`)
}

async function waitForStableCount(queryId, expectedCount, durationMs, label) {
  const deadline = Date.now() + durationMs
  while (Date.now() < deadline) {
    assert.equal(queryResultCount(queryId), expectedCount, label)
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

async function waitForStableDiffCount(queryId, expectedCount, durationMs, label) {
  const deadline = Date.now() + durationMs
  while (Date.now() < deadline) {
    assert.equal(queryDiffCount(queryId), expectedCount, label)
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

async function waitForConnectionQueryTables(userId, expectedTables) {
  const deadline = Date.now() + 5_000
  const expectedKeys = Object.keys(expectedTables).sort()
  let latest
  while (Date.now() < deadline) {
    const response = await fetch(`${endpoint}/v1/admin/connections?userId=${encodeURIComponent(userId)}`)
    latest = await response.json()
    const session = latest.sessions.find((session) => queryTablesEqual(session.subscribedQueryTables ?? {}, expectedTables))
    const summary = latest.userSummaries.find((summary) => summary.userId === userId)
    if (
      session !== undefined &&
      summary !== undefined &&
      queryTablesEqual(summary.subscribedQueryTables ?? {}, expectedTables) &&
      Object.keys(summary.subscribedQueryTables ?? {}).sort().join(",") === expectedKeys.join(",")
    ) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error(`timed out waiting for ${userId} query table counts ${JSON.stringify(expectedTables)}; latest=${JSON.stringify(latest)}`)
}

async function liveQueryMetrics() {
  const healthResponse = await fetch(`${endpoint}/v1/health`)
  const health = await healthResponse.json()
  const metrics = await (await fetch(`${endpoint}/v1/metrics`)).text()
  return { health: health.liveQueries, metrics }
}

function queryTablesEqual(actual, expected) {
  const actualKeys = Object.keys(actual).sort()
  const expectedKeys = Object.keys(expected).sort()
  return (
    actualKeys.length === expectedKeys.length &&
    actualKeys.every((key, index) => key === expectedKeys[index] && actual[key] === expected[key])
  )
}

async function assertQueryResumeUnchanged({ endpoint, wsEndpoint, ...frame }) {
  const resumeFrames = []
  const resumeWs = new WebSocket(`${wsEndpoint}/v1/connect?userId=live-query-smoke-resume`)
  resumeWs.addEventListener("message", (event) => resumeFrames.push(JSON.parse(event.data)))
  await waitForLocal(() => resumeFrames.some((message) => message.type === "hello"), "resume hello", resumeFrames)
  resumeWs.send(JSON.stringify({ type: "subscribeQuery", limit: 20, ...frame }))
  await waitForLocal(
    () => resumeFrames.some((message) => message.type === "queryUnchanged" && message.queryId === frame.queryId),
    "query unchanged resume",
    resumeFrames,
  )
  assert(
    !resumeFrames.some((message) => message.type === "queryResult" && message.queryId === frame.queryId),
    "unchanged resume should not send full query result",
  )
  assert.equal(
    resumeFrames.find((message) => message.type === "queryUnchanged" && message.queryId === frame.queryId).resultId,
    frame.resultId,
  )
  resumeWs.close()
}

async function waitForLocal(predicate, label, localFrames) {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error(`timed out waiting for ${label}; frames=${JSON.stringify(localFrames)}`)
}
