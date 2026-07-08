# NextDB

NextDB is an experimental typed actor database runtime. This repository currently contains a single-node MVP:

- Rust server with durable WAL group commit and local batch latency metrics.
- Configurable single-node WAL sharding with global LSN ordering.
- Synchronous local WAL replica mirrors with startup restore from replica.
- Shard ownership topology with owner/replica metadata and optional direct-write gating.
- Room live state for `rooms/{room_id}/messages`.
- Configurable virtual-actor residency with thread-owned sharded hot room state, LRU eviction, shared kernel activation/status, bucket/parent-partition row activation for scope actors, and table actor scope directories with row/byte threshold split metadata, recursive child-scope migration, and optional split maintenance sweeps.
- Runtime snapshots preserve resident room windows plus non-room scope/table actor state; startup overlays later record WAL facts onto already-resident actor states without activating cold tables.
- Generic schema-defined records with WAL-backed disk projections and schema-driven hot live state.
- Scylla/Cassandra-style chat log projection files for cold message reads.
- Built-in object store for large payloads and schema-declared `ObjectRef` fields.
- Message attachments as typed object references.
- Actor snapshot/checkpoint, background WAL checkpoint compaction, and projection rebuild admin APIs.
- Object reference index and object GC dry-run/delete APIs.
- Wasm behavior runtime with reload-time module compilation, manifest loading, and host-command execution.
- Schema registry with default nested-table/ObjectRef schema and TypeScript type generation.
- Logical nested table paths without hard foreign keys.
- WebSocket connection layer for subscriptions, with health-exposed transport capabilities and a WebTransport-ready SDK transport using the same frame contract.
- TypeScript client SDK with memory/IndexedDB local cache, cache leases, and server-driven invalidation.

## Design docs

- [Target design](docs/DESIGN.md): memory-first actor-native database design, activation indexes, behavior Wasm, user-defined tables, RLS, backup, and partitioning.
- [Stable external contracts](docs/STABLE_CONTRACTS.md): SDK, wire protocol, and persistence format stability boundaries.
- [Architecture](docs/ARCHITECTURE.md): current runtime architecture and implementation details.

## Run

```sh
cargo run -p nextdb-server
```

The server listens on `127.0.0.1:3188` by default.

```sh
curl http://127.0.0.1:3188/v1/ready
curl http://127.0.0.1:3188/v1/health
curl http://127.0.0.1:3188/v1/metrics
```

`/v1/ready` is the application-facing readiness check. It reports separate `readReady`, `writeReady`, and `realtimeReady` flags plus per-check details for WAL availability, runtime drain, local shard ownership, schema, and the connection layer. During a prepared restart or drain it stays readable but returns `writeReady=false` and `realtimeReady=false`, so clients and gateways can stop new writes/realtime handshakes without treating the process as dead.

For behavior development, start the same server with a behavior hot-reload
watcher:

```sh
cargo run -p nextdb-server -- dev --watch
```

The watcher polls `data/behaviors` by default and reloads through the same
atomic publish path as `POST /v1/admin/behaviors/reload`: compile and validate
the complete behavior set, append `BehaviorPublished` to WAL, then swap the
active epoch. Set `NEXTDB_BEHAVIOR_WATCH_INTERVAL_MS` to tune the polling
interval.

Behavior Wasm instance reuse is tunable at process start. Set
`NEXTDB_BEHAVIOR_INSTANCE_POOL_MAX=0` to disable resident instance reuse during
diagnostics, or raise it above the default `4` for highly concurrent behavior
loads. `NEXTDB_BEHAVIOR_POOL_TOTAL_CORE_INSTANCES`,
`NEXTDB_BEHAVIOR_POOL_TOTAL_MEMORIES`, and `NEXTDB_BEHAVIOR_POOL_TOTAL_TABLES`
tune the wasmtime pooling allocator totals. Set
`NEXTDB_BEHAVIOR_FUEL_ENABLED=false` to disable fuel instrumentation on the
behavior hot path while keeping epoch-interruption deadlines derived from
`maxFuel`. `/v1/health` reports these values under `behaviorRuntime`, and
`/v1/metrics` exports aggregate and per-behavior invocation, success,
unknown-message, guest-error, command-rejection, instance lifecycle, pool-error,
fuel, and pool gauges/counters.

Run the admin UI:

```sh
npm run dev:admin -- --port 5173
```

Open `http://127.0.0.1:5173`. The admin console connects to `http://127.0.0.1:3188` by default and provides runtime health, WAL audit, schema-derived Data Explorer reads/writes for top-level and nested partition tables, virtual actor residency, object storage, schema, behavior invocation with manifest permission summaries, realtime channel state/member/recent-event/recent-signal visibility plus admin-sent broadcast and signal test events, and operation panels. If `NEXTDB_ADMIN_TOKEN` is set, enter the same token in the admin sidebar.

Run the full local smoke suite:

```sh
npm run test:full
```

This checks Rust formatting/build, TypeScript builds, TS behavior package compilation, SDK cache/subscription/realtime behavior, readiness/drain semantics, object and record predicates, schema-version drift rejection, schema history, schema proposals, schema peer preflight, Admin UI browser rendering, runtime restart/export-import, Wasm behavior paths including `dev --watch` hot reload, cluster handoff/failover flows, and generated TypeScript bindings. By default the suite starts isolated temporary NextDB servers on free local ports, copies compiled behavior artifacts into the main smoke data directory, and deletes those temporary data directories on success. Set `NEXTDB_ENDPOINT` to run the endpoint-backed tests against an existing server, `NEXTDB_BASE_URL` when the realtime-channel smoke should use a different base URL, `NEXTDB_FULL_CODEGEN_ENDPOINT` to force the generated-client smoke endpoint, or `NEXTDB_KEEP_FULL_SMOKE_DATA=true` to keep temporary server data for inspection.

The prototype acceptance matrix in `docs/PROTOTYPE_ACCEPTANCE.md` maps the original design goals to implementation surfaces and smoke evidence. For a faster running-node acceptance check, run `NEXTDB_ENDPOINT=http://127.0.0.1:3188 npm run test:prototype`.

Run a local benchmark harness:

```sh
npm run benchmark:micro
npm run benchmark:local
```

`benchmark:micro` runs Criterion baselines for WAL-like JSON encode/decode,
payload hashing, batch message preparation, and fan-out frame serialization.
The benchmark builds and starts an optimized isolated temporary server, defaults
that server to 4 WAL shards, and reports JSON for strict, relaxed, and volatile
message writes, strict record upserts, key-order record list reads, projection
status, and object puts.
Tune it with `NEXTDB_BENCH_MESSAGES`, `NEXTDB_BENCH_RECORDS`,
`NEXTDB_BENCH_RECORD_LIST_PAGES`, `NEXTDB_BENCH_RECORD_LIST_PAGE_SIZE`,
`NEXTDB_BENCH_OBJECTS`, `NEXTDB_BENCH_CONCURRENCY`, and
`NEXTDB_BENCH_OBJECT_BYTES`. Set `NEXTDB_BENCH_OUT=benchmarks/local.json` to
persist the JSON result, or `NEXTDB_BENCH_KEEP_DATA=true` to inspect the data
directory after the run.

Run a server flamegraph baseline on Linux with `cargo-flamegraph` installed:

```sh
npm run benchmark:flamegraph
```

The flamegraph harness starts a release server under `cargo flamegraph`, runs
the local benchmark against that profiled server, then writes
`target/nextdb-server-flamegraph.svg` and
`target/nextdb-flamegraph-benchmark.json`. Use
`NEXTDB_FLAMEGRAPH_OUT` and `NEXTDB_FLAMEGRAPH_BENCH_OUT` to override those
paths.

Run a local soak harness:

```sh
npm run soak:local
```

The soak harness starts an isolated temporary server and continuously mixes
strict, relaxed, and volatile message writes, strict record upserts, key-order
and secondary-index record reads, object puts, WebSocket subscription delivery
checks, periodic health/readiness sampling, sync wait, projection status, and
WAL accounting. Tune it with
`NEXTDB_SOAK_DURATION_MS`, `NEXTDB_SOAK_CONCURRENCY`,
`NEXTDB_SOAK_HEALTH_INTERVAL_MS`, and `NEXTDB_SOAK_OBJECT_BYTES`. Set
`NEXTDB_SOAK_OUT=benchmarks/soak.json` to persist the JSON result, or
`NEXTDB_SOAK_KEEP_DATA=true` to inspect the data directory after the run.

Create and verify a local release bundle:

```sh
npm run release:verify
```

This builds the optimized Rust server, builds the TypeScript packages and Admin
UI, compiles the example behavior module, creates
`dist/release/nextdb-<version>-<platform>-<arch>.tar.gz`, writes a release
manifest plus SBOM with file SHA-256 values, verifies the archive sidecar,
manifest hashes, archive path safety, and SBOM contents, then starts the
packaged server from the bundle and verifies readiness, health, behavior
loading, record writes, WAL integrity, and Admin static assets.

Run the final prototype completion audit:

```sh
npm run completion:audit
```

The audit checks that the acceptance matrix covers the original goals, key
scripts and smoke files exist, benchmark/soak/release commands are documented,
release manifest/SBOM artifacts are present, and known non-production boundaries
are explicit.

Optional auth gates:

```sh
NEXTDB_ADMIN_TOKEN=admin-secret \
NEXTDB_CLIENT_TOKEN=client-secret \
NEXTDB_CLIENT_USER_TOKENS=alice=alice-secret,bob=bob-secret \
cargo run -p nextdb-server
```

When configured, admin endpoints require `Authorization: Bearer $NEXTDB_ADMIN_TOKEN` or `x-nextdb-admin-token`. Client writes, object upload, realtime operations, behavior invocation, and realtime connection require `Authorization: Bearer $NEXTDB_CLIENT_TOKEN`, `x-nextdb-client-token`, or `authToken` on the connection URL. `NEXTDB_CLIENT_USER_TOKENS` enables identity-bound client tokens for user-scoped operations: `alice=alice-secret` can send messages, durable user events, realtime joins/signals, behavior invocations, and realtime connections only as `alice`. Anonymous realtime connections and behavior invocations without `userId` still require the global client token or admin token. Non-user-scoped record/object writes and room-scoped volatile publishes also require the global client token or admin token. Admin tokens are accepted for client-protected requests. WAL and object replication keep their dedicated replication tokens. `npm run test:auth` verifies the HTTP gates with a real token-protected node; `npm run test:connection-auth` verifies WebSocket and JSONL connection handshakes plus admin-only connection events.

Runtime write limits protect high-risk payload paths before schema validation, object-store writes, or WAL append:

```sh
NEXTDB_MAX_OBJECT_BYTES=67108864 \
NEXTDB_MAX_MESSAGE_BYTES=65536 \
NEXTDB_MAX_USER_EVENT_BYTES=1048576 \
NEXTDB_MAX_RECORD_VALUE_BYTES=1048576 \
cargo run -p nextdb-server
```

Set a limit to `0` to disable that specific guard. Volatile realtime signals and broadcasts are checked before allocating a channel sequence, so rejected frames do not create timeline gaps. `/v1/health.limits` and `/v1/metrics` expose the active values so SDKs, the Admin UI, and Prometheus can confirm the deployed runtime envelope.

Runtime record tables can prewarm durable hot state after startup:

```sh
NEXTDB_RECORD_HOT_PREWARM_LIMIT=1000 \
cargo run -p nextdb-server
```

When enabled, the server scans the disk projection for each schema hot table and hydrates the newest durable rows into record hot state after snapshot recovery. This is useful for `lru` or `actorPartition` tables where a snapshot intentionally captured only part of the working set. Health, metrics, runtime activation, and the Admin UI expose prewarm start/finish timestamps, found rows, activated rows, and per-table results.

WAL group commit can be tuned without rebuilding the server:

```sh
NEXTDB_WAL_BATCH_MAX=1024 \
NEXTDB_WAL_BATCH_WAIT_MS=2 \
cargo run -p nextdb-server
```

`NEXTDB_WAL_BATCH_MAX` caps how many append requests a shard worker coalesces into one local write. `NEXTDB_WAL_BATCH_WAIT_MS` is the maximum coalescing window; set it to `0` for lowest append latency, or raise it when throughput and fewer fsync calls matter more. `/v1/health.walReplicas[].remoteStatus` and `/v1/metrics` expose the active batch settings, local queue depth, batch counts, byte counts, sync counts, and last write/sync timings.

Create a message:

```sh
curl -X POST http://127.0.0.1:3188/v1/mutate \
  -H 'content-type: application/json' \
  -d '{"type":"sendMessage","roomId":"general","userId":"alice","body":"hello","durability":"strict","clientMutationId":"alice-msg-1"}'
```

Create a message with an object attachment:

```sh
curl -X POST http://127.0.0.1:3188/v1/mutate \
  -H 'content-type: application/json' \
  -d '{"type":"sendMessage","roomId":"general","userId":"alice","body":"see attachment","attachments":["OBJECT_ID"],"durability":"strict"}'
```

Query latest messages:

```sh
curl 'http://127.0.0.1:3188/v1/rooms/general/messages/latest?limit=20'
```

Upsert and read a schema-defined table record:

```sh
curl -X POST http://127.0.0.1:3188/v1/records/rooms/general \
  -H 'content-type: application/json' \
  -d '{"value":{"id":"general","title":"General"},"durability":"strict","clientMutationId":"rooms-general-v1"}'

curl http://127.0.0.1:3188/v1/records/rooms/general
curl 'http://127.0.0.1:3188/v1/records/rooms?limit=20'
curl 'http://127.0.0.1:3188/v1/records/rooms/indexes/byTitle?value=General&limit=20'
curl 'http://127.0.0.1:3188/v1/records/rooms/indexes/byTitle?lower=G&upper=Z&limit=20'
curl 'http://127.0.0.1:3188/v1/records/rooms/indexes/byTitle?lower=G&upper=Z&afterCursor=CURSOR&limit=20'
curl -X POST http://127.0.0.1:3188/v1/records/rooms/general/messages/manual-1 \
  -H 'content-type: application/json' \
  -d '{"value":{"id":"manual-1","roomId":"general","senderId":"alice","body":"nested record","createdAtMs":1893456000000,"attachments":[],"path":"tables/rooms/general/messages/manual-1"},"durability":"strict","clientMutationId":"nested-msg-1"}'
curl 'http://127.0.0.1:3188/v1/records/rooms/general/messages?limit=20'
curl 'http://127.0.0.1:3188/v1/records/rooms/general/messages/indexes/bySender?value=alice&limit=20'
curl -X DELETE 'http://127.0.0.1:3188/v1/records/rooms/general?expectedLsn=1'
curl -X POST http://127.0.0.1:3188/v1/records/transaction \
  -H 'content-type: application/json' \
  -d '{"durability":"strict","clientMutationId":"rooms-txn-1","operations":[{"type":"upsert","table":"rooms","key":"general","value":{"id":"general","title":"General"}},{"type":"delete","table":"rooms","key":"old-room"}]}'
curl -X POST http://127.0.0.1:3188/v1/records/transaction \
  -H 'content-type: application/json' \
  -d '{"durability":"strict","clientMutationId":"nested-txn-1","operations":[{"type":"nestedUpsert","table":"rooms","parentKey":"general","nested":"messages","nestedKey":"manual-2","value":{"id":"manual-2","roomId":"general","senderId":"alice","body":"batch nested","createdAtMs":1893456000000,"attachments":[],"path":"tables/rooms/general/messages/manual-2"}},{"type":"nestedDelete","table":"rooms","parentKey":"general","nested":"messages","nestedKey":"manual-1"}]}'
```

Upload an object:

```sh
curl -X POST 'http://127.0.0.1:3188/v1/objects?contentType=text/plain' \
  -H 'content-type: text/plain' \
  --data-binary 'large payload'
```

## TypeScript SDK

```ts
import { NextDbClient } from "@nextdb/client"

const db = new NextDbClient({
  endpoint: "http://127.0.0.1:3188",
  userId: "alice",
  cacheNamespace: "app-main",
  authToken: "client-secret",
  adminToken: "admin-secret",
  offlineWrites: true,
  autoFlushPendingWrites: {
    intervalMs: 5000,
    retryOnStart: true,
  },
})

const room = db.room("general")
await db.upsertUser(undefined, {
  displayName: "Alice",
  metadata: { plan: "team" },
})
const me = await db.getUser()

room.messages.subscribe((event) => {
  console.log(event)
})
room.messages.subscribe((event) => {
  console.log("future-only", event.message.body)
}, { catchUp: false })
const stopCacheWatcher = db.onCacheChange((event) => {
  console.log("local cache changed", event.source, event.type)
})
const stopLatestMessages = room.messages.watchLatest((snapshot) => {
  console.log("latest local messages", snapshot.source, snapshot.messages)
}, { limit: 50 })

await room.messages.send("hello")
await room.messages.send("retry-safe", {
  clientMutationId: "alice-msg-1",
})
await room.messages.send("typing preview", {
  durability: "volatile",
})
await room.messages.activateRuntime({ order: "schema", limit: 50 })
const latest = await room.messages.latest(50)

const object = await db.putObject("large payload", {
  contentType: "text/plain",
  objectId: "alice-object-1",
  clientMutationId: "alice-object-1",
})
const objects = await db.listObjects({ limit: 20 })
await room.messages.send("see attachment", {
  attachments: [object.id],
})
const body = await db.getObjectBody(object.id)
const refs = await db.getObjectReferences(object.id)
await db.deleteObject(object.id, {
  clientMutationId: "alice-object-delete-1",
})

const rooms = db.table("rooms")
rooms.subscribe((event) => {
  console.log("record changed", event.record)
})
await rooms.upsert("general", { id: "general", title: "General" }, {
  clientMutationId: "rooms-general-v1",
})
await rooms.upsert("typing-room", { id: "typing-room", title: "Typing" }, {
  durability: "volatile",
})
const general = await rooms.get<{ id: string; title: string }>("general")
await rooms.upsert("general", { id: "general", title: "Renamed" }, {
  expectedLsn: general.lsn,
})
const roomPage = await rooms.list<{ id: string; title: string }>(20)
const titleMatches = await rooms.index<{ id: string; title: string }>("byTitle", {
  value: "Renamed",
  limit: 20,
})
const titleRange = await rooms.index<{ id: string; title: string }>("byTitle", {
  lower: "G",
  upper: "Z",
  limit: 20,
})
if (titleRange.hasMore) {
  await rooms.index<{ id: string; title: string }>("byTitle", {
    lower: "G",
    upper: "Z",
    afterCursor: titleRange.nextCursor,
    limit: 20,
  })
}
const stopRoomList = rooms.watchList((snapshot) => {
  console.log("local room records", snapshot.source, snapshot.records)
}, { limit: 50 })
const stopRoomDetail = rooms.watch("general", (snapshot) => {
  console.log("local room detail", snapshot.source, snapshot.record?.value.title)
})
const stopRoomLiveQuery = rooms.subscribeQuery<{ id: string; title: string }>((result) => {
  console.log("server live query", result.currentLsn, result.response.records)
}, {
  queryId: "rooms-live",
  limit: 50,
  predicate: { all: [{ field: "title", op: "startsWith", value: "G" }] },
})
const stopRoomsByTitleLiveQuery = rooms.subscribeQuery<{ id: string; title: string }>((result) => {
  console.log("indexed live query", result.response.records)
}, {
  queryId: "rooms-by-title-live",
  indexName: "byTitle",
  value: "General",
  predicate: { all: [{ field: "title", op: "eq", value: "General" }] },
  limit: 20,
})
stopLatestMessages()
stopRoomList()
stopRoomDetail()
stopRoomLiveQuery()
stopRoomsByTitleLiveQuery()
stopCacheWatcher()
const manualMessages = db.nestedTable("rooms", "general", "messages")
await manualMessages.upsert("manual-1", {
  id: "manual-1",
  roomId: "general",
  senderId: "alice",
  body: "nested record",
  createdAtMs: Date.now(),
  attachments: [],
  path: "tables/rooms/general/messages/manual-1",
}, { clientMutationId: "nested-msg-1" })
await manualMessages.list(20)
const stopManualMessagesLiveQuery = manualMessages.subscribeQuery((result) => {
  console.log("nested live query", result.response.records)
}, {
  queryId: "general-messages-live",
  limit: 50,
  order: "schema",
})
const stopManualMessageDetail = manualMessages.watch("manual-1", (snapshot) => {
  console.log("nested detail", snapshot.record?.value.body)
})
const aliceMessages = await manualMessages.index("bySender", {
  value: "alice",
  limit: 20,
})
await manualMessages.transaction([
  {
    type: "upsert",
    key: "manual-2",
    value: {
      id: "manual-2",
      roomId: "general",
      senderId: "alice",
      body: "batch nested",
      createdAtMs: Date.now(),
      attachments: [],
      path: "tables/rooms/general/messages/manual-2",
    },
  },
  { type: "delete", key: "manual-1" },
], { clientMutationId: "nested-txn-1" })
stopManualMessagesLiveQuery()
stopManualMessageDetail()

Nested table lists are partition reads. The durable logical key is `{parentKey}:{nestedKey}`, while the record store keeps a parent-partition projection under `data/records/_partitions` and a bounded `.manifest` for nested-key pages, so a hot parent partition can answer covered pages without scanning every child file. The SDK local cache uses the same key-prefix range for cached nested reads.
Use `manualMessages.listBySchemaOrder(20)` or `?order=schema` to apply the nested table's declared storage order. The default `rooms.messages` schema orders by `desc(createdAtMs), id`, while plain `manualMessages.list(20)` remains nested-key order. Schema-ordered reads use a persistent clustering projection under `data/records/_orders`; each order directory keeps a bounded `.manifest` of ordered cursors plus bounded record filenames so covered cursor pages do not scan or sort the whole parent partition.
`rooms.messages.storage.liveWindow` also makes the chat log a bounded record-hot table: recent durable message rows stay resident in process memory, while point reads and pages outside the live window lazily fall back to WAL-derived disk projections and rehydrate the window.
For schema-ordered pagination, prefer the opaque cursor:

```ts
const first = await manualMessages.listBySchemaOrder({ limit: 20 })
const next = await manualMessages.listBySchemaOrder({
  limit: 20,
  afterCursor: first.nextCursor,
})
```

The SDK uses the same schema order to read cached nested records. If enough records are already present in the local cache, `listBySchemaOrder` can return a page without hitting the records API.
IndexedDB-backed clients persist this local ordered projection in `recordOrderMetadata` and `recordOrders` stores.
Top-level and nested index reads are also cache-first when the local projection is purely durable. The SDK reads index field definitions from schema, filters cached records by exact-match scalar values or inclusive range bounds, and only calls the records API when the local cache cannot fill the requested page. If a table has a process-local volatile record overlay, list and index reads bypass local page hits for that table and ask the server for the current hot-state result, because volatile rows are intentionally absent from the durable SDK cache. Range cache hits use the same `nextCursor` format as the server, so a caller can continue with the same pagination contract even if a later page falls back to the server.

const deleteResult = await rooms.delete("general", {
  expectedLsn: titleMatches.records[0]?.lsn,
})
const txn = await db.recordTransaction([
  { type: "upsert", table: "rooms", key: "general", value: { id: "general", title: "General" } },
  { type: "delete", table: "rooms", key: "old-room" },
], { clientMutationId: "rooms-txn-1" })
await rooms.cache.clear()

const stats = await db.cacheStats()
const coverage = await db.cacheCoverage()
const status = await db.localDataStatus()
const stopLocalStatusWatcher = db.watchLocalDataStatus((snapshot) => {
  console.log("local data", snapshot.source, snapshot.status.cache.totalRecords, snapshot.pendingQueue.stats.total)
})
const cacheLease = await db.refreshCacheLease()
const enforced = await db.enforceLocalCacheProfile()
const cachedUsers = await db.listCachedUsers({ limit: 20 })
const cachedObjects = await db.objectStore("Object").listCached({ limit: 20 })
const cachedObject = await db.objectStore("Object").getCachedMetadata("object-1")
const cachedBody = await db.objectStore("Object").getCachedBody("object-1")
const cachedMessages = await room.messages.cached({ limit: 50 })
const cachedUser = await db.getCachedUser("alice")
const cachedRooms = await rooms.cache.list({ limit: 20 })
const cachedRoom = await rooms.cache.get("general")
const cachedMessage = await db.nestedTable("rooms", "general", "messages").cache.get("msg-1")
await db.nestedTable("rooms", "general", "messages").cache.clear()
await room.cache.trim(1_000)
await room.cache.clear()
await db.clearUserCache("alice")
await db.clearObjectCache()
await db.clearCache()

const audit = await db.auditWal({
  payloadType: "messageCreated",
  roomId: "general",
  afterLsn: 0,
  limit: 100,
})
const delta = await db.syncPull({
  rooms: ["general"],
  tables: ["rooms"],
  afterLsn: audit.nextAfterLsn,
})
const caughtUp = await db.syncUntilCaughtUp({
  rooms: ["general"],
  tables: ["rooms"],
  nestedTables: [{ table: "rooms", parentKey: "general", nested: "messages" }],
  limit: 500,
})
db.subscribeObjects((event) => {
  console.log("object changed", event.type)
})
const stopObjectList = db.objectStore("Object").watchList((snapshot) => {
  console.log("objects", snapshot.source, snapshot.objects.map((object) => object.id))
}, { limit: 20 })
const stopObjectDetail = db.objectStore("Object").watch("object-1", (snapshot) => {
  console.log("object detail", snapshot.metadata?.contentType, snapshot.cachedBodyAvailable)
})
await db.syncObjects({ limit: 500 })
await room.messages.sync()
await rooms.sync()
const pending = await db.pendingWriteStats()
const queue = await db.pendingWriteQueueStatus()
const stopPendingWatcher = db.watchPendingWrites((snapshot) => {
  console.log("pending queue", snapshot.source, snapshot.queue.stats.total)
})
const flushed = await db.flushPendingWrites()
if (queue.writes[0]?.lastError) {
  await db.resetPendingWrite(queue.writes[0].id)
}
if (queue.writes[0]) {
  await db.discardPendingWrite(queue.writes[0].id, { removeOptimistic: true })
}
db.startPendingWriteAutoFlush({ intervalMs: 5000 })
db.stopPendingWriteAutoFlush()
stopPendingWatcher()
stopLocalStatusWatcher()
stopObjectList()
stopObjectDetail()

const topology = await db.clusterTopology()
const route = await db.clusterRoute({ roomId: "general" })
console.log(topology.nodeId, route.owner, route.localAcceptsWrites)

await db.createSnapshot()
const manifest = await db.exportManifest({ includeSamples: true, sampleLimit: 5 })
await db.compactWal()
await db.rebuildProjections()
await db.gcObjects({ dryRun: true })
await db.gcObjects({ dryRun: true, force: true })
await db.gcObjects({ dryRun: false, graceMs: 86_400_000 })

const behaviors = await db.listBehaviors()
await db.invokeBehavior({
  behavior: "echo",
  mutation: "echo.send",
  clientMutationId: "echo-send-1",
  input: { roomId: "general", body: "from wasm" },
  read: {
    records: [{ table: "rooms", key: "general" }],
    nestedRecords: [{ table: "rooms", parentKey: "general", nested: "messages", nestedKey: "manual-2" }],
    latestMessages: [{ roomId: "general", limit: 10 }],
    connectionSessions: [{ userId: "alice", transport: "webSocket" }],
  },
})

db.onUserEvent((event) => {
  if (event.type === "userEvent") {
    console.log("durable user event", event.event.name, event.event.payload)
  } else if (event.type === "userUpserted") {
    console.log("durable user profile", event.user.displayName)
  } else {
    console.log("volatile user event", event.name, event.payload)
  }
})
await db.publishUserEvent("alice", "notification.created", { text: "durable inbox item" })
const inbox = await db.listCurrentUserEvents({ limit: 50 })
db.watchCurrentUserEvents((snapshot) => {
  console.log("cached inbox size", snapshot.events.length)
})
await db.publishUserVolatile("alice", "presence.ping", { at: Date.now() })

const channel = db.realtimeChannel("call-general")
await channel.join({ media: ["audio", "video"], role: "host" })
channel.onMemberUpdated((event) => {
  console.log("member presence", event.sequence, event.member.userId, event.member.metadata)
})
channel.watchMembers((snapshot) => {
  console.log("cached members", snapshot.snapshot?.members.length ?? 0)
})
channel.onSignal((signal) => {
  console.log("rtc/game signal", signal.sequence, signal.kind, signal.payload)
})
channel.onSignalKind("renegotiate", (signal) => {
  console.log("custom signal", signal.sequence, signal.payload)
})
channel.watchRecentSignals((snapshot) => {
  console.log("recent volatile signals", snapshot.signals.length)
}, { limit: 20 })
channel.onOffer((signal) => {
  console.log("rtc offer", signal.sequence, signal.payload)
})
channel.onGameInput((event) => {
  console.log("game input", event.sequence, event.payload)
})
channel.onEventKind("lobbyReady", (event) => {
  console.log("custom channel event", event.sequence, event.payload)
})
channel.watchRecentEvents((snapshot) => {
  console.log("recent volatile events", snapshot.events.length)
}, { limit: 20 })
channel.onVoice((event) => {
  console.log("voice control/frame", event.sequence, event.payload)
})
channel.onVideo((event) => {
  console.log("video control/frame", event.sequence, event.payload)
})
channel.onState((event) => {
  console.log("current channel state", event.state.version, event.state.state)
})
channel.watchState((snapshot) => {
  console.log("sdk channel state projection", snapshot.snapshot?.version)
})
await channel.updatePresence({ media: ["audio"], muted: true, ready: true })
await channel.sendOffer("bob", { sdp: "..." })
const snapshot = await channel.state()
console.log(channel.cachedState()?.version)
await channel.updateState({ phase: "lobby", tick: 1 }, { expectedVersion: snapshot.state.version })
await channel.sendGameInput({ buttons: ["jump"], frame: 42 }, { includeSelf: false })
await channel.leave()

const schema = await db.getSchema()
const schemaHistory = await db.schemaHistory()
const oldSchema = await db.getSchemaVersion(1)
const report = await db.validateSchema()
await db.reloadSchema()
const ts = await db.generateTypescriptSchema()
```

Event payloads can be schema-declared under `events`. Declared durable user events, user-targeted volatile events, realtime channel signals, realtime broadcasts, and behavior-published volatile room events are validated before delivery or WAL append. Undeclared event names remain JSON passthrough for incremental rollout.

Room and table events are delivered by subscription. User-scoped durable profile and inbox events are written to WAL and can be recovered by `syncCurrentUserEvents()` or realtime catch-up. `sendMessage` can use `durability: "volatile"` for lossy chat-state messages: the server validates the message, applies it to the resident room actor, and broadcasts it to current subscribers with `lsn: 0`, but it does not append WAL, update the chat-log projection, retain object references, expose it through audit/sync, or persist it in the SDK durable cache. Record upserts/deletes can also use `durability: "volatile"` when the target table is `actorPartition`, `resident`, or `lru`; those rows live only in record hot state, publish current table subscriptions, and skip WAL, disk projections, indexes, durable sync, and SDK durable cache. Hot table point reads, key-order lists, predicate reads, and secondary-index exact/range reads merge that record hot state over the disk projection, so a volatile row is the current server result even when an older durable row for the same key still exists in the persistent index. The SDK also treats a volatile record as a runtime overlay so cached or indexed durable rows for the same key cannot hide it; a later authoritative durable write or point read clears that overlay and resumes durable caching. Room-scoped volatile events target the currently subscribed room sessions, return a `delivered` session count, and never enter WAL or durable SDK cache. Generic user-scoped volatile events target a logical `userId`, not a transport connection. Realtime channel volatile events are narrower: when a channel member joined with a `sessionId`, member, state, signal, and broadcast events are delivered only to that joined session; a member without a `sessionId` intentionally represents every active session for that logical user. The SDK exposes a `NextDbRealtimeTransport` boundary; the default transport is WebSocket, and the SDK also ships `WebTransportRealtimeTransport` / `webTransportRealtimeTransport()` for HTTP/3 deployments. WebSocket carries one JSON `ClientFrame` / `ServerFrame` per message, while stream transports use the same frame contract as newline-delimited JSON through shared SDK encode/decode helpers. The Rust runtime now has transport-neutral `ClientFrame` source and `ServerFrame` sink boundaries around the WebSocket listener, plus an in-process HTTP JSONL gateway at `POST /v1/connect/jsonl` for custom stream transports and external connection gateways. The SDK exposes `JsonLineHttpRealtimeTransport` / `jsonLineHttpRealtimeTransport()` and a built-in `realtimeTransportKind: "jsonl"` option for runtimes or gateways that support bidirectional request/response streaming over HTTP. `health().connectionLayer` advertises the node's actual built-in realtime capability; the current Rust listener reports `supportedTransports: ["webSocket", "custom"]`, where `custom.connectPath` points at the JSONL gateway, while WebTransport remains an SDK/gateway extension point until a native HTTP/3 listener is attached. `db.realtimeTransportCompatibility()` and `realtimeTransportCompatibility(health, kind)` make that preflight explicit: they report whether the requested transport is supported, the node's default transport, and a fallback candidate without silently changing the client's configured transport. `await db.connectCompatibleRealtime()` is the opt-in path that performs the same preflight, applies a WebSocket fallback by default when the node does not advertise the configured transport, can instead apply JSONL fallback with `{ fallbackTo: "jsonl" }` when the node advertises `custom`, starts the realtime connection, and returns whether a fallback was applied.

```ts
const db = new NextDbClient({
  endpoint: "https://db.example.com",
  userId: "alice",
  realtimeTransportKind: "webtransport",
})
```

The connection layer tracks active sessions as runtime state:

```sh
curl http://127.0.0.1:3188/v1/admin/connections
curl 'http://127.0.0.1:3188/v1/admin/connections?userId=alice'
curl 'http://127.0.0.1:3188/v1/admin/connections?userId=alice&transport=webSocket'
curl -X POST http://127.0.0.1:3188/v1/admin/connections/disconnect \
  -H 'content-type: application/json' \
  -d '{"userId":"alice","sessionId":"alice-phone","reason":"admin requested disconnect"}'
```

Each session records `userId`, `sessionId`, transport, a JSON `metadata` document, last-seen time, subscribed rooms, subscribed tables, subscribed nested parent partitions, live query ids, live query counts by logical table, whether the user inbox feed is subscribed, and whether the object feed is subscribed. `NEXTDB_MAX_LIVE_QUERIES_PER_CONNECTION`, `NEXTDB_MAX_LIVE_QUERIES_PER_TABLE_PER_CONNECTION`, and `NEXTDB_MAX_LIVE_QUERIES_PER_USER` can reject new live-query subscriptions before query evaluation when a session or logical user exceeds its fanout budget; `0` keeps that budget unlimited. Clients can pass initial metadata with `connectionMetadata` in `NextDbClientOptions` and replace it later with `db.updateConnectionMetadata({ device: "desktop", capabilities: ["audio", "video", "game"], transportPreference: "webtransport" })`; the SDK restores that connection-state intent across reconnects. This metadata is runtime connection state, not a WAL fact: use it for device, codec, transport, latency, tab, game, or media capabilities, while durable user profile data stays in user records/events. The response also includes `transports.webSocket`, `transports.webTransport`, and `transports.custom` counts for the filtered session set, plus `userSummaries` grouped by logical `userId`. A user summary contains session ids, per-transport counts, deduplicated subscription coverage, live query table counts, inbox/object-feed session counts, and last-seen time, so control-plane code can treat users as logical recipients while still observing which physical protocols and sessions are carrying them. Admin WebSocket clients can send `subscribeConnectionEvents` to receive `connectionEvent` frames for `connected`, `disconnected`, `subscriptionsUpdated`, `metadataUpdated`, and `disconnectRequested`; the TypeScript SDK exposes this as `db.onConnectionEvent(listener)`. `db.listConnections()` seeds an SDK-owned runtime projection, `db.cachedConnections()` reads it without a network hop, and `db.watchConnections(listener)` keeps it current from connection events; Admin UI uses this projection for the Connection Layer panel. Admin disconnect can target a logical user, a single session, or both; matching sessions receive `connectionClosing` and then leave through the normal unregister/channel-cleanup path. Realtime channel joins that include a `sessionId` require that session to be currently connected for the same `userId`, preventing stale or forged channel members that cannot receive events. Room-targeted volatile publish responses include `delivered`, the number of sessions currently subscribed to that room; generic user-targeted volatile publish responses count currently connected sessions for that user; realtime channel responses count only the active channel-member sessions actually targeted. Durable user events return their committed event with LSN and path, and are visible through WAL audit:

```sh
curl -X POST http://127.0.0.1:3188/v1/users/alice \
  -H 'content-type: application/json' \
  -d '{"displayName":"Alice","metadata":{"plan":"team"}}'
curl http://127.0.0.1:3188/v1/users/alice
curl http://127.0.0.1:3188/v1/admin/users
curl -X POST http://127.0.0.1:3188/v1/mutate \
  -H 'content-type: application/json' \
  -d '{"type":"publishUserEvent","userId":"alice","name":"notification.created","payload":{"text":"hello"},"durability":"strict"}'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=userEventPublished&userId=alice'
curl 'http://127.0.0.1:3188/v1/sync/pull?users=alice&afterLsn=0'
curl 'http://127.0.0.1:3188/v1/sync/pull?objects=true&afterLsn=0'
```

The SDK owns local object, message, user profile, user inbox, and record caching through a `NextDbLocalCache` interface. The default cache is IndexedDB in browsers and memory elsewhere. Applications can inspect cache size, list cached objects, room messages, user profiles, user events, table records, and nested records without touching the network, refresh the server cache lease, explicitly enforce the active cache profile with `enforceLocalCacheProfile()`, trim a room to the newest N messages, clear object metadata/bodies, clear one room, clear one user, clear one table, clear one nested parent partition, or clear the whole local cache without touching browser storage APIs directly.

Applications can also observe cache projection directly through `db.onCacheChange(listener)`. The SDK emits `messageUpserted`, `userProfileUpserted`, `userEventUpserted`, `recordUpserted`, `recordDeleted`, `objectUpserted`, `objectDeleted`, pending-write queue events, explicit cache-profile enforcement events, and cache invalidation events with a `source` of `mutation`, `realtime`, `sync`, `offline`, `cacheInvalidation`, or `manual`. Pending-write events cover queued, rejected, reset, discarded, cleared, committed, and failed writes; every pending-write event carries fresh queue stats, and rejected events include the same structured limit details exposed by `NextDbPendingWriteLimitError`. `enforceLocalCacheProfile()` returns before/after cache stats plus per-scope removed counts for object metadata/bodies/ranges, room messages, user events, top-level records, nested records, and nested partitions. Durable object metadata can be read with `listObjects`, recovered with `syncObjects`, and watched with `subscribeObjects`; durable user profiles and events are cached per logical user and can be read with `getUser` plus `listCurrentUserEvents` / `listUserEvents`; volatile room and user events remain lossy connection-layer signals and are not cached. This makes the SDK-owned local cache usable as the app state boundary: WebSocket frames, reconnect catch-up, durable sync pulls, direct writes, offline optimistic writes, pending retry lifecycle changes, local profile enforcement, and server-driven invalidations all flow through the same local change stream.

Subscription intent can be SDK-owned too. Pass `persistent: true` to `subscribeRoom`, `subscribeTable`, `nestedTable(...).subscribe`, `subscribeQuery`, `subscribeObjects`, `onUserEvent`, or the watcher helpers to store the subscription registry in the same local cache. `subscribeTable` accepts `keyRange: { lowerKey, upperKey }` for ordered-key ranges and `indexPrefix: { indexName, values }` for declared secondary-index equality prefixes; `indexPrefix.fields` is optional SDK-only metadata for local upsert filtering, while the server validates `indexName` and `values` against the active schema. Index-prefix subscriptions participate in realtime fan-out, WAL catch-up for matching upserts, and `serverSnapshot`; full equality prefixes use the declared secondary-index query path, while shorter prefixes use an equivalent equality predicate over the indexed fields. When `keyRange` and `indexPrefix` are combined, the server snapshot scans past in-range non-matches until it fills the matching page or reaches the range upper bound. The global fan-out registry also compiles schema-aware index-prefix candidates, so non-matching same-table writes are excluded before connection-local filtering. Delete delivery is exact on fresh realtime events that carry a previous record, while WAL catch-up skips index-prefix deletes that have no before-image. A fresh SDK instance can call `await db.restoreSubscriptions()` during app startup, or set `autoRestoreSubscriptions: true` to let the SDK restore immediately after construction. Restored nested-table subscriptions keep the `{table, parentKey, nested}` partition identity and reconnect with `subscribeNestedTable`, so chat-sized partitions do not become anonymous whole-table intent in the client registry or connection layer. Restored subscriptions reconnect with the cached LSN cursors and refresh the local cache even before UI listeners are attached. Use `listStoredSubscriptions()` for inspection and `clearStoredSubscriptions()` to drop the registry without clearing cached records or pending writes. Clearing stored subscriptions also cancels restored room, table, nested-table, live-query, user-event, and object feeds that have no runtime listener left; if a restore is still waiting for the realtime transport to open, the SDK removes those pending subscribe frames so the cleared intent cannot reappear on connect.

Removing the last runtime object or user-event listener sends `unsubscribeObjects` or `unsubscribeUserEvents` unless a persistent subscription still owns that feed, so `/v1/admin/connections` reflects the active SDK data subscriptions instead of stale listener history.

Use `await db.localDataStatus()` when the application needs one diagnostic view of the SDK-owned data layer. It returns the active endpoint, configured realtime transport kind, active realtime transport kind, configured and active connection transports, realtime transport state, last seen LSNs including nested parent-partition cursors, cache statistics, pending write counts, stored subscription registry, active runtime subscriptions, persistent subscriptions, realtime channel state/member/recent-event summaries, cache metadata, and the last cache profile seen by the SDK. `db.cacheCoverage()` and `localDataStatus().coverage` summarize durable cache ownership plus volatile realtime channel runtime projections, so diagnostics can see cached records and joined channel state from one report. `db.watchLocalDataStatus(listener, { limit })` emits the same status plus a bounded `PendingWriteQueueStatus` whenever the SDK local change stream changes, so diagnostics panels can follow cache projection, invalidation, subscription recovery, profile enforcement, transport fallback, runtime channel projections, and pending-write lifecycle without wiring `onCacheChange()` manually. Object coverage separates metadata bytes from actual cached body/range bytes so media-heavy clients can see whether local storage is holding full blobs, partial ranges, or metadata only. The Admin UI has an "Admin Local Data" panel that shows this same status for the Admin page's own SDK instance, separate from the server-wide cache-control panel. The panel subscribes to `watchLocalDataStatus()` and refreshes automatically after cache projection, invalidation, local profile enforcement, or pending-write queue changes. It can also inspect, flush, reset, discard, and clear the Admin page's pending-write queue, and can call `enforceLocalCacheProfile()`, `restoreSubscriptions()`, `clearStoredSubscriptions()`, and `clearCache()` for the Admin page's local data layer.

For UI state, use snapshot watchers instead of rebuilding list state manually. `room.messages.watchLatest(listener, { limit })`, `db.watchCurrentUserEvents(listener, { limit })`, `table.watchList(listener, { limit })`, `table.watch(key, listener)`, `nestedTable.watchList(listener, { limit })`, `nestedTable.watch(key, listener)`, `db.objectStore("Object").watchList(listener, { limit })`, and `db.objectStore("Object").watch(objectId, listener)` subscribe to the durable stream, keep the SDK cache projected, and emit the current cached view after each relevant mutation, realtime event, sync catch-up, offline write, or invalidation. Single-record snapshots return the cached row or `undefined` after delete/invalidation; single-object snapshots report metadata and whether the full body is cached locally. Pass object `{ includeBody: true }` only when the UI really needs the cached Blob. `db.watchPendingWrites(listener, { limit })` emits `PendingWriteQueueStatus` snapshots after pending-write queue changes, including rejected writes that never enter the queue, so offline banners and retry panels can follow the SDK-owned queue without polling. `db.watchLocalDataStatus(listener, { limit })` is the broader diagnostics watcher for admin panels and developer tooling.

For server-owned live query semantics, use `db.subscribeQuery(...)`, `table.subscribeQuery(...)`, or `nestedTable.subscribeQuery(...)`. A live query subscription sends a full `queryResult` page immediately and then sends a refreshed page or an incremental `queryDiff` when a matching record upsert/delete changes the query result. Each result carries a `resultId` fingerprint over the page shape and record content, and the server suppresses refresh frames when the subscribed table changed but the query page did not. Before re-reading the projection, the server indexes subscribed query ids by logical table and uses a subscription-time impact filter to skip upserts that cannot affect predicate or exact secondary-index query pages when the key is not already present in the current page; schema-version changes fall back to conservative refresh. On each connection wake-up, the server drains a bounded micro-batch of queued delivery events, still emits ordinary subscribed events in order, deduplicates affected live query ids, and reuses one projection result for duplicate query shapes in that batch while still sending each query id its own result or diff. Initial/resume subscriptions and record-event batches can also reuse a short-lived node-level query evaluation cache keyed by `currentLsn + scoped volatile generation + query shape`, so separate connections watching the same durable or volatile page do not all hit the projection at once. Record hot state maintains per-table and nested key-prefix volatile counters/generations, so volatile updates invalidate only the affected table or parent partition cache scope, even for large resident or actor-partition tables. `NEXTDB_REALTIME_EVENT_BATCH_MAX` controls that per-wake drain limit and defaults to `128`; lower it for tighter per-event latency, raise it when write bursts should collapse more live-query refresh work. `/v1/health.liveQueries`, `/v1/metrics`, and the Admin Runtime panel expose active live queries, the active event batch max, subscribe/unsubscribe totals, event batches, batched events, refresh candidates before dedupe, actual refresh attempts, actual query executions, evaluation cache hits, full-result frames, diff frames, unchanged suppressions, and refresh errors so operators can spot fanout amplification or ineffective filters while tuning hot tables and indexes. The TypeScript SDK requests diffs by default, merges them into a complete listener `response`, and exposes `event.diff` with added, updated, and removed records. Removed records distinguish real deletes with `deleted=true`, `lsn`, and `deletedAtMs`, so the SDK can clear stale local cache without deleting records that merely moved out of the query window. The SDK only sends a resume `resultId` when it also has an in-memory baseline for that query; restored subscriptions or manual stale `resultId` values without a baseline force a fresh full result before later diffs are accepted. Supported query shapes include list-style records with `limit`, optional pagination cursor, `order: "schema"` for nested tables that declare a clustering order, schema-declared index exact/range reads, and deterministic JSON predicates. Predicates use `all` terms with `field`, `op`, and optional `value`; supported operators are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `contains`, `startsWith`, and `exists`. This complements cache-backed watchers: watchers are client projection helpers, while live queries are server-evaluated subscription results over the same record projection.

For low-cost aggregate state, use `db.subscribeAggregateCount("tableName", listener)`, `db.subscribeAggregateSum("tableName", "numericField", listener)`, or `db.subscribeAggregatePresence("channelId", listener)`. The server hydrates table aggregates from the record projection, sends an initial `snapshot`, then maintains them from `RecordUpserted` and `RecordDeleted` delivery events without re-running a list query for every subscriber. Duplicate count upserts for the same key do not change the count; sum subscriptions replace that key's previous numeric contribution and ignore missing or non-numeric field values. Channel presence aggregates hydrate from realtime channel members and publish `memberCount` plus logical `userCount` when members join, leave, disconnect, or are removed by stale-session maintenance. Presence aggregates are runtime connection state rather than WAL facts. Active aggregate count, sum, and presence subscriptions are replayed by the SDK after reconnect.

The pre-broadcast fan-out registry shares targeted event batches with `Arc<DeliveryEvent>`, so one routed record/object/realtime event selected for many sessions does not clone the full payload for every candidate before connection-local filtering. Table-range candidates are compacted into key buckets with a fallback bucket for full-width ranges, and nested-table prefix candidates are compacted by stable prefix hash bucket before probing the changed key's actual prefixes. For sessions whose candidate event list is identical, the registry now also attaches one shared pre-encoded event frame. Each connection reuses that frame when the whole routed batch is visible, otherwise it falls back to connection-local RLS/query filtering and borrowed-event encoding. The connection sink batches the server frames produced by one drained realtime event batch and flushes WebSocket/JSONL output once in order.

When a client already has a query result, it can pass `resultId` in `subscribeQuery`. If the server's current fingerprint still matches, it returns `queryUnchanged` instead of resending the full page; the SDK stores the latest `resultId` and reuses it on reconnect.

HTTP/SDK list and index reads accept the same `predicate` option. When a predicate is present, the SDK asks the server for the authoritative filtered page, then writes returned records into the local cache. Generated TypeScript clients bind `predicate.field` to the table or nested-table fields and bind `predicate.value` to that field's scalar type or list element type. Live queries can also target schema-declared secondary indexes by passing `indexName` plus the same exact-match or range options used by HTTP/SDK index reads:

```ts
const stop = db.table("rooms").subscribeQuery((result) => {
  console.log(result.response.records)
}, {
  indexName: "byTitle",
  value: "General",
  predicate: { all: [{ field: "title", op: "eq", value: "General" }] },
  limit: 20,
})
```

When `offlineWrites` is enabled, the SDK queues network-failed `sendMessage`, durable user profile upsert, durable user event publish, top-level record upsert/delete, nested record upsert/delete, record transaction, object upload, and object delete calls in the same pending-write store. Object uploads use a client-preallocated `objectId` so offline messages can safely attach the object before `flushPendingWrites()` commits it. Nested pending writes keep their parent partition fields, so `flushPendingWrites()` replays them through the nested records API instead of bypassing schema and partition validation. Record transactions stay queued as one atomic pending write and are projected into the local cache only after the server commits the transaction result. Durable user event publishes return an uncommitted `lsn: 0` event to the caller but do not insert an optimistic inbox row, because the server owns the final event id. `autoFlushPendingWrites` lets the SDK own the retry lifecycle too: it retries on startup, after a pending write is queued, and when the realtime transport opens. `flushPendingWrites()` serializes concurrent flushes and returns per-write errors with `retryable` so an app can surface conflicts separately from temporary network/topology failures. `pendingWriteStats()` and `pendingWriteQueueStatus()` report type counts, estimated queued bytes, object-upload body bytes, failed item count, accumulated attempts, oldest/newest pending timestamps, active pending-write profile limits, and whether the queue is over those limits. If a new offline write would cross `maxPendingWrites` or `maxPendingWriteBytes`, the SDK throws `NextDbPendingWriteLimitError` with structured write-count and byte fields for UI handling. `resetPendingWrite(id)` clears attempts and `lastError` for one write, and `discardPendingWrite(id, { removeOptimistic: true })` drops one queued write while removing SDK-created optimistic placeholders when that is still possible.

Cache clearing has explicit scope. `clearCache()` removes cached objects, messages, durable user profiles/events, records, cursors, pending writes, stored subscriptions, and cache lease metadata; it also clears in-memory persistent subscription intent and cancels restored feeds that no runtime listener still owns. `clearPendingWrites()` only drops the offline queue and leaves cache lease metadata and stored subscriptions intact. When the SDK creates its default browser cache, it derives the IndexedDB database name from `endpoint`, `userId`, and optional `cacheNamespace`; a browser refresh with the same scope rehydrates messages, durable user profiles/events, objects, records, cursors, pending writes, stored subscriptions, exact-match record queries, and schema-ordered nested projections without a server round trip when a full page is locally available, while different users or endpoints do not share the same local database. Pass a custom `cache` when you need full control over the storage backend or database name. The SDK cache smoke test covers both memory and IndexedDB implementations:

```sh
npm run test:cache
npm run test:cache-profile
npm run test:cache-control
npm run test:live-query
```

The server owns cache policy and invalidation. `GET /v1/cache/profile` returns a cache lease, profile version, schema version, local capacity policy, and invalidations after the client's last applied generation. The SDK automatically checks this lease before serving cached object/list, room/user/table pages, and before durable sync/reconnect recovery. Online realtime clients also receive `cacheInvalidated` frames as soon as an admin invalidation is committed, apply the same local clear/reset logic immediately, and persist the applied generation so the next lease check does not repeat the work. `maxObjects`, `maxObjectBytes`, `maxRoomMessages`, `maxUserEvents`, `maxRecordsPerTable`, and `maxNestedPartitions` are enforced locally after cache writes, so the server profile controls client cache growth. `maxPendingWrites` and `maxPendingWriteBytes` guard offline write admission: the SDK rejects new queued writes that would cross those limits and reports the active limits plus over-limit state in pending-write stats, but it does not silently discard already queued user operations. `maxObjectBytes` is measured against actual cached object body bytes, including partial range chunks, while `totalObjectBytes` remains the logical size of cached object metadata. Top-level records are trimmed per table. Nested logical tables are trimmed per parent key prefix to `maxRecordsPerTable`, then optionally trimmed across parent prefixes to the hottest `maxNestedPartitions`, so a client can keep bounded message windows for only its recent hot rooms instead of retaining every room it has ever opened. Admins can force clients to clear all cache, one object, one room, one user inbox, or one table:

```sh
curl 'http://127.0.0.1:3188/v1/cache/profile?clientId=web-1&afterInvalidationGeneration=0&cursorLsn=0'
curl -X POST http://127.0.0.1:3188/v1/admin/cache/profile \
  -H 'content-type: application/json' \
  -d '{"expectedVersion":1,"maxNestedPartitions":50,"reason":"mobile cache budget"}'
curl -X POST http://127.0.0.1:3188/v1/admin/cache/invalidate \
  -H 'content-type: application/json' \
  -d '{"scope":"all","reason":"schema repair"}'
curl -X POST http://127.0.0.1:3188/v1/admin/cache/invalidate \
  -H 'content-type: application/json' \
  -d '{"scope":"object","key":"OBJECT_ID","reason":"object replaced"}'
curl -X POST http://127.0.0.1:3188/v1/admin/cache/invalidate \
  -H 'content-type: application/json' \
  -d '{"scope":"user","key":"alice","minValidLsn":0,"reason":"user inbox repair"}'
```

`db.updateClientCacheProfile(...)` exposes the same profile patch API in the TypeScript SDK. The server bumps the profile version, persists `data/cache/control.json`, and appends a `profile` cache invalidation so connected clients refresh policy immediately without clearing cached rows while offline clients converge on their next lease refresh.

Durable sync can be pulled by LSN cursor. An unfiltered pull returns global database events, including object commit/delete metadata events. Room, user, table, and nested-table filters narrow the stream to that scope:

```sh
curl 'http://127.0.0.1:3188/v1/sync/wait?minLsn=42&timeoutMs=5000'
curl 'http://127.0.0.1:3188/v1/sync/pull?afterLsn=10&rooms=general&limit=100'
curl 'http://127.0.0.1:3188/v1/sync/pull?afterLsn=10&tables=rooms&limit=100'
curl 'http://127.0.0.1:3188/v1/sync/pull?afterLsn=10&nestedTables=rooms:general:messages&limit=100'
```

`/v1/sync/wait` waits until the serving node has applied at least `minLsn`, or returns `caughtUp=false` after `timeoutMs`. For point reads on a known shard, add `consistency=quorum|all` plus `shardKey=...` or `shard=...` to also require the shard's remote WAL acknowledgements to reach that LSN. The TypeScript SDK exposes the same primitive as `db.waitForLsn(minLsn, { timeoutMs, consistency, shardKey })`, which is the client-side building block for read-your-writes checks, replica freshness checks before reading from a follower, and deterministic subscription recovery tests.

SDK read helpers also accept the same freshness options. When `minLsn` is supplied, the client waits for the node to catch up before reading; cache-backed reads only return locally if the cached records already prove that LSN, otherwise they fall through to the server:

```ts
const write = await db.table("rooms").upsert("general", { id: "general", title: "General" })
const room = await db.table("rooms").get("general", { minLsn: write.lsn, timeoutMs: 5000 })
const quorumRoom = await db.table("rooms").get("general", {
  minLsn: write.lsn,
  timeoutMs: 5000,
  consistency: "quorum",
})
const page = await db.table("rooms").list({ limit: 20, minLsn: write.lsn })
const object = await db.objectStore("Object").getMetadata("object-id", { minLsn: write.lsn })
```

`table.get` and nested-table point reads derive the shard route automatically for `consistency: "quorum"` or `"all"` and then read owner plus replica URLs in parallel. `quorum` returns after a majority of endpoints produce a fresh record, `all` requires every routed endpoint, and the SDK merges by choosing the highest record LSN before updating the local cache. Key-order `table.list`, including predicate-filtered lists, fans out across every topology shard, requires quorum/all fresh page responses per shard, merges rows by key with highest-LSN wins, then applies the requested page limit. Exact-match secondary-index reads use the same sharded quorum path; secondary-index range reads merge by decoded index tuple plus primary key and return a server-compatible `nextCursor`. Room `latest`/`before` message reads derive the room shard route, require quorum/all fresh pages, merge by message id, and return the newest messages by LSN. User profile reads derive the user-id shard route, require quorum/all fresh profile responses, and cache the highest-LSN profile. Durable user inbox reads derive the user-id shard route, require quorum/all fresh event pages, merge events by id with highest-LSN wins, and update the SDK user-event cache. User directory reads fan out across user-id shards, require quorum/all fresh page responses per shard, merge profiles by user id with highest-LSN wins, and update the SDK profile cache. Object list reads fan out by object-id shard and merge by object id. Object metadata, full body, and byte-range body reads derive the object shard route; full bodies are cached only after byte size and SHA-256 match metadata, while range reads require the range response to match the quorum metadata byte size and content type.

The SDK tracks the highest seen LSN from reads, writes, realtime events, and sync pulls. On transport reconnect or a lag notification it pulls missed durable room and table events for active subscriptions, updates local cache, then resumes realtime delivery.

When shard ownership enforcement returns `409 Conflict` with an owner URL, the SDK automatically retries durable `sendMessage`, `table.upsert`, and `putObject` calls against the owning node. Clients can keep a stable bootstrap endpoint while shard handoff moves writes to a new owner.

Realtime channels provide volatile membership and signaling for WebRTC voice/video and game transports:

```sh
curl http://127.0.0.1:3188/v1/realtime/channels
curl -X POST http://127.0.0.1:3188/v1/realtime/channels/call-general/join \
  -H 'content-type: application/json' \
  -d '{"userId":"alice","metadata":{"media":["audio","video"]}}'
curl -X POST http://127.0.0.1:3188/v1/realtime/channels/call-general/presence \
  -H 'content-type: application/json' \
  -d '{"userId":"alice","metadata":{"media":["audio"],"muted":true,"ready":true}}'
```

Signals are delivered as user-targeted volatile events between users who have joined the same realtime channel. The database coordinates which joined sessions should receive `offer`, `answer`, `ice`, `gameInput`, or `statePatch` payloads; the SDK exposes `sendOffer()`, `sendAnswer()`, `sendIce()`, `onOffer()`, `onAnswer()`, and `onIce()` on top of the generic `signal()` / `onSignal()` path. Applications can use `onSignalKind(kind, listener)` for app-specific signal types such as renegotiation or game-session control messages without manually filtering the generic signal stream. `cachedRecentSignals()` and `watchRecentSignals()` expose a bounded SDK-owned runtime window of recently received point-to-point channel signals for diagnostics and UI panels; it is volatile local state, clears on channel leave, and is not WAL-backed durable cache. Media frames and high-frequency game traffic should use WebRTC DataChannel, WebTransport, or another client transport.

Channel membership is session-scoped: the same logical user can join from multiple clients without overwriting another session. Member metadata is the database-owned volatile presence document for that joined user/session, with `{ joinedAtMs, updatedAtMs }`; `updatePresence` replaces the metadata, increments the channel-local sequence, and delivers `realtime.channel.memberUpdated` to joined channel sessions. The SDK keeps an in-memory projection of channel members from `join()`, `members()`, `updatePresence()`, reconnect rejoins, and member joined/left/updated events; `cachedMembers()` reads that projection and `watchMembers()` emits snapshots so UI code can treat presence like database-owned current state instead of rebuilding lists from raw events. When a WebSocket session disconnects, its channel memberships are removed automatically so presence cannot leave stale ghost members. The SDK keeps joined realtime channels as runtime membership intent; after a transport reconnect or server restart it reopens the connection and replays joins for the current session using the last SDK presence metadata. Channel signals and broadcasts share the same channel-local sequence. Signals target one joined user and return `delivered` plus `deliveredSessions`; broadcasts target unique logical users in the channel and fan out only to the sessions that joined the channel. Oversized or schema-invalid signal/broadcast payloads are rejected before sequence allocation. If a member joined without a `sessionId`, it is treated as a logical-user membership and fan-out covers every active session for that user. Broadcast `delivered` counts active routed sessions after this channel-member filter. Broadcasts deliver sequenced volatile events to joined members:

```sh
curl http://127.0.0.1:3188/v1/realtime/channels/call-general/state
curl -X POST http://127.0.0.1:3188/v1/realtime/channels/call-general/state \
  -H 'content-type: application/json' \
  -d '{"fromUserId":"alice","expectedVersion":0,"state":{"phase":"lobby","tick":1}}'
curl -X POST http://127.0.0.1:3188/v1/realtime/channels/call-general/broadcast \
  -H 'content-type: application/json' \
  -d '{"fromUserId":"alice","kind":"gameInput","payload":{"frame":42},"includeSelf":false}'
```

Each realtime channel also has an in-memory state snapshot with `{ version, state, updatedAtMs }`. `updateState` requires the caller to be a joined member, optionally checks `expectedVersion`, increments the channel-local sequence, and delivers a `realtime.channel.state` volatile event to joined channel sessions. This is the database-owned current state for lobby/game/collaboration coordination; it is deliberately not a WAL fact and is rebuilt by clients or behavior after a runtime restart. When the last member leaves a channel, the runtime drops that channel's state snapshot and sequence counter; `NEXTDB_REALTIME_MAINTENANCE_INTERVAL_MS` also runs a background sweep for legacy or abnormal cleanup paths. That sweep reconciles channel members against active connection sessions, removes stale session-scoped members that can no longer receive events, then removes orphan state/sequence entries whose channel has no members. `GET /v1/health` and metrics expose active channel, state, sequence, stale-member cleanup, and orphan cleanup counters so hidden realtime memory-state is observable. The SDK keeps a runtime projection of this state from `state()`, `updateState()`, reconnect refreshes, and `realtime.channel.state` events; `cachedState()` reads that projection and `watchState()` emits snapshots for UI state without forcing each app to manually join HTTP reads and volatile events. Behavior Wasm can read channel members/state plus active `connectionSessions` filtered by user/session/transport, including connection metadata such as device or media capabilities, and can return host commands to broadcast a realtime channel event, publish durable or volatile user events, update channel state, update presence, or request connection disconnects. The behavior still never owns WebSocket/WebTransport handles directly; it observes logical connection state and asks the database host to route or control sessions.

For small binary realtime frames, the SDK exposes `sendGameInputFrame()`, `sendVoiceFrame()`, and `sendVideoFrame()`. They accept `Uint8Array`, `ArrayBuffer`, `Blob`, or `string`, encode it into a JSON payload with `{ dataBase64, byteLength, contentType, codec, timestampMs, metadata }`, and route it through the same channel broadcast path as `sendGameInput()`, `sendVoice()`, and `sendVideo()`. Receivers can use `onVoice()` / `onVideo()` for typed media events, `onEventKind(kind, listener)` for app-defined channel event kinds, and call `decodeRealtimeBinaryFrame(payload)` to recover a `Uint8Array`. `cachedRecentEvents()` and `watchRecentEvents()` expose a bounded SDK-owned runtime window of recently received channel broadcast events for UI/debug panels; it is volatile local state, clears on channel leave, and is not WAL-backed durable cache. These helpers are volatile connection-layer frames for game input, audio/video control frames, small keyframes, and app-specific room control events; durable media bodies still belong in the object store, and high-bandwidth streams should use WebRTC or a dedicated WebTransport media/data stream.

The SDK smoke starts an isolated node and verifies logical-user delivery through `realtimeChannel()`, including `updatePresence`, `cachedMembers`, `watchMembers`, `onMemberUpdated`, `state`, `updateState`, `onState`, `signal`, `onSignalKind`, `cachedRecentSignals`, `watchRecentSignals`, `sendOffer`, `sendAnswer`, `sendIce`, `onOffer`, `onAnswer`, `onIce`, `sendGameInput`, `sendGameInputFrame`, `sendStatePatch`, `onEventKind`, `cachedRecentEvents`, `watchRecentEvents`, `sendVoice`, `sendVoiceFrame`, `onVoice`, `sendVideo`, `sendVideoFrame`, `onVideo`, membership events, and channel summaries:

```sh
npm run test:realtime-channel-sdk
```

WAL audit reads expose the durable event stream for event-sourcing, tracing, and admin tooling:

```sh
curl 'http://127.0.0.1:3188/v1/audit/wal?afterLsn=0&limit=100'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=messageCreated&roomId=general'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=recordUpserted&table=rooms'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=recordDeleted&table=rooms'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=recordTransactionCommitted&table=rooms'
curl 'http://127.0.0.1:3188/v1/audit/wal?objectId=OBJECT_ID'
curl 'http://127.0.0.1:3188/v1/audit/wal?table=rooms&recordKey=general'
curl 'http://127.0.0.1:3188/v1/audit/wal?path=tables/rooms/general'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=schemaApplied'
curl 'http://127.0.0.1:3188/v1/audit/wal?path=schema/versions/2'
curl 'http://127.0.0.1:3188/v1/audit/wal?clientMutationId=rooms-general-v1'
curl 'http://127.0.0.1:3188/v1/audit/trace?kind=room&id=general'
curl 'http://127.0.0.1:3188/v1/audit/trace?kind=record&table=rooms&recordKey=general'
curl 'http://127.0.0.1:3188/v1/audit/trace?kind=nestedRecord&table=rooms&parentKey=general&nested=messages&nestedKey=msg-1'
curl 'http://127.0.0.1:3188/v1/audit/trace?kind=object&id=OBJECT_ID'
curl 'http://127.0.0.1:3188/v1/audit/trace?kind=clientMutation&clientMutationId=rooms-general-v1'
curl 'http://127.0.0.1:3188/v1/audit/replay?kind=record&table=rooms&recordKey=general&atLsn=42'
curl 'http://127.0.0.1:3188/v1/audit/replay?kind=nestedRecord&table=rooms&parentKey=general&nested=messages&nestedKey=msg-1&atLsn=42'
curl 'http://127.0.0.1:3188/v1/audit/replay?kind=user&id=alice&atLsn=42'
curl 'http://127.0.0.1:3188/v1/audit/replay?kind=object&id=OBJECT_ID&atLsn=42'
curl 'http://127.0.0.1:3188/v1/schema/history'
curl 'http://127.0.0.1:3188/v1/schema/history/1'
```

`db.traceEntity()` wraps `/v1/audit/trace` for room, user, object, record, nested record, path, and client mutation traces. It still returns raw WAL records plus `nextAfterLsn` / `hasMore`; the endpoint only standardizes the entity-to-WAL matching rules, so WAL remains the source of truth. `db.replayEntity()` wraps `/v1/audit/replay` for entities with a single durable state (`record`, `nestedRecord`, `user`, and `object`), returning `exists`, `deleted`, or `missing` plus the reconstructed state at `atLsn` and the WAL LSN that produced it. Generated TypeScript clients bind audit `record` / `nestedRecord` parameters to schema table names and branded keys, and replayed record values come back as the matching generated table or nested-table type.

Each WAL fact carries `schemaVersion`. Successful schema apply operations also append a `schemaApplied` WAL fact with the full applied schema and migration plan. In clustered mode, schema apply is a shard-0 control write: when `NEXTDB_ENFORCE_SHARD_OWNERSHIP=true`, non-owner nodes reject direct apply with `409` plus `ownerUrl`, and frozen shard 0 rejects apply with `423`. Before the owner mutates schema files or projections, it asks shard-0 WAL replicas to run `/v1/admin/schema/preflight`; the apply is rejected unless that peer preflight satisfies the shard's `NEXTDB_WAL_REMOTE_ACKS` policy. Replicas then apply the committed `schemaApplied` fact through the normal WAL replication path, persist schema history, and rebuild record projections before later replicated records are projected. The schema history endpoints map schema versions back to immutable schema files stored under `data/schema/history/v{version}.json`, so audit and replay tools can explain old records with the field contract that was active when they were written.

Logical export planning starts from the same WAL truth surface:

```sh
curl 'http://127.0.0.1:3188/v1/admin/export/manifest'
curl 'http://127.0.0.1:3188/v1/admin/export/manifest?includeSamples=true&sampleLimit=5'
curl 'http://127.0.0.1:3188/v1/admin/export/manifest?baseLsn=1000'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/bundle'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/bundle' \
  -H 'content-type: application/json' \
  -d '{"baseLsn":1000}'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/backup/run' \
  -H 'content-type: application/json' \
  -d '{"archiveObject":true}'
curl 'http://127.0.0.1:3188/v1/admin/export/backup/runs'
curl 'http://127.0.0.1:3188/v1/admin/export/backup/policy'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/backup/policy' \
  -H 'content-type: application/json' \
  -d '{"enabled":false,"intervalMs":0,"archiveObject":true,"retentionKeepLast":8,"retentionDeleteBundles":true,"retentionDeleteArchiveObjects":false}'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/backup/policy/run'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/backup/retention' \
  -H 'content-type: application/json' \
  -d '{"dryRun":true,"keepLast":8,"deleteBundles":true,"deleteArchiveObjects":false}'
curl 'http://127.0.0.1:3188/v1/admin/export/bundles'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/bundles/EXPORT_ID/verify'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/bundles/verify-chain' \
  -H 'content-type: application/json' \
  -d '{"bundleIds":["FULL_EXPORT_ID","DELTA_EXPORT_ID"]}'
curl -X POST 'http://127.0.0.1:3188/v1/admin/export/bundles/EXPORT_ID/archive-object'
curl -X POST 'http://127.0.0.1:3188/v1/admin/import/bundles/from-object/OBJECT_ID'
curl -X POST 'http://127.0.0.1:3188/v1/admin/import/bundles/EXPORT_ID/preflight'
curl -X POST 'http://127.0.0.1:3188/v1/admin/import/bundles/EXPORT_ID/restore'
curl -X POST 'http://127.0.0.1:3188/v1/admin/import/bundles/DELTA_EXPORT_ID/preflight-delta'
curl -X POST 'http://127.0.0.1:3188/v1/admin/import/bundles/DELTA_EXPORT_ID/apply-delta'
curl -X POST 'http://127.0.0.1:3188/v1/admin/import/bundles/restore-chain' \
  -H 'content-type: application/json' \
  -d '{"bundleIds":["BASE_EXPORT_ID","DELTA_EXPORT_ID"]}'
```

The manifest returns the export format id, base LSN, whether the export is incremental, current LSN, snapshot/compaction LSNs, schema version, schema history versions, schema proposal count, cluster-control counts, WAL record range, checksum summary, per-payload counts, per-table/room/user counts, object live/deleted byte totals, optional encryption metadata, and optional WAL samples. `POST /v1/admin/export/bundle` materializes a local bundle under `data/exports/{id}` with `manifest.json`, `schema.json`, `schema/history/v{version}.json`, `schema/proposals.json`, `cluster/topology-overrides.json`, `cluster/topology-log.jsonl`, `cluster/topology-proposals.json`, `cluster/topology-lease.json`, `cluster/handoff-workflows.json`, `wal-records.jsonl`, `objects/metadata/*.json`, and `objects/blobs/*.bin`. Pass `{ "encryptionKey": "..." }` or set `NEXTDB_EXPORT_BUNDLE_KEY` to leave `manifest.json` readable but encrypt every other bundle file with AES-256-GCM using a SHA-256-derived key. Pass `{ "baseLsn": N }` to create an incremental bundle containing only WAL records after `N` plus object bodies committed in that delta and still live by the end of the delta. `POST /v1/admin/export/backup/run` turns this into an operator runbook primitive: it finds the local verified full+delta chain with the highest LSN, creates a full bundle when no base exists or `forceFull=true`, otherwise creates the next incremental bundle from the chain tail, archives it to the built-in object store by default, verifies the resulting chain, and appends a persistent run record to `data/exports/backup-runs.json`. If the chain tail already equals current LSN, it returns `noOp=true` without writing an empty delta, but still records the run. `npm run test:wal-export-corruption` verifies that manifest, bundle, and backup-run creation fail closed on WAL checksum mismatch and do not append a backup-run record for the failed attempt. `GET /v1/admin/export/backup/runs` returns that local backup run catalog, including run id, mode, base/current LSNs, bundle id, archive object id, chain ids, chain status, and byte/count summaries. `GET/POST /v1/admin/export/backup/policy` persists `data/exports/backup-policy.json`; the policy controls optional in-process backup scheduling, default archive behavior, and post-run retention. Environment defaults are available through `NEXTDB_BACKUP_ENABLED`, `NEXTDB_BACKUP_INTERVAL_MS`, `NEXTDB_BACKUP_ARCHIVE_OBJECT`, `NEXTDB_BACKUP_KEEP_LAST`, `NEXTDB_BACKUP_RETENTION_DELETE_BUNDLES`, and `NEXTDB_BACKUP_RETENTION_DELETE_ARCHIVE_OBJECTS`, but the policy file wins after it exists. `POST /v1/admin/export/backup/policy/run` executes one backup with the saved policy and then applies retention if configured. `POST /v1/admin/export/backup/retention` prunes that catalog by `keepLast` and/or `beforeTimestampMs`; it defaults to dry-run, protects bundle/object ids still referenced by retained runs, can remove local bundle directories, and only deletes archived backup objects when `deleteArchiveObjects=true`, using the normal object-delete WAL path. `GET /v1/admin/export/bundles` lists local bundle directories by reading their manifest, schema files, proposal ledger, and cluster-control ledger; encrypted bundles list from the manifest only. `POST /v1/admin/export/bundles/{id}/verify` checks the local artifact again: manifest parsing, optional decrypt/authenticate, schema parsing/version match, schema history parsing/version match, schema proposal parsing/count match/candidate validation, cluster-control parsing/count match/ledger reference checks, WAL JSONL readability/counts/LSN range/baseLsn bounds, WAL schemaVersion resolvability, object metadata safety, blob byte sizes, and blob SHA-256 values. `POST /v1/admin/export/bundles/verify-chain` verifies every bundle and checks chain continuity: the first bundle must be full with `baseLsn=0`, every later bundle must be incremental, and each delta `baseLsn` must equal the previous bundle `currentLsn`. `POST /v1/admin/export/bundles/{id}/archive-object` packs those exact bundle bytes into the built-in object store with content type `application/vnd.nextdb.export-bundle-archive+json`, so object replication or an external object copy can move the backup independently of WAL. `POST /v1/admin/import/bundles/from-object/{objectId}` validates that archive object, checks every embedded file path, byte length, and SHA-256, then materializes it back under `data/exports/{id}` for the normal import path. `POST /v1/admin/import/bundles/{id}/preflight` is read-only full-import planning: it reuses bundle verification, rejects non-empty current databases, checks the export format and checksum summary, rejects incremental bundles for empty-database restore, and reports whether the artifact is ready for restore. `POST /v1/admin/import/bundles/{id}/restore` enforces the same preflight, decrypts encrypted full bundles with the supplied key, replaces the empty database schema from `schema.json`, restores schema history files, the schema proposal ledger, and cluster-control files, writes object metadata/blobs through the replicated object path, writes WAL records with their original LSNs, applies projections, refreshes WAL remote-replica routing from restored topology overrides, and refuses to run unless the current database is empty. `POST /v1/admin/import/bundles/{id}/preflight-delta` verifies an incremental bundle against an already-restored base and requires the current LSN to exactly match the bundle `baseLsn`. `POST /v1/admin/import/bundles/{id}/apply-delta` enforces that preflight, restores the delta object files, writes the delta WAL records with their original LSNs, and applies them through the same projection path as inbound WAL replication. `POST /v1/admin/import/bundles/restore-chain` verifies the supplied full+delta chain, restores the first full bundle, then applies each delta in order so an operator can recover to the chain tail with one guarded action.

Run the export/import smoke to verify the portable artifact path locally:

```sh
npm run test:export-import
```

Checkpointing can also run automatically by LSN interval:

```sh
NEXTDB_CHECKPOINT_EVERY_LSN=1000 cargo run -p nextdb-server
```

Automatic WAL compaction can run immediately after an automatic checkpoint:

```sh
NEXTDB_CHECKPOINT_EVERY_LSN=1000 \
NEXTDB_AUTO_COMPACT_WAL=true \
cargo run -p nextdb-server
```

After a snapshot exists, WAL checkpoint compaction can archive records at or before the snapshot LSN while keeping newer records in the active WAL:

```sh
curl -X POST http://127.0.0.1:3188/v1/admin/snapshot \
  -H 'content-type: application/json' \
  -d '{}'
curl -X POST http://127.0.0.1:3188/v1/admin/wal/compact \
  -H 'content-type: application/json' \
  -d '{}'
```

Compacted records move under `data/wal/archive`. Runtime replay starts from the snapshot plus the active WAL, while audit, sync pull, and projection rebuild still read active and archived WAL records as one ordered event stream. The snapshot stores resident room actors, non-room `actorStates` for scope/table actors, and durable record-hot entries; volatile records are intentionally skipped. On boot, NextDB loads the schema file, scans WAL `schemaApplied` facts and rewrites the active schema/history from the highest schema apply LSN, overlays record WAL facts after the snapshot LSN onto already-resident scope/table actor states, and then rebuilds projections. Cold tables without snapshot actor state remain cold. `GET /v1/health` includes `startupRecovery` so operators and SDK tooling can verify the last boot: snapshot hit/miss, snapshot LSN, snapshot room and record-hot counts, schema WAL recovery, per-shard WAL restore from local replicas, scanned WAL records, records replayed after the snapshot, highest recovered LSN, and rebuilt chat-log/record/object-reference projection counts.

Run the restart recovery smoke to verify a real process restart from snapshot plus WAL:

```sh
npm run test:runtime-restart
```

Run the write-throughput smoke to establish a local chat-path baseline while still checking correctness. It writes configurable strict and relaxed durable batches through the HTTP/SDK path, verifies WAL integrity and latest-message ordering, snapshots the runtime, restarts the process, and reads both recovered windows back. It then writes a volatile batch, verifies the live-only window and unchanged WAL LSN, restarts again, and confirms the volatile room is gone. The output includes observed strict, relaxed, and volatile messages-per-second for the current machine:

```sh
npm run test:write-throughput
```

Operators can verify the WAL-as-truth boundary without replaying the whole server:

```sh
curl http://127.0.0.1:3188/v1/admin/wal/integrity
```

New WAL records are persisted in binary frames with a small magic/version/length
header and a checksummed record body. The runtime reader still accepts legacy
JSONL WAL files, and export bundles continue to materialize `wal-records.jsonl`
for portable backup/import inspection. New WAL records carry a `checksum` field
over the committed record body. The integrity report scans active shard files
and their archive directories, returning `ok=false` for checksum mismatches,
parse errors, duplicate LSNs, shard mismatches, zero LSNs, zero shard epochs,
zero schema versions, file-local LSN regressions, or persisted volatile events.
LSN gaps, legacy records without checksums, and legacy empty owner ids are
warnings because they can exist without corrupting committed facts.

`npm run test:wal-integrity-corruption` verifies the operator negative path by writing a durable fact, changing its active WAL JSON without updating the checksum, and checking that `/v1/admin/wal/integrity` reports a checksum mismatch for the exact LSN. `npm run test:wal-startup-corruption` verifies the recovery negative path: after the same on-disk corruption, a fresh server process must fail startup instead of replaying a damaged WAL.

Legacy WAL records can be sealed in place through the WAL worker, which rewrites active shard files, local replica files, and archive files with checksums while preserving record LSNs:

```sh
curl -X POST http://127.0.0.1:3188/v1/admin/wal/seal-checksums \
  -H 'content-type: application/json' \
  -d '{}'
```

Archived WAL files are retained until an operator deletes them. Retention is whole-file only and defaults to dry-run:

```sh
curl -X POST 'http://127.0.0.1:3188/v1/admin/wal/archive/retention?beforeLsn=100000'
curl -X POST 'http://127.0.0.1:3188/v1/admin/wal/archive/retention?dryRun=false&beforeLsn=100000'
curl -X POST 'http://127.0.0.1:3188/v1/admin/wal/archive/retention?dryRun=false&beforeTimestampMs=1893456000000'
```

The server never deletes active WAL files through this endpoint. Deleting archives reduces the historical range available to WAL audit, durable sync catch-up, projection rebuild, object-reference repair, and `clientMutationId` idempotency lookup.

`npm run test:wal-archive-retention` verifies this boundary with a real server: it writes durable facts, snapshots and compacts them into an archive, dry-runs retain/delete thresholds, applies retention, then checks the archive is gone while the active WAL, integrity report, and export manifest remain consistent.

Virtual actor residency is bounded by hot room count:

```sh
NEXTDB_MAX_HOT_ROOMS=10000 cargo run -p nextdb-server
```

`npm run test:actor-window` runs a real server with a two-message hot window and one resident room. It verifies that large latest reads merge the live actor window with the chat-log projection, `before()` can cold-read history outside the hot window, LRU room eviction does not delete durable history, room and `rooms.messages` nested subscriptions reactivate an evicted room from the chat-log projection, and restart restores only the bounded resident actor snapshot while both rooms remain readable from durable projection.

Shard ownership is configured independently of WAL file sharding:

```sh
NEXTDB_NODE_ID=node-a \
NEXTDB_NODE_URL=http://127.0.0.1:3188 \
NEXTDB_CLUSTER_NODES=node-a=http://127.0.0.1:3188,node-b=http://127.0.0.1:3189 \
NEXTDB_WAL_SHARDS=4 \
NEXTDB_SHARD_OWNERS='0-1=node-a,2-3=node-b' \
NEXTDB_SHARD_EPOCHS='0-1=1,2-3=2' \
NEXTDB_SHARD_REPLICAS='0-1=node-b;2-3=node-a' \
NEXTDB_ENFORCE_SHARD_OWNERSHIP=true \
NEXTDB_WAL_REMOTE_ACKS=quorum \
cargo run -p nextdb-server
```

```sh
curl http://127.0.0.1:3188/v1/cluster/topology
curl 'http://127.0.0.1:3188/v1/cluster/route?roomId=general'
curl 'http://127.0.0.1:3188/v1/cluster/route?table=rooms&recordKey=general'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/shards/0/freeze \
  -H 'content-type: application/json' \
  -d '{"reason":"handoff preparation"}'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/handoff/plan \
  -H 'content-type: application/json' \
  -d '{"shard":0,"targetOwner":"node-b"}'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/handoff/workflows \
  -H 'content-type: application/json' \
  -d '{"shard":0,"targetOwner":"node-b"}'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/handoff/workflows/WORKFLOW_ID/auto \
  -H 'content-type: application/json' \
  -d '{}'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/handoff/workflows/WORKFLOW_ID/apply \
  -H 'content-type: application/json' \
  -d '{}'
curl http://127.0.0.1:3188/v1/admin/cluster/topology/proposals
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/topology/proposals/PROPOSAL_ID/retry \
  -H 'content-type: application/json' \
  -d '{}'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/topology/proposals/PROPOSAL_ID/abort \
  -H 'content-type: application/json' \
  -d '{}'
curl -X POST http://127.0.0.1:3188/v1/admin/cluster/topology/lease/cleanup \
  -H 'content-type: application/json' \
  -d '{}'
curl http://127.0.0.1:3188/v1/admin/cluster/topology/log
```

For rolling restart, prepare the runtime before stopping a node. The prepare endpoint sets runtime drain, waits for in-flight durable writes to quiesce, and writes a runtime snapshot at the current LSN in one operator step. The snapshot response includes resident room count plus durable record-hot table and record counts, so operators can see how much memory-state will be restored. The default write wait is 10 seconds; pass `waitForWritesMs` to tune it. If writes do not quiesce in time, the response keeps `readyForRestart=false` and skips snapshot/compaction so operators do not mistake a busy runtime for a safe stop point. Ctrl-C and SIGTERM run the same drain-and-snapshot path before the HTTP server exits, so process managers still get a safe shutdown fallback. Draining keeps reads, audit, schema, admin operations, replica receivers, and already-open realtime sessions online, but rejects new writes, behavior invocations, realtime channel mutations, and new WebSocket/JSONL realtime connections with `503` plus `draining=true`:

```sh
curl -X POST http://127.0.0.1:3188/v1/admin/runtime/prepare-restart \
  -H 'content-type: application/json' \
  -d '{"reason":"rolling restart","snapshot":true,"compactWal":false,"waitForWritesMs":10000}'
curl -X POST http://127.0.0.1:3188/v1/admin/runtime/drain \
  -H 'content-type: application/json' \
  -d '{"draining":true,"reason":"rolling restart"}'
curl http://127.0.0.1:3188/v1/health | jq '{draining,acceptingWrites,runtimeDrain,runtimeWrites}'
curl -X POST http://127.0.0.1:3188/v1/admin/runtime/drain \
  -H 'content-type: application/json' \
  -d '{"draining":false,"reason":"restart complete"}'
```

`npm run test:runtime-drain-connection` verifies the drain boundary with a real process: an existing WebSocket session stays alive, a new WebSocket handshake is rejected, the JSONL connection gateway returns `503` with `draining=true`, writes fail closed, and clearing drain restores write readiness.

The TypeScript SDK treats owner conflicts and draining as retryable topology events. A `409` containing `ownerUrl` is retried once against that owner, including schema apply control writes on shard 0. `applySchema()` returns `peerPreflight` when the owner asks replicas to validate the candidate before commit. Provide peer endpoints directly or expose them through `NEXTDB_CLUSTER_NODES`; HTTP writes and WebSocket subscription reconnects move to a node whose health reports `acceptingWrites=true`:

```ts
const db = new NextDbClient({
  endpoint: "http://127.0.0.1:3188",
  replicaEndpoints: ["http://127.0.0.1:3189"],
  userId: "alice",
})
```

When ownership enforcement is enabled, direct writes to shards owned by another node return `409` with the owning node id and URL. Inbound WAL replication still writes through `/v1/admin/wal/replicate`, so owner nodes can mirror committed records to replica nodes.

Each WAL record includes `shard`, `shardEpoch`, and `ownerNodeId`. Replicas reject records whose shard, epoch, or owner does not match their configured topology. To transfer ownership manually, bump `NEXTDB_SHARD_EPOCHS` for that shard on every node and update `NEXTDB_SHARD_OWNERS` plus `NEXTDB_SHARD_REPLICAS`; stale owners will no longer be able to replicate old-epoch records to replicas.

Shard handoff starts with an operator freeze. Frozen shards reject direct writes with `423 Locked`. The handoff plan endpoint reports the next epoch, current shard LSN, target replica acked LSN, whether the target is caught up, and the env overrides needed for the new owner. The workflow endpoint persists this coordinator state, freezes the shard, and advances through `waitingForCatchUp` to `readyToReconfigure` as replicas catch up. `POST /auto` is safe for an operator loop: it refreshes the workflow from the latest replica ack state and returns `applied=false` until the target is caught up; once the workflow is ready, it runs the same apply path and returns `applied=true`.

Run the two-node handoff smoke to verify the full path locally: node A starts as shard owner, node B starts as replica, A writes records and objects that replicate to B, `/auto` moves ownership to B, then B writes records and objects that replicate back to A. The object checks read both metadata and body bytes from the replica, so blob placement and `objectCommitted` WAL replication are verified together.

```sh
npm run test:cluster-handoff
```

Set `NEXTDB_HANDOFF_CONTROLLER_INTERVAL_MS` to let a node run the same auto step internally. The controller is disabled by default; when enabled it periodically advances the oldest non-terminal workflow and exposes status in `/v1/health.handoffController`.

```sh
NEXTDB_HANDOFF_CONTROLLER_INTERVAL_MS=1000 cargo run -p nextdb-server
npm run test:cluster-handoff-controller
```

Set `NEXTDB_PEER_MONITOR_INTERVAL_MS` to enable lightweight peer health probing. The monitor is disabled by default; when enabled it polls peer `/v1/health` endpoints from the current topology and exposes reachability, `acceptingWrites`, peer LSN, latency, and last error under `/v1/health.peerHealth`. The handoff smoke enables it on both temporary nodes and waits until each node observes the other before writes begin.

Failover is deliberately split into planning and proposal. `POST /v1/admin/cluster/failover/plan` runs on the target replica, uses peer health to confirm the current owner is unhealthy, checks that the local WAL has reached the owner's last healthy LSN, and returns the owner/epoch/replica override that would promote the local node. `POST /v1/admin/cluster/failover/proposals` takes a ready plan and creates the corresponding topology proposal. If quorum is unavailable because the old owner is down, the proposal is persisted as `failed` with its prepare results instead of mutating ownership. This gives operators and controllers an auditable failover candidate without bypassing epoch fencing or topology quorum.

```sh
curl -X POST http://127.0.0.1:3189/v1/admin/cluster/failover/plan \
  -H 'content-type: application/json' \
  -d '{"shard":0}'
curl -X POST http://127.0.0.1:3189/v1/admin/cluster/failover/proposals \
  -H 'content-type: application/json' \
  -d '{"shard":0}'
npm run test:cluster-failover-plan
```

Set `NEXTDB_FAILOVER_CONTROLLER_INTERVAL_MS` on replica nodes to let the same failover election run in-process. The controller scans local replica shards, waits for peer health to mark the owner unhealthy and for the local WAL to catch up to the owner's last healthy LSN, creates one failover topology proposal for that shard, and automatically commits it when the topology majority accepts prepare and commit. It does not bypass quorum: in a two-node owner failure the generated proposal is persisted as failed and topology remains unchanged; in a three-node `A(owner), B/C(replicas)` layout, B can promote itself when C participates in the majority. Health exposes the loop under `/v1/health.failoverController`, including `lastProposalId` and `lastCommittedProposalId`.

```sh
NEXTDB_FAILOVER_CONTROLLER_INTERVAL_MS=1000 cargo run -p nextdb-server
npm run test:cluster-failover-controller
npm run test:cluster-failover-election
```

Applying a ready workflow now runs a two-phase topology proposal. The current node first acquires a topology lease with a monotonically increasing term, prepares the same proposal on peers, requires a majority of prepare acknowledgements, commits the proposal on peers, then commits locally. Peers reject stale terms and reject a conflicting coordinator while an unexpired lease is held. `NEXTDB_TOPOLOGY_LEASE_MS` controls the lease window and defaults to 30000 ms.

Commit appends a control event to `data/cluster/topology-log.jsonl`, writes the latest snapshot to `data/cluster/topology-overrides.json`, updates owner/epoch/replicas without restart, reconfigures the WAL writer's remote replica targets, and unfreezes the local shard. On startup, nodes load the snapshot and replay the JSONL control log, so the log remains the auditable recovery source. The current term is persisted under `data/cluster/topology-lease.json`. Failed prepare or commit attempts release the matching lease and persist the failed proposal for audit. A failed proposal can be retried as a fresh proposal with a new term; a prepared proposal can be aborted and propagated to peers. If a coordinator dies while holding a lease, `/v1/admin/cluster/topology/lease/cleanup` clears the expired holder while preserving the current term, allowing the next proposal to acquire a higher term. For an `A -> B` handoff, the default runtime overlay makes `B` the owner and moves `A` into the replica set, so post-handoff writes from `B` replicate back to `A`. The response includes commit results for participating nodes. Direct `POST /v1/admin/cluster/topology/overrides` remains a single-node escape hatch for operators and tests.

If `NEXTDB_WAL_REMOTE_REPLICAS` is not set, each shard uses the URLs of its configured `NEXTDB_SHARD_REPLICAS` as remote WAL mirrors. `NEXTDB_WAL_REMOTE_ACKS` controls how many remote mirrors must confirm a batch before the owner acknowledges the write:

```text
all      # default, preserve every configured remote mirror before ack
quorum   # local owner plus enough remote acks for majority
none     # do not require remote ack; status still records failures
N        # require exactly N remote acks, capped by replica count
```

WAL writes can be split across multiple local shard files while keeping one global LSN cursor:

```sh
NEXTDB_WAL_SHARDS=4 cargo run -p nextdb-server
```

Messages route by `roomId`; generic records route by `table:key`; object commits route by object id. Audit, sync, projection rebuild, and compaction merge all shard files and their archives by LSN.

Local WAL replica mirrors can be enabled with comma-separated replica data roots:

```sh
NEXTDB_WAL_REPLICA_DIRS=/mnt/nextdb-replica-a,/mnt/nextdb-replica-b cargo run -p nextdb-server
```

Each primary shard writes to matching replica shard files before acknowledging the batch. WAL compaction also archives replica segments, and startup restores a missing primary shard plus archive files from the first available replica. This is local filesystem replication. Networked shard ownership, epoch fencing, remote WAL acknowledgement policy, SDK record point-read, sharded key-order table-list, secondary-index quorum routing for exact and range reads, point-read freshness checks against remote WAL acks, operator-driven handoff, and quorum-bound failover election are covered by the cluster topology paths below; full replicated-log consensus remains future distributed-system work.

Network WAL replicas can be enabled by pointing a primary node at one or more replica HTTP nodes:

```sh
# replica
NEXTDB_DATA_DIR=/tmp/nextdb-replica \
NEXTDB_ADDR=127.0.0.1:3189 \
NEXTDB_WAL_REPLICATION_TOKEN=dev-secret \
cargo run -p nextdb-server

# primary
NEXTDB_DATA_DIR=/tmp/nextdb-primary \
NEXTDB_ADDR=127.0.0.1:3188 \
NEXTDB_WAL_REMOTE_REPLICAS=http://127.0.0.1:3189 \
NEXTDB_WAL_REPLICATION_TOKEN=dev-secret \
cargo run -p nextdb-server
```

The primary shard worker posts each WAL batch to `/v1/admin/wal/replicate` before acknowledging the append according to `NEXTDB_WAL_REMOTE_ACKS`. The replica preserves original LSNs, skips duplicate LSNs, rejects stale shard epochs, writes its own WAL, and updates read projections for replicated messages and records. Health output includes each shard's epoch, remote ack policy, required ack count, highest acked LSN, and last replica error. `POST /v1/admin/wal/replicate/repair?shard=N` replays active and archived WAL records after each remote's last acked LSN, so a temporarily down non-quorum replica can catch up after it returns. Set `NEXTDB_WAL_REPAIR_CONTROLLER_INTERVAL_MS` on owner nodes to run that same repair loop in-process; health exposes it under `/v1/health.walRepairController`. This is synchronous remote mirroring plus explicit shard ownership, epoch fencing, SDK record point-read, sharded key-order table-list, secondary-index quorum routing/merge for exact and range reads, point-read freshness checks against remote WAL acks, operator-driven online handoff, quorum-bound failover election, and automatic replica catch-up; it is not yet a full Raft-style replicated log.

Object blobs are replicated before their `objectCommitted` WAL record is appended, and the required object replica acknowledgements follow the shard's `NEXTDB_WAL_REMOTE_ACKS` policy. If object replication or WAL append fails before that fact is recorded, the local blob and metadata are rolled back so local object reads still follow the WAL-as-truth boundary. A remote blob can still become orphaned if a remote accepted the bytes and the local WAL append failed afterwards; object GC/repair is responsible for cleaning that remote-only residue. If explicit object or WAL remotes are not configured, object replication follows the current runtime shard replica topology. After a handoff, the new owner pushes both the blob and the WAL event to the new replica set. Override this with `NEXTDB_OBJECT_REMOTE_REPLICAS` when blob placement should differ. Receivers can require `NEXTDB_OBJECT_REPLICATION_TOKEN`; if it is not set, they fall back to `NEXTDB_WAL_REPLICATION_TOKEN`. `POST /v1/admin/objects/repair?shard=N&objectId=...` replays live object bytes to object remotes, and `NEXTDB_OBJECT_REPAIR_CONTROLLER_INTERVAL_MS` enables automatic owner-side object repair.

The owner gate runs before the local blob is written, so a stale owner does not leave orphan object files during SDK owner retry.

```sh
NEXTDB_OBJECT_REMOTE_REPLICAS=http://127.0.0.1:3189 \
NEXTDB_OBJECT_REPLICATION_TOKEN=dev-secret \
cargo run -p nextdb-server
```

When the in-memory room actor limit is exceeded, the least recently accessed room state is evicted. `NEXTDB_HOT_ROOM_IDLE_TTL_MS` can also passivate rooms that have not been touched recently; `0` disables idle passivation. Eviction and passivation only remove runtime memory-state. Durable history remains available through WAL and the chat-log projection, and the next read or subscription can reactivate the room. Set `NEXTDB_MAX_HOT_ROOMS=0` to keep no room actors resident and force room reads through cold projections.

Object GC uses reference tracking plus a grace window:

```sh
NEXTDB_OBJECT_GC_GRACE_MS=86400000 cargo run -p nextdb-server
curl -X POST 'http://127.0.0.1:3188/v1/admin/objects/gc?dryRun=true'
curl -X POST 'http://127.0.0.1:3188/v1/admin/objects/gc?dryRun=true&force=true'
```

Referenced objects are retained. Unreferenced objects younger than the grace window are protected unless `force=true`.

Object uploads can carry `clientMutationId` and an optional `objectId`. Retrying the same mutation id returns the original object metadata and does not append another `objectCommitted` WAL fact. The SDK preallocates an object id for uploads so queued offline attachments keep a stable reference. It only caches an uploaded body when its SHA-256 and size match the metadata returned by the server, so a mismatched retry cannot poison the local object-body cache.

Object bodies support single-range HTTP reads for large media and attachment lazy loading:

```sh
curl -H 'range: bytes=0-1048575' \
  'http://127.0.0.1:3188/v1/objects/OBJECT_ID/body'
```

Valid ranges return `206 Partial Content` with `Accept-Ranges: bytes` and `Content-Range`; unsatisfiable ranges return `416`. The SDK mirrors this through `getObjectBodyRange(objectId, { start, end })` and `objectStore("Object").getBodyRange(objectId, { suffixLength })`. Range reads do not write partial bodies into the full-object cache; instead, the SDK keeps metadata-bound partial range chunks and serves later subrange reads locally when the cached chunk's `sha256`, byte size, and content type still match the object metadata. Previously cached complete bodies still satisfy range reads first when local freshness rules allow it.

Objects can be deleted directly:

```sh
curl -X DELETE 'http://127.0.0.1:3188/v1/objects/OBJECT_ID?clientMutationId=delete-object-1'
curl -X DELETE 'http://127.0.0.1:3188/v1/objects/OBJECT_ID?force=true'
curl 'http://127.0.0.1:3188/v1/audit/wal?payloadType=objectDeleted&objectId=OBJECT_ID'
```

Delete refuses referenced objects unless `force=true`. Successful deletes append an `objectDeleted` WAL fact, including the `force` decision, remove metadata/body files, and add an object-scoped cache invalidation so SDK clients clear stale object bodies on the next cache lease refresh. `getObjectReferences()` reports `objectExists` and `dangling`, so a forced delete leaves an auditable dangling reference until the source records change or the same object id is restored.

## Build the sample behavior

Rust behavior modules can use `nextdb-behavior-sdk` to avoid hand-writing the guest ABI:

```rust
use nextdb_behavior_sdk::{
    runtime_context, BehaviorCommand, BehaviorInvokeOutput, BehaviorInvokeRequest,
};

fn handle(request: BehaviorInvokeRequest<MyInput>) -> BehaviorInvokeOutput {
    let room_id = request.input.room_id.clone();
    let ctx = runtime_context(&request);
    BehaviorInvokeOutput::new(serde_json::json!({
        "ok": true,
        "rngSeed": ctx.as_ref().map(|ctx| ctx.rng_seed.as_str()),
    }))
        .with_command(BehaviorCommand::upsert_record(
            "rooms",
            room_id.clone(),
            serde_json::json!({
                "id": room_id,
                "title": "General",
            }),
        ))
        .with_command(BehaviorCommand::put_object(
            b"behavior object",
            "text/plain",
            None,
        ))
        .with_command(BehaviorCommand::send_message(request.input.room_id, "hello"))
}

nextdb_behavior_sdk::nextdb_behavior!(MyInput, handle);
```

Use `nextdb_behavior_sdk::nextdb_behavior_postcard!(MyInput, handle)` together
with `"abiEncoding": "postcard"` in `nextdb.behavior.json` to opt a Rust
behavior into postcard-framed host calls. Use `"abiEncoding":
"postcardTypedSchema"` to have the host send schema-neutral `typedSchema`
request frames and receive typedSchema output frames; the Rust SDK converts the
input payload into `MyInput` before calling the handler. The AssemblyScript
`compile` command currently emits JSON ABI wrappers; use `pack` for precompiled
postcard Wasm. Plain postcard frames keep `encoding: "json"` for compatibility.

Behavior commands run through the same host path as clients. They can send messages, publish durable user inbox events, publish volatile user-targeted events, commit object uploads/deletes, upsert/delete records, return record transactions, broadcast realtime channel events, update realtime channel state, update realtime member presence, request connection disconnects, explicitly activate/evict runtime room or record hot state, schedule future behavior turns with `scheduleActorReminder` / `scheduleBehaviorReminder`, and request async host HTTP with `requestHostHttp`, including nested-table `nestedUpsert` / `nestedDelete` operations for parent-partition batches. Continuations can carry `replyTo` targets built with `behaviorReplyTo`; when the parent continuation succeeds, the host schedules the callback as a strict actor reminder, inherits call-chain bounds when omitted, and injects the parent `behaviorResponse` into callback input. Host HTTP requests are idempotent by `requestId`: duplicate identical requests return the original accepted request without appending a second `HostHttpRequested` fact or starting another outbound HTTP call, and a behavior invocation `clientMutationId` derives stable per-command request ids. Behavior scheduled reminders are idempotent under invocation-level `clientMutationId` when they use absolute `dueAtMs`: the host derives a stable reminder id if the command omitted one, checks a WAL-derived actor-reminder index, and returns the original schedule response for duplicate identical schedules. Outbound HTTP carries `x-nextdb-request-id` plus an `idempotency-key` defaulting to the same request id when the behavior did not provide one; `x-nextdb-request-id` is host-managed. Object command bodies cross the Wasm ABI as base64 and are decoded by the host before going through object replication and WAL commit. Behavior read plans can hydrate records, nested records, latest messages, object metadata, object bodies, realtime channel members/state, active connection sessions, WAL audit traces, and WAL audit replays; record and nested-record reads use the same live-or-disk projection as client reads, so volatile, resident, and LRU hot table state is visible to Wasm before falling back to durable files. Object body context entries include `bodyBase64`; audit context entries use `auditTraces` and `auditReplays` with the same response shape as `/v1/audit/trace` and `/v1/audit/replay`.

Behavior invocations can carry `clientMutationId` of up to 128 characters, and both Rust and TypeScript behavior SDKs expose that value on the invoke request. The host derives stable child mutation ids for each durable command, so retrying the same behavior call returns the original committed message, durable user event, object, record, transaction, host HTTP request, or absolute-time scheduled reminder response without appending duplicates. `scheduleActorReminder` with invocation-level idempotency must use `dueAtMs`; `delayMs` is rejected because a relative delay cannot be replayed to the exact same due time. When an invocation-level id is present, `publishVolatile`, `publishUserVolatile`, `broadcastRealtimeChannel`, `updateRealtimePresence`, `updateRealtimeChannelState`, `disconnectConnections`, and runtime activation commands are rejected because lossy realtime events, in-memory channel state, transport control messages, and memory-residency changes cannot be replayed idempotently.

Behavior reload is the publish boundary. `POST /v1/admin/behaviors/reload` scans `data/behaviors`, rejects duplicate behavior names, compiles every referenced Wasm module up front, checks manifest-declared `inputs` against the active schema's `behaviors.{name}.mutations`, and only swaps the active behavior set after the full reload succeeds. Existing loaded behaviors remain active if a new module is malformed, missing, or declares an input contract that conflicts with the database schema. Active schema inputs may add optional fields beyond the behavior manifest, but new required fields or type conflicts reject the reload. `health().behaviorRuntime` and `metrics()` expose behavior ops counters for invocations, successes, unknown-message turns, guest errors, command rejections, instance lifecycle, and pool errors, both globally and per loaded behavior.
The same compatibility check runs during schema apply, schema proposal preflight, and replicated `SchemaApplied` replay for every currently loaded behavior manifest. This keeps behavior packages and database schema from drifting in either direction: publish refuses an incompatible behavior, and schema evolution refuses to break a loaded behavior's declared input contract.
Manifests can also declare `reads`, `recordScopes`, `objectScopes`, `realtimeScopes`, `connectionScopes`, `userScopes`, `eventScopes`, and `commands`. `reads` is a read-plan allowlist over `records`, `nestedRecords`, `latestMessages`, `objects`, `objectBodies`, `realtimeChannelMembers`, `realtimeChannelStates`, `connectionSessions`, `auditTraces`, and `auditReplays`; omitting it keeps legacy packages unrestricted, while an explicit empty array means the behavior accepts no read plan. `recordScopes` narrows record access to declared top-level table names and logical nested table names such as `rooms.messages` for `read`, `write`, `nestedRead`, and `nestedWrite`; record/nested audit trace and replay reads use the same record read scopes, and room traces require `rooms` plus `rooms.messages` read scope. `objectScopes` narrows object metadata/body reads, object audit trace/replay reads, and object put/delete commands by object id. `realtimeScopes` narrows volatile channel reads and writes by channel id. `connectionScopes` narrows active session reads and connection-control commands by logical user id; reading or writing all users requires `["*"]`, and scoped user ids can use trailing-prefix wildcards such as `behavior-*`. `userScopes.read` narrows user audit trace/replay reads by logical user id, while `userScopes.publish` narrows durable and volatile user event targets. `eventScopes.publish` narrows the event name. Object, realtime, connection, user, and event scope entries can be exact values, `*`, or a trailing-prefix wildcard such as `call-*`; a `putObject` command without an explicit object id requires `objectScopes.write: ["*"]` because the host-generated id cannot be prefix-checked before commit. Declared read allowlists and scopes reject undeclared read-plan sections before the host reads records, objects, object bodies, realtime channel snapshots, connection sessions, or WAL audit views into Wasm context. `commands` is a host-command allowlist. An omitted or empty list keeps legacy packages unrestricted; a non-empty list rejects any undeclared command after Wasm execution but before the host commits records, objects, messages, user events, WAL facts, or realtime/channel/connection side effects.

Behavior manifests can carry their invocation contract beside the Wasm module:

```json
{
  "name": "echo-ts",
  "version": "0.1.0",
  "modulePath": "echo-ts.wasm",
  "mutations": ["echo.send"],
  "inputs": {
    "echo.send": {
      "type": {
        "kind": "object",
        "fields": {
          "roomId": { "type": { "kind": "id", "entity": "Room" } },
          "body": { "type": { "kind": "string" } }
        }
      }
    }
  },
  "reads": [],
  "recordScopes": {
    "write": ["rooms"],
    "nestedWrite": ["rooms.messages"]
  },
  "objectScopes": {
    "write": ["behavior-object-*"]
  },
  "realtimeScopes": {
    "write": ["behavior-state-*", "behavior-broadcast-*", "behavior-presence-*"]
  },
  "connectionScopes": {
    "read": ["alice"],
    "write": ["behavior-disconnect-*"]
  },
  "userScopes": {
    "publish": ["behavior-inbox-*"]
  },
  "eventScopes": {
    "publish": ["notification.created", "presence.ping"],
    "realtimeBroadcast": ["behavior.channel.*"]
  },
  "commands": ["upsertRecord", "putObject", "publishUserEvent", "publishUserVolatile", "broadcastRealtimeChannel", "updateRealtimeChannelState", "updateRealtimePresence", "disconnectConnections", "sendMessage"]
}
```

```sh
cargo build --manifest-path examples/behaviors/echo/Cargo.toml \
  --target wasm32-unknown-unknown --release
```

Install it into a data directory:

```sh
mkdir -p data/behaviors/echo
cp examples/behaviors/echo/nextdb.behavior.json data/behaviors/echo/
cp target/wasm32-unknown-unknown/release/nextdb_echo_behavior.wasm \
  data/behaviors/echo/echo.wasm
```

Run the Rust behavior Wasm smoke to verify the Rust SDK ABI, read-plan hydration, host command execution, and idempotent retry path end to end:

```sh
npm run test:behavior-rust-wasm
```

TypeScript behavior authors can use the AssemblyScript-compatible surface of `@nextdb/behavior-sdk` and compile it directly to the NextDB Wasm ABI:

```ts
import {
  commandArray,
  inputString,
  jsonString,
  object1,
  output,
  sendMessage,
} from "@nextdb/behavior-sdk/assembly"

export function handle(requestJson: string): string {
  const roomId = inputString(requestJson, "roomId")
  return output(
    object1("ok", "true"),
    commandArray(sendMessage(roomId, "hello from TypeScript")),
  )
}
```

The `compile` command generates a server-ready behavior directory by compiling the entry module with AssemblyScript and wrapping it with NextDB's `memory` / `alloc` / `dealloc` / `invoke` ABI. Manifests can use the default JSON ABI or `abiEncoding: "postcard"`; the AssemblyScript wrapper decodes/encodes postcard JSON frames while preserving the same `handle(requestJson: string): string` authoring shape. Use `pack` for precompiled Rust or custom Wasm, including `postcardTypedSchema` behaviors that expose a typed postcard entrypoint.

```sh
npm run build:behavior-sdk
npm run typecheck:behavior-ts
npm run compile:behavior-ts

npm exec -w @nextdb/behavior-sdk -- nextdb-behavior compile \
  --manifest /absolute/path/nextdb.behavior.json \
  --entry /absolute/path/src/index.ts \
  --out /absolute/path/data/behaviors/name
```

Run the behavior Wasm smoke to verify the TypeScript behavior toolchain and host commit path end to end:

```sh
npm run test:behavior-wasm
```

## Schema

On first start, NextDB writes:

```text
data/schema/nextdb.schema.json
```

Read schema and generated TypeScript:

```sh
curl http://127.0.0.1:3188/v1/schema
curl http://127.0.0.1:3188/v1/schema/validate
curl http://127.0.0.1:3188/v1/schema/migration-plan
curl http://127.0.0.1:3188/v1/schema/storage-policy
curl http://127.0.0.1:3188/v1/schema/typescript
curl -X POST http://127.0.0.1:3188/v1/admin/schema/apply \
  -H 'content-type: application/json' \
  -d '{"dryRun":true,"schema":{...}}'
```

Generate local TypeScript bindings through the client SDK:

```sh
npm exec -w @nextdb/client -- nextdb-codegen \
  --endpoint http://127.0.0.1:3188 \
  --out src/generated/nextdb.schema.ts
```

In an app that has `@nextdb/client` installed, the same binary is available as `nextdb-codegen` from `node_modules/.bin`. The CLI also reads `NEXTDB_ENDPOINT`, `NEXTDB_CLIENT_TOKEN`, and `NEXTDB_ADMIN_TOKEN`.

The generated TypeScript includes schema interfaces, typed audit trace/replay options, typed index query options, typed local-data, stored-subscription, cache-lease, and pending-write diagnostics/management methods, typed realtime channel state/recent-event/recent-signal helpers, and a zero-runtime typed SDK facade:

```ts
import { NextDbClient } from "@nextdb/client"
import { NEXTDB_SCHEMA_VERSION, typedNextDb, type Id } from "./generated/nextdb.schema"

const raw = new NextDbClient({ endpoint: "http://127.0.0.1:3188" })
const db = typedNextDb(raw)
db.withSchemaVersion(NEXTDB_SCHEMA_VERSION)
const roomId = "general" as Id<"Room">
const messageId = "m1" as Id<"Message">
const objectId = "object-1" as Id<"Object">

const objectStore = db.objectStore("Object")
const object = await objectStore.put("typed body", {
  contentType: "text/plain",
  objectId,
})
await objectStore.getBody(object.id)
await objectStore.getBodyRange(object.id, { start: 0, end: 3 })

await db.table("rooms").upsert(roomId, {
  id: roomId,
  title: "General",
})

const room = db.room(roomId)
const sent = await room.messages.send("typed hello", {
  attachments: [objectId],
  clientMutationId: "typed-room-message-1",
})
sent.lsn.toFixed()
sent.attachments.forEach((attachment) => attachment.id satisfies Id<"Object">)
await room.messages.latest({ limit: 20, minLsn: 1 })
room.messages.subscribe((event) => {
  if (event.type === "messageCreated") {
    event.message.senderId satisfies Id<"User">
  }
})
await room.publishVolatile("presence.ping", { at: Date.now() })

await db.table("rooms").index("byTitle", { value: "General" })
await db.table("rooms").index("byTitle", {
  lower: "A",
  upper: "Z",
  afterCursor: "cursor-from-previous-page",
})
await db.table("rooms").transaction([
  { type: "upsert", key: roomId, value: { id: roomId, title: "General" } },
  { type: "delete", key: roomId },
])
db.table("rooms").watchList((snapshot) => {
  snapshot.records.forEach((record) => record.value.title.toUpperCase())
})

const messages = db.nestedTable("rooms", roomId, "messages")
await messages.upsert(messageId, {
  id: messageId,
  roomId,
  senderId: "alice" as Id<"User">,
  body: "hello",
  attachments: [],
  createdAtMs: Date.now(),
  path: "rooms/general/messages/m1",
})
await messages.index("bySender", { value: "alice" as Id<"User"> })
await messages.index("bySender", {
  lower: "alice" as Id<"User">,
  upper: "zara" as Id<"User">,
})
await db.recordTransaction([
  { type: "upsert", table: "rooms", key: roomId, value: { id: roomId, title: "General" } },
  {
    type: "nestedUpsert",
    table: "rooms",
    parentKey: roomId,
    nested: "messages",
    nestedKey: messageId,
    value: {
      id: messageId,
      roomId,
      senderId: "alice" as Id<"User">,
      body: "batched hello",
      attachments: [],
      createdAtMs: Date.now(),
      path: "rooms/general/messages/m1",
    },
  },
])
messages.watchList((snapshot) => {
  snapshot.records.forEach((record) => record.value.body.toUpperCase())
})

await db.publishUserEvent("alice" as Id<"User">, "notification.created", {
  text: "typed durable event",
})
await db.publishUserVolatile("alice" as Id<"User">, "presence.ping", {
  at: Date.now(),
})
const channel = db.realtimeChannel("call-general" as Id<"RealtimeChannel">)
type CallState = { phase: "lobby" | "started"; tick: number }
const callState = await channel.state<CallState>()
await channel.updateState<CallState>(
  { phase: "lobby", tick: callState.state.version + 1 },
  { expectedVersion: callState.state.version },
)
channel.watchState<CallState>((snapshot) => {
  snapshot.snapshot?.state.tick.toFixed()
})
channel.onSignal((signal) => {
  signal.fromUserId.toUpperCase()
  signal.sequence.toFixed()
})
channel.onSignalKind("renegotiate", (signal) => {
  signal.payload
})
channel.watchRecentSignals((snapshot) => {
  snapshot.signals.map((signal) => signal.sequence)
}, { kind: "renegotiate", limit: 5 })
channel.onEvent((event) => {
  event.sequence.toFixed()
})
channel.onEventKind("lobbyReady", (event) => {
  event.payload
})
channel.watchRecentEvents((snapshot) => {
  snapshot.events.map((event) => event.sequence)
}, { kind: "lobbyReady", limit: 5 })
channel.onState<CallState>((event) => {
  event.state.state.phase.toUpperCase()
})

await db.invokeBehavior({
  behavior: "echo-ts",
  mutation: "echo.send",
  input: {
    roomId,
    body: "from typed behavior",
  },
  read: {
    records: [{ table: "rooms", key: roomId }],
    nestedRecords: [{
      table: "rooms",
      parentKey: roomId,
      nested: "messages",
      nestedKey: messageId,
    }],
    objects: [{ object: "Object", objectId }],
    objectBodies: [{ object: "Object", objectId }],
  },
})
```

The admin console can also invoke loaded behavior modules directly. It renders the input form from the behavior mutation field schema, shows the committed host facts, and lets the WAL audit stream trace the corresponding records.

Unknown table names, unknown nested table names, unknown index names, malformed transaction operations, unknown behavior names, unknown behavior mutations, malformed record values, and malformed behavior inputs are rejected at compile time.
Generated clients also export `NEXTDB_SCHEMA_VERSION`; `typedNextDb(raw)` calls `raw.withSchemaVersion(NEXTDB_SCHEMA_VERSION)` when the runtime client supports it. Pinned clients send `x-nextdb-schema-version` on HTTP requests and `schemaVersion` on realtime connects. The server rejects client-protected writes, object mutations, behavior invocations, realtime mutations, and subscription connections with `409 schemaVersionMismatch` when the pinned generated types no longer match the active schema.
Generated index calls are bound to the declared index field types: single-field indexes use `value` / `lower` / `upper` with that scalar type, while compound indexes use tuple-shaped `values` / `lowerValues` / `upperValues` in schema field order.
Generated query predicates bind `field` to known table or nested-table fields and bind `value` to that field type. `contains` over list fields uses the list item type, so object-reference attachment predicates require object metadata rather than a plain object id.
Generated object store calls are bound to `schema.objects`: `db.objectStore("Object")` returns metadata, ids, list pages, delete responses, and object subscription events typed from that object schema while delegating to the same runtime object API.
Generated chat room calls are bound to `tables.rooms` and `tables.rooms.nested.messages`: `db.room(roomId)` carries the branded room id, committed message shape, object-reference attachments, latest/before reads, runtime message-window activation, room subscriptions, local room cache controls, and schema-declared volatile event payloads while delegating to the runtime chat APIs.
Generated event calls are bound to `schema.events`: `publishUserEvent`, `publishUserVolatile`, `onUserEvent`, `watchCurrentUserEvents`, and realtime channel signal/event/state/member listeners carry the declared payload type for each event name. Realtime channel member helpers expose typed presence metadata through `join<T>()`, `members<T>()`, `cachedMembers<T>()`, `watchMembers<T>()`, and `updatePresence<T>()`, and `onMemberUpdated()` carries the structured member payload from the schema. Realtime channel state helpers also expose generic state snapshots, so `state<T>()`, `updateState<T>()`, `cachedState<T>()`, `watchState<T>()`, and `onState<T>()` can bind an application-defined lobby/game state shape while still carrying the branded channel id.
Generated behavior calls bind the mutation input and the read plan: `records`, `nestedRecords`, `objects`, and `objectBodies` use schema table, nested-table, record-key, and object-id types before the request reaches the Wasm host, while `latestMessages`, `realtimeChannelMembers`, and `realtimeChannelStates` require branded room or realtime-channel ids for volatile coordination reads. The optional behavior `userId` is also a branded user id, so generated behavior invocations cannot accidentally pass plain strings where entity ids are expected. At runtime, record and nested-record read plans resolve through the server live-or-disk projection, so typed Wasm inputs observe memory-state tables instead of only the persistent projection.

The default runtime validates `sendMessage` drafts, top-level record writes, nested record writes, record transactions, declared event payloads, and behavior inputs against this schema before appending WAL records, delivering realtime events, or invoking Wasm behaviors. `Text.inlineUntil` is enforced as a byte limit, `Int64` / `TimeMs` fields must be JSON integers, and `ObjectRef.byteSize` must be a non-negative integer. Declared `ObjectRef` fields must point at an existing object and their `path`, `contentType`, `byteSize`, and `sha256` must match the object store metadata before the record write, event publish, or behavior invocation can proceed. Behavior host commands reuse the same commit paths as external clients, so malformed behavior-authored records and declared volatile events fail before WAL append or realtime delivery too.
Committed WAL records and runtime snapshots include the active `schemaVersion`, so replay and future migrations can reason about the schema that produced each fact.

Storage policy is also read from schema at startup. `tables.rooms.nested.messages.storage.liveWindow` controls the default hot message window, and `tables.rooms.storage = { "kind": "lru", "maxItems": ... }` can provide the default resident room limit. `NEXTDB_HOT_ROOM_IDLE_TTL_MS` passivates idle room actors, and `NEXTDB_HOT_ROOM_MAINTENANCE_INTERVAL_MS` controls the background room sweep interval when that TTL is enabled. Schema-defined top-level and nested record tables using `resident`, `actorPartition`, `lru`, or `chatLog` also get a WAL-derived in-memory read model while `disk` tables continue to read from the persistent projection only. Windowed hot tables evict from memory without deleting the disk projection, so WAL and disk remain the durable truth. `NEXTDB_RECORD_HOT_DURABLE_IDLE_TTL_MS` can passivate durable hot records that have not been touched recently; volatile hot records are not idle-evicted because they are process-local current state. When the durable idle TTL is enabled, `NEXTDB_RECORD_HOT_MAINTENANCE_INTERVAL_MS` controls the background record sweep interval; if unset, the runtime derives a bounded interval from the TTL. The persistent record projection maintains `_key_order` files for table list pages, `_recent` files for newest durable rows, `_partitions` files for nested-key parent partitions, and `_orders` files for schema-order parent partitions. These projections keep bounded manifests so common key-order pages, live-query initial pages, activation windows, nested parent pages, schema-order cursor pages, and startup hot prewarm can page without deserializing and sorting the whole table or parent partition on every request. If a manifest cannot prove it covers the requested window, the server falls back to the sorted projection files and repairs the manifest. Projection status and metrics expose key-order and recent entry counts next to record, secondary-index, partition, and schema-order counts. Point reads, key-order lists, predicate scans, schema-order nested lists, and secondary-index reads batch-rehydrate returned durable rows into the bounded hot set without overwriting current volatile rows for the same key. Live query initial results and refreshes use the same read path, so returned durable rows also act as subscription-driven record activation signals for hot tables. Manual runtime activation through `POST /v1/admin/runtime/activate-records`, `db.activateRuntimeRecords(...)`, table/nested handles such as `db.table("rooms").activateRuntime(...)` and `db.nestedTable("rooms", roomId, "messages").activateRuntime(...)`, and the Admin UI can hydrate exact keys, key-order pages, nested parent pages via `parentKey` + `nested`, schema-ordered nested pages via `order: "schema"`, or secondary-index exact/range query windows using `indexName` plus `value`, `values`, `lower`, `upper`, `lowerValues`, `upperValues`, `afterKey`, `afterCursor`, `limit`, and optional deterministic `predicate`. Key-order, predicate, and secondary-index exact/range reads also overlay the current hot record state on the disk result, so live queries and HTTP reads see volatile rows and volatile replacements as the current memory-state. Runtime snapshots persist durable record-hot entries and access order, so restart preserves the hot set for LRU tables while still deriving correctness from WAL-backed projections; volatile rows remain process-local and disappear on restart. `GET /v1/health` exposes room and record hot maintenance state with active hot tables, resident counts, global and per-table point-read hit/miss totals, list totals, durable rehydration totals, runtime upsert/delete/evict totals, LRU eviction totals, idle TTLs, sweep timestamps, last evicted counts, and total evicted counts; `/v1/metrics` exports both global `nextdb_record_hot_*` counters and per-table `nextdb_record_hot_table_*{table=...}` counters for Prometheus. `NEXTDB_HOT_WINDOW`, `NEXTDB_MAX_HOT_ROOMS`, `NEXTDB_HOT_ROOM_IDLE_TTL_MS`, `NEXTDB_HOT_ROOM_MAINTENANCE_INTERVAL_MS`, `NEXTDB_RECORD_HOT_DURABLE_IDLE_TTL_MS`, and `NEXTDB_RECORD_HOT_MAINTENANCE_INTERVAL_MS` override schema-derived room actor and record-hot values.

Schema-defined top-level tables can also declare secondary indexes. The default schema includes `tables.rooms.indexes.byTitle` over the `title` field. Indexes are disk projections under `data/records/_indexes`; each index-value directory keeps a bounded manifest for key-ordered exact reads and range scan pages. WAL remains the durable truth, and index projections are rebuilt during startup, projection rebuild, and schema reload. Rebuilds are staged in a temporary record-store directory and swapped into place only after the candidate projection succeeds, so a failed unique-index rebuild leaves the previous projection readable. Unique indexes are enforced against durable WAL-derived rows and the current hot overlay: durable writes cannot reuse an older durable unique value hidden by a volatile replacement, and volatile writes cannot introduce a duplicate in the current hot-table state.
Indexes support exact-match reads and inclusive range reads. Single-field exact matches can use `value=...`; compound exact matches use `values=[...]`. Single-field ranges can use `lower=...` and `upper=...`; compound ranges use JSON scalar arrays through `lowerValues=[...]` and `upperValues=[...]` in index field order. Range reads walk ordered index-value directories, read per-value key manifests when they cover the requested window, and stop as soon as the requested page is full; if a manifest is missing or uncertain, that value directory is scanned and repaired. Range responses include `nextCursor`; pass it back as `afterCursor` with the same bounds for ordered pagination. List and index reads can add a deterministic `predicate` over record values; indexed predicates scan the selected index range first, then filter by predicate. The TypeScript SDK mirrors these options as `value`, `values`, `lower`, `upper`, `lowerValues`, `upperValues`, `afterCursor`, and `predicate`. SDK local cache serves exact-match and range index reads when it can fill the requested page, no predicate is present, and the table has no active volatile overlay; predicate reads and volatile-overlaid tables fall back to the server projection to avoid incomplete local matches.
Nested tables can declare secondary indexes too. The default `tables.rooms.nested.messages.indexes.bySender` index is queried through `/v1/records/rooms/{roomId}/messages/indexes/bySender?value=alice`; exact-match and range results are filtered to that parent partition, so they do not recreate global table relationships.
Nested schema-order clustering projections live under `data/records/_orders` and are rebuilt by the same startup repair, projection rebuild, and schema reload paths. Their manifests store ordered cursors separately from bounded record filenames, preserving opaque cursor pagination without making long keys or ordered text values part of the filesystem name.

Reload an edited schema:

```sh
curl -X POST http://127.0.0.1:3188/v1/admin/schema/reload \
  -H 'content-type: application/json' \
  -d '{}'
```

Apply a candidate schema directly through the admin API:

```sh
current="$(curl -s http://127.0.0.1:3188/v1/schema)"
curl -X POST http://127.0.0.1:3188/v1/admin/schema/apply \
  -H 'content-type: application/json' \
  -d "{\"dryRun\":true,\"schema\":$current}"
```

`/v1/admin/schema/apply` validates the submitted schema, checks its migration plan against the active in-memory schema, and preflights record projection rebuild only when the migration plan requires projection work. With `dryRun=true`, it returns the validation/migration result without mutating memory or disk. With `dryRun=false`, it atomically persists `data/schema/nextdb.schema.json`, rebuilds record/index/order projections only for replay-safe breaking migrations or compatible index/storage/order shape edits, reconfigures record hot-cache residency and room actor hot-window/resident limits from the new storage policy, and swaps the active registry. The response includes `replayRebuild`, `breakingReplayAllowed`, `projectionRebuilt`, and structured migration fields: `requiresReplayRebuild`, `replaySafeBreakingChanges`, `unsafeBreakingChanges`, `projectionRebuildRequired`, and `projectionRebuildReasons`. By default field, event schema, and object schema removals are rejected with the rest of the incompatible migration class; pass `allowBreakingReplay: true` only when you want replay-safe table, nested-table, or object field removals to rebuild projections from retained WAL facts, event schema removals to preserve retained WAL data while future events fall back to undeclared JSON passthrough, or unreferenced object schema removals to preserve object WAL/blob data while dropping the typed schema shell. Compatible index/storage/order shape edits also report projection rebuild reasons so dry-runs, proposals, Admin UI, and WAL audit can explain the work before apply. `/v1/admin/schema/reload` uses the same apply path after reading the candidate from disk. `npm run test:schema-actor-policy` verifies that applying `rooms.storage = lru` and `rooms.messages.storage.liveWindow` updates `health()` and `/v1/schema/storage-policy` immediately, truncates resident actor windows, evicts over-limit rooms, and survives restart through the persisted schema. `npm run test:schema-replay` verifies the explicit field/event/object-schema-removal replay path and unsafe type-change rejection.

Pass `expectedVersion` with schema apply to get compare-and-swap semantics. Non-dry-run schema apply is serialized inside the server; if another apply has already moved the active schema version, the stale request fails with `409 schemaVersionConflict` before replica preflight, projection rebuild, schema persistence, or WAL append.

Schema proposals make the same operation explicit and auditable before commit:

```sh
curl -X POST http://127.0.0.1:3188/v1/admin/schema/proposals \
  -H 'content-type: application/json' \
  -d "{\"expectedVersion\":1,\"reason\":\"add room topic\",\"schema\":$current}"
curl http://127.0.0.1:3188/v1/admin/schema/proposals
curl -X POST http://127.0.0.1:3188/v1/admin/schema/proposals/PROPOSAL_ID/commit \
  -H 'content-type: application/json' \
  -d '{}'
curl -X POST http://127.0.0.1:3188/v1/admin/schema/proposals/PROPOSAL_ID/abort \
  -H 'content-type: application/json' \
  -d '{}'
```

Preparing a proposal validates the candidate, records the migration plan, projection preflight result, and `allowBreakingReplay` decision under `data/schema/schema-proposals.json`, and asks shard-0 WAL replicas to persist the same prepared proposal. Prepare must satisfy the shard's `NEXTDB_WAL_REMOTE_ACKS` policy, but it does not mutate the active schema. Commit reuses the same serialized `expectedVersion`, owner/freeze, replica-preflight, projection rebuild, schema persistence, and `SchemaApplied` WAL path as direct apply, then asks prepared peers to mark their proposal ledger committed. Failed or aborted proposals remain in the ledger for operator audit, and abort propagates to prepared peers.

Validation walks object, table, nested-table, event, and behavior fields recursively, including `ObjectRef` targets, `Text.inlineUntil`, `Id.entity`, indexes, and `ChatLog` bucket/order/live-window settings. Additive changes are allowed; version downgrades, referenced object schema removals, and field type or optional-shape changes across records, objects, event payloads, and behavior mutation inputs are rejected as unsafe breaking changes. Field removals, table removals, nested table removals, event schema removals, unreferenced object schema removals, and behavior schema removals are rejected unless the admin explicitly sets `allowBreakingReplay`, in which case table/nested-table/object field removal proceeds through WAL replay rebuild, table and nested table removal preserve retained record WAL/projection facts while the active schema stops exposing that table, event schema removal preserves retained WAL data while future events fall back to undeclared JSON passthrough, object schema removal preserves object WAL/blob data, and behavior schema removal preserves retained behavior WAL/audit facts while future invocations require an active schema declaration. If apply/reload fails, the previous active schema remains in memory and the previous readable projection is preserved.
