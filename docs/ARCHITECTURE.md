# NextDB Architecture MVP

For the target design direction, including memory-first actor activation,
activation indexes, behavior Wasm boundaries, user-defined tables, RLS, backup,
and partitioning, see [NextDB Design](DESIGN.md). This file describes the
current MVP architecture and implementation details.

## Core Model

NextDB uses logical nested tables and physical access paths:

```text
logical path:
  rooms/{room_id}/messages/{message_id}
  tables/{table}/{key}

physical path:
  shard WAL -> RoomActor live state + chat log projection -> query/subscription delivery
  shard WAL -> record projection -> query/subscription delivery
```

The MVP starts with `rooms/{room_id}/messages` because chat messages are the primary large-scale workload.

## Truth Layers

```text
WAL
  Durable replay truth. Records committed facts in global LSN order across one or more local WAL shards, tagged with the active schema version. Record deletes are tombstone facts, not projection-only removals.

WAL Replicas
  Local filesystem mirrors of primary WAL shards. They receive the same batches before acknowledgement and can restore missing primary WAL files at startup.

WAL Audit
  Event-sourcing and tracing read surface over durable WAL records, including archived checkpoint segments.

Live State
  Runtime truth for active actors. `RoomLiveState` keeps the hot message window for resident rooms. Schema-defined `actorPartition`, `resident`, and `lru` record tables can also hold volatile rows in the record hot state.

Projection / Query View
  Read surface. The current MVP queries live state first and falls back to a bucketed chat log projection for messages; generic table records use a WAL-backed disk projection.

Object Store
  Large payload truth. Object bodies are stored separately from rows, with metadata addressed by typed object ids.

Object Reference Index
  Rebuildable projection from message attachments and generic record ObjectRef-shaped values to object reference counts and source paths.

Snapshots
  Runtime live-state checkpoints. Startup can load `snapshots/actors.json` with resident room actors plus durable record hot-cache entries, then replay newer WAL events.

Client Cache
  SDK-managed IndexedDB or memory cache for local startup, subscription deltas, cache stats, trimming, and invalidation.

Live Queries
  Server-evaluated record list subscriptions. The connection layer sends an initial query result and refreshes that result when record upsert/delete events affect the subscribed table or nested parent partition.

Behavior Runtime
  Wasm modules loaded from manifests. A behavior cannot write storage directly; it returns host commands that NextDB validates and commits, including messages, records, transactions, object storage, realtime channel coordination, and explicit runtime activation of hot rooms or records. The runtime keeps a bounded resident Wasm instance pool per loaded behavior epoch, calls optional `on_activate()` when a fresh instance is created, calls optional `on_deactivate()` when resident instances are discarded, routes normal turns through `handle_message` with legacy `invoke` fallback, and can route stale mutation names to optional `on_unknown_message`.

Schema Registry
  Durable schema declaration for objects, tables, nested tables, storage classes, and behavior input types. TypeScript definitions are generated from this schema. Historical versions are kept so WAL `schemaVersion` values remain explainable during audit and replay.

Schema Apply
  Admin-controlled schema evolution path. A submitted candidate is validated, checked against the active schema migration rules, and preflighted against a staged record/index/order projection rebuild before it can become active.

Admin UI
  React/Vite operator console for runtime health, WAL audit, actor residency, object storage, schema, behavior module invocation, and operations.
```

This is intentionally not modeled as "database plus cache". Live state participates in the write path.

## Auth Gates

Authentication is optional for local development and enabled by environment variables:

```text
NEXTDB_ADMIN_TOKEN=...
NEXTDB_CLIENT_TOKEN=...
NEXTDB_WAL_REPLICATION_TOKEN=...
NEXTDB_OBJECT_REPLICATION_TOKEN=...
```

`NEXTDB_ADMIN_TOKEN` protects operator surfaces under `/v1/admin/*`, excluding the dedicated WAL/object replication receivers and topology peer prepare/commit/abort receivers. `NEXTDB_CLIENT_TOKEN` protects client-side mutation surfaces: `/v1/mutate`, record upserts, object upload, realtime channel operations, behavior invocation, and `/v1/connect`.

`NEXTDB_CLIENT_USER_TOKENS` adds identity-bound client tokens:

```text
NEXTDB_CLIENT_USER_TOKENS=alice=alice-secret,bob=bob-secret
```

When this map is configured, user-scoped operations that carry a `userId` require the matching token unless an admin token is supplied. This covers `sendMessage`, `publishUserEvent`, `publishUserVolatile`, realtime join/leave/signal/broadcast, behavior invocation with `userId`, and realtime connection with `userId`. Anonymous realtime connections and behavior invocations without `userId` still use the global client/admin gate, as do non-user-scoped record/object operations and room-scoped volatile publishes. `npm run test:auth` verifies that user tokens cannot write those non-user surfaces or impersonate another `userId`; `npm run test:connection-auth` verifies WebSocket and JSONL connection handshakes plus admin-only connection-event subscription.

HTTP clients can send `Authorization: Bearer ...`, `x-nextdb-admin-token`, or `x-nextdb-client-token`. Browser WebSocket clients cannot set custom headers, so the SDK sends `authToken` or `adminToken` as a connection query parameter. Admin tokens are accepted for client-protected requests so an operator console can exercise the full runtime.

Replication remains separate. WAL replication uses `NEXTDB_WAL_REPLICATION_TOKEN`; object replication uses `NEXTDB_OBJECT_REPLICATION_TOKEN` or falls back to the WAL replication token. Auth gates are transport/API boundaries and do not change WAL records, replay, projection rebuild, or event-sourcing semantics.

## Schema Evolution

NextDB exposes two schema control paths:

```text
POST /v1/admin/schema/apply
POST /v1/admin/schema/reload
GET  /v1/admin/schema/proposals
POST /v1/admin/schema/proposals
POST /v1/admin/schema/proposals/{proposal_id}/commit
POST /v1/admin/schema/proposals/{proposal_id}/abort
```

`apply` accepts a full schema candidate in the request body. `dryRun=true` validates the candidate, computes a migration plan from the active in-memory schema, and checks whether record/index/order projections can be rebuilt from WAL-derived records when the plan requires projection work. It does not mutate memory or disk. `dryRun=false` is serialized by a server-local schema apply mutex, persists `data/schema/history/v{version}.json` and `data/schema/nextdb.schema.json`, rebuilds projections through the staged record-store swap path only for replay-safe breaking migrations or compatible index/storage/order shape edits, reconfigures schema-derived hot-cache residency, and then swaps the active registry. The response includes `replayRebuild`, `breakingReplayAllowed`, `projectionRebuilt`, `projectionRebuildRequired`, and `projectionRebuildReasons` so operators and SDK callers can tell whether an incompatible-but-replay-safe migration path was used, whether a staged projection rebuild actually ran, and why compatible index/storage/order shape changes still require projection rebuild work.

Requests can include `expectedVersion` for compare-and-swap semantics. The active schema version is checked before validation, replica preflight, projection rebuild, schema persistence, or WAL append. A stale request returns `409 schemaVersionConflict` with the expected and active versions.

Schema proposals are the operator-facing schema workflow. Preparing a proposal runs the same validation, migration planning, and projection preflight as dry-run apply, then persists a ledger entry under `data/schema/schema-proposals.json` with the candidate schema, expected version, reason, report, migration, projection status, and `allowBreakingReplay` decision. The owner also sends the prepared proposal to shard-0 WAL replicas through `/v1/admin/schema/proposals/prepare`; the required prepare acknowledgements follow the shard's `NEXTDB_WAL_REMOTE_ACKS` policy. Commit reuses the direct apply path and records the resulting `schemaAuditLsn` and `peerPreflight`, then sends the committed proposal to prepared peers through `/v1/admin/schema/proposals/commit` so their operator ledgers converge. Abort propagates to prepared peers, and failed or aborted attempts remain visible in the same ledger. Replicas still learn active schema changes from `SchemaApplied` WAL facts, keeping WAL as the durable truth while the proposal ledger tracks operator intent and acknowledgement.

`reload` reads `data/schema/nextdb.schema.json` and delegates to the same apply path without rewriting the file. Both paths reject incompatible migrations before touching the active registry unless the request explicitly sets `allowBreakingReplay=true` and every incompatibility is a field removal under `tables.*.fields.*`, `tables.*.nested.*.fields.*`, or `objects.*.fields.*`, a table or nested table schema removal, an event schema removal, an unreferenced object schema removal, or a behavior schema removal. The migration plan exposes this decision as structured `requiresReplayRebuild`, `replaySafeBreakingChanges`, and `unsafeBreakingChanges` fields, so SDKs, Admin UI, WAL audit, and schema proposals do not need to parse human-readable error strings. It also exposes `projectionRebuildRequired` and `projectionRebuildReasons`, covering replay-safe field removals plus compatible index/storage/order shape changes that need the staged projection rebuild path. The replay-safe path replays retained WAL records into the candidate projection shape, ignores removed record/object fields in new indexes/orders, swaps the rebuilt projection, and appends the normal `SchemaApplied` audit fact. Table and nested table schema removal preserves retained record WAL/projection facts, but the active schema stops exposing that table. Event schema removal preserves retained WAL facts and future events with that name become undeclared JSON passthrough. Object schema removal preserves object WAL/blob data and is only reachable when validation proves no declared `ObjectRef` still targets the removed object. Behavior schema removal preserves retained behavior WAL/audit facts, but future invocations require an active schema declaration. Version downgrades and field type/optional-shape changes across record fields, object fields, event payloads, and behavior mutation inputs remain hard rejects. Additive schema changes are compatible, and content changes at the same version are allowed with a warning so local development can iterate before a formal version bump.

This keeps schema evolution in the database control plane instead of requiring applications to coordinate raw file edits, projection rebuilds, SDK code generation, and cache-policy changes separately.

## Virtual Actor Residency

Each room is a virtual actor keyed by `room_id`. Resident actors keep only a bounded hot message window in memory; full durable history lives in WAL and the chat-log projection.

Runtime controls:

```text
NEXTDB_HOT_WINDOW=5000
NEXTDB_MAX_HOT_ROOMS=10000
NEXTDB_HOT_ROOM_IDLE_TTL_MS=0
NEXTDB_HOT_ROOM_MAINTENANCE_INTERVAL_MS=0
NEXTDB_ACTOR_SHARDS=<available parallelism>
NEXTDB_ACTOR_PIN_THREADS=true
NEXTDB_ACTOR_SCOPE_SPLIT_ROWS=1024
NEXTDB_ACTOR_SCOPE_SPLIT_BYTES=0
NEXTDB_ACTOR_SPLIT_MAINTENANCE_INTERVAL_MS=0
NEXTDB_ACTOR_SPLIT_MAINTENANCE_LIMIT=64
NEXTDB_ACTOR_REMINDER_MAINTENANCE_INTERVAL_MS=0
NEXTDB_ACTOR_REMINDER_MAINTENANCE_LIMIT=64
NEXTDB_RECORD_HOT_DURABLE_IDLE_TTL_MS=0
NEXTDB_RECORD_HOT_MAINTENANCE_INTERVAL_MS=0
NEXTDB_RECORD_HOT_PREWARM_LIMIT=0
```

The schema also contributes runtime defaults. `tables.rooms.nested.messages.storage.liveWindow` sets the default message hot window. If `tables.rooms.storage` is `lru`, its `maxItems` sets the default resident room limit. Schema-defined top-level and nested record tables with `resident`, `actorPartition`, `lru`, or `chatLog` storage get a WAL-derived in-memory record hot cache; `chatLog` uses `liveWindow` as its bounded capacity. `disk` tables bypass this cache and read the persistent projection directly. Window eviction removes only the memory copy; WAL and disk projection remain the durable truth. `NEXTDB_RECORD_HOT_DURABLE_IDLE_TTL_MS` can also passivate durable hot records that have not been touched recently; volatile records are not idle-evicted because they are process-local current state rather than durable cache entries. When the durable idle TTL is enabled, `NEXTDB_RECORD_HOT_MAINTENANCE_INTERVAL_MS` controls a background sweep loop; if unset, the runtime derives a bounded interval from the TTL. Health and metrics expose global and per-table counters for the interval, last sweep timestamp, last evicted count, total evicted count, volatile resident records, point-read hit/miss totals, list totals, durable rehydration totals, runtime mutation totals, explicit evictions, and window evictions. The disk record store keeps `_key_order` projections for table list pages, `_recent` projections for newest durable rows, `_partitions` projections for nested-key parent partitions, and `_orders` projections for schema-order parent partitions. These projections keep bounded manifests, so common key-order reads, activation windows, nested parent pages, schema-order cursor pages, and startup prewarm can page without deserializing and sorting the full table or parent partition on every request. If a manifest is missing, uncertain, or too small for the requested window, the read falls back to sorted projection files and rewrites the manifest from authoritative filenames or ordered cursors. WAL rebuilds mark these projections complete along with records, secondary indexes, and nested order projections, and projection status exposes key-order and recent entries as first-class counts. Point reads, key-order lists, predicate scans, schema-order nested lists, and secondary-index reads on hot record tables batch-rehydrate returned durable rows into the bounded hot set with one record-hot write section per page. Top-level key-order hot reads start from the requested `afterKey` in the ordered in-memory map, and windowed table list reads linearly merge the ordered disk page with the ordered hot page while preferring hot records for duplicate keys. Predicate and shard-filtered key-order reads scan the hot overlay first, then collect up to `limit + hotOverrideCount + 1` matching disk rows before the ordered merge, so current-state hot rows that shadow stale disk matches do not shorten pages. Nested schema-order reads compute the same order cursor for hot rows and merge them with the `_orders` projection, so chat-log live-window rows and resident or volatile nested records participate in `order=schema` pages. Secondary-index exact reads use the same disk window expansion for stale index entries shadowed by hot current-state rows. Secondary-index range reads without extra predicate or shard filters use the same bounded disk window before merging by index cursor; filtered range reads keep the broader scan to preserve correctness. Nested parent-partition hot overlays use the same logical-key prefix range as the disk partition projection, so overlaying hot rows for one parent does not scan hot rows for every other parent. Live query results reuse those read paths, so a subscribed query that returns a durable row also reactivates that row in record hot state. Operators, behaviors, and SDK table handles can explicitly activate hot records by exact key, key-order page, nested parent page (`parentKey` + `nested`), schema-ordered nested page (`order=schema`), or secondary-index exact/range query window through the runtime activation API; the indexed path reuses the same projection and hot-overlay merge as normal reads. Rehydration never overwrites a current volatile row for the same key. Key-order, schema-order, predicate, and secondary-index exact/range reads also overlay the current hot record state on top of the disk result, so volatile records participate in HTTP reads and live query result evaluation without being written to WAL or persistent index files. Environment variables override schema-derived room actor values.

The actor runtime routes actors through `NEXTDB_ACTOR_SHARDS` stable hash shards. Actor identity is `ActorId { kind, key }`; built-in kinds are `room`, `scope`, `table`, `view`, and `aggregate`. Room actors dogfood the path with the room id as key and keep the hot message window; non-room actors can already be activated through a minimal kernel `Touch` turn that is serialized by the same shard owner thread. `POST /v1/admin/runtime/activate-actor` and the SDK `activateRuntimeActor()` method expose that generic activation path for operators and future built-in actors. Explicit record runtime activation hydrates returned rows into table-scoped `scope` actors: top-level records route to stable `table:{logicalTable}/bucket:{00..ff}` hash buckets, while nested records route to `table:{logicalTable}/parent:{parentKey}` parent partitions. The matching `table` actor lives on its own route shard and maintains the minimal scope directory with each activated scope's row count, estimated byte count, access stamp, split metadata, snapshot-backed `splitReminderAtMs`, and deterministic child-scope routing; it does not own the rows. `NEXTDB_ACTOR_SCOPE_SPLIT_ROWS` controls the row threshold for marking a scope `splitPending`, and `NEXTDB_ACTOR_SCOPE_SPLIT_BYTES` optionally enables an estimated-byte threshold when set above zero. Oversize scopes are now drained into two child scope actors, oversize child scopes recursively split until the policy is satisfied or the bounded split depth is reached, future parent-scope writes route through the table directory to the deepest child, and actor snapshots preserve that routing plus due split reminders for restart/WAL-tail overlay. `NEXTDB_ACTOR_SPLIT_MAINTENANCE_INTERVAL_MS` can enable a bounded background sweep for scopes whose restored or current `splitPending` reminder is due, with `NEXTDB_ACTOR_SPLIT_MAINTENANCE_LIMIT` limiting work per sweep.

Durable actor reminders are represented as WAL facts: `actorReminderScheduled`, `actorReminderCancelled`, and `actorReminderFired`. Startup rebuilds the pending reminder wheel from WAL, `POST /v1/admin/runtime/reminders` schedules a reminder, `/cancel` cancels one, and `/run-due` drains due reminders into actor turns. The SDK mirrors these as `scheduleActorReminder()`, `cancelActorReminder()`, and `runDueActorReminders()`. `NEXTDB_ACTOR_REMINDER_MAINTENANCE_INTERVAL_MS` can run due reminders periodically with `NEXTDB_ACTOR_REMINDER_MAINTENANCE_LIMIT` as the per-tick cap. Firing is currently at-least-once across the crash window; P4 behavior continuations will tighten the ergonomics for user Wasm scheduled work. Health/runtime activation status expose `scopeBytes`, `tablePendingSplits`, actor split maintenance counters, pending actor reminders, and reminder maintenance counters. Each shard is an owner OS thread with a blocking mailbox; the runtime handle keeps only the shard sender, and the thread owns a plain actor map for its residents. Room mutations, reads, snapshots, LRU candidate collection, explicit evictions, scope row activation, table directory updates, and generic kernel turns are serialized by that shard's mailbox, so they no longer pass through one global room directory lock, a per-room mutex, or Tokio worker scheduling. The runtime keeps an atomic resident-room count updated from shard replies, so the common write path does not scan every shard just to decide whether LRU work is needed. Generic scope/table/view/aggregate actors do not count against the room LRU while view-projection, aggregate, and split state machines are still being landed. Health and runtime activation status include `actorKernel` totals, room-vs-kernel counts, scope row/byte counts, table scope-directory counts, pending split counts, per-kind counts, and oldest/newest access stamps gathered from shard-owned maps. The default shard count is the host's available parallelism, and the environment variable can force a smaller or larger count for deterministic tests or constrained deployments. `NEXTDB_ACTOR_PIN_THREADS` defaults to true and asks each shard thread to pin itself to the corresponding available core when the platform supports it; health and runtime activation status expose the thread name, target core id, whether pinning was requested, and whether it succeeded. Global LRU and idle passivation still collect room access stamps across shards, so `NEXTDB_MAX_HOT_ROOMS` remains a process-wide resident-room limit rather than a per-shard limit.

The actor runtime evicts least recently accessed room actors when resident room count exceeds this limit. It can also passivate idle room actors with `NEXTDB_HOT_ROOM_IDLE_TTL_MS`; `0` disables idle passivation, while a positive value evicts rooms whose live state has not been read, written, or subscription-activated within that window. When room idle passivation is enabled, `NEXTDB_HOT_ROOM_MAINTENANCE_INTERVAL_MS` controls the background actor sweep interval; if unset, the runtime derives a bounded interval from the TTL. Health and metrics expose the interval, last sweep timestamp, last evicted count, and total evicted count. Reads first ask live state after applying idle passivation; if a room was evicted, passivated, or the live window is too small, the query falls back to the bucketed chat-log projection and reactivates the room with the bounded hot window. Realtime `subscribeRoom` and `subscribeNestedTable("rooms", room_id, "messages")` frames are also activation signals: the server reloads that room's latest hot window from the chat-log projection before acknowledging the subscription, so a client entering a room naturally makes its message actor resident. `NEXTDB_MAX_HOT_ROOMS=0` keeps no room actors resident, which is useful for proving that disk projections are sufficient for cold reads. `npm run test:actor-window` verifies this boundary with a two-message hot window and one resident room: large latest reads merge live and chat-log rows, `before()` reads cold history outside the hot window, LRU eviction keeps durable history readable, subscription re-entry reactivates evicted rooms, and restart restores only the bounded actor snapshot. `npm run test:actor-idle` verifies background idle passivation and cold reactivation.

Actor snapshots include resident room actors, each room's `lastAccessedMs`, a durable record-hot snapshot for schema-defined hot tables, and `actorStates` for non-room `scope`/`table`/generic actors. Scope actor snapshots carry their resident rows; table actor snapshots carry their scope directory entries including child-scope routes; generic actor snapshots carry turn/access state. Volatile records are intentionally skipped. Startup restores the room snapshot, restores the non-room actor states into the same sharded actor runtime, restores the record hot working set when it still matches the durable WAL-derived projection, then replays newer WAL records so LRU tables keep their hot set without treating the snapshot as durable truth. WAL records after the snapshot LSN are also folded into existing scope/table actor states for resident tables: upserts route through any restored child-scope directory before updating scope rows and table counts, deletes remove resident rows, and cold tables without snapshot actor state remain cold. `NEXTDB_RECORD_HOT_PREWARM_LIMIT` optionally runs a one-shot background prewarm after snapshot recovery: for each schema hot table, the runtime reads the newest durable projection rows and hydrates them into record hot state in LSN order, respecting LRU limits. Health, metrics, and runtime activation status expose found and activated counts per run so operators can see whether memory-state activation happened before traffic arrived.

## Write Path

```text
sendMessage
  -> validate request
  -> resolve attachment object ids into typed ObjectRef metadata
  -> route to WAL shard by room_id and append with group commit
  -> append chat log projection
  -> advance RoomLiveState
  -> publish DeliveryEvent
  -> client SDK updates local cache

sendMessage volatile
  -> validate request
  -> resolve attachment object ids into typed ObjectRef metadata
  -> create a message with lsn=0 and a volatile path
  -> advance RoomLiveState only
  -> publish DeliveryEvent to current room subscribers
  -> skip WAL, chat-log projection, object reference retention, audit, durable sync, and SDK durable cache

upsertRecord
  -> validate table and key
  -> validate value against schema.tables.{table}.fields
  -> route to WAL shard by table:key and append with group commit
  -> upsert record projection and schema-declared index projections
  -> update ObjectRef index from ObjectRef-shaped values
  -> publish DeliveryEvent
  -> client SDK updates local cache

upsertRecord/deleteRecord volatile
  -> validate table, key, expected LSN, and value against schema
  -> require actorPartition, resident, or lru storage
  -> write only the record hot state with lsn=0 and a volatile path
  -> publish DeliveryEvent to current table subscribers
  -> skip WAL, disk projection, indexes, object reference retention, audit, durable sync, and SDK durable cache

recordTransaction
  -> validate every operation and expected LSN before WAL append
  -> require every table:key route to the same WAL shard
  -> append one RecordTransactionCommitted WAL fact
  -> apply deletes then upserts to record/index projections in one record-store critical section
  -> publish one table event per operation with the same LSN
  -> client SDK updates local cache from the transaction result
```

Nested table indexes are stored under the logical table name, for example `rooms.messages`, but query APIs require a parent key and filter by the parent partition prefix. That gives message-style lookups such as `room.messages.bySender` without reintroducing global foreign-key relationships.

`strict` durability performs `sync_data` per WAL batch, not per mutation. `relaxed` writes to the WAL file without forcing a flush for that batch. The WAL shard worker exposes local queue depth, batch count, record count, byte count, sync count, last batch size, and write/sync timing through health and metrics, so strict-vs-relaxed latency and batching pressure are visible without inspecting files. `volatile` message writes bypass WAL and exist only in the resident room actor plus current subscriber delivery. `volatile` record writes bypass WAL and exist only in schema-declared record hot state; `disk`, `chatLog`, and object-backed tables reject them. `volatile` events also bypass WAL and only flow through the connection layer. Room volatile events target active sessions subscribed to the room and return that delivered session count. Generic user volatile events target active sessions for the logical user. Realtime channel volatile events add a session-scope filter on top of the logical user target, so a channel state update, signal, broadcast, or membership notification reaches only the sessions that joined the channel unless the member intentionally joined without a `sessionId`.

Durable writes can carry `clientMutationId`. Before appending WAL, the server scans committed WAL records for that id. If it already exists for the same mutation kind, the original response is reconstructed from WAL and returned without appending another record. This makes message sends, record upserts/deletes, and record transactions retry-safe across process restarts. Reusing the same id for a different mutation kind returns `409`.

No-op durable writes are recorded too when they carry a `clientMutationId`. A delete of an already-missing record and a transaction whose operations all reduce to no-ops append a `ClientMutationRecorded` WAL marker. The marker is visible to audit and idempotency lookup, but it does not update record projections, indexes, object references, sync output, or subscriptions. This prevents a lost-response retry from deleting or changing data that appeared after the original no-op.

## WAL Sharding

Single-node WAL sharding is controlled by:

```text
NEXTDB_WAL_SHARDS=1
NEXTDB_WAL_BATCH_MAX=1024
NEXTDB_WAL_BATCH_WAIT_MS=2
```

Shard files live at:

```text
data/wal/shard-0000.jsonl  # binary framed WAL; legacy JSONL remains readable
data/wal/shard-0001.jsonl
...
```

The server allocates a global monotonically increasing LSN before sending a record to its shard worker. Messages route by `room_id`; generic records route by `table:key`; object commits route by object id. Each shard still does local group commit and checkpoint compaction. Audit, durable sync, projection rebuild, startup projection repair, and object-reference repair read every shard plus its archive directory and merge records by LSN.

This shard boundary is also the input to shard ownership, replica placement, and shard-local recovery.

`NEXTDB_WAL_BATCH_MAX` and `NEXTDB_WAL_BATCH_WAIT_MS` control the shard worker's group-commit window. The default keeps a small 2ms coalescing delay so concurrent writes share one local write and one strict `sync_data`; setting the wait to `0` favors append latency, while increasing it favors throughput under bursty write load. The active values are included in each shard's health status and Prometheus metrics beside queue depth and local write timing.

Automatic checkpointing is triggered from write paths but executed as a single background task. When `NEXTDB_CHECKPOINT_EVERY_LSN` is reached, the request that notices the threshold only schedules the checkpoint if none is already running; it does not synchronously write the actor snapshot or compact WAL archives. Explicit snapshot and prepare-restart APIs still wait for their snapshot work to finish.

## Cluster Ownership

NextDB exposes an explicit single-owner topology for WAL shards. It is configured separately from the number of local WAL files:

```text
NEXTDB_NODE_ID=node-a
NEXTDB_NODE_URL=http://127.0.0.1:3188
NEXTDB_CLUSTER_NODES=node-a=http://127.0.0.1:3188,node-b=http://127.0.0.1:3189
NEXTDB_WAL_SHARDS=4
NEXTDB_SHARD_OWNERS=0-1=node-a,2-3=node-b
NEXTDB_SHARD_EPOCHS=0-1=1,2-3=2
NEXTDB_SHARD_REPLICAS=0-1=node-b;2-3=node-a
NEXTDB_ENFORCE_SHARD_OWNERSHIP=true
NEXTDB_WAL_REMOTE_ACKS=quorum
```

API:

```text
GET /v1/cluster/topology
GET /v1/cluster/route?roomId=general
GET /v1/cluster/route?table=rooms&recordKey=general
GET /v1/cluster/route?objectId=...
POST /v1/admin/cluster/shards/{shard}/freeze
POST /v1/admin/cluster/shards/{shard}/unfreeze
POST /v1/admin/cluster/handoff/plan
GET  /v1/admin/cluster/handoff/workflows
POST /v1/admin/cluster/handoff/workflows
POST /v1/admin/cluster/handoff/workflows/{workflow_id}/step
POST /v1/admin/cluster/handoff/workflows/{workflow_id}/auto
POST /v1/admin/cluster/handoff/workflows/{workflow_id}/abort
POST /v1/admin/cluster/handoff/workflows/{workflow_id}/apply
GET  /v1/admin/cluster/topology/overrides
POST /v1/admin/cluster/topology/overrides
GET  /v1/admin/cluster/topology/log
GET  /v1/admin/cluster/topology/proposals
POST /v1/admin/cluster/topology/proposals
POST /v1/admin/cluster/topology/proposals/{proposal_id}/commit
POST /v1/admin/cluster/topology/proposals/{proposal_id}/retry
POST /v1/admin/cluster/topology/proposals/{proposal_id}/abort
POST /v1/admin/cluster/topology/lease/cleanup
POST /v1/admin/cluster/topology/proposals/prepare
POST /v1/admin/cluster/topology/proposals/commit
POST /v1/admin/cluster/topology/proposals/abort
```

The routing key is the same key used by the WAL path: messages route by `room_id`, records by `table:key`, and object commits by object id. The route response returns the shard id, shard epoch, owner id, owner URL, replica ids, replica URLs, local role, and whether the local node accepts direct writes.

If `NEXTDB_ENFORCE_SHARD_OWNERSHIP=true`, `sendMessage`, `upsertRecord`, object upload, and schema apply reject direct writes for shards whose owner is not the local node. Schema apply is treated as a global control write on shard 0, so a non-owner returns `409` with the current `ownerUrl` before mutating schema files or projections. Before the owner commits the schema, it asks shard-0 WAL replicas to validate the candidate through `/v1/admin/schema/preflight`; the required successful preflight count follows the shard's `NEXTDB_WAL_REMOTE_ACKS` policy. WAL replication remains accepted through `/v1/admin/wal/replicate`, which is how an owner mirrors committed facts to replica nodes.

Every owner-written WAL record carries:

```text
shard
shardEpoch
ownerNodeId
```

Replica nodes validate those fields before accepting `/v1/admin/wal/replicate`. A record is rejected when its shard id differs from the request shard, its epoch differs from `NEXTDB_SHARD_EPOCHS`, or its owner differs from `NEXTDB_SHARD_OWNERS`. This is the first fencing layer: after a manual ownership transfer, replicas can reject stale writes from an old owner running an old epoch.

Handoff is represented as a runbook-backed management flow:

```text
freeze shard on current owner
wait for target replica highestAckedLsn >= current shard LSN
generate next epoch and target owner env overrides
restart or reconfigure nodes with the new owner, epoch, and replicas
unfreeze the shard on the new owner
```

Frozen shards reject direct writes with `423 Locked`; WAL replication remains available so a current owner can drain committed facts to replicas before the ownership switch. The handoff plan endpoint does not mutate cluster ownership. It computes readiness from WAL/replica status and returns the required `NEXTDB_SHARD_OWNERS`, `NEXTDB_SHARD_EPOCHS`, and `NEXTDB_SHARD_REPLICAS` overrides.

The workflow endpoint is the first coordinator surface. It persists workflow state under `data/cluster/handoff-workflows.json`, freezes the shard when started, and advances by polling replica ack state:

```text
waitingForCatchUp -> readyToReconfigure -> applied
```

`auto` is the idempotent operator-loop primitive for online handoff. It first performs the same polling update as `step`. If the target replica has not acknowledged the current shard LSN, it persists the refreshed `waitingForCatchUp` workflow and returns `applied=false`. If the workflow is ready, it immediately runs the existing `apply` path and returns `applied=true` with the topology proposal propagation result. This lets an external controller safely call one endpoint until the transfer commits without bypassing freeze, replica catch-up, epoch bumping, proposal quorum, or topology-log persistence.

`npm run test:cluster-handoff` is the local end-to-end proof for this path. It launches two temporary nodes, verifies owner-to-replica WAL mirroring, calls workflow `auto`, checks that both nodes commit the epoch/owner override, then verifies the new owner can write and replicate back to the old owner. The same smoke writes object blobs before and after handoff and reads metadata plus body bytes from the opposite node, proving object byte replication and `objectCommitted` WAL replication follow the runtime topology.

`NEXTDB_HANDOFF_CONTROLLER_INTERVAL_MS` enables the same flow as an in-process controller. The default `0` keeps the controller disabled. When enabled, the node periodically selects the oldest `waitingForCatchUp` or `readyToReconfigure` workflow and runs the same `auto` transition. `/v1/health.handoffController` exposes whether the loop is enabled, its interval, the last processed workflow, the last applied workflow, and the last error. `npm run test:cluster-handoff-controller` verifies that a node can complete the A -> B handoff without an external `/auto` call after the workflow is created.

`NEXTDB_PEER_MONITOR_INTERVAL_MS` enables the failure-detection observation layer. The default `0` keeps it disabled. When enabled, the node polls peer `/v1/health` endpoints from the current topology and materializes `/v1/health.peerHealth`: per-peer reachability, HTTP status, `acceptingWrites`, peer LSN, latency, last check, last successful check, and last error. The handoff smoke enables this on both temporary nodes and requires each node to observe the other before it starts the replication and handoff assertions.

Failover reuses the same topology proposal machinery, but starts from failure detection instead of an operator freeze. `POST /v1/admin/cluster/failover/plan` is evaluated on the target node. The target defaults to the local node and must already be a replica for the shard. The plan is ready only when peer health has observed the current owner, the owner is currently unhealthy, and the local WAL LSN is at least the owner's `lastSeenOkLsn`. The response includes the exact `ApplyTopologyOverrideRequest` that would promote the target owner, bump the epoch, and move the old owner into the replica set.

`POST /v1/admin/cluster/failover/proposals` turns a ready plan into a persisted topology proposal. It returns the proposal object even when prepare quorum cannot be reached. In a two-node `A(owner) -> B(replica)` failure, B can prove it has caught up to A's last healthy LSN, but B cannot collect a majority while A is down; the proposal is therefore recorded as `failed` and ownership remains unchanged. This keeps failover observable and auditable without introducing an implicit split-brain escape hatch. `npm run test:cluster-failover-plan` verifies this behavior by stopping A, waiting for B's peer monitor to mark A unhealthy, creating the failover proposal on B, and checking that B's topology still names A as owner.

`NEXTDB_FAILOVER_CONTROLLER_INTERVAL_MS` enables automatic failover election around the same primitive. The loop is node-local and intended for replica nodes: on each tick it computes a local failover plan for each shard, skips non-ready plans, and creates a failover topology proposal for the first ready shard unless a prepared or failed failover proposal for that shard already exists. If the proposal reaches the prepared phase, the controller immediately runs the normal topology commit path. `/v1/health.failoverController` exposes the interval, last checked shard, last proposal id, last committed proposal id, and last error. The controller still does not bypass quorum or lease rules: `npm run test:cluster-failover-controller` verifies that B automatically records a failed proposal in a two-node owner failure, while `npm run test:cluster-failover-election` verifies that B automatically commits ownership in a three-node `A(owner), B/C(replicas)` layout when C forms a majority with B.

`apply` runs a two-phase topology proposal. The coordinator first acquires a local topology lease with a monotonically increasing term, persists the proposal locally, and prepares it on peers through `/v1/admin/cluster/topology/proposals/prepare`. Peers persist the term in `data/cluster/topology-lease.json`, reject stale terms, and reject a conflicting coordinator while an unexpired same-term lease is held. The proposal requires a majority of prepare acknowledgements before commit. Commit then calls `/v1/admin/cluster/topology/proposals/commit` on peers and commits locally. If prepare quorum or commit cannot be reached, the proposal is marked failed, the matching lease is released, and the local owner/epoch/replicas remain unchanged.

On commit, each node appends a control event to `data/cluster/topology-log.jsonl` and writes the latest materialized snapshot to `data/cluster/topology-overrides.json`. Owner, epoch, replicas, route, direct-write gating, replica fencing, and WAL remote replica targets switch without a process restart. Startup loads the snapshot and replays the append-only topology log, so deleting the snapshot does not lose the control-plane state. In the common `A -> B` handoff, the overlay makes `B` the owner and `A` a replica, so new writes from `B` replicate back to `A`. The apply response includes per-node commit results. `POST /v1/admin/cluster/topology/overrides` is intentionally lower level and applies only to the node that receives it.

`GET /v1/admin/cluster/topology/log` returns the applied control events for audit and admin UI display. `GET /v1/admin/cluster/topology/proposals` returns prepared, committed, failed, and aborted proposal records from `data/cluster/topology-proposals.json`. `POST /v1/admin/cluster/topology/proposals/{proposal_id}/retry` turns a failed proposal into a fresh proposal with a new term. `POST /v1/admin/cluster/topology/proposals/{proposal_id}/abort` marks a prepared proposal aborted, releases its matching lease, and propagates abort to peers. `POST /v1/admin/cluster/topology/lease/cleanup` clears an expired holder/proposal from `data/cluster/topology-lease.json` while preserving `currentTerm`. `NEXTDB_TOPOLOGY_LEASE_MS` controls the proposal lease window and defaults to 30000 ms.

Workflow `abort` marks the workflow aborted and unfreezes the shard on the current node. Proposal abort is separate: it is the control-plane cancellation primitive for a prepared topology proposal. This is still an operator-assisted coordinator; the next layer is automatic coordinator election around expired leases.

## Runtime Draining and Rolling Restart

Process-level draining is separate from shard freeze. `POST /v1/admin/runtime/drain` toggles a node-local readiness gate and `GET /v1/health` exposes `draining`, `acceptingWrites`, the current drain reason, and `runtimeWrites` for in-flight durable write visibility. `POST /v1/admin/runtime/prepare-restart` is the higher-level operator primitive: it sets drain, waits for in-flight durable writes to quiesce, writes a runtime snapshot at the current LSN by default, and can optionally run WAL compaction after that snapshot. The default write wait is 10 seconds and can be overridden with `waitForWritesMs`. If the wait times out, the response reports `readyForRestart=false`, `writeWaitTimedOut=true`, and skips snapshot/compaction rather than presenting a busy node as restart-ready. Ctrl-C and SIGTERM run the same drain-and-snapshot preparation before the HTTP server exits, giving process managers a safe shutdown fallback. While draining, the node continues to serve reads, schema, audit, admin, WAL replication, object replication, and already-open realtime sessions. It rejects new direct writes, object uploads, behavior invocations, realtime channel mutations, and new WebSocket/JSONL realtime connections with `503` and `draining=true`. `npm run test:runtime-drain-connection` verifies this with a real process and both connection entry points.

This gives operators a rolling-restart sequence:

```text
prepare-restart or set draining=true
wait for load balancers and SDK clients to move new work away
stop or restart the process after in-flight writes complete
start the process, replay WAL/snapshots/projections
set draining=false
```

The prepare response includes the updated drain state, runtime write counters, whether writes quiesced, whether the wait timed out, the snapshot LSN, resident room count, durable record-hot table count, durable record-hot record count when snapshotting is enabled, optional WAL compaction totals, `readyForRestart`, and the node's current LSN. This makes restart readiness observable from the Admin UI and SDK without stitching together multiple admin calls.

The TypeScript SDK treats an owner-conflict `409` with `ownerUrl` and a draining `503` as retryable topology signals. Owner conflicts are retried once against the reported owner, including schema apply control writes on shard 0. Schema apply responses include `peerPreflight` when the owner asked replicas to validate the candidate before commit. For draining recovery, it discovers peer endpoints from explicit `replicaEndpoints`, `NEXTDB_CLUSTER_NODES` surfaced through health, shard owner/replica URLs, and WAL/object replica URLs. HTTP writes retry once against a peer whose health reports `acceptingWrites=true`. Realtime transport reconnects run the same endpoint recovery before opening the next connection, so room/table/user subscriptions can move away from a draining node without changing the durable subscription contract.

`GET /v1/metrics` exposes a Prometheus-compatible text snapshot for external monitoring. It reports process up/readiness, current/snapshot/compaction LSNs, in-flight durable writes, room/record/object counts, connection and realtime-channel counts, WAL shard and remote-replica status, backup run/controller state, handoff/failover controller state, WAL/object repair controller state, and peer monitor state. The TypeScript SDK exposes the same endpoint as `db.metrics()` for operator tooling that wants to archive or forward the raw scrape text.

Realtime fan-out uses a two-stage delivery path. The global fan-out registry indexes candidate sessions by room, full table, table range, nested-table prefix, live-query table, user-event user id, and object subscription, then enqueues targeted per-session batches before connection-local RLS and live-query refresh checks. Targeted batches carry shared `Arc<DeliveryEvent>` values, so a large record, object, or realtime payload selected for many sessions is allocated once by the registry and cloned only as a cheap reference until the connection decides it is actually deliverable. After one connection drains a realtime event batch, it batches the resulting event, live-query, and lag/error server frames through the transport-neutral sink and flushes WebSocket/JSONL output once while preserving frame order. Before that sink write, the server serializes the batch into `EncodedServerFrame` values backed by shared bytes, so encoded frames can be cloned and reused by multiple sinks without re-running JSON serialization. Event frames are encoded from borrowed `DeliveryEvent` references after connection-local RLS filtering, avoiding full event-payload clones on the connection hot path. When multiple candidate sessions receive an identical event list, the registry now attaches one shared pre-encoded event frame to those routed batches; connection-local processing reuses it when the whole batch is visible and falls back to per-connection filtering and encoding when RLS, query-only refresh, or partial visibility changes the deliverable frame.

Runtime write limits are configured with `NEXTDB_MAX_OBJECT_BYTES`, `NEXTDB_MAX_MESSAGE_BYTES`, `NEXTDB_MAX_USER_EVENT_BYTES`, and `NEXTDB_MAX_RECORD_VALUE_BYTES`. Realtime live-query fanout is bounded separately with `NEXTDB_MAX_LIVE_QUERIES_PER_CONNECTION`, `NEXTDB_MAX_LIVE_QUERIES_PER_TABLE_PER_CONNECTION`, and `NEXTDB_MAX_LIVE_QUERIES_PER_USER`; these checks run before a query result is evaluated, so rejected subscriptions do not create snapshots, refresh work, or connection-registry state. Payload checks run before expensive validation, object-store writes, and WAL append on HTTP and behavior-runtime writes. Volatile realtime signals and broadcasts are checked before channel sequence allocation, so rejected frames do not create timeline gaps. A limit of `0` disables the specific check. `/v1/health.limits` exposes the structured values for SDK/UI consumers, and `/v1/metrics` exports the same values as gauges for deployment drift detection.

## Client Subscription Registry

The SDK local cache owns more than materialized rows and pending writes. Persistent subscriptions are stored as a registry in the same `NextDbLocalCache` implementation, backed by memory in non-browser runtimes and IndexedDB in browsers. A caller opts in with `persistent: true` on room, table, nested-table, live-query, object, user-event, or watcher subscriptions. Nested-table registry entries preserve `{table, parentKey, nested}` instead of flattening the application intent into only the logical table name; restore, diagnostics, local cache management, WebSocket frames, and WAL catch-up all keep the parent partition identity. `restoreSubscriptions()` reloads that registry in a fresh SDK instance, reconnects each target using the stored options, and uses the cached per-target LSN cursors for catch-up. `autoRestoreSubscriptions: true` runs the same restore path immediately after client construction, so application boot can delegate subscription recovery to the SDK. This lets applications treat subscriptions as durable client data-layer state instead of page-local listener setup. `clearStoredSubscriptions()` removes that durable intent and cancels any restored room, table, nested-table, live-query, user-event, or object feed that is not still held by a runtime listener; if restore has queued subscribe frames but the transport has not opened, those pending frames are dropped rather than reconnecting stale intent. Server-driven cache invalidation clears cached projections but preserves the subscription registry, while explicit `clearCache()` removes the registry along with cached rows and pending writes.

`localDataStatus()` is the SDK's observability surface for that local data layer. It summarizes the active endpoint, configured realtime transport kind, active realtime transport kind, configured and active connection transports, realtime transport state, last seen global/object/room/user/table LSNs, cache statistics, pending write counts, stored subscriptions, active runtime subscriptions including nested table partitions, persistent subscriptions, runtime realtime channel state versions, cache metadata, and the current cache profile. This gives product UIs and diagnostics a single API for explaining whether data is coming from local projection, pending offline state, restored subscriptions, a transport fallback, or the live transport. The Admin UI includes an "Admin Local Data" panel for the Admin page's own SDK instance; it is intentionally separate from server-wide cache policy because client caches are per browser/runtime. The panel can inspect auto-flush state, flush pending writes, reset or discard one pending write, clear only pending writes, restore the stored subscription registry, clear only stored subscriptions, or clear the Admin page's full local cache without touching server data.

## Live Query Subscriptions

Room, table, user, and object subscriptions deliver durable or volatile events. Live query subscriptions add a server-evaluated result surface:

```text
ClientFrame.subscribeQuery
ServerFrame.queryResult
```

The first live query shapes are record list queries and schema-declared secondary-index queries over a top-level table or a nested parent partition. On subscribe, the server validates the target against the active schema and sends the current `ListRecordsResponse` with the node's current LSN and a `resultId` fingerprint over page shape and record content. The result fingerprint streams each record value's JSON encoding directly into the SHA-256 hasher, avoiding a per-record value buffer during refresh. The subscription stores its logical table, nested parent key prefix, schema version, current page key set, and a precomputed impact filter for predicates and exact secondary-index values. Each connection also indexes subscribed query ids by logical table, so a record event only scans live queries for that table before checking the query's precomputed fields and deciding whether to re-read the projection; non-matching same-table query ids are filtered before cloning the affected id list. A connection wake-up drains an already queued delivery-event micro-batch; `NEXTDB_REALTIME_EVENT_BATCH_MAX` controls the per-wake limit and defaults to 128. Normal subscribed events are still sent to the client one by one in delivery order, but live query refresh candidates are accumulated and deduplicated by query id before refresh. During refresh, subscriptions with the same query shape share a batch-local projection result, so a normal result subscription and a diff subscription over the same index page do not re-read the same record projection twice. Initial/resume subscriptions and record-event batches can also use a bounded node-level evaluation cache keyed by `current_lsn + scoped volatile generation + query shape`, allowing separate connections that observe the same durable or volatile page to share an already computed result. Record hot state keeps per-table and key-prefix volatile counters/generations, so volatile upserts, updates, deletes, or evictions invalidate only the affected table or nested parent cache scope. The connection temporarily moves the affected subscription state out of the map, updates it in place, and reinserts it, avoiding a clone of the previous page response and key set. Upsert events that cannot affect a query are skipped when the key is not in the current page key set and the new record fails the query predicate or exact secondary-index value; if the schema version changes, subscriptions conservatively refresh instead of trusting stale index metadata. Matching queries are re-read from the same record or index projection used by HTTP reads. For hot tables, list, predicate, and exact/range-index live queries evaluate the disk projection plus the current record hot overlay, so volatile records can enter or leave server-evaluated query pages even though they never enter WAL, sync, audit, or persistent index storage. Returned durable rows are batch-rehydrated into record hot state just like HTTP reads, making live query subscription a bounded activation path rather than a passive cache fill. A fresh `queryResult` frame is sent only when the result fingerprint changes, so non-matching writes in the same table do not fan out to indexed live queries, while same-key volatile value updates still produce `updated` diffs.

Clients can resume a live query with a previously seen `resultId`. If the recomputed fingerprint still matches, the server returns `queryUnchanged` with the current LSN instead of a full `queryResult` page. The TypeScript SDK remembers result fingerprints and includes them when reconnecting active live queries, reducing reconnect bandwidth while preserving the same server-evaluated result contract.

This is stricter than event-only subscription: the client can render the server's current answer directly instead of reconstructing a list from individual deltas. The SDK still writes records from each query result into its local cache, so cache-backed watchers and server-backed live queries share the same storage boundary. When a table has an active volatile record overlay, the SDK does not seed a live-query resume baseline from local cached pages for that table; it asks the server for a fresh current-state result because volatile rows are intentionally not stored in the durable client cache. The MVP supports list pagination inputs, nested `order="schema"` clustering order, index exact/range options using the same `value` / `values` / `lower` / `upper` / `lowerValues` / `upperValues` contract as HTTP index reads, and deterministic JSON predicates over record values. Predicates are `all` term lists with `field`, `op`, and optional `value`; index predicates scan the selected index access path before filtering.

Live query subscriptions can opt into incremental `queryDiff` frames. The first response is still a full `queryResult` baseline. Later changed results send added records, updated records, removed records, the new ordered key list, pagination cursors, `hasMore`, `currentLsn`, and the new `resultId`. The server computes the diff with borrowed-key hash lookups over the previous and next pages, so it does not clone every key just to detect added, updated, and removed rows. Removed records carry `deleted=true`, deletion `lsn`, and `deletedAtMs` only when the triggering fact was a record tombstone; records that merely leave the result because of an update, predicate change, or page boundary remain `deleted=false`. Raw WebSocket clients that omit `diff` keep the older full-result refresh behavior and the server keeps only their `resultId` plus current page key set, not the full previous page response. The TypeScript SDK requests diffs by default, merges each diff with its in-memory query baseline, updates the shared record cache for added/updated records, clears cached records only for deleted removed entries, and still calls live-query listeners with a complete `response` plus optional `diff` metadata. A resume `resultId` is only sent when the SDK also has the matching in-memory query baseline. If a restored subscription or caller-supplied `resultId` has no baseline, the SDK asks for a full result first; if a diff ever arrives without a baseline, it is discarded and the SDK immediately resubscribes without `resultId`.

The first aggregate actor-family slices maintain subscribable table counts, numeric field sums, and realtime channel presence counts. A `subscribeAggregateCount` frame hydrates the table key set from the durable record projection and returns `aggregateCountSubscribed` with `{ table, count, currentLsn }`. A `subscribeAggregateSum` frame hydrates per-key numeric values for one table field and returns `aggregateSumSubscribed` with `{ table, field, sum, currentLsn }`; non-numeric or missing field values contribute nothing. After that, the aggregate registry consumes the same `DeliveryEvent` stream as realtime fan-out: `RecordUpserted` inserts or replaces the key's aggregate contribution, `RecordDeleted` removes it, duplicate count upserts do not change the count, and subscribers receive `aggregateCountUpdated` or `aggregateSumUpdated` frames with the new value and event LSN. A `subscribeAggregatePresence` frame hydrates from realtime channel membership and returns `aggregatePresenceSubscribed` with `{ channelId, memberCount, userCount, currentLsn, updatedAtMs }`; later joins, explicit leaves, disconnect cleanup, and stale-session maintenance publish `aggregatePresenceUpdated`. Presence aggregates are runtime state over the connection layer, not WAL facts. The TypeScript SDK exposes these as `db.subscribeAggregateCount(table, listener)`, `db.subscribeAggregateSum(table, field, listener)`, and `db.subscribeAggregatePresence(channelId, listener)`, and replays active aggregate subscriptions after reconnect.

`/v1/health.liveQueries` and `/v1/metrics` expose the live-query control loop as counters: current subscriptions, event batch max, subscribe/unsubscribe totals, event batches, batched events, refresh candidates before dedupe, actual refresh attempts, actual query executions, evaluation cache hits, full-result frames, diff frames, unchanged suppressions, and refresh errors. These metrics separate "an event could have caused a query re-read" from "a query id needed refresh", "the projection was actually executed", "a node-level result was reused", and "a frame was actually sent", which is the signal needed to tune partitioning, secondary indexes, predicates, hot-table policies, query-shape fanout, and micro-batch sizing without guessing from client traffic alone.

If `NEXTDB_WAL_REMOTE_REPLICAS` is omitted, the owner derives each shard's remote WAL mirrors from `NEXTDB_SHARD_REPLICAS` and `NEXTDB_CLUSTER_NODES`. `NEXTDB_WAL_REMOTE_ACKS` controls write confirmation:

```text
all      require every remote WAL mirror before acknowledging the write
quorum   require local owner plus enough remote mirrors for a majority
none     acknowledge after local WAL write while recording remote failures
N        require N remote mirror acknowledgements
```

Health exposes the active epoch and policy per shard, required remote acks, highest acked LSN per remote mirror, last attempt/success/error timestamps, and the last error text. That makes replication lag, stale-owner writes, and remote failures visible to the admin UI and SDK. `POST /v1/admin/wal/replicate/repair?shard=N` replays this owner's active and archived WAL records after each remote's last acked LSN through the same `/v1/admin/wal/replicate` receiver. It is the operator repair primitive for a quorum-acknowledged write that succeeded while a non-quorum replica was temporarily down. `NEXTDB_WAL_REPAIR_CONTROLLER_INTERVAL_MS` enables the same repair loop in-process on owner nodes; `/v1/health.walRepairController` reports its interval, last repaired shards, records sent, repaired replica count, satisfaction status, and last error. `npm run test:cluster-wal-repair-controller` verifies that a restarted replica catches up without an external repair call.

This is deliberately smaller than Raft. It gives the database concrete owner/replica routing, write-ack policy, epoch fencing, online handoff, quorum-bound failover election, read quorum routing, and remote catch-up repair now, while leaving full replicated-log consensus and rollback as later distributed-systems layers.

## WAL Replica Mirrors

Local WAL replica mirrors are controlled by:

```text
NEXTDB_WAL_REPLICA_DIRS=/mnt/replica-a,/mnt/replica-b
```

For each primary shard:

```text
data/wal/shard-0001.jsonl
```

NextDB writes matching replica files:

```text
/mnt/replica-a/wal/shard-0001.jsonl
/mnt/replica-b/wal/shard-0001.jsonl
```

The WAL worker writes the encoded batch to primary and every configured replica before acknowledging the mutations. `strict` durability also syncs every replica file for that batch. Checkpoint compaction archives primary and replica segments together, and startup can restore a missing primary shard plus archive files from the first available replica.

This is local mirror replication, not a distributed quorum protocol. It gives the WAL layer a real replica contract and a recovery path while leaving networked placement, quorum reads/writes, and rebalancing for the distributed ownership layer.

## Network WAL Replicas

Synchronous HTTP WAL replicas are controlled by:

```text
NEXTDB_WAL_REMOTE_REPLICAS=http://127.0.0.1:3189
NEXTDB_WAL_REPLICATION_TOKEN=shared-secret
```

`NEXTDB_WAL_REMOTE_REPLICAS` accepts comma-separated node base URLs or full `/v1/admin/wal/replicate` endpoints. When configured, each primary shard worker posts the same committed WAL batch to each remote endpoint before acknowledging the append. The remote node preserves the original LSN, writes the records through its own shard worker, skips already-present LSNs, and updates chat-log, generic-record, object-reference, actor, and subscription projections for replicated message and record events.

If `NEXTDB_WAL_REPLICATION_TOKEN` is set on the receiver, the sender must provide it as a bearer token or `x-nextdb-replication-token`. The primary uses the same env var for outgoing remote replica requests.

This is synchronous remote mirroring under the current ownership map and selected remote-ack policy. A remote failure only blocks writes when the policy cannot be satisfied. Runtime handoff and quorum-bound failover election can change owner, epoch, replicas, and WAL remote targets without a restart. Read quorum merge semantics are implemented in the SDK for point reads, key-order lists, secondary-index exact/range reads, and object lists; the current design still does not provide consensus rollback.

## Object Blob Replication

Object blob replication is controlled by:

```text
NEXTDB_OBJECT_REMOTE_REPLICAS=http://127.0.0.1:3189
NEXTDB_OBJECT_REPLICATION_TOKEN=shared-secret
```

If `NEXTDB_OBJECT_REMOTE_REPLICAS` is not set, NextDB uses `NEXTDB_WAL_REMOTE_REPLICAS` when that explicit list exists; otherwise it derives object blob targets from the current runtime shard replica topology. This keeps object bytes and WAL facts on the same mirror nodes across handoff. If `NEXTDB_OBJECT_REPLICATION_TOKEN` is not set, it reuses `NEXTDB_WAL_REPLICATION_TOKEN`.

The upload path is:

```text
generate object id
check shard owner before writing blob
write local blob + metadata
replicate blob + metadata to remote object endpoints
append objectCommitted WAL record
replicate WAL record
acknowledge upload
```

Object blob replication follows the shard's `NEXTDB_WAL_REMOTE_ACKS` policy, so a `quorum` object upload can succeed while a non-quorum object replica is temporarily down. If the required object replication acknowledgements or local WAL append fail before `objectCommitted` is recorded, the server deletes the local blob and metadata before returning the error. This keeps local object visibility tied to the WAL fact. Remote-only orphan blobs can still exist after partial remote success followed by local WAL failure, so object GC/repair owns that cleanup.

Remote nodes accept object blobs through `/v1/admin/objects/replicate`, validate id/path/byte-size/sha256 against the supplied metadata, and store the same object id. This lets a remote WAL replica serve `GET /v1/objects/{id}/body` after it receives the matching `objectCommitted` WAL fact.

`POST /v1/admin/objects/repair?shard=N&objectId=...` pushes live local object metadata and body bytes to the current object remotes for the selected shard, using the same replication receiver and idempotent `stored=false` response for already-present blobs. `NEXTDB_OBJECT_REPAIR_CONTROLLER_INTERVAL_MS` enables the same loop in-process on owner nodes; `/v1/health.objectRepairController` reports the last repaired shards, objects sent, repaired replica count, satisfaction status, and last error. This complements WAL repair: WAL repair catches a restarted replica up to the `objectCommitted` fact, while object repair ensures the referenced blob bytes are present.

## Durable Sync

Client recovery uses WAL-derived durable events:

```text
GET /v1/sync/pull?afterLsn=100&rooms=general,random&tables=rooms&limit=500
GET /v1/sync/pull?afterLsn=100&nestedTables=rooms:general:messages&limit=500
GET /v1/sync/pull?afterLsn=100&users=alice&limit=500
GET /v1/sync/pull?afterLsn=100&objects=true&limit=500
```

  The response includes `events`, `nextAfterLsn`, `currentLsn`, and `hasMore`. With no scope filters, it emits all durable sync event categories. With any scope filter, it emits only selected categories: `MessageCreated` events for selected rooms, `UserUpserted` profile events and `UserEventPublished` inbox events for selected logical users, `RecordUpserted`/`RecordDeleted` events for selected tables, nested record events whose logical table and `{parentKey}:` key prefix match `nestedTables=table:parentKey:nested`, and object commit/delete metadata events when `objects=true`. A `RecordTransactionCommitted` WAL fact expands into per-record table events that share the transaction LSN; sync pagination does not split one transaction across pages. Message attachments and record values can contain typed `ObjectRef` metadata.

The TypeScript SDK tracks the highest seen LSN from:

```text
sendMessage responses
latest/before reads
realtime messageCreated events
realtime userEvent events
realtime recordUpserted events
realtime objectCommitted/objectDeleted events
syncPull responses
```

The SDK persists these cursors in the configured local cache:

```text
global cursor
objects cursor
room:<roomId> cursor
user:<userId> cursor
table:<table> cursor
nested:<logicalTable>:<parentKey> cursor
```

`cacheCoverage()` and `localDataStatus().coverage` expose that local ownership boundary as a structured report. The report combines cached object/message/user/record counts, persisted cursors, queued offline writes, active runtime subscriptions, stored persistent subscription intents, and volatile realtime channel runtime projections. For objects, rooms, users, top-level tables, and nested parent partitions it reports durable cache coverage; for realtime channels it reports joined state, member snapshots, and bounded recent event/signal windows. This gives application shells and the Admin UI a direct way to decide what can be rendered from local state, what needs catch-up, which partitions are safe to trim or invalidate, and which volatile channel state is currently owned by this client.

`syncPull` fetches one page. `syncUntilCaughtUp`, `syncObjects()`, `syncCurrentUserEvents()`, `room.messages.sync()`, `table.sync()`, and `nestedTable.sync()` keep paging until the server reports no more durable events or the configured `maxPages` is reached. Each page is applied to the local cache before listeners are notified. Nested-table sync advances the parent-partition cursor, not the whole logical table cursor, so one chat room cannot make another room appear caught up. The local cache stores durable object metadata, room messages, user profiles/events, and table records; volatile room/user events are deliberately not cached because they represent lossy presence, signaling, and game traffic.

`GET /v1/sync/wait?minLsn=...&timeoutMs=...` is the freshness gate for reads that need a known lower bound. The endpoint polls the node's applied `currentLsn` until it reaches `minLsn` or the bounded timeout expires, and returns `{ caughtUp, currentLsn, waitedMs, consistency, remoteRequiredAcks, remoteAcked, remoteCaughtUp }` without streaming events. For point reads on a known shard, `consistency=quorum|all` plus `shardKey=...` or `shard=...` also requires the shard's remote WAL acknowledgement state to reach `minLsn`. The SDK exposes this as `waitForLsn(minLsn, { timeoutMs, consistency, shardKey })`. Owners can use it as an explicit read-your-writes barrier, and clients or tests reading from replicas can wait until a follower has applied the LSN returned by a previous write before issuing a normal record/object read.

Freshness options are also wired directly into SDK read helpers. `table.get` and nested-table point reads accept `{ minLsn, timeoutMs, consistency }`, derive the shard route automatically for `quorum` or `all`, and read owner plus replica URLs in parallel. `quorum` requires a majority of routed endpoints to return a record satisfying `minLsn`, `all` requires every routed endpoint, and the SDK merges by selecting the highest record LSN before updating the local cache. Key-order `table.list`, including predicate-filtered lists, fans out across every topology shard, asks each shard's owner/replica set for `?shard=N`, requires a majority/all fresh page responses per shard, merges rows by key with highest-LSN wins, then applies the requested limit. Exact-match secondary-index reads use the same shard fanout path with `?shard=N` and merge by primary key. Secondary-index range reads also fan out per shard, merge by decoded index tuple plus primary key, and return a server-compatible range `nextCursor`. Room `latest` and `before` message reads derive the room shard route, require quorum/all fresh page responses from the owner/replica set, merge by message id, and return the newest messages by LSN. User profile point reads derive the user-id shard route, require quorum/all fresh profile responses from the owner/replica set, and cache the highest-LSN profile. Durable user inbox reads derive the user-id shard route, require quorum/all fresh event pages from the owner/replica set, merge events by event id with highest-LSN wins, and update the SDK user-event cache. User directory reads fan out across user-id shards with `?shard=N`, require quorum/all fresh page responses per shard, merge profiles by user id with highest-LSN wins, and update the SDK profile cache. Object list reads fan out across object-id shards, require quorum/all fresh page responses per shard, merge metadata by object id, and return the merged `nextAfterId`. Object metadata, full body, and byte-range body reads derive the object shard route and require quorum/all responses from the owner/replica set; full body reads validate byte size and SHA-256 against the quorum metadata before caching the blob, while byte-range reads validate content range, total byte size, content type, and returned byte count against quorum metadata. Cross-table reads accept local freshness only unless the caller performs an explicit `waitForLsn` against a selected shard first. For cache-backed record/message/user/object pages the SDK only returns cached data when the cached item LSNs satisfy the requested lower bound where an LSN is available; otherwise it performs the normal server read and updates the cache. Record list and index cache hits are also disabled for any table with an active volatile record overlay, because those overlay rows are current server memory-state but are not durable cache rows. Object metadata does not carry an LSN, so object reads with `minLsn` first validate each endpoint through local `sync/wait` before accepting that endpoint's metadata or body. This keeps the local cache fast by default while making read-your-writes explicit where the caller needs it.

Realtime room, user, table, nested-table, and object subscriptions can also carry `afterLsn` plus a bounded `catchUpLimit`. `subscribeNestedTable` carries `{table, parentKey, nested}` and the server filters both live record events and retained WAL catch-up by the logical nested table plus `{parentKey}:` key prefix. On subscribe, the server registers the live subscription, reads retained WAL and archive records for that target, sends matching durable events, then sends a `subscriptionCatchUp` frame with `nextAfterLsn`, `currentLsn`, `hasMore`, and the nested targets that were caught up. Live events that arrive during the catch-up stay buffered in the connection's broadcast receiver and are de-duplicated by the SDK's LSN cursors.

The SDK enables subscription catch-up by default. `room.messages.subscribe(listener)`, `onUserEvent(listener)`, `table.subscribe(listener)`, `nestedTable.subscribe(listener)`, and `subscribeObjects(listener)` hydrate the target cursor from local cache, send it as `afterLsn`, apply catch-up events to cache, and notify listeners. Nested subscriptions use the `nested:<logicalTable>:<parentKey>` cursor for `afterLsn`; the logical table cursor is only used for whole-table subscriptions. `onUserEvent` receives durable `userUpserted` profile events, durable inbox `userEvent` events, and volatile user-targeted signals. `subscribeObjects` receives durable `objectCommitted` / `objectDeleted` metadata events. `listCurrentUserEvents` / `listUserEvents` read the cached durable user-event inbox, and `watchCurrentUserEvents` emits the current cached inbox whenever publish, realtime catch-up, sync, or invalidation changes it. If the server reports `hasMore`, the SDK continues with HTTP `syncUntilCaughtUp` from `nextAfterLsn`; nested catch-up continues with the same `nestedTables` parent-partition filter instead of broadening to the logical table. Use `{ catchUp: false }` when a caller explicitly wants only future realtime events.

When the last runtime `onUserEvent` listener is removed and no persistent user-event subscription remains, the SDK sends `unsubscribeUserEvents` so the server-side connection registry clears the user inbox feed flag. `subscribeObjects` already has the same lifecycle through `unsubscribeObjects`; both unsubscribe paths also clear pending SDK subscription intents if a listener is removed before the socket opens.

When a realtime transport reconnects or receives a lag warning, the SDK hydrates persisted cursors, pulls missed room, user, table, nested-table, and object events for active subscriptions with the same catch-up loop, then resubscribes. This is the first hard boundary between reliable durable sync and lossy realtime transport events.

## Offline Write Queue

The TypeScript SDK can own offline writes when `offlineWrites: true` is set:

```ts
const db = new NextDbClient({
  endpoint: "http://127.0.0.1:3188",
  userId: "alice",
  offlineWrites: true,
  autoFlushPendingWrites: true,
})
```

If `sendMessage`, durable user profile upsert, durable user event publish, top-level record upsert/delete, nested record upsert/delete, record transaction, object upload, or object delete fails because the network request cannot reach the server, the SDK stores a pending write in the local cache. Pending writes are persisted in memory or IndexedDB depending on the configured cache. The call returns an optimistic local object or delete response with `lsn: 0`; this is explicitly not a committed database fact. Durable user event publishes return an uncommitted event to the caller but wait for flush before caching an inbox row, because the committed event id is assigned by the server. Record transactions stay queued as one atomic pending write and return `{ lsn: 0, operations: [] }` until flush commits the server transaction result into the local record cache.

Recovery can be explicit:

```ts
await db.flushPendingWrites()
```

or SDK-managed with `autoFlushPendingWrites`. Auto flush schedules a retry when the client starts, when a pending write is queued, and when the realtime transport opens after a reconnect. Only one flush runs at a time, so application-triggered and SDK-triggered recovery cannot submit the same pending write concurrently.

Flush submits pending writes in creation order. Successful message flushes remove the optimistic pending message from cache and replace it with the committed server message. Record and nested-record upserts share the same logical key, so the committed record overwrites the optimistic record. Object uploads keep a client-preallocated `objectId`; this lets a queued message attach an offline object without needing a later id remap. Nested pending writes retain `table`, `parentKey`, `nested`, and `nestedKey`; flush replays them through the nested records API so parent-partition routing and schema validation remain intact. Validation and other server-side errors stay on the pending write as `lastError`; network failures stop the current flush so the queue can retry later without reordering subsequent writes. Flush results expose `retryable` per error; automatic flush keeps retrying only while retryable failures remain.

The pending queue is also an SDK-managed storage surface. `pendingWriteQueueStatus()` returns a bounded view of queued writes plus aggregate counts and auto-flush state. `resetPendingWrite(id)` clears one write's attempts and `lastError` without moving it in the queue. `discardPendingWrite(id)` removes a single queued write, and `discardPendingWrite(id, { removeOptimistic: true })` also removes SDK-created optimistic placeholders when the current cache still identifies them as uncommitted `lsn: 0` rows, pending messages, or pending object uploads. Offline deletes and overwritten previous values cannot be reconstructed unless the application kept its own before-image, so discard is intentionally queue management rather than a general rollback system.

Record writes support compare-and-set preconditions:

```ts
const current = await db.table("rooms").get("general")
await db.table("rooms").upsert("general", nextValue, {
  expectedLsn: current.lsn,
})
```

The server rejects mismatched record versions with `409`. When the SDK queues an offline record upsert, it stores the local cached record LSN as `expectedLsn`. During `flushPendingWrites`, that pending write is submitted with the saved baseline. If the server-side record changed while the client was offline, the pending write remains in the queue with a conflict `lastError` instead of overwriting the newer server value.

## WAL Audit

The audit API reads the durable event stream directly:

```text
GET /v1/audit/wal
GET /v1/audit/wal?afterLsn=100&limit=100
GET /v1/audit/wal?payloadType=messageCreated&roomId=general
GET /v1/audit/wal?payloadType=recordUpserted&table=rooms
GET /v1/audit/wal?objectId=...
GET /v1/audit/wal?table=rooms&recordKey=general
GET /v1/audit/wal?path=tables/rooms/general
GET /v1/audit/wal?clientMutationId=rooms-general-v1
GET /v1/audit/trace?kind=room&id=general
GET /v1/audit/trace?kind=record&table=rooms&recordKey=general
GET /v1/audit/trace?kind=nestedRecord&table=rooms&parentKey=general&nested=messages&nestedKey=msg-1
GET /v1/audit/trace?kind=object&id=...
GET /v1/audit/trace?kind=clientMutation&clientMutationId=rooms-general-v1
GET /v1/audit/replay?kind=record&table=rooms&recordKey=general&atLsn=42
GET /v1/audit/replay?kind=nestedRecord&table=rooms&parentKey=general&nested=messages&nestedKey=msg-1&atLsn=42
GET /v1/audit/replay?kind=user&id=alice&atLsn=42
GET /v1/audit/replay?kind=object&id=...&atLsn=42
```

The response includes `records`, `nextAfterLsn`, and `hasMore`. Records are raw `WalRecord` values, including `schemaVersion`, `durability`, `timestampMs`, and typed payloads. This is the first audit/tracing surface for event sourcing: projections and admin UI can page through committed facts without scanning chat-log buckets or actor snapshots. The read surface merges active WAL and archived checkpoint segments into one LSN-ordered stream. The trace endpoint adds an entity view over that same stream: room traces include the room record, its messages, and `rooms.messages` nested rows; user traces include profile, durable inbox events, and sent messages; object traces include object commit/delete plus message and record references; record, nested-record, path, and client-mutation traces isolate the exact logical row, path, or retry-safe mutation across regular WAL facts and transaction/no-op idempotency records. The replay endpoint is the state view over the same stream: for `record`, `nestedRecord`, `user`, and `object`, it scans committed facts through `atLsn` and returns `exists`, `deleted`, or `missing` plus the reconstructed entity/delete marker and `sourceLsn`. Generated TypeScript clients expose the same audit surfaces with schema-bound table names, nested table names, branded record keys, and typed replayed record values. Trace and replay are query surfaces over WAL, not second audit stores.

The logical export manifest is a WAL-derived preflight surface:

```text
GET /v1/admin/export/manifest
GET /v1/admin/export/manifest?includeSamples=true&sampleLimit=5
POST /v1/admin/export/bundle
POST /v1/admin/export/backup/run
GET /v1/admin/export/backup/runs
GET /v1/admin/export/backup/policy
POST /v1/admin/export/backup/policy
POST /v1/admin/export/backup/policy/run
POST /v1/admin/export/backup/retention
GET /v1/admin/export/bundles
POST /v1/admin/export/bundles/{id}/verify
POST /v1/admin/export/bundles/verify-chain
POST /v1/admin/export/bundles/{id}/archive-object
POST /v1/admin/import/bundles/from-object/{object_id}
POST /v1/admin/import/bundles/{id}/preflight
POST /v1/admin/import/bundles/{id}/restore
POST /v1/admin/import/bundles/{id}/preflight-delta
POST /v1/admin/import/bundles/{id}/apply-delta
POST /v1/admin/import/bundles/restore-chain
```

It reads the same active-plus-archived WAL stream and reports the manifest format id, generated time, node id, base LSN, incremental flag, current/snapshot/compaction LSNs, schema version, schema history versions, schema proposal count, cluster-control counts, WAL record range, checksum missing/mismatch counts, per-shard ranges, per-payload counts, table/room/user counts, live/deleted object counts, live object bytes, optional encryption metadata, and optional WAL samples. `POST /v1/admin/export/bundle` creates a local filesystem bundle under `data/exports/{id}` containing `manifest.json`, `schema.json`, `schema/history/v{version}.json`, `schema/proposals.json`, `cluster/topology-overrides.json`, `cluster/topology-log.jsonl`, `cluster/topology-proposals.json`, `cluster/topology-lease.json`, `cluster/handoff-workflows.json`, `wal-records.jsonl`, `objects/metadata/*.json`, and `objects/blobs/*.bin`. The bundle refuses to run when checksum mismatches are present. If the request includes `encryptionKey` or `NEXTDB_EXPORT_BUNDLE_KEY` is set, the bundle keeps only `manifest.json` readable and encrypts every other file in place with AES-256-GCM; the AES key is SHA-256 of the supplied export key and file authentication binds the relative path as AAD. If the request includes `baseLsn`, the bundle is incremental: `wal-records.jsonl` contains only records after that LSN, and `objects/` contains only object bodies committed in the delta window and still live at the end of the delta. `POST /v1/admin/export/backup/run` is the operator runbook primitive above those pieces. It discovers the local valid full+delta chain with the highest LSN, creates a full bundle when no base exists or `forceFull=true`, otherwise creates the next incremental bundle from the chain tail, archives that bundle into the object store by default, verifies the resulting chain, and appends a run record to `data/exports/backup-runs.json`. If the chain tail already equals current LSN, it returns `noOp=true` instead of producing an empty delta, but still records the run. `GET /v1/admin/export/backup/runs` returns that local backup catalog with run ids, mode, LSN range, bundle ids, archive object ids, chain ids, chain status, and byte/count summaries. `GET/POST /v1/admin/export/backup/policy` persists `data/exports/backup-policy.json`, which controls optional in-process scheduling, default archive behavior, and post-run retention. The first boot can seed it from `NEXTDB_BACKUP_ENABLED`, `NEXTDB_BACKUP_INTERVAL_MS`, `NEXTDB_BACKUP_ARCHIVE_OBJECT`, `NEXTDB_BACKUP_KEEP_LAST`, `NEXTDB_BACKUP_RETENTION_DELETE_BUNDLES`, and `NEXTDB_BACKUP_RETENTION_DELETE_ARCHIVE_OBJECTS`; after the file exists, the file is authoritative. `POST /v1/admin/export/backup/policy/run` executes one run with that saved policy and then applies retention if configured. Health exposes `exportBackupController` with enabled state, interval, last run id, and last error. `POST /v1/admin/export/backup/retention` applies catalog retention by `keepLast` and/or `beforeTimestampMs`, defaults to dry-run, protects bundles and archive objects still referenced by retained runs, removes local bundle directories when enabled, and deletes archive objects only when explicitly requested through the normal object-delete WAL path. `GET /v1/admin/export/bundles` is a lightweight discovery surface that scans local bundle directories and reads the manifest, schema files, schema proposal ledger, and cluster-control ledger; encrypted bundles are listed from the manifest only, and byte-level trust still comes from verify. `POST /v1/admin/export/bundles/{id}/verify` reopens that artifact and validates manifest parsing, optional decrypt/authenticate, schema parsing/version match, schema history parsing/version match, schema proposal parsing/count match/candidate validation, cluster-control parsing/count match/ledger references, WAL JSONL readability/counts/LSN range/baseLsn bounds, WAL schemaVersion resolvability, object metadata ids, blob byte sizes, and blob SHA-256 values. `POST /v1/admin/export/bundles/verify-chain` reuses single-bundle verification for every requested artifact, then verifies chain continuity: first bundle full at `baseLsn=0`, later bundles incremental, and each delta `baseLsn` equal to the previous bundle `currentLsn`. `POST /v1/admin/export/bundles/{id}/archive-object` stores the original bundle bytes as an object with content type `application/vnd.nextdb.export-bundle-archive+json`, preserving encrypted files exactly as written. `POST /v1/admin/import/bundles/from-object/{object_id}` reads that object, validates the archive format, safe relative file paths, byte lengths, and SHA-256 values, then materializes it under `data/exports/{id}` so normal verification and restore continue to own import correctness. `POST /v1/admin/import/bundles/{id}/preflight` is deliberately read-only full-import planning: it reuses bundle verification, requires the current database to be empty, checks the export format and manifest checksum summary, rejects incremental bundles for empty-database restore, and returns problems/notes for restore. `POST /v1/admin/import/bundles/{id}/restore` enforces the same preflight, decrypts encrypted full bundles into a temporary read directory when needed, replaces the empty database schema from `schema.json`, restores schema history, proposal ledger, and cluster-control files, copies objects through `put_replicated`, appends WAL records through the shard replication writer with original LSNs, refreshes WAL remote-replica routing from restored topology overrides, and applies the same projection path used by inbound WAL replication. `POST /v1/admin/import/bundles/{id}/preflight-delta` is read-only delta planning: it verifies the incremental bundle, requires the current database LSN to equal the bundle `baseLsn`, and checks that the delta WAL can be projected with the bundle schema. `POST /v1/admin/import/bundles/{id}/apply-delta` enforces that preflight, copies delta objects through `put_replicated`, appends delta WAL records through the shard replication writer with original LSNs, and applies them through the same projection path used by inbound WAL replication. Cross-version migration remains a separate layer.

`POST /v1/admin/import/bundles/restore-chain` verifies the requested full+delta chain, restores the first full bundle, then applies each delta in order so recovery can advance to the chain tail in one guarded control-plane action.

`npm run test:wal-export-corruption` verifies that export manifest, bundle creation, and backup-run creation all fail closed on WAL checksum mismatch instead of producing a partial or misleading backup artifact.

New WAL records carry `checksum: "sha256:..."`, computed over the committed record body excluding the checksum field itself. Ordinary WAL reads reject records when a present checksum does not match, so startup replay, audit, sync, and projection rebuild fail closed on signed record corruption. `GET /v1/admin/wal/integrity` is the operator-facing consistency check for the WAL source of truth. It scans every active shard file plus archive JSONL file, reports per-file line/record ranges, and returns structured issues for checksum mismatches, malformed JSON, duplicate LSNs, shard mismatches, invalid zero metadata, non-monotonic file ordering, and volatile events that were incorrectly persisted. Gaps and missing checksums are surfaced as warnings rather than corruption because a failed append can consume an LSN without committing a fact, and older WAL records did not include checksums.

`npm run test:wal-integrity-corruption` covers the operator negative path by mutating a committed active WAL record while preserving valid JSON, then asserting that integrity reports `checksumMismatch` for the corrupted LSN. `npm run test:wal-startup-corruption` covers the recovery negative path: a new server process pointed at that damaged WAL must exit before becoming healthy.

`POST /v1/admin/wal/seal-checksums` upgrades legacy WAL files in place. Active shard files are rewritten through the shard WAL worker so the open append handle is closed and reopened atomically; local replica files and archive files are rewritten with the same record LSNs and newly computed checksums. The operation is idempotent and refuses to seal records that already carry a mismatched checksum.

## Chat Log Projection

Messages are exposed as a logical nested table:

```text
rooms/{room_id}/messages/{message_id}
```

Cold reads are served from a physical projection:

```text
data/chat-log/rooms/{hashed_room_id}/bucket-{day}.jsonl
```

This is the first Scylla/Cassandra-inspired access path. It keeps owner-local reads explicit while avoiding full WAL scans for history reads.

Projection repair is available through:

```text
POST /v1/admin/projections/rebuild
```

The rebuild reads durable WAL facts and rewrites the chat log projection.

## Generic Record Projection

Schema-defined top-level tables can be written as generic records:

```text
POST /v1/records/{table}/{key}
DELETE /v1/records/{table}/{key}?expectedLsn=...
POST /v1/records/transaction
GET  /v1/records/{table}/{key}
GET  /v1/records/{table}?limit=...
GET  /v1/records/{table}/indexes/{index_name}?value=...
GET  /v1/records/{table}/indexes/{index_name}?values=[...]
```

The committed logical path is:

```text
tables/{table}/{key}
```

The server validates the JSON value against `schema.tables.{table}.fields`, expected LSN, indexed scalar fields, and unique-index conflicts before appending a `RecordUpserted` WAL record. Durable writes protect both WAL-derived uniqueness and the current hot overlay, so a volatile row cannot create a current duplicate and a volatile replacement cannot make an older durable unique value reusable in WAL. Volatile record writes validate the same indexed scalar fields and protect current hot-table uniqueness, but still do not append WAL or update persistent index files. If the value contains an `id` field, it must match the URL key. The projection stores one JSON document per key under `data/records`, and can be rebuilt from WAL with:

```text
POST /v1/admin/projections/rebuild
```

Schema-defined nested tables can be written without loading or embedding the parent row:

```text
POST /v1/records/{table}/{parent_key}/{nested}/{nested_key}
DELETE /v1/records/{table}/{parent_key}/{nested}/{nested_key}?expectedLsn=...
GET  /v1/records/{table}/{parent_key}/{nested}/{nested_key}
GET  /v1/records/{table}/{parent_key}/{nested}?limit=...
POST /v1/records/transaction
```

The committed logical path is:

```text
tables/{table}/{parent_key}/{nested}/{nested_key}
```

The WAL and projection use a logical table name of `{table}.{nested}` and a logical key of `{parent_key}:{nested_key}`. The parent key is the partition and shard routing key; it is not a foreign-key constraint and the parent row does not need to be loaded or even present. The primary record remains addressable by logical table/key, and the record store also maintains a parent-partition projection under `data/records/_partitions/{logical_table}/{parent_key}` with a bounded `.manifest` for nested-key page reads. `GET /v1/records/{table}/{parent_key}/{nested}` reads that partition projection, so large child collections do not require scanning the whole nested logical table, and covered nested-key pages do not need to scan every child file in the parent partition. By default the partition list is ordered by nested key. Passing `order=schema` applies the nested table's storage order through a persistent clustering projection under `data/records/_orders/{logical_table}/{parent_key}/{order_id}`. Each order directory keeps a bounded `.manifest` containing ordered cursors plus bounded record filenames, so covered cursor pages can read the needed files directly without scanning and sorting every child record in the parent partition. Startup projection repair, `/v1/admin/projections/rebuild`, schema reload, WAL replication replay, transactions, upserts, and deletes all maintain this clustering projection from WAL-backed records. The response includes `nextCursor`; callers should pass it back as `afterCursor` for efficient ordered pagination. `nextAfterKey` remains available for compatibility, but it requires locating that key inside the clustering projection. The default `rooms.messages` schema uses `desc(createdAtMs), id`, matching a Cassandra/Scylla-style partition key plus clustering order. The server validates the nested value against `schema.tables.{table}.nested.{nested}.fields`; if `id` is present it must match `nested_key`, and if `parentId` or `parentKey` is present it must match `parent_key`.

Generic nested records reuse the same WAL facts, projection files, object-reference tracking, sync stream, subscriptions, SDK cache, CAS, and `clientMutationId` idempotency as top-level records. The TypeScript SDK local cache exposes the same partition-shaped read internally through a bounded key-prefix range, and it can apply schema-order cursors to cached nested records after reading the schema order. Memory cache keeps an in-memory ordered projection; IndexedDB cache persists `recordOrderMetadata` and `recordOrders` stores so cached schema-ordered nested reads do not need to re-sort the full local parent partition. Nested table reads can therefore be served locally without scanning cached records for other parents, and `nestedTable(...).cache.clear()` deletes only the selected parent partition while removing its ordered projection entries. Cache capacity follows the same partition boundary: top-level records use the per-table `maxRecordsPerTable` budget, while nested logical tables apply that budget independently to each `{parentKey}:` prefix and can additionally cap the number of retained parent prefixes with `maxNestedPartitions`. Nested secondary indexes are projections scoped by the parent partition; the SDK reads index field definitions from schema, filters cached scalar fields locally for exact matches or inclusive range bounds when it can fill a page and the logical nested table has no active volatile overlay, and falls back to the server projection otherwise. Range cache hits use the same `nextCursor` format as the server. The specialized `rooms/{room_id}/messages` API remains the hot actor/chat-log optimized path for the primary chat workload.

`POST /v1/records/transaction` supports `nestedUpsert` and `nestedDelete` operations in addition to top-level `upsert` and `delete`. Nested transaction operations route by `{table}:{parent_key}`, so batches within the same parent partition commit as one WAL fact and emit per-child record events at the same LSN. A transaction that mixes different parent partitions or different top-level shard keys is rejected as cross-shard.

Schema indexes live at `schema.tables.{table}.indexes.{index}`. Each index declares one or more scalar fields and can be marked `unique`. After the WAL commit succeeds, record projection and index projection update in the same record-store critical section: the previous index entries for that key are removed, the primary record is written, and the new index entries are written under `data/records/_indexes`. Each index-value directory keeps a bounded key manifest; exact reads use it directly when it covers the requested page, and writes/deletes maintain it when present.

Indexed queries support exact-match and inclusive range access paths. Single-field exact matches can use `value=...`; compound exact matches use `values=[...]`, where values are JSON scalars matching the declared field order. Single-field ranges can use `lower=...` and `upper=...`; compound ranges use `lowerValues=[...]` and `upperValues=[...]` in index field order. Range scans sort index-value directories by decoded tuple, then stream each value directory in primary-key order using its manifest when possible, stopping when the requested page is full. Missing or uncertain manifests fall back to scanning that one value directory and are rewritten from authoritative filenames. Range responses return `nextCursor` for continuation through `afterCursor`. Startup, projection rebuild, schema reload, and remote WAL replay all rebuild or maintain these projections from `RecordUpserted` facts. Projection rebuilds are staged in a temporary record-store directory and swapped into place only after the full primary, index, partition, and clustering projection has been built successfully.

Deletes append a `RecordDeleted` WAL tombstone when the record exists. The primary record file, all secondary index entries for that key, and ObjectRef sources for `tables/{table}/{key}` are removed after the WAL commit. If `expectedLsn` is provided, both upsert and delete reject stale client writes with `409` before appending a WAL fact. Deleting an already-missing record without an `expectedLsn` is idempotent and does not create a new tombstone.

Record transactions are same-shard atomic batches. The server validates schema, expected LSNs, shard ownership, shard freeze state, indexed scalar values, and unique-index conflicts before appending WAL. If any operation fails validation, no WAL record is written and no projection changes. On success, the WAL contains one `RecordTransactionCommitted` fact with multiple upsert/delete operations; replay, remote replication, audit, sync, subscriptions, and SDK cache updates all derive from that one fact. Cross-shard transactions intentionally remain outside this primitive until the cluster control plane has real distributed commit semantics.

This layer deliberately does not introduce foreign-key constraints. It provides typed logical rows, durable event sourcing, object-reference tracking, table subscriptions, and SDK-managed local cache while preserving partition-friendly ownership by key.

## Object Store

Objects use database-native metadata and externalized bodies:

```text
POST /v1/objects?contentType=...&objectId=...
GET  /v1/objects?limit=...&afterId=...
DELETE /v1/objects/{object_id}?force=...&clientMutationId=...
GET  /v1/objects/{object_id}/metadata
GET  /v1/objects/{object_id}/body
```

The object body lives under `data/objects/blobs`, and metadata lives under `data/objects/metadata`. The body endpoint supports single `Range: bytes=...` reads, returning `206 Partial Content`, `Accept-Ranges: bytes`, and `Content-Range` for satisfiable ranges or `416` for unsatisfiable ranges. This keeps media/document metadata in records while letting large bytes stay in object storage and load lazily by segment. The list endpoint returns metadata in object-id order with `nextAfterId` pagination. `ObjectCommitted` / `ObjectDeleted` are recorded in the WAL, can be pulled with `objects=true`, and can be watched with SDK `subscribeObjects`.

Uploads can include `clientMutationId` and optional `objectId`. If `objectId` is omitted, the server allocates one; if it is supplied, the server validates it as a safe single path component and rejects an existing id. The SDK supplies an id for its own uploads, especially offline writes, so attachment references remain stable before the object reaches the server. The server scans committed WAL records before writing a new blob; if the mutation id already committed an object upload, the original metadata is returned and no second `ObjectCommitted` fact is appended. Reusing the same mutation id for a different mutation kind returns `409`, matching message and record idempotency semantics.

Direct deletes append `ObjectDeleted` WAL facts with the `force` flag used for the decision. A delete checks the object-reference projection first and returns `409` while the object is still referenced unless `force=true` is supplied. Deleting an already-missing object returns `deleted=false`; with `clientMutationId`, that no-op is recorded so retries keep a stable response. Successful deletes remove the body and metadata files and append an object-scoped cache invalidation with the delete LSN.

Messages can store attachments as typed object references. Generic records can also contain ObjectRef-shaped values. A send request supplies object ids, and the server resolves message attachments into metadata before committing the message event.

Object references are tracked as a rebuildable projection:

```text
GET  /v1/objects/{object_id}/references
POST /v1/admin/objects/gc?dryRun=true
POST /v1/admin/objects/gc?dryRun=false
```

The references response includes `objectExists` and `dangling`. A forced delete does not erase the source paths that still point at the object id; it changes the response to `objectExists=false` and `dangling=true` until those sources are updated or a replacement object with the same id is committed.

GC retains referenced objects. Unreferenced objects are deleted only when `dryRun=false` and either their age exceeds `NEXTDB_OBJECT_GC_GRACE_MS` or the request passes `force=true`. The response separates `retained`, `protected`, and `deleted` ids so an operator can preview policy impact before deleting.

## Actor Snapshots

Active actor live state can be checkpointed:

```text
POST /v1/admin/snapshot
```

The snapshot stores room hot windows, durable record-hot entries, and the latest committed LSN. On startup, NextDB restores missing WAL files from local replicas, loads the schema file, scans active/archive WAL for `SchemaApplied` facts, rewrites schema history and the current schema from the highest schema apply LSN, then loads the runtime snapshot and replays WAL records after that LSN. Record projections are rebuilt from WAL-derived records using the recovered schema; record hot state restores matching durable snapshot entries and applies newer durable records, while volatile records remain restart-local and are not restored. Health exposes `startupRecovery`, a boot-time recovery report with snapshot hit/miss, snapshot LSN/schema, room count, record-hot snapshot counts, schema WAL recovery, per-shard WAL restore reports, per-shard replay counts, highest recovered LSN, and rebuilt projection sizes. This makes restart correctness observable without inferring it from current LSN alone.

`npm run test:runtime-restart` verifies this path with a real server process: it writes records, objects, and messages, prepares a snapshot, writes an additional WAL fact, kills the process, restarts against the same data directory, and reads the restored projections and object body back while checking `startupRecovery`. `npm run test:write-throughput` adds a chat-path throughput baseline: it writes configurable strict and relaxed durable SDK message batches with bounded concurrency, checks WAL integrity and latest-message ordering at the highest LSN, snapshots the runtime, restarts, and verifies both recovered hot windows. It then writes a volatile message batch, verifies the live-only room window and unchanged durable LSN/WAL count, restarts again, and confirms the volatile room is not recovered while printing strict, relaxed, and volatile messages-per-second for the current machine.

Automatic checkpointing is controlled by:

```text
NEXTDB_CHECKPOINT_EVERY_LSN=1000
```

Set it to `0` to disable automatic checkpoints.

Checkpoint-triggered WAL compaction is controlled separately:

```text
NEXTDB_AUTO_COMPACT_WAL=true
```

When enabled, each successful automatic checkpoint asks every WAL shard to compact through the checkpoint LSN. Primary WAL files and local replica WAL files are archived independently, and the active files keep only records after the checkpoint. If this automatic compaction fails, the already-confirmed write remains committed and the server logs a warning; manual compaction still returns errors to the admin caller.

Once a snapshot exists, the active WAL can be compacted:

```text
POST /v1/admin/wal/compact
```

Compaction runs inside each WAL worker, so it is serialized with appends and safely reopens the active WAL file after rewriting it. Runtime WAL files and archives are binary framed records with a magic/version/length header; the reader also accepts legacy JSONL WAL files so older data directories and exported `wal-records.jsonl` remain readable. Records at or before `lastSnapshotLsn` are copied to `data/wal/archive/*.jsonl`; records after the snapshot stay in their active shard WAL.

Startup actor recovery uses:

```text
snapshot rooms + active WAL records after snapshot LSN
```

Event-sourcing surfaces use:

```text
archived WAL + active WAL
```

That distinction keeps hot actor restart bounded without deleting the durable audit history needed for sync, projection rebuild, and object-reference repair.

Archived WAL deletion is a separate operator action:

```text
POST /v1/admin/wal/archive/retention?beforeLsn=100000
POST /v1/admin/wal/archive/retention?dryRun=false&beforeLsn=100000
POST /v1/admin/wal/archive/retention?dryRun=false&beforeTimestampMs=1893456000000
```

Retention scans only `archive/*.jsonl` next to each WAL shard and never modifies active WAL files. The archive extension is historical; new archive contents use the same binary framed WAL format as active shards. A file is eligible only when every record in it is strictly before the supplied `beforeLsn` and/or `beforeTimestampMs`; if both thresholds are supplied, both must match. The default is `dryRun=true`, returning per-file LSN and timestamp ranges without deleting anything.

Deleting archives intentionally shortens the historical event horizon. WAL audit, durable sync catch-up, projection rebuild, object-reference repair, and `clientMutationId` idempotency lookup can only see retained archive files plus active WAL after retention.

`npm run test:wal-archive-retention` exercises the operator path end-to-end: durable write, snapshot, compaction into `archive/*.jsonl`, dry-run retain/delete thresholds, real deletion, and post-retention WAL integrity/export manifest checks.

## Connection Layer

The data layer emits logical delivery events:

```text
DeliveryEvent::MessageCreated { room_id, message }
DeliveryEvent::RecordUpserted { table, key, record }
DeliveryEvent::UserUpserted { user_id, user }
DeliveryEvent::VolatileRoomEvent { room_id, name, payload }
DeliveryEvent::VolatileUserEvent { user_id, name, payload }
```

The connection layer maps those events to physical transports. Room events are delivered to connections that subscribed to the room. Table events are delivered to connections that subscribed to the table, and nested table events can be delivered only to sessions subscribed to that parent partition through `subscribeNestedTable`. User events are delivered to connections authenticated or opened as that logical `user_id`.

Logical users also have a durable profile directory and durable inbox. `POST /v1/users/{user_id}` appends a `UserUpserted` WAL fact containing display name, metadata, timestamps, and path. `GET /v1/users/{user_id}` materializes that user's latest profile from WAL, `GET /v1/users/{user_id}/events` materializes the newest durable `UserEventPublished` facts for that user with optional `beforeLsn`, `GET /v1/admin/users` lists the materialized directory, and `GET /v1/admin/users?shard=N` lists only profiles whose user id hashes to that shard for quorum/all SDK directory reads. Durable user sync/realtime delivers `userUpserted` and `userEvent` so the SDK can cache the profile beside that user's inbox. This profile is the durable user entity; connection sessions below are only the current physical presence of that entity.

API:

```text
GET  /v1/users/{user_id}
POST /v1/users/{user_id}
GET  /v1/users/{user_id}/events
GET  /v1/admin/users
```

Connection sessions are runtime state and are not written to WAL. A session records:

```text
session id
logical user id
transport
connected at
last seen at
subscribed rooms
subscribed tables
subscribed live query ids
user inbox feed subscription flag
object feed subscription flag
```

API:

```text
GET /v1/admin/connections
GET /v1/admin/connections?userId=alice
GET /v1/admin/connections?userId=alice&transport=webSocket
```

`transport` accepts `webSocket`, `webTransport`, or `custom`. The response includes `transports.webSocket`, `transports.webTransport`, and `transports.custom` counts for the filtered runtime set. `publishUserVolatile` and realtime signaling use this registry to report whether a logical user currently has routable sessions. The `delivered` count means "active local sessions targeted by the data layer"; it is not a durable acknowledgement and does not imply a media packet or high-frequency game packet was processed by the client.

Event payloads can be declared in `schema.events.{eventName}.payload`. Declared durable user events are validated before WAL append; declared user-targeted volatile events, realtime channel signals, realtime broadcasts, and behavior-published volatile room events are validated before delivery. Undeclared event names remain JSON passthrough so applications can migrate event schemas gradually.

The SDK exposes `NextDbRealtimeTransport` and uses `WebSocketRealtimeTransport` by default. It declares the intended connection transport on `/v1/connect` with `transport=webSocket`, `transport=webTransport`, or `transport=custom`, so the runtime registry can remain protocol-aware even though the data layer still targets logical users, rooms, tables, queries, and objects. `GET /v1/health.connectionLayer` advertises the node's runtime connection protocol, connect path, default transport, and built-in supported transports. The current Rust listener reports WebSocket and custom JSONL support: WebSocket connects at `GET /v1/connect`, while custom stream gateways can POST newline-delimited `ClientFrame` values to `/v1/connect/jsonl` and receive newline-delimited `ServerFrame` values in the response body. WebTransport selection remains explicit until a native HTTP/3 listener is attached. `db.realtimeTransportCompatibility()` and the standalone `realtimeTransportCompatibility(health, kind)` helper compare a requested transport with that advertised capability and return support status plus a fallback candidate; they do not silently mutate the client's configured transport. `db.connectCompatibleRealtime()` is the opt-in connection path for application boot: it performs that preflight, switches the current SDK instance to WebSocket by default when the configured transport is unsupported and the node advertises WebSocket fallback, can switch to the built-in JSONL HTTP transport with `{ fallbackTo: "jsonl" }` when the node advertises `custom`, starts the realtime connection, and reports the configured versus active transport in the result and `localDataStatus()`. The server-side realtime connection lifecycle is now a transport-neutral loop over `ClientFrame` source and `ServerFrame` sink traits. WebSocket and HTTP JSONL only supply those endpoints; hello, session registration, subscription/query/metadata handling, broadcast fan-out, cache invalidation, connection-control frames, and cleanup all live outside transport-specific functions. Frame serialization is also centralized: WebSocket carries one JSON `ClientFrame` / `ServerFrame` per message, while stream transports use the same frames as newline-delimited JSON. The SDK exposes shared frame encode/decode helpers, a JSONL server-frame decoder, `JsonLineHttpRealtimeTransport` / `jsonLineHttpRealtimeTransport()` for explicit custom HTTP stream deployments, and a built-in `realtimeTransportKind: "jsonl"` selector that maps to the connection layer's `custom` transport. The Rust runtime has matching frame encode/decode plus `AsyncBufRead`/`AsyncWrite` JSONL source/sink adapters. The SDK also ships `WebTransportRealtimeTransport`, selectable through `realtimeTransportKind: "webtransport"` or `webTransportRealtimeTransport()`. WebTransport uses the same data-layer target contract and serializes the existing frame protocol as JSONL on a reliable bidirectional stream. An HTTP/3 WebTransport listener or gateway can attach to the same connection lifecycle without changing subscriptions, catch-up, user-targeted delivery, or local cache projection.

The server-side pre-broadcast registry keeps a coarse global candidate index before each connection applies its exact local router, RLS, and live-query refresh logic. Room, full-table, query-table, user-event, and object subscriptions are direct session sets. Table-range subscriptions use a bucketed interval index: bounded ranges are duplicated only into the possible key buckets, while full-width ranges stay in a fallback index. Table index-prefix subscriptions validate against declared secondary indexes, support `serverSnapshot`, scan range-combined snapshots until the matching page is filled, and compile schema-aware index-prefix candidate entries, so non-matching same-table writes are excluded before connection-local filtering. Nested-table subscriptions use a bucketed prefix index keyed by stable prefix hash, then probe only the actual prefixes of the changed logical key. These buckets reduce global candidate-map size and hot-path cache pressure without changing the external subscription protocol or the final connection-local correctness check.

## Realtime Channels

Realtime channels are volatile coordination objects for audio, video, and game sessions. They are not durable tables and are not written to WAL. Members are session-scoped connection state; channel state snapshots and sequence counters are runtime state. When the last member leaves, the runtime removes that channel's state and sequence counter. `NEXTDB_REALTIME_MAINTENANCE_INTERVAL_MS` controls a background sweep that reconciles channel members against active connection sessions, removes stale session-scoped members, and removes state/sequence entries whose channel no longer has members. Health and metrics expose active channel, state, sequence, stale-member cleanup, and orphan cleanup counters. Behaviors can read channel members/state and return host commands to broadcast channel events or update channel state, so server-side lobby/game logic can target logical users while the connection layer owns WebSocket/WebTransport delivery.

API:

```text
GET  /v1/realtime/channels
GET  /v1/realtime/channels/{channel_id}/members
GET  /v1/realtime/channels/{channel_id}/state
POST /v1/realtime/channels/{channel_id}/join
POST /v1/realtime/channels/{channel_id}/leave
POST /v1/realtime/channels/{channel_id}/state
POST /v1/realtime/channels/{channel_id}/signal
POST /v1/realtime/channels/{channel_id}/broadcast
```

The server stores channel membership in memory and forwards signaling payloads as `VolatileUserEvent` messages:

```text
realtime.channel.memberJoined
realtime.channel.memberLeft
realtime.channel.state
realtime.channel.signal
realtime.channel.event
```

`GET /v1/realtime/channels` returns the in-memory channel list with member counts, current sequence, current state version, state update time, and members for operator visibility. Membership is session-scoped, so the same logical user can join a channel from multiple clients without overwriting another session. When a join request includes `sessionId`, that session must already be an active connection for the same `userId`; otherwise the server rejects the join instead of creating an unroutable channel member. WebSocket disconnect cleanup removes that session's memberships from all channels, keeping presence tied to routable sessions rather than durable business membership. `GET /state` returns the current in-memory `{ version, state, updatedAtMs }` snapshot, or version `0` with `state: null` before the first update. `POST /state` requires `fromUserId` to be a joined member, optionally checks `expectedVersion` for CAS, increments the same per-channel `sequence` used by signals and broadcasts, and fans out a `realtime.channel.state` event to joined channel sessions. This snapshot is the database-owned current state for lobbies, games, and collaborative cursors; it is volatile and not reconstructed from WAL after restart. The SDK keeps a runtime projection of channel state from HTTP reads, successful writes, reconnect refreshes, and volatile state events; `cachedState()` reads it and `watchState()` emits snapshot views through the same local data notification path as durable cache watchers. `signal` targets one joined user from another joined user for WebRTC offer/answer/ICE flows; the server rejects signals whose sender or recipient is outside the channel or whose `kind` is empty. `signal`, `state`, and `broadcast` all allocate the next per-channel `sequence` plus `timestampMs`, so clients can order point-to-point handshakes, authoritative state snapshots, and fan-out state patches on the same volatile timeline. Oversized or schema-invalid signal/broadcast payloads are rejected before sequence allocation. A signal response keeps the boolean `delivered` and also reports `deliveredSessions`, the number of currently connected sessions for the target user. `broadcast` targets unique logical users in the joined member set, then fans out through the connection registry to that user's active sessions. The SDK keeps bounded in-memory recent projections for received channel broadcasts and point-to-point signals; `cachedRecentEvents()` / `watchRecentEvents()` expose broadcast events, `cachedRecentSignals()` / `watchRecentSignals()` expose signals, and `leave()` clears those runtime projections with the rest of that channel state. SDK helpers expose this as `channel.state`, `channel.cachedState`, `channel.watchState`, `channel.updateState`, `channel.onState`, `channel.cachedRecentEvents`, `channel.watchRecentEvents`, `channel.cachedRecentSignals`, `channel.watchRecentSignals`, `channel.signal`, `channel.sendOffer`, `channel.sendAnswer`, `channel.sendIce`, `channel.broadcast`, `channel.sendGameInput`, `channel.sendGameInputFrame`, `channel.sendStatePatch`, `channel.sendVoice`, `channel.sendVoiceFrame`, `channel.sendVideo`, `channel.sendVideoFrame`, `channel.onSignal`, `channel.onSignalKind`, `channel.onOffer`, `channel.onAnswer`, `channel.onIce`, `channel.onEvent`, `channel.onEventKind`, `channel.onGameInput`, `channel.onStatePatch`, `channel.onVoice`, and `channel.onVideo`.

This lets the data layer target logical users while the client decides the physical transport. WebRTC offers, answers, ICE candidates, game input, and state patches can be signaled through NextDB. Applications can subscribe to custom signal and broadcast kinds with `onSignalKind` and `onEventKind`, keeping app-specific room control messages on the same sequenced volatile timeline as built-in media/game helpers. Recent channel signals and events are SDK-owned runtime state, not durable cache: they are useful for UI surfaces and diagnostics that need the latest volatile control frames, while audit and replay remain WAL-only. For small binary realtime frames, `createRealtimeBinaryFrame()` encodes `Uint8Array`, `ArrayBuffer`, `Blob`, or `string` into `{ dataBase64, byteLength, contentType, codec, timestampMs, metadata }`, and `decodeRealtimeBinaryFrame()` recovers the bytes. The channel helpers `sendGameInputFrame`, `sendVoiceFrame`, and `sendVideoFrame` route those binary-frame payloads through the normal volatile broadcast path, preserving channel sequence and logical-user delivery while remaining transport-neutral; `onVoice` and `onVideo` are typed receivers for media control messages and those small binary payloads. Durable media bodies still belong in the object store, and high-bandwidth audio/video frames or very high-frequency game traffic should flow through WebRTC media tracks, WebRTC DataChannel, WebTransport, or an equivalent connection-layer transport.

`npm run test:realtime-channel-sdk` starts an isolated node and exercises the SDK-level channel helpers end to end.

## Client Cache

The TypeScript SDK owns the local cache through `NextDbLocalCache`:

```text
putObject
getObjectMetadata
getObjectBody
listObjects
deleteObject
putRoomMessages
getRoomMessages
putRecords
getRecord
listRecords
getMetadata / setMetadata
stats
clearAll
clearObjects
clearRoom
clearTable
trimRoom
```

Browser clients use IndexedDB by default; non-browser runtimes use the memory cache. The SDK derives its default IndexedDB database name from `endpoint`, `userId` or `anonymous`, and optional `cacheNamespace`, so a browser refresh for the same logical data scope reuses local state while a different user, endpoint, or namespace gets a separate local database. Applications that need a custom storage boundary can still pass an explicit `NextDbLocalCache`. Object uploads cache metadata and body after commit only when the local body matches the returned metadata size and SHA-256; this prevents a mismatched idempotent retry from overwriting the cached body for the original object. Object metadata/body reads consult the cache first, then fall back to the server and write returned values back into the cache. Object list pages cache returned metadata. Room, table, nested-table, schema-ordered nested, exact-match index, and range index reads consult the cache first when a full page is available, then fall back to the server and write returned values back into the cache. Durable realtime `messageCreated` and `recordUpserted` events also update the cache before user listeners run; volatile message and record events are delivered to subscribers but skipped by the SDK durable cache and cursor advancement. Volatile records additionally install a runtime overlay that blocks stale cached/indexed durable rows for the same key until an authoritative durable write or point read clears it. IndexedDB stores are durable across SDK instances with the same scope; messages, objects, records, cursors, cache lease metadata, pending writes, stored subscriptions, index-filterable records, and `recordOrderMetadata` / `recordOrders` schema-order projections are rehydratable after a browser refresh.

The server owns cache policy and invalidation:

```text
GET  /v1/cache/profile?clientId=...&afterInvalidationGeneration=...&schemaVersion=...&cursorLsn=...
POST /v1/admin/cache/profile
POST /v1/admin/cache/invalidate
```

`/v1/cache/profile` returns a lease, profile version, schema version, current LSN, and invalidation entries newer than the client's last applied generation. The SDK persists this metadata beside cursors in IndexedDB or memory:

```text
client id
profile version
schema version
last invalidation generation
lease expiration
last validation timestamp
max objects
max object bytes
max room messages
max user events
max records per table
max nested partitions
```

Before serving a cached object/list, room/user/table page, before durable sync, and before subscription reconnect recovery, the SDK checks the lease. If the profile or schema version changed, or the server reports `resetRequired`, the SDK clears local cache and cursors. `POST /v1/admin/cache/profile` patches the persisted profile with optional compare-and-swap `expectedVersion`, bumps the profile version, and appends a `profile` invalidation so connected clients adopt the new policy immediately without clearing cached rows. The same invalidation entries are also pushed to online WebSocket clients as `ServerFrame.cacheInvalidated`. The SDK applies that frame through the same invalidation path as a lease refresh, clears the affected local region immediately for data scopes, resets any scoped cursor to `minValidLsn`, emits cache-change notifications, and records the applied generation in cache metadata. Offline or disconnected clients still converge through the next `/v1/cache/profile` lease check. The active profile also enforces client-side capacity: object metadata/body writes are trimmed to `maxObjects` and `maxObjectBytes`, room message writes are trimmed to `maxRoomMessages`, durable user-event inbox writes are trimmed to `maxUserEvents`, top-level record writes are trimmed per table to `maxRecordsPerTable`, nested record writes are trimmed per parent partition to `maxRecordsPerTable`, and nested logical tables can retain only the hottest `maxNestedPartitions` parent prefixes. Admin invalidations can target `all`, `profile`, `object`, `room`, `user`, `table`, or `nestedTable`; scoped invalidations clear only the selected cache region. Room/user/table invalidations also reset that cursor to the provided `minValidLsn`. Nested-table invalidations and client-initiated nested partition clears are narrower than table invalidations: they delete cached records with the `{parentKey}:` prefix, remove matching schema-order rows, reset the logical nested table cursor, and emit a table invalidation change for local watchers.

For table sync, the SDK tracks both the highest table LSN and the record-event identities seen at that LSN. This matters for `RecordTransactionCommitted`: several `RecordUpserted`/`RecordDeleted` events can share one LSN. The SDK applies every distinct same-LSN event from a transaction while still suppressing duplicate recovery of the exact same event. On realtime transport lag recovery it pulls from `seenLsn - 1` so a partially received same-LSN transaction can be completed through durable sync.

Unfiltered durable sync also returns object commit/delete metadata events, allowing a client to rebuild SDK-owned object metadata cache from WAL without listing the object store directly. Room/user/table filtered sync skips object events so scoped subscription catch-up only carries the requested partition.

This makes client storage part of the database contract: applications can ask the SDK for cache stats, point-read cached object metadata/bodies, user profiles, table records, and nested records, list cached objects, room messages, user profiles, user events, table records, and nested records without a network read, inspect pending-write queue state, reset or discard one pending write, trim a hot room to the newest N messages, clear object metadata/bodies, clear one room, clear one user profile/inbox, clear one table, force a lease refresh, or clear all cached data without reaching into IndexedDB directly. Full cache clear removes cached data, cursors, pending writes, stored subscriptions, and cache lease metadata; it also clears in-memory persistent subscription intent and cancels restored feeds that no runtime listener still owns. Pending-write clear only drops the offline queue, preserving lease metadata and stored subscriptions so retry management does not accidentally force a cache-control reset.

## Admin UI

The admin console lives in:

```text
packages/nextdb-admin
```

It is a Vite/React app that talks to the public HTTP API. The first screen exposes:

```text
runtime health
startup recovery report
WAL audit stream
schema-derived Data Explorer
virtual actor residency
schema validation and reload
object GC dry-run
behavior module reload
realtime channel membership and state
operation log
storage paths
```

Run it with:

```text
npm run dev:admin -- --port 5173
```

The console defaults to `http://127.0.0.1:3188` and lets the operator change the endpoint in the left rail.

Data Explorer builds its target list from the active schema. Operators can page top-level tables through `GET /v1/records/{table}` and nested partition tables through `GET /v1/records/{table}/{parent_key}/{nested}?order=schema`, then upsert or delete rows through the same schema-validated record APIs used by clients and behaviors. Nested targets require only the parent partition key; the parent row does not need to be loaded.

The Realtime panel lists active channel members, current channel sequence, state version, and state update time. Operators can load a channel state snapshot and submit a JSON replacement with `fromUserId` plus optional `expectedVersion`, so CAS conflicts and member-only update rules stay identical to normal clients.

## Behavior Runtime

Behavior modules live under:

```text
data/behaviors/{name}/nextdb.behavior.json
data/behaviors/{name}/{module}.wasm
```

`POST /v1/admin/behaviors/reload` is an atomic publish step. The runtime scans the behavior directory, rejects duplicate behavior names, compiles every referenced Wasm module with Wasmtime, validates manifest-declared `inputs` against the active schema's behavior mutation fields, and swaps the active behavior map only after the full set succeeds. Invocation reuses the compiled module through a bounded resident instance pool for the loaded behavior epoch; the Wasmtime engine uses the pooling allocator for core instances, memories, and tables, and each activation/call/deactivation turn resets the epoch-interruption deadline. `NEXTDB_BEHAVIOR_FUEL_ENABLED` controls fuel instrumentation separately from preemption: when fuel is disabled, `maxFuel` still maps to epoch-deadline ticks, but the store avoids per-op fuel accounting. The resident pool is runtime-tunable with `NEXTDB_BEHAVIOR_INSTANCE_POOL_MAX` (`0` disables reuse), while `NEXTDB_BEHAVIOR_POOL_TOTAL_CORE_INSTANCES`, `NEXTDB_BEHAVIOR_POOL_TOTAL_MEMORIES`, and `NEXTDB_BEHAVIOR_POOL_TOTAL_TABLES` tune the wasmtime pooling allocator totals. `/v1/health.behaviorRuntime` reports the active behavior epoch, fuel mode, configured pool limits, current pooled instance counts, and runtime counters for invocations, successes, unknown-message turns, guest errors, command rejections, instance create/reuse/return/discard, and pool errors. `/v1/metrics` exposes the same counters globally and with `behavior` labels beside the aggregate fuel/pool gauges. A fresh instance receives optional `on_activate()` once, known mutations are routed to `handle_message` with legacy `invoke` fallback, and stale or unknown mutation names are routed to optional `on_unknown_message`. Trapping or invalid instances are discarded instead of returned to the pool. Discarding a healthy pooled instance, including old epoch pool release after reload, calls optional `on_deactivate()` best-effort; it is not a persistence hook. If reload fails because a manifest is invalid, a module cannot compile, or a declared input contract conflicts with the database schema, the previous behavior set remains active. Schema inputs may contain optional fields not declared by the behavior package, but required additions and type conflicts reject reload.
Each successful reload advances a monotonic behavior publish epoch and stamps every loaded module with that epoch; failed reloads leave the previous epoch intact. Reload is prepared before publication: NextDB compiles and validates the complete behavior set, appends a strict `BehaviorPublished` WAL fact containing the target epoch and manifest summaries, then commits the in-memory epoch swap only after that WAL append is acknowledged. `POST /v1/admin/behaviors/reload` returns the active epoch and `publishedLsn`, and behavior invocation responses include `metadata.behavior`, `metadata.behaviorVersion`, and `metadata.epoch`, so later actor turns and resident Wasm instance pools can bind in-flight work to the code version they started with while new turns move to the new epoch. `nextdb dev --watch` starts the normal server plus a behavior-directory watcher; on `data/behaviors` file signature changes it calls the same internal reload path as the HTTP endpoint, so development hot reload preserves schema validation, runtime write gating, WAL publication, and failure-preserves-previous-code semantics.
Schema apply and schema proposal preflight run the same compatibility check against the currently loaded behavior manifests before rebuilding projections or appending `SchemaApplied`. Replicated schema replay repeats the check locally before replacing the active schema. This makes behavior/package compatibility a database invariant rather than only a packaging-time lint.
Behavior manifests may also declare `reads`, `recordScopes`, `objectScopes`, `realtimeScopes`, `connectionScopes`, `userScopes`, `eventScopes`, `hostHttpScopes`, and `commands`. `reads` is a read-plan allowlist using `records`, `nestedRecords`, `latestMessages`, `objects`, `objectBodies`, `realtimeChannelMembers`, `realtimeChannelStates`, `connectionSessions`, `auditTraces`, and `auditReplays`; when the field is present, the host rejects undeclared read sections before hydrating records, object metadata/bodies, realtime channel snapshots, active connection sessions, or WAL audit views into `request.context`, and an explicit empty array means no reads are allowed. `recordScopes` is a resource allowlist for schema records: top-level `read` / `write` entries use table names such as `rooms`, while `nestedRead` / `nestedWrite` entries use logical nested table names such as `rooms.messages`. `latestMessages`, `sendMessage`, and nested record transactions are checked against `rooms.messages`; record/nested audit trace and replay reads use the same record read scopes, and room traces require `rooms` plus `rooms.messages` read scope. `objectScopes` is the equivalent allowlist for object ids: `objects`, `objectBodies`, and object audit trace/replay require read scope, while `putObject` and `deleteObject` require write scope. A `putObject` command that omits `objectId` requires `objectScopes.write: ["*"]`, because a generated id cannot be prefix-checked before commit. `realtimeScopes` controls volatile channel reads and writes. `connectionScopes` controls active session reads and connection-control writes by logical user id; all-user operations require `["*"]`, while exact values and trailing-prefix wildcards such as `behavior-*` let a behavior manage only its own shard of logical users. `userScopes.read` controls user audit trace/replay reads, `userScopes.publish` controls durable and volatile user event targets by logical user id, while `eventScopes.publish` controls the event name and `eventScopes.realtimeBroadcast` controls broadcast kinds. `hostHttpScopes.allowUrlPrefixes` is required for `requestHostHttp` and allows only exact URL-prefix matches that start with `http://` or `https://`. Object, channel, connection, user, and event scope entries can be exact values, `*`, or trailing-prefix wildcards such as `call-*`; `realtimeChannelMembers` and `realtimeChannelStates` require read scope, while `broadcastRealtimeChannel`, `updateRealtimeChannelState`, and `updateRealtimePresence` require realtime write scope, `publishUserEvent` and `publishUserVolatile` require user publish scope plus event publish scope, `disconnectConnections` requires connection write scope, and `requestHostHttp` requires a matching host HTTP URL prefix. Omitting `reads`, `recordScopes`, `objectScopes`, `realtimeScopes`, `connectionScopes`, `userScopes`, or `eventScopes` preserves legacy unrestricted behavior for that axis; omitting `hostHttpScopes` denies `requestHostHttp`. `commands` is a host-command allowlist using the same command names returned by Wasm output: `sendMessage`, `publishVolatile`, `publishUserEvent`, `publishUserVolatile`, `putObject`, `deleteObject`, `upsertRecord`, `deleteRecord`, `recordTransaction`, `broadcastRealtimeChannel`, `updateRealtimeChannelState`, `updateRealtimePresence`, `disconnectConnections`, `activateRuntimeRecords`, `evictRuntimeRecords`, `activateRuntimeRoom`, `evictRuntimeRoom`, `scheduleActorReminder`, and `requestHostHttp`; empty or omitted `commands` preserves legacy unrestricted writes. When `commands` is non-empty, the runtime validates every returned command before the main host loop commits side effects, so a behavior cannot escalate from a declared read/compute module into arbitrary storage writes or realtime/connection side effects by returning extra JSON commands. Invocation-level `clientMutationId` is accepted only for replay-safe durable host commands; volatile publish, realtime channel state, connection control, and runtime activation are rejected. `requestHostHttp` is idempotent by derived `requestId`, and `scheduleActorReminder` is accepted only with absolute `dueAtMs` so the derived or explicit `reminderId` can be checked against the WAL-derived actor-reminder index.

The current ABI is deliberately small:

```text
exports:
  memory
  alloc(len) -> ptr
  dealloc(ptr, len)
  handle_message(ptr, len) -> packed(ptr, len)
  invoke(ptr, len) -> packed(ptr, len)           # legacy fallback
  on_unknown_message(ptr, len) -> packed(ptr, len) # optional stale-message hook
  on_activate()                                  # optional
  on_deactivate()                                # optional, best-effort
```

Behavior manifests default to `abiEncoding: "json"`, where `handle_message` /
`invoke` receive UTF-8 JSON bytes and return UTF-8 JSON bytes. Rust behaviors
can opt into `abiEncoding: "postcard"` with
`nextdb_behavior_postcard!(Input, handler)`: the same pointer/length entrypoints
then receive and return a postcard frame with explicit `encoding` and `payload`
fields. The default emitted payload encoding is `json`, carrying the stable JSON
ABI byte vector; legacy postcard frames with a single `json` field are still
accepted. Rust behaviors can instead opt into `abiEncoding:
"postcardTypedSchema"` to have the host send `typedSchema` frames. In that mode
fixed request/output fields use postcard structs, while dynamic JSON positions
use an explicit `PostcardJsonValue` enum instead of relying on postcard
self-description; the Rust guest SDK converts schema-neutral input into the
behavior's concrete `Input` type before calling the handler. This keeps the
outer Wasm ABI stable while moving schema-generated field-level payloads onto a
binary host/guest path.

The caller can request deterministic host reads before the behavior runs:

```json
{
  "behavior": "echo",
  "mutation": "echo.send",
  "input": { "roomId": "general", "body": "hello" },
  "read": {
    "records": [{ "table": "rooms", "key": "general" }],
    "nestedRecords": [
      {
        "table": "rooms",
        "parentKey": "general",
        "nested": "messages",
        "nestedKey": "manual-2"
      }
    ],
    "latestMessages": [{ "roomId": "general", "limit": 10 }]
  }
}
```

The Admin UI uses the same invoke endpoint. Operators can pick a loaded module and mutation, fill an input form rendered from the mutation's `FieldSchema`, inspect the committed host facts, and then trace those facts through the WAL audit panel.

NextDB resolves that read plan before Wasm execution and passes the result as `request.context`:

```json
{
  "records": [
    {
      "table": "rooms",
      "key": "general",
      "record": {
        "table": "rooms",
        "key": "general",
        "value": { "id": "general", "title": "General" },
        "lsn": 1
      }
    }
  ],
  "nestedRecords": [
    {
      "table": "rooms",
      "parentKey": "general",
      "nested": "messages",
      "nestedKey": "manual-2",
      "logicalTable": "rooms.messages",
      "logicalKey": "general:manual-2",
      "record": {
        "table": "rooms.messages",
        "key": "general:manual-2",
        "value": { "id": "manual-2", "body": "nested" },
        "lsn": 2
      }
    }
  ],
  "latestMessages": [
    {
      "roomId": "general",
      "messages": []
    }
  ],
  "requestContext": {
    "ctx": {
      "timestampMs": 1760000000000,
      "sender": {
        "kind": "user",
        "userId": "alice",
        "behavior": "rooms",
        "mutation": "send"
      },
      "rngSeed": "64-character-sha256-hex-seed"
    }
  }
}
```

`request.context.requestContext` preserves the caller-provided context and adds
deterministic runtime context under `ctx`. `ctx.timestampMs` is generated once
for the turn and preserved when a continuation supplies an existing `ctx`;
`ctx.sender` identifies the logical sender and behavior mutation; `ctx.rngSeed`
is a stable SHA-256 seed derived from the runtime id and turn identity unless a
continuation carries an existing seed. The TypeScript Behavior SDK exposes this
as `runtimeContext(request)`.

The guest returns a JSON `BehaviorInvokeOutput`:

```json
{
  "commands": [
    {
      "type": "upsertRecord",
      "table": "rooms",
      "key": "general",
      "value": {
        "id": "general",
        "title": "General"
      },
      "durability": "strict"
    },
    {
      "type": "sendMessage",
      "roomId": "general",
      "body": "hello",
      "attachments": [],
      "durability": "strict"
    }
  ],
  "result": {}
}
```

NextDB executes the commands through its normal host path, so WAL, live state, chat log projections, durable user inboxes, object references, subscriptions, connection control events, and checkpoints remain authoritative.
The host invoke response wraps that output with committed host facts and execution metadata:

```json
{
  "output": { "commands": [], "result": {} },
  "metadata": {
    "behavior": "echo",
    "behaviorVersion": "0.1.0",
    "epoch": 1
  },
  "committed": []
}
```

Rust behavior authors can use:

```text
crates/nextdb-behavior-sdk
```

The SDK exports the required ABI with `nextdb_behavior!(Input, handler)` and provides typed request/output/command helpers. The macro exports both legacy `invoke` and v2 `handle_message`, so new hosts use the message entrypoint while old packages remain loadable. `runtime_context(&request)` decodes the host-injected deterministic context (`timestampMs`, sender, and `rngSeed`) from `request.context.requestContext.ctx`. Behavior code stays focused on business logic and returns host commands rather than writing storage directly.

Behavior record transactions use the same operation vocabulary as the client API: `upsert`, `delete`, `nestedUpsert`, and `nestedDelete`. The host maps those commands into `RecordTransactionCommitted` WAL facts, so behavior-authored nested-table batches keep the same parent-partition checks, cache events, audit trail, object-reference tracking, and same-LSN semantics as external client writes.

Behavior object commands use `putObject` and `deleteObject`. `putObject` carries `bodyBase64`, `contentType`, and optional `objectId` / `clientMutationId`; the host decodes the body and routes it through the same object-store path as HTTP uploads, including blob replication, `ObjectCommitted` WAL, durable object sync, and object subscriptions.

Behavior continuation scheduling uses `scheduleActorReminder`. It writes the existing strict `ActorReminderScheduled` WAL fact and inserts the reminder into the actor reminder wheel, so a later `runDueActorReminders` maintenance pass turns it into an actor message. If the reminder payload is `{ "type": "behaviorContinuation", "behavior": "...", "mutation": "...", ... }`, the due runner also rebuilds a `BehaviorInvokeRequest` and re-enters the target Behavior Wasm after the actor reminder turn. The fire response includes the normal actor `turn`, optional `behavior` invoke output/committed facts, and optional `reply` schedule result when a continuation callback was durably enqueued. This is the third slice of the P4 continuation ABI: it persists "turn ended, resume this behavior later" without adding a second scheduler, and rejects expired or runaway chains with `deadlineMs`, `callDepth` / `maxDepth`, and path-based cycle checks at both schedule and fire time. Call-chain metadata is carried through `request.context.requestContext` as `callChainId`, `callDepth`, `maxDepth`, `deadlineMs`, and `path`. The Rust, TypeScript, and AssemblyScript behavior SDKs expose `scheduleBehaviorReminder(kind, key, behavior, mutation, options)`, which is the scheduled-table-style authoring layer: it emits the same durable actor reminder command but fills the payload with a `behaviorContinuation`, so the protocol and WAL format stay stable while behavior authors schedule future mutation turns directly. A continuation can also declare `replyTo` with a target actor and callback continuation; after the parent behavior succeeds, the host schedules that callback as another strict actor reminder, inherits call-chain deadline/depth/path fields when the callback omits them, and injects the parent `behaviorResponse` into callback input. Behavior invocations with a root `clientMutationId` derive a stable per-command reminder id when the command omits `reminderId`; under that idempotent mode the command must use absolute `dueAtMs`, and duplicate identical schedules return the original `ActorReminderScheduled` response from a WAL-derived actor-reminder index instead of appending another reminder fact. `requestHostHttp` is the first async host IO command: the host validates method, headers, body size, timeout, manifest command capability, and `hostHttpScopes.allowUrlPrefixes`, appends a strict `HostHttpRequested` WAL fact, then executes the HTTP request outside the Wasm turn. Completion wraps the result as `input.hostHttp`, schedules the supplied `behaviorContinuation` through the same actor reminder path, and appends `HostHttpCompleted` after the callback reminder is durable. Startup scans WAL for requested entries that have neither `HostHttpCompleted` nor an already scheduled callback reminder and replays them. Host HTTP keeps a WAL-derived `requestId` index in memory: a duplicate identical request returns the original accepted request and never appends another `HostHttpRequested` fact or starts another outbound request. Behavior invocations with a root `clientMutationId` derive deterministic per-command host HTTP `requestId`s, so a retried invocation is request-idempotent. Every outbound request carries `x-nextdb-request-id: <requestId>` and, unless the behavior supplied its own value, `idempotency-key: <requestId>`; behaviors cannot spoof `x-nextdb-request-id`. Host HTTP still remains at-least-once across the crash window before a callback reminder or completion fact is durable; exactly-once external side effects depend on the downstream service honoring the stable idempotency key.

Behavior invocations may also carry an invocation-level `clientMutationId` of up to 128 characters. The Rust and TypeScript guest SDKs expose that value on the invoke request, while the host normalizes it and derives per-command ids such as `{id}:000:upsertRecord`, then submits each durable host command through the existing idempotent client commit path. A retry of the same behavior invocation therefore reconstructs the original committed command responses, including durable user events, from WAL instead of appending duplicate facts. For `requestHostHttp`, the same derived id becomes the host HTTP `requestId`; retries return the original accepted request from the WAL-derived request index without sending the external request again. For `scheduleActorReminder`, the same derived id becomes the reminder id when the behavior omitted one; retries with the same absolute `dueAtMs` and payload return the original schedule response. Invocation-level idempotency deliberately still rejects `publishVolatile`, `publishUserVolatile`, realtime channel broadcast/state/presence, connection disconnect, and runtime activation output because those committed responses are not durable replay targets.

Behavior read plans can hydrate `records`, `nestedRecords`, `latestMessages`, `objects`, `objectBodies`, realtime channel members/state, and active connection sessions into `request.context`. Record and nested-record reads use the same live-or-disk projection as external record reads: hot volatile, resident, and LRU table entries are checked first, then durable record files are used as the cold fallback. Object body reads return metadata plus `bodyBase64`, keeping the Wasm ABI JSON-only while still allowing a behavior to inspect object payloads before returning host commands.

TypeScript behavior authors can use:

```text
packages/nextdb-behavior-sdk
```

The TypeScript SDK ships an AssemblyScript-compatible authoring surface under `@nextdb/behavior-sdk/assembly`. A behavior entry exports:

```text
handle(requestJson: string) -> string
```

The `nextdb-behavior compile` CLI generates a temporary ABI wrapper, invokes AssemblyScript, and writes a server-ready behavior directory with `nextdb.behavior.json` plus `.wasm`. The wrapper exports `memory`, `alloc`, `dealloc`, and `invoke`, matching the Rust behavior ABI. `nextdb-behavior pack` remains available for packaging precompiled Wasm.

This is a practical TypeScript-family backend, not full Node/TypeScript semantics. Behavior code must stay within the AssemblyScript subset and use JSON-string helpers from the SDK.

## Schema Registry

The schema registry persists a default schema on first boot:

```text
data/schema/nextdb.schema.json
data/schema/history/v{version}.json
```

It describes:

```text
objects.Object
tables.rooms
tables.rooms.nested.messages
behaviors.echo.mutations.echo.send
behaviors.echo-ts.mutations.echo.send
```

API:

```text
GET /v1/schema
GET /v1/schema/history
GET /v1/schema/history/{version}
GET /v1/schema/validate
GET /v1/schema/migration-plan
GET /v1/schema/storage-policy
GET /v1/schema/typescript
POST /v1/admin/schema/reload
POST /v1/admin/schema/apply
```

The generated TypeScript includes branded `Id<T>` types, object metadata types, typed object-store bindings, typed chat room bindings, room/message nested table types, event payload types, behavior input and read-plan types, typed audit trace/replay options, typed index query options, typed record predicates, typed local-data, stored-subscription, cache-lease, and pending-write diagnostics/management methods, typed realtime channel state/recent-event/recent-signal helpers, and a generated `NEXTDB_SCHEMA_VERSION` constant. Object store calls are tied to `schema.objects`, so metadata, object ids, list pages, delete responses, and object subscription events carry the declared object metadata type while using the same runtime object API. `db.room(roomId)` is tied to `tables.rooms` and `tables.rooms.nested.messages`, so primary chat-path reads, message sends, room subscriptions, local room cache controls, object-reference attachments, and room volatile publishes carry the declared room, message, object, and event payload types. Event calls are tied to `schema.events`, so `publishUserEvent`, `publishUserVolatile`, `onUserEvent`, `watchCurrentUserEvents`, and realtime channel signal/event/state listeners carry the declared payload type for each event name. Realtime channel state helpers expose generic state snapshots, allowing `state<T>()`, `updateState<T>()`, `cachedState<T>()`, `watchState<T>()`, and `onState<T>()` to bind app-defined lobby/game state while retaining the branded channel id; recent event/signal helpers and `onEventKind` / `onSignalKind` preserve the schema payload shape and narrow the requested `kind` literal. Local-data helpers expose typed `cacheCoverage`, `localDataStatus`, `watchLocalDataStatus`, `pendingWriteQueueStatus`, `watchPendingWrites`, `pendingWriteStats`, `flushPendingWrites`, stored-subscription registry controls, cache lease refresh, and local cache clearing so generated clients can inspect and manage cache, volatile channel projections, restored subscriptions, and offline queue state without dropping to untyped runtime calls. Audit calls are tied to the schema too: `traceEntity` and `replayEntity` reject unknown table/nested-table names and plain string keys for schema-bound records, while replayed record values carry the generated table or nested-table value type. Behavior calls are tied to `schema.behaviors` for mutation input, and their read plans bind `records`, `nestedRecords`, `latestMessages`, `objects`, `objectBodies`, `realtimeChannelMembers`, and `realtimeChannelStates` to schema table, nested-table, room id, object-id, and realtime-channel id types before Wasm execution; behavior `userId` is also a branded user id. The runtime resolves those record plans through live-or-disk state, so typed behaviors see current hot-table rows. Index calls are tied to the declared index field list: single-field indexes use `value` / `lower` / `upper` with that field's scalar type, while compound indexes use tuple-shaped `values` / `lowerValues` / `upperValues` in schema order. Predicate calls bind `field` to known table or nested-table fields and bind `value` to the selected field type or list item type for `contains`. Runtime record reads and live queries also accept `RecordPredicate` for deterministic server-side filtering. `typedNextDb(raw)` pins runtime clients to the generated schema version through `withSchemaVersion`, causing HTTP writes and realtime connects to carry the client schema version. Client-protected writes, object mutations, behavior invocations, realtime channel mutations, and subscription connects fail with `409 schemaVersionMismatch` if that version differs from the active server schema. The runtime SDK exports `FieldSchema`, `FieldType`, table schema, nested-table schema, object schema, and behavior schema shapes so tools such as the Admin UI can render schema-derived controls without private schema copies.

Schema validation walks every object, top-level table, nested table, and behavior mutation field before reload. It rejects empty `Text.inlineUntil`, empty `Id.entity`, `ObjectRef` targets that are not declared under `objects`, invalid nested `Object` fields, invalid indexes, and invalid `ChatLog` storage declarations. For `ChatLog`, `bucket` must currently be `day(field)`, `order` must be non-empty, `liveWindow` must be positive, and the bucket/order fields must exist on the nested table.

The registry is also used at runtime:

```text
sendMessage -> validate rooms.messages draft before WAL append
upsert/delete/transaction -> validate top-level or nested record values before WAL append
publishUserEvent/publishVolatile -> validate declared event payload before WAL append or realtime delivery
invokeBehavior -> validate behavior input before Wasm execution
```

Validation failures return `400` and do not append WAL records. `Text.inlineUntil` is enforced as a byte limit, `Int64` and `TimeMs` require JSON integers, and `ObjectRef.byteSize` must be a non-negative integer. Declared `ObjectRef` fields are also resolved against the built-in object store before commit, delivery, or behavior invocation: the object id must exist, and `path`, `contentType`, `byteSize`, and `sha256` must match the stored metadata. Behavior host commands reuse the same client commit paths, so behavior-authored records, transactions, and declared volatile events are validated before commit or delivery as well.

Schema reload validates the edited schema and preflights the record projection rebuild before replacing the in-memory registry. If reload fails, the previous schema stays active and the previous record projection remains readable.

`GET /v1/schema/migration-plan` compares the disk schema with the active in-memory schema. Additive evolution is compatible. Version downgrades and field type or optional-shape changes across record fields, object fields, event payloads, and behavior mutation inputs are unsafe breaking changes. Table, nested-table, and object field removals plus table schema removals, nested table schema removals, event schema removals, unreferenced object schema removals, and behavior schema removals are replay-safe breaking changes only when an operator explicitly sets `allowBreakingReplay`; the migration plan reports this with `requiresReplayRebuild`, `replaySafeBreakingChanges`, and `unsafeBreakingChanges`. The plan also reports `projectionRebuildRequired` with concrete reasons for field removals and record projection shape changes such as index or storage/order edits. `POST /v1/admin/schema/reload` runs the same check, preflights candidate indexes and clustering order against retained WAL facts, swaps the rebuilt projection into place, and only then swaps the registry. If a new unique index conflicts with existing rows, reload fails before the active schema changes.

`GET /v1/schema/history` lists the durable schema versions known to the node, and `GET /v1/schema/history/{version}` returns the exact schema file for that version. Startup writes the active schema into history if it is missing, and schema apply writes the immutable version file before replacing the current schema file. Successful schema apply also appends a `SchemaApplied` WAL control fact containing the full schema and migration plan; audit can filter it with `payloadType=schemaApplied` or `path=schema/versions/{version}`. The apply path uses the local schema apply mutex, optional `expectedVersion`, shard 0's owner and freeze gates, then runs replica preflight according to the shard WAL remote-ack policy before mutating schema files or projections, so clustered replicas learn schema changes only through validated, replicated WAL facts. Replicas apply this control fact when it arrives through WAL replication: they persist schema history, replace current schema, and rebuild record projections from local WAL before accepting later replicated record facts. This keeps WAL audit, replay, hot replication, and export tools from depending on today's schema when explaining old facts.

Export bundles copy the same history under `schema/history/v{version}.json`, copy the operator schema proposal ledger under `schema/proposals.json`, and copy cluster-control ledgers under `cluster/`. Bundle verification compares manifest history versions with those files, checks every WAL record's `schemaVersion` is present either in `schema.json` or in schema history, validates proposal candidate schemas without making uncommitted proposals part of active schema history, and validates topology proposal/workflow/lease references without re-running cluster coordination. Restore writes the history files, schema proposal ledger, topology overrides/log/proposals/lease, and handoff workflow ledger back to the target node after applying the current schema, so imported audit trails can still resolve old records and explain prepared, aborted, failed, or committed schema and topology rollout decisions without access to the source node.

`GET /v1/schema/storage-policy` returns a runtime view of table and nested-table storage classes plus the active hot window and resident room limit. `GET /v1/health` also includes `recordHotCache`, showing which logical record tables currently have memory-resident projections and how many records are resident. A table's storage class is a versioned schema policy, so `disk`, `lru`, `resident`, `actorPartition`, `chatLog`, and `object` are shapes a logical table can move between instead of permanent client-visible table kinds. Shape changes go through the same schema apply/reload path as field and index changes: the candidate schema is validated, the migration is committed as a `SchemaApplied` WAL control fact, record/index/order projections are rebuilt or verified, and only then does the runtime swap the active registry and reconfigure hot residency. Durable shapes keep WAL plus disk projections as the table truth during the transition; memory-resident state is activated, prewarmed, drained, or dropped only after the projection covers the migration LSN. Reads may fall back to disk until a target hot shape is ready, and `/v1/schema/storage-policy` should expose requested shape, effective shape, migration status, hot counts, and fallback status for the Admin UI and SDK diagnostics. `npm run test:schema-actor-policy` verifies the runtime transition by applying `rooms.storage = lru` and a smaller `rooms.messages.storage.liveWindow`, checking immediate health/storage-policy changes, bounded actor snapshots, LRU eviction, and restart recovery from the persisted schema. This is how the admin UI shows whether a logical table is actor-resident, LRU-backed, disk-backed, chat-log-backed, or object-backed. The detailed stable contract is documented in [Stable External Contracts](STABLE_CONTRACTS.md#table-storage-shape-contract).

WAL records and runtime snapshots carry `schemaVersion`. This keeps event-sourcing audit trails tied to the contract that produced each fact, and gives later replay/migration code enough context to handle historical records deliberately. Historical schema files are the read-optimized index for the schema WAL facts; replicated WAL records remain the durable data truth. Multi-node schema rollout is owner-written, compare-and-swap guarded, proposal-prepared on replicas, replica-preflighted, proposal-commit-acknowledged, and WAL-replicated.

## What Is Not Implemented Yet

- Native server-side HTTP/3 WebTransport listener. The SDK connection layer can select transports conceptually, but the current server runtime still exposes HTTP and WebSocket surfaces.
- Full Raft-style replicated log consensus. The current cluster layer has shard ownership, epoch fencing, remote WAL mirroring, quorum acknowledgement policies, handoff, failover proposals, and SDK read quorum routing, but it does not yet implement a consensus log with rollback.
