# NextDB Implementation Plan v2

Status: accepted, in progress.
Date: 2026-07-02.
Scope: this document records the repositioning decisions and the phased
implementation roadmap. For the target data/runtime design see
[DESIGN.md](DESIGN.md); for the current MVP see [ARCHITECTURE.md](ARCHITECTURE.md).

Current implementation notes:

- P0/P1 safety and storage-engine groundwork is partially landed: WAL v2 framed
  encoding with inline CRC32C and v1 compatibility, zstd/postcard snapshots,
  asynchronous projection apply, fjall-backed record projection keyspaces, and
  strict kill-9 durability smoke coverage.
- P2 has started with room runtime sharding: `RoomLiveState` is now owned by
  stable actor shard OS threads, each thread holding a plain room HashMap
  behind its blocking mailbox. This removes the previous global room-directory
  `RwLock` and per-room mutex from the hot room write/read path while preserving
  process-wide LRU and idle passivation. Shard threads are named, optionally
  pinned to cores through `NEXTDB_ACTOR_PIN_THREADS`, and exposed in health /
  runtime activation status. The internal directory key has moved from raw
  `room_id` strings to `ActorId { kind, key }`, with `room` actors using the
  same route path that scope/table/view/aggregate actors share. A minimal
  actor-kernel turn path now activates non-room actors on the same shard owner
  threads and serializes their turns through the mailbox. The admin/runtime
  control plane can activate a generic actor and exposes aggregate
  `actorKernel` counts by kind through health/runtime activation status.
  Explicit runtime record activation now also hydrates found rows into a
  table-scoped set of `scope` actors, updates the owning `table` actor's
  minimal scope directory on the table actor's own shard, and exposes
  `actorScopes` row counts plus aggregate `tableScopes`. Top-level records
  currently route into stable 256-way hash buckets and nested records route by
  parent partition. Actor snapshots now persist and restore non-room
  `scope`/`table`/generic actor state through `actorStates`, so graceful
  restart preserves activated scope rows and table directories. Startup also
  overlays record WAL tail facts after the snapshot LSN onto already-resident
  actor states without activating cold tables. Table actors now persist
  threshold-based split metadata (`tablePendingSplits` and per-scope
  `splitPending`) using `NEXTDB_ACTOR_SCOPE_SPLIT_ROWS` and
  `NEXTDB_ACTOR_SCOPE_SPLIT_BYTES`, drain oversize parent scopes into
  deterministic two-way child scopes, recursively split oversize child scopes,
  preserve child routing through actor snapshots, and route WAL-tail overlay
  updates through the restored table directory. Optional actor split maintenance
  (`NEXTDB_ACTOR_SPLIT_MAINTENANCE_INTERVAL_MS` /
  `NEXTDB_ACTOR_SPLIT_MAINTENANCE_LIMIT`) sweeps already-marked pending scopes
  without waiting for a future write, and snapshot-backed `splitReminderAtMs`
  preserves due split work across graceful restart. A first WAL-backed durable
  actor reminder path is also landed: `actorReminderScheduled` /
  `actorReminderCancelled` / `actorReminderFired` facts rebuild a runtime
  reminder wheel at startup, admin/SDK APIs can schedule, cancel, inspect, and
  run due reminders, and optional maintenance is controlled by
  `NEXTDB_ACTOR_REMINDER_MAINTENANCE_INTERVAL_MS` /
  `NEXTDB_ACTOR_REMINDER_MAINTENANCE_LIMIT`. Reminder firing is currently
  at-least-once across the crash window. The P4 behavior binding now uses this
  path for durable `behaviorContinuation` reminders, scheduled behavior turns,
  host HTTP callbacks, and `replyTo` callback routing.
- P3/P4/P5 remain target work: subscription refcount residency, full actor
  kernel semantics, guest ABI v2 hardening, hot reload epoch swap, and the full
  background replay migration pipeline are not complete. P3 groundwork has now started at the
  actor-kernel layer: scope actors carry subscription refcounts, lingering
  deadlines, and L3/L1 residency tier metadata in snapshot-compatible state;
  nested-table WebSocket subscriptions retain/release their parent scope; and
  optional `NEXTDB_ACTOR_SCOPE_RESIDENCY_MAINTENANCE_INTERVAL_MS` /
  `NEXTDB_ACTOR_SCOPE_RESIDENCY_MAINTENANCE_LIMIT` sweeps idle released scopes
  from full row residency to index-only residency. Live query subscriptions
  now also retain the scope set represented by their current result page,
  update that set on query refresh, and release it on unsubscribe,
  replacement, or disconnect. Full-table and key-range WebSocket
  subscriptions now retain/release the subscribed table's full top-level
  256-bucket scope set; declared-index prefix subscriptions continue to rely
  on concrete result/live-query residency because their equality prefix does
  not map to a stable key-scope subset. Record HTTP reads now expose P3 cold-read
  consistency controls: `eventual` remains the default, `read-your-writes`
  waits for the projection applier to reach `minLsn`, and `strong` waits for
  the projection applier to reach the node's current WAL LSN; the TS SDK
  exposes this as `recordConsistency` without overloading cluster quorum
  `consistency`. Oversized live query result pages are now guarded by
  `NEXTDB_MAX_LIVE_QUERY_RESULT_ROWS` (default 250); over-limit query
  subscriptions are rejected before sending the result or retaining their
  scope set. A first schema-declared read-visibility policy is now enforced
  on the realtime fan-out path: table/nested-table `readVisibility` rules can
  require a record field to equal the authenticated user id, so record upserts
  are filtered per row before delivery to table or nested-table subscribers.
  Direct record deletes now carry an optional before-image in the delivery
  event, allowing the same read-visibility predicate to fan out deletes only
  to users who could see the deleted row and letting those clients clear local
  caches without leaking protected keys to others. Subscription catch-up /
  resume events also pass through the same read-visibility filter before
  being sent to clients, so replayed table and nested-table events do not
  bypass realtime RLS. The TS SDK table and nested-table watchers now perform
  a default hydration pass before opening their realtime subscription: they
  sync the watched table/partition to the current cursor, emit the hydrated
  cache snapshot, then subscribe with the stored resume cursor so subsequent
  catch-up covers events after that snapshot LSN. `hydrate: false` remains
  available for callers that want the old cache-first/event-only behavior.
  Realtime connections now maintain a record subscription router keyed by
  logical table, with parent-key prefix matching for nested-table
  subscriptions and lower-bound ordered key-range matching as the first
  interval primitive. Nested-table fan-out no longer linearly scans every
  nested subscription on the connection. Table WebSocket subscriptions now
  accept optional `lowerKey` / `upperKey` bounds, carry those bounds through
  the TS SDK active/pending/persistent subscription lifecycle, filter SDK
  listeners by target range, and apply the same range filter to WAL catch-up.
  Table subscriptions now also accept declared-index equality prefixes through
  `indexName` + JSON `indexValues`: the server validates the index against the
  active schema, routes matching upserts/deletes through connection-local
  record-value filtering, and carries the option through TS SDK
  active/persistent subscription identity. Index-prefix subscriptions support
  `serverSnapshot`: full equality prefixes use the declared secondary-index
  query path, while shorter prefixes fall back to an equivalent equality
  predicate over the indexed fields; when combined with key ranges, the
  snapshot scanner continues through in-range non-matches until it fills the
  matching page or reaches the upper bound. The global pre-broadcast registry
  now compiles schema-aware index-prefix candidate entries, so non-matching
  same-table writes are excluded before connection-local RLS/query filtering runs.
  Table and nested-table WebSocket subscriptions can also request a
  server-side initial snapshot at a projection-consistent WAL LSN; the TS SDK
  exposes this as `serverSnapshot` / `snapshotLimit`, applies the snapshot to
  local cache, advances full-table or nested-partition cursors for empty
  snapshots, keeps range snapshots from poisoning full-table cursors, filters
  local watcher snapshots by target range, and keeps RLS filtering on the
  snapshot path. Key-range initial snapshots currently use a conservative
  key-order scan. Connection-local range routing now maintains a rebuilt
  prefix-best upper-bound cache on subscription changes, so record fan-out
  range checks are O(log n) on the hot event path instead of scanning every
  lower-bound candidate. A first global pre-broadcast fan-out registry is now
  in place: each realtime connection registers a targeted event queue, and
  delivery event publishing indexes candidates by room, full table, table-range
  table, nested logical table, live-query logical table, user-event user id, and
  object subscription before the connection-local RLS/query refresh filter runs.
  Table-range and nested-table global routing now keep exact candidate indexes:
  range subscriptions are stored in a per-table interval index with a rebuilt
  prefix-best upper-bound cache for early stop during reverse lower-bound
  traversal, and nested-table subscriptions are stored in a per-logical-table
  prefix index that probes only actual string prefixes of the changed key
  instead of scanning all lexicographically lower candidate prefixes. A first
  compaction pass now buckets global fan-out indexes internally: bounded table
  ranges are stored only under the possible key buckets plus a conservative
  fallback for full-width ranges, and nested prefix subscriptions are stored by
  stable prefix hash bucket while still probing only actual prefixes of the
  changed key. The connection-local router remains as the final correctness and
  RLS/query-refresh guard.
- P4 hot-reload groundwork has started at the behavior runtime boundary:
  behavior reload now performs an atomic map swap with a monotonic publish
  epoch, failed reloads preserve both the previous behavior set and epoch, and
  behavior invocation responses report the behavior name, behavior version, and
  epoch used for that invocation. This gives actor turns a stable code epoch to
  bind to before the full continuation ABI lands. Behavior reload now also
  prepares the new compiled behavior set, appends a strict `BehaviorPublished`
  WAL fact with epoch and manifest summaries, and only then commits the
  in-memory epoch swap; the fact is visible through WAL audit and round-trips
  through the postcard WAL v2 encoding. Behavior invocation now keeps a bounded
  per-behavior resident Wasm instance pool for the active epoch, calls optional
  `on_activate()` once when creating an instance, calls optional
  `on_deactivate()` when pooled instances are discarded or old epoch pools are
  released, routes normal turns through `handle_message` with legacy `invoke`
  fallback, routes stale/unknown mutation names to optional
  `on_unknown_message`, exposes `scheduleActorReminder` as the first durable
  continuation host command backed by existing actor-reminder WAL facts,
  re-enters Behavior Wasm when a due reminder carries a `behaviorContinuation`
  payload, enforces continuation `deadlineMs`, `maxDepth`, and path-based cycle
  checks at schedule and fire time, exposes `requestHostHttp` as the first async
  host IO command with manifest `hostHttpScopes.allowUrlPrefixes`, schedules
  HTTP responses back through the same actor-reminder continuation path, records
  `HostHttpRequested` / `HostHttpCompleted` WAL facts so startup can replay
  in-flight requests with at-least-once semantics, supports continuation
  `replyTo` targets for durable cross-actor callback scheduling with the parent
  `behaviorResponse` injected into callback input, and drops instances that trap
  or return invalid host commands instead of returning them to the pool. The
  AssemblyScript compile path now supports `abiEncoding: "postcard"` by
  wrapping the existing string-handler authoring model in postcard JSON frames;
  `postcardTypedSchema` remains a precompiled/custom-Wasm boundary because it
  requires a typed postcard entrypoint.

## Positioning

NextDB is a **single-node virtual actor application server** with built-in
event-sourced durability (WAL -> projections/snapshots), subscription-driven
tiered memory residency, and Wasm-sandboxed user logic.

Comparable systems: SpacetimeDB, Cloudflare Durable Objects, Orleans
(single-silo). Differentiators against SpacetimeDB, each backed by a
structural cause in our architecture:

| Dimension          | SpacetimeDB 2.x                     | NextDB target                                   |
| ------------------ | ----------------------------------- | ----------------------------------------------- |
| Concurrency        | single-writer per database          | per-scope serial turns, cross-scope multi-core  |
| Blocking IO        | procedures block the serial queue   | IO-as-message, never blocks a turn              |
| Memory             | all tables resident in RAM          | subscription-refcounted L0–L3 residency         |
| Subscriptions      | write-set triggered re-evaluation   | incremental apply + delta push                  |
| Schema migration   | column/table removal wipes the DB   | WAL replay rebuilds projections, zero data loss |
| Guest languages    | Rust modules only                   | language-neutral ABI, Rust first, TS later      |

Explicit non-goals: distribution, consensus, replication, failover, SQL.
Backup/DR is cold: WAL segment archival + periodic snapshots.

## Decision Records

### D1. Stay on Rust + wasmtime (no Elixir/BEAM rewrite)

User code runs on the Wasm side, so OTP ergonomics never reach the user. The
OTP properties we want are provided at the Wasm boundary, often stronger:

```text
preemptive scheduling  -> wasmtime fuel / epoch interruption
crash isolation        -> Wasm trap kills one instance, host unaffected
per-process heap       -> per-instance linear memory
hot code upgrade       -> module reload + epoch swap (see D5)
sandbox / capabilities -> behavior scopes (already implemented)
```

The performance ceiling of a database is in the storage engine (WAL batching,
serialization, KV engine, memory layout), where BEAM requires NIFs and Rust is
native. Supervision-style restart of host background tasks is implemented in a
few hundred lines (or via `ractor`) without changing runtimes.

### D2. Row / Scope / Table / View hierarchy

Actor semantics live on consistency boundaries; addressability sinks to rows.

```text
View actor    cross-table read model, subscribes to event streams (physical)
Table actor   table metadata + scope directory + hydration policy (physical)
Scope actor   a key range / partition. THE physical activation unit:
              pinned to one shard thread, owns all rows in range,
              serial turn execution, zero locks             (physical)
Row           addressable logical entity: value + lsn/version + flags.
              Messages to a row are routed to its scope's mailbox (logical)
```

Forbidden: one physical actor (task + mailbox + instance) per row. Cost of the
anti-pattern: +400B–1.5KB per row for task/mailbox state (10M rows: +4–15GB vs
+0.2–0.5GB for logical rows), and ~100–300ns message tax per row access turning
a 2–5ms million-row scan into 0.2–0.6s. Rows get identity, per-row event
streams, per-row subscriptions, optimistic versions — without physical cost
(+16–48B metadata per row, ~10–20% over raw data).

Scopes split automatically by row-count/byte thresholds (tablet-style).
Transactions stay within one shard (existing same-shard rule preserved).

### D3. Subscription-driven tiered activation

Memory is paid only for data someone is watching. Subscription count is the
truth signal for heat — no LRU guessing.

```text
subscribe(range):
  find/create covering scope actor; refcount += 1
  cold -> hydrate from projection engine, record hydration LSN L
  send initial snapshot @L, then stream every event with LSN > L   (gapless)

unsubscribe / disconnect:
  refcount -= 1
  zero -> lingering grace period (30s–5min, configurable)
       -> tier down: L3 (full rows) -> L1 (index only) -> L0 (cold)

Residency tiers (map to DESIGN.md memory policies):
  L0 cold      nothing resident                          (diskOnly)
  L1 index     activation index resident, rows on disk   (activation index)
  L2 windowed  hot window rows resident                  (windowed)
  L3 full      whole scope resident                      (resident/onActivate)

cold read:   straight to projection engine (µs-level point/range read)
cold write:  WAL append -> async projection apply; memory untouched
             UNLESS the event matches an active subscription range (Rule 1)
```

Cold-read consistency is explicit, three levels: `eventual` (default, read
projection), `read-your-writes` (wait for projection to reach the client's
last-write LSN), `strong` (route through the live scope or wait for WAL tail).

Guards: subscriptions over a size threshold are rejected or degraded to
L1 + event stream + fetch-row-on-demand.

Subscription shapes v1: key ranges and declared-index equality prefixes only.
No predicate subscriptions (RethinkDB/Convex trap); richer filtering is served
by view actors or client-side filtering. Aggregates (counts, sums, presence)
are built-in counter actors maintained from event streams — never re-scans.

Row-level security: predicates declared on schema/scope (e.g.
`owner == sender`), evaluated per-row O(1) on the fan-out path. This adopts
SpacetimeDB's `#[view]` concept while avoiding its re-evaluation cost.

### D4. Five iron rules

Each rule corresponds to a verified failure elsewhere.

1. **Every write goes through WAL and subscription range matching.** Never
   write storage blind — a non-subscriber's write must still reach
   subscribers' activated scopes. (Closes the update-loss hole of
   "direct writes go only to disk".)
2. **No blocking IO inside a turn.** External IO = message to a system actor;
   the result returns as a new mailbox message (same continuation ABI as
   cross-actor calls). (SpacetimeDB's synchronous `ctx.http` stalls the whole
   database's serial queue.)
3. **Actor semantics on scopes, addressability on rows.** No process-per-row.
   (Erlang folklore; cost table in D2.)
4. **Subscription updates are incremental apply + delta push**, never re-run
   the query per subscriber. (SpacetimeDB write-set re-evaluation bottleneck.)
5. **Linear memory is only a cache.** Authoritative actor state = snapshot +
   event stream. Dropping any Wasm instance at any moment must be lossless.
   `on_deactivate` must not carry critical persistence (ABI hard constraint).

### D5. Hot reload via epoch swap (mandatory feature)

Because of Rule 5, hot reload needs no Erlang-style `code_change` state
migration: **event replay is the universal state migration** — new code
re-derives state from the same history on activation.

```text
publish flow:
  1. upload module -> compile/instantiate check, ABI version check,
     schema compatibility check (incompatible schema -> P5 replay-rebuild path)
  2. registry prepares vN+1; append BehaviorPublished fact to WAL
     (upgrade history is auditable and replayable)
  3. commit epoch switch only after WAL ack: in-flight turns finish on vN (turns are natural safe
     points, ms-level); every new turn uses vN+1
  4. resident instances: lazy (default) — mark stale, next message triggers
     passivate + activate on vN+1; or eager — background drain per shard,
     rate-limited to avoid activation storms
  5. vN refcount hits zero -> unload, reclaim pooling-allocator slots
```

Compatibility rules for in-flight messages and durable reminders follow the
schema rules: added methods/optional params are compatible; removed methods
require explicit confirmation and route stale messages to
`on_unknown_message` instead of trapping.

Developer experience: `nextdb dev --watch` = file change -> incremental build
-> auto publish -> lazy swap. Connections stay up, room state survives.

### D6. Guest language: Rust first, TypeScript later

The ABI is defined at the Wasm boundary and is language-neutral (postcard
byte protocol); the kernel is language-agnostic. v1 ships only the Rust guest
SDK (direct reuse for the cypridina migration). A TS/AssemblyScript SDK is a
pure addition later — no kernel changes. Rust edit-compile-reload latency is
mitigated with a dev profile (`opt-level = 1`, incremental) for `dev --watch`;
production publishes use release builds.

## Target Architecture

```text
┌────────────────────────────────────────────────────────────┐
│ IO boundary (tokio): WS/HTTP accept, codec,                │
│ fan-out (serialize once -> Arc<Bytes> broadcast)           │
└──────────────────────────┬─────────────────────────────────┘
               hash(actor_id) % N, message dispatch
┌──────────────────────────▼─────────────────────────────────┐
│ N shard threads (thread-per-core, shared-nothing)          │
│  each thread exclusively owns:                             │
│   • scope actors (turn-serial, plain HashMap, zero locks)  │
│   •   └ rows (value + lsn/version + flags)                 │
│   • table actors (scope directory, split policy)           │
│   • view / aggregate actors (materialized from events)     │
│   • subscription range router (interval tree + RLS filter) │
│   • resident wasmtime instance pool (epoch preemption)     │
└──────┬─────────────────────────────────┬───────────────────┘
       │ events (SPSC ring, batched)     │ async IO / cross-actor
┌──────▼─────────────────┐   ┌───────────▼───────────────────┐
│ WAL writer thread      │   │ system actors: outbound HTTP, │
│ (group commit,         │   │ reminders, object store,      │
│  inline CRC32C)        │   │ projection apply              │
└──────┬─────────────────┘   └───────────────────────────────┘
┌──────▼─────────────────────────────────────────────────────┐
│ projection engine: fjall (single async applier chasing     │
│ WAL) + snapshots (postcard + zstd)                         │
│ = cold truth: cold reads, L0–L3 hydration source           │
└────────────────────────────────────────────────────────────┘
```

## Roadmap

Total estimate: 18–25 weeks (single full-time engineer). Phases are strictly
sequential; each is independently releasable and rollbackable.

### P0 — Safety net and baseline (1–2 weeks)

- Split `main.rs` (~26K lines) into `api/`, `live_query/`, `tasks/`,
  `config` modules. Pure code motion; `cargo test` green is the gate.
- Panic hygiene: zero `unwrap/expect` on wal/record_store/schema paths;
  recover poisoned locks (`unwrap_or_else(|e| e.into_inner())`);
  `now_ms()` falls back to 0 instead of panicking. Enforce via clippy lints.
- WAL fault-injection tests: truncate / bit-flip / torn-write x recovery
  assertions (intact prefix, corrupted frames quarantined).
- Benchmark baseline: criterion micro-benches + end-to-end load script +
  flamegraph. **Acceptance runs on Linux** (macOS `F_FULLFSYNC` semantics
  distort fsync-bound numbers).

### P1 — Storage engine swap (3–4 weeks)

- Projections: per-record JSON files + five manifest kinds (4096-entry caps)
  -> **fjall** keyspaces. Records: `records/{table}\0{key}`; all manifests
  replaced by native ordered prefix scans (secondary index keyspace
  `{table}\0{index}\0{value}\0{key}`); nested tables and chat-log buckets
  become prefix keyspaces.
- Move projection apply **off the write path**: single applier thread chases
  WAL; write visibility = memory state + WAL ack. (The global
  `RecordStore.write_lock` disappears with the design.)
- Encoding: WAL/snapshot/projection payloads JSON -> **postcard**; snapshots
  zstd-compressed; WAL frame v2 with **inline CRC32C** (reader keeps v1
  compat; today's async `seal_checksums` leaves a crash window).
- Zero-risk migration: projections are declared rebuildable — on startup,
  old dir present + new store empty -> rebuild from WAL replay; old dir kept
  for rollback.
- Delete/feature-gate `cluster.rs`, WAL remote replicas, shard ownership.
- Acceptance: ≥5x write throughput, ≥5x restart recovery time,
  100x kill -9 loop with zero loss (strict durability).

### P2 — Shared-nothing shards + Actor Kernel (5–7 weeks)

- N shard threads pinned to cores;
  `Arc<RwLock<HashMap<_, Arc<Mutex<RoomLiveState>>>>>` -> one plain
  `HashMap` per thread. Cross-shard communication is messages only.
- Actor Kernel: `ActorId = (type, key)` directory; on-demand activation
  (snapshot + per-actor logical event stream tail replay over the global-LSN
  WAL); turn-serial execution (thread execution order IS the serialization);
  idle passivation; volatile timers + durable reminders (WAL facts, restored
  into a timer wheel at startup). Basic durable actor reminders are implemented
  for built-in actor turns; behavior-level scheduling ergonomics remain P4.
- Land the D2 hierarchy: scope actors host rows; table actors own scope
  directories and auto-split; rooms, chat-log and record projections are
  refactored into built-in actors (dogfooding).
- Acceptance: near-linear multi-core write scaling (≥3x at 4 cores);
  single-shard latency not regressed.

### P3 — Subscription-driven residency (3–4 weeks)

- Scope refcounts; hydration LSN handshake (snapshot @L + events >L, reusing
  the SDK resume-cursor semantics); lingering grace + L3->L1->L0 tier-down.
- Per-shard interval-tree subscription router; O(log n) match per event;
  v1 shapes: key range + index equality prefix.
- RLS predicate filtering on the fan-out path.
- Cold-read consistency levels: eventual / read-your-writes / strong.
- Oversized-subscription guard (reject or degrade to L1 + on-demand rows).
- Acceptance: unsubscribed data occupies zero heat memory; subscribe-to-
  first-snapshot P99 < 50ms on warm projections; reconnect churn does not
  re-hydrate (lingering).

### P4 — Guest ABI v2 + hot reload (4–5 weeks)

- Resident instances for the activation lifetime (state in linear memory,
  subject to Rule 5). A bounded per-behavior resident instance pool and
  `on_activate()` / `on_deactivate()` hooks are implemented. The runtime now
  enables wasmtime's pooling allocator and sets an **epoch interruption**
  deadline on each activation/call/deactivation turn. Pool sizing now has a
  runtime control/feedback surface: `NEXTDB_BEHAVIOR_INSTANCE_POOL_MAX` tunes
  per-behavior resident instance reuse, wasmtime pooling totals can be tuned by
  env, and health/metrics expose current pooled instances. Fuel
  instrumentation is also runtime-tunable through
  `NEXTDB_BEHAVIOR_FUEL_ENABLED`; when it is disabled, `maxFuel` still maps to
  the epoch-interruption deadline so hot paths can avoid fuel accounting
  without losing turn preemption. Non-fuel runtime ops counters are now exposed
  globally and per behavior through health/metrics: invocations, successes,
  unknown-message turns, guest errors, command rejections, instance lifecycle,
  and pool errors.
- Lifecycle hooks: `on_activate` is implemented for instance creation and
  `on_deactivate` for resident instance discard / old epoch pool release;
  `handle_message` is implemented as the v2 message entrypoint with legacy
  `invoke` fallback, and `on_unknown_message` handles stale mutation names when
  exported.
- **Continuation ABI**: cross-actor calls and async IO unified as
  "turn ends -> host executes -> result re-enqueued as a message";
  call-chain IDs + timeouts + cycle detection (error on re-entrance v1);
  SDK codegen wraps continuations in promise-style ergonomics. First slice is
  implemented: behaviors can return `scheduleActorReminder` to persist a future
  actor message through `ActorReminderScheduled`, and due reminders with
  `type: "behaviorContinuation"` rebuild a `BehaviorInvokeRequest` and invoke
  the target behavior. Call-chain IDs are carried through continuation context;
  `deadlineMs`, `callDepth` / `maxDepth`, and path-based cycle checks are
  enforced when scheduling and firing continuation reminders. First async host
  IO slice is implemented: `requestHostHttp` is accepted only when the manifest
  declares both the command and `hostHttpScopes.allowUrlPrefixes`, executes the
  HTTP request outside the Wasm turn, and re-enqueues the response as
  `input.hostHttp` on a `behaviorContinuation` reminder. Accepted requests first
  append `HostHttpRequested`; completion schedules the callback reminder and then
  appends `HostHttpCompleted`, while startup replays requested entries that have
  neither a completion fact nor an already-scheduled callback reminder. Host HTTP
  now keeps a WAL-derived `requestId` index: duplicate identical requests return
  the original `HostHttpRequested` response without appending WAL or starting a
  second outbound request, and behavior-level `clientMutationId` derives a stable
  command `requestId`. Outbound requests also carry `x-nextdb-request-id` and,
  when the behavior did not provide one, `idempotency-key` with that same
  request id. This makes behavior retries request-idempotent and gives external
  services a stable dedupe key, while exactly-once external side effects across
  crash windows remain delegated to downstream idempotency. Cross-actor response
  routing now uses continuation `replyTo`: when a due continuation finishes, the
  host can schedule a durable callback reminder on another actor, inherit the
  call-chain deadline/depth/path when omitted, and inject the parent
  `behaviorResponse` into callback input. Runtime context injection is
  implemented:
  `request.context.requestContext.ctx` carries deterministic `timestampMs`,
  `sender`, and `rngSeed`, and the TypeScript Behavior SDK exposes
  `runtimeContext(request)`. Behavior-level `clientMutationId` also enables
  retry-safe `scheduleActorReminder` when the command uses an absolute
  `dueAtMs`: the host derives a stable reminder id when the behavior omits one
  and checks a WAL-derived actor-reminder index so duplicate identical schedules
  return the original schedule response instead of appending another reminder
  fact. Relative `delayMs` remains rejected under behavior invocation
  idempotency because it cannot be replayed to the exact same due time.
- Scheduled reminders with SpacetimeDB scheduled-table ergonomics have an
  initial SDK layer: `scheduleBehaviorReminder(kind, key, behavior, mutation,
  options)` builds the durable `scheduleActorReminder` command and embeds a
  `behaviorContinuation` payload, so behavior authors schedule future mutation
  turns without manually assembling reminder payload JSON.
- Wasm boundary encoding has a compatible postcard frame: manifests can set
  `abiEncoding: "postcard"` for postcard-framed JSON payloads or
  `abiEncoding: "postcardTypedSchema"` for opt-in `typedSchema` request/output
  payloads, and the Rust guest SDK exposes
  `nextdb_behavior_postcard!(Input, handler)`. The frame carries explicit
  `encoding` and `payload` fields, still accepts the previous single-`json`
  postcard frame for compatibility, and now lets the host send schema-neutral
  typedSchema requests using postcard structs plus an explicit
  `PostcardJsonValue` enum for dynamic JSON positions. The Rust guest SDK
  decodes that schema-neutral input into the behavior's concrete `Input` type,
  so generated schema types can be used at the behavior boundary without
  relying on postcard self-description.
- Hot reload epoch-swap protocol as specified in D5 is implemented, and
  `nextdb dev --watch` starts the server with a behavior-directory watcher that
  polls `data/behaviors`, prepares the new compiled behavior set, writes the
  strict `BehaviorPublished` WAL fact, and commits the epoch swap only after the
  publish record is durable.
- Rust guest SDK v1 is implemented: `nextdb_behavior!(Input, handler)` exports
  both legacy `invoke` and v2 `handle_message`, typed request/read/output
  structs include runtime context and audit read plans, and command helpers now
  cover record/object/realtime/connection/runtime activation plus
  `scheduleActorReminder`, `scheduleBehaviorReminder`, and `requestHostHttp`.
- Acceptance: behavior invoke latency down an order of magnitude vs
  per-call instantiation; slow outbound IO blocks zero other actors;
  publish under sustained load loses no messages and drops no connections.

### P5 — Surpass features (2–3 weeks)

- **Migration-as-replay**: breaking schema change -> background WAL replay
  builds new projections -> atomic switch, zero data loss (directly answers
  SpacetimeDB's `--clear-database`). First slice landed: direct apply and
  schema proposals accept `allowBreakingReplay=true` for replay-safe field
  removals, event schema removals, and unreferenced object schema removals,
  rebuild record/index/order projections from WAL when projection shape changes
  require it, preserve the active data set, append `SchemaApplied`, and report
  `replayRebuild=true`.
  The migration planner now exposes structured `requiresReplayRebuild`,
  `replaySafeBreakingChanges`, and `unsafeBreakingChanges` fields instead of
  making apply parse error strings, plus `projectionRebuildRequired` and
  `projectionRebuildReasons` so compatible index/storage/order shape changes
  explain why the record projection preflight/rebuild path must run. Field
  type/optional-shape changes are explicit unsafe hard rejects across table
  fields, object fields, event payloads, and behavior mutation inputs. Event
  schema removals are now replay-safe breaking changes: retained WAL data is
  preserved, and future undeclared events fall back to JSON passthrough.
  Unreferenced object schema removals are also replay-safe and preserve object
  WAL/blob data; schema validation still rejects removal while any declared
  `ObjectRef` points at that object. Behavior schema removals are replay-safe
  breaking changes too: retained behavior WAL/audit facts are preserved, while
  future invocations require a behavior still declared in the active schema.
  Table and nested-table schema removals are now replay-safe as well: retained
  record WAL/projection facts are preserved, while the active schema stops
  exposing the removed table or nested table. Version downgrades remain hard
  rejects until the full migration planner exists.
  Schema apply/proposals
  now execute staged projection rebuilds only when the migration plan requires
  replay or projection-shape work; purely additive compatible changes keep the
  active projection and report `projectionRebuilt=false`. Local schema apply
  now treats the strict `SchemaApplied` WAL append as the commit point before
  mutating the active schema file, record projection, ObjectRef index, hot
  cache policy, or in-memory schema registry: replay/projection migrations
  first build replacement state off to the side, append the auditable WAL fact,
  and only then atomically swap local projections and runtime state. The
  projection rebuild control plane now also has a first background execution
  slice: `POST /v1/admin/projections/rebuild` accepts `background=true`,
  records a run id and `running`/`succeeded`/`failed` status under
  `/v1/admin/projections/rebuild/status`, and reuses the same WAL replay
  rebuild path as synchronous rebuilds. Schema replay apply now has the first
  replay-specific background orchestration slice as well: `applySchema(...,
  { allowBreakingReplay: true, backgroundReplay: true })` performs the same
  preflight as synchronous apply, requires a replay/projection rebuild plan,
  starts an asynchronous WAL-backed schema apply under
  `/v1/admin/schema/replay/status`, and reports the run id through the SDK.
  The schema replay job status is now persisted in
  `data/schema/schema-replay-status.json`: successful runs survive restart for
  audit/readback; a process restart while a run is `running` is recovered as a
  failed interrupted run instead of disappearing; and a restart while a run is
  `committing` is reconciled against the recovered `SchemaApplied` WAL fact so
  already-durable commits come back as `succeeded` with the recovered audit LSN.
  The replay control plane now also has an explicit pre-commit cancellation
  slice: `/v1/admin/schema/replay/cancel` /
  `db.cancelSchemaReplayApply()` can move a `running` replay to `cancelled`
  before the `SchemaApplied` WAL commit point, while an already `committing`
  replay returns conflict and is allowed to finish. This still needs broader
  resume orchestration before the P5 background replay migration item is
  complete, but the first resume/retry slice is now live: the persisted status
  includes the candidate schema and expected version, and
  `/v1/admin/schema/replay/resume` / `db.resumeSchemaReplayApply()` can restart
  a failed interrupted replay job through the same preflight and WAL-backed
  apply path while recording `resumedFromRunId` for audit lineage. Replay status
  now also exposes `resumeEligible` and `resumeReason`, so admin tooling can
  distinguish interrupted resumable failures from running, cancelled, or
  already-committed runs without parsing error strings or probing the resume
  endpoint. The older `/v1/admin/schema/replay/retry` /
  `db.retrySchemaReplayApply()` path remains as a compatibility alias for the
  same restart flow.
- Aggregate actor family: counts/sums/presence maintained from event
  streams, subscribable. Table count, numeric field sum, and realtime channel
  presence slices are landed: record aggregates hydrate from the record
  projection and update from `RecordUpserted` / `RecordDeleted` delivery
  events, while channel presence hydrates from realtime channel members and
  updates on join/leave/session cleanup. The runtime publishes
  `aggregateCountUpdated`, `aggregateSumUpdated`, and
  `aggregatePresenceUpdated` frames. The TypeScript SDK exposes these as
  `db.subscribeAggregateCount(table, listener)`,
  `db.subscribeAggregateSum(table, field, listener)`, and
  `db.subscribeAggregatePresence(channelId, listener)`.
- Fan-out zero-copy: serialize once -> `Arc<Bytes>` to all subscribers,
  batched WS flush. First slice landed in the global pre-broadcast registry:
  targeted per-session event batches now share `Arc<DeliveryEvent>` instances
  across all matching sessions, so the hot routing path no longer clones full
  event payloads for every candidate before connection-local RLS/live-query
  filtering. The connection sink now also batches all server frames produced by
  one drained realtime event batch and flushes the WebSocket/JSONL sink once
  after writing them in order. A follow-up slice introduced `EncodedServerFrame`
  so a frame batch is serialized once into shared bytes before transport write;
  cloned encoded frames can be reused by multiple sinks. Event frames now use a
  borrowed `DeliveryEvent` encoder after connection-local RLS filtering, so the
  connection hot path no longer clones full event payloads merely to serialize
  them. Registry-level pre-encoded batch sharing now groups identical candidate
  event batches and attaches one shared encoded event frame; connection-local
  processing reuses it when the whole routed batch is visible and falls back to
  per-connection filtering/encoding for RLS, query-only, or partial-visibility
  cases.

### North-star acceptance: the cypridina migration

The author's production SpacetimeDB 2.4 project defines "done":

1. Message history fits without an external ScyllaDB (tiered residency +
   projection engine) — P3.
2. An SMS HTTP call blocks no other user's operation — P4.
3. ~50 per-sender views become RLS incremental subscriptions; CPU does not
   scale with subscriber re-evaluation — P3.
4. Removing a schema field does not wipe the database — P5.

## Dependency and rollback matrix

```text
P0 ──> P1 ──> P2 ──> P3 ──> P4 ──> P5
net    engine kernel residency ABI  surpass
```

| Phase | Rollback lever                                          |
| ----- | ------------------------------------------------------- |
| P1    | old projection dir retained; WAL frame v1 still readable |
| P2    | shard count configurable to 1 (equivalent to old model)  |
| P3    | infinite lingering degrades to plain LRU residency       |
| P4    | ABI v1 compatibility layer retained until v2 is proven   |

## Reference notes (what we adopt / avoid from SpacetimeDB 2.4)

Adopt: commitlog segmenting/CRC/offset-index engineering; scheduled tables
(timers as data); `#[view]`/RLS as a product concept; schema-as-ABI codegen
pipeline; energy metering surfaced as per-actor ops metrics.

Avoid: per-database single-writer execution; synchronous procedures on the
serial queue; subscription re-evaluation; destructive migrations; mandatory
full-RAM residency.
