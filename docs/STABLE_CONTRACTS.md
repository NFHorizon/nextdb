# Stable External Contracts

This document defines the external surfaces which must remain stable while the
runtime internals evolve toward actor-native execution:

- TypeScript SDK API and generated typed clients.
- Realtime and HTTP protocol frames.
- Durable persistence formats: WAL, schema history, object metadata, and
  snapshots/checkpoints.

The actor runtime may change names, placement, mailbox scheduling, batching, or
ownership internally. External clients and durable files must not depend on those
runtime details.

The target memory-first actor design is described in [NextDB Design](DESIGN.md).
This document only defines the stable external contracts that survive internal
runtime changes.

## Design Goals

NextDB has three stability boundaries:

1. Client API stability: applications should keep compiling and reconnecting
   across compatible runtime upgrades.
2. Wire stability: connection gateways can carry frames over WebSocket,
   WebTransport, HTTP JSONL, or custom transports without understanding storage
   internals.
3. Persistence stability: WAL and schema history are the durable truth and must
   remain replayable after runtime refactors.

Actor concepts map to stable contract nouns as follows:

| Runtime concept | Stable external noun | Notes |
| --- | --- | --- |
| SessionActor | connection/session | A transport instance and authenticated context. |
| DomainActor | domain/stream/table scope | An activated business consistency boundary. Rooms, channels, games, documents, user inboxes, table partitions, and tenant shards can all be actor keys. |
| DomainActor local view | view | A subscribed read model executed by the same domain actor that serves reducers and writes. Rows live in shared HotStore; cross-domain views are asynchronous projections over committed facts. |
| WalShardActor | WAL shard | Durable append and replication unit. |

The SDK can expose convenient domain helpers such as `room(id)`, but the stable
protocol should also support generic `streamKey` and `viewKey` forms so the
database is not permanently shaped around chat rooms.

## Compatibility Rules

All externally visible payloads use JSON-compatible values and `camelCase`
field names.

Compatible changes:

- Add optional fields with well-defined defaults.
- Add new frame or payload `type` values.
- Add new SDK methods without changing existing method semantics.
- Add new durability modes only when older clients can reject or ignore them
  explicitly.
- Add new WAL payload types with replay fallback behavior.

Breaking changes:

- Rename, remove, or change the type of an existing field.
- Reuse a retired `type` string for different semantics.
- Change the meaning of `lsn`, `schemaVersion`, `clientMutationId`,
  `durability`, `path`, or `checksum`.
- Change replay order, idempotency, or durability acknowledgement semantics for
  an existing operation.
- Make derived projections required for recovery.

Unknown handling:

- Clients must ignore unknown optional fields.
- Servers must reject unknown client frame `type` values with a structured
  error.
- Replayers must preserve or safely skip future WAL payloads only when the
  payload declares itself skippable. Durable state-changing facts are not
  skippable by default.

## Version Model

Use separate versions for separate compatibility axes:

```text
sdkMajor            TypeScript SDK public API major.
protocolVersion     Realtime/HTTP frame contract version.
walFormatVersion    On-disk WAL frame and logical record format.
schemaVersion       User data/schema contract version.
cacheFormatVersion  SDK local cache layout version.
```

Handshake responses must expose the active versions and capabilities:

```json
{
  "type": "hello",
  "protocolVersion": 1,
  "runtimeId": "01...",
  "sessionId": "01...",
  "userId": "alice",
  "capabilities": {
    "transports": ["webSocket", "jsonl", "webTransport"],
    "durability": ["volatile", "relaxed", "strict", "transactional"],
    "frames": ["events", "viewSnapshot", "viewDiff", "backpressure"]
  }
}
```

Current server frames can keep their existing fields. New fields should be added
to `hello` first, then used by clients only after capability checks.

## Table Storage Shape Contract

A table's storage shape is a versioned schema policy, not a permanent client
contract. Applications see typed tables, nested tables, streams, views,
subscriptions, and durability acknowledgements. They must not need to know
whether a table is currently disk-backed, LRU-backed, resident, actor-partitioned,
chat-log-backed, or object-backed to use the same logical API.

Stable storage shapes:

| Shape | Durable truth | Runtime state | Primary use |
| --- | --- | --- | --- |
| `disk` | WAL plus disk projection | Optional HotStore activation | Large cold tables and default durable records. |
| `lru` | WAL plus disk projection | Bounded hot record set and activation indexes | Large tables with a working set. |
| `resident` | WAL plus disk projection | Full or prewarmed hot record set and activation indexes | Small hot tables such as config, roles, presence metadata, or room directories. |
| `actorPartition` | WAL plus disk projection | HotStore partition pinned by domain actors | Tables whose current state is naturally scoped by a stream or partition key. |
| `chatLog` | WAL plus ordered disk projection | HotStore live window pinned by domain actors | Append-heavy ordered nested tables such as messages, feeds, and game events. |
| `object` / `blob` | Metadata WAL plus binary body layout | Optional metadata/body caches | Lightweight binary data addressed by blob id; current object APIs remain compatibility surfaces. |

The default durable shape is still `disk`: WAL plus disk projection remain the
recovery and cold-read truth. The runtime default may still be memory-first
`onActivate`, where touching a table, partition, stream, or view pins a HotStore
scope, hydrates hot rows, and builds or reuses activation indexes.
Memory-resident state must be treated as the current runtime view over durable
facts, not as a replacement for WAL and disk projections. `view` is intentionally
not a storage shape for truth: a view is a derived materialized read model that
can be started by subscription, snapshotted as a warm-start hint, discarded, and
rebuilt.

### Shape Migration Rules

Changing a table shape is a schema migration. The migration must be recorded in
schema history and committed through a `SchemaApplied` WAL control fact before
the new shape becomes authoritative.

Rules:

- Shape migrations are idempotent and resumable. Runtime status must include
  `from`, `to`, `schemaVersion`, `startedAtLsn`, optional `completedAtLsn`, and
  whether reads are falling back to disk.
- Durable shapes keep WAL plus disk projection as the durable table store unless
  the schema explicitly declares volatile-only data. Switching into a hot shape
  must not make old durable rows unreadable.
- `disk -> lru` enables the hot set after schema apply. No full data rewrite is
  required; point reads, list pages, live queries, and explicit activation can
  hydrate hot rows lazily.
- `lru -> disk` flushes or drops the hot overlay only after every durable hot row
  is confirmed present in the disk projection at or after the migration LSN.
- `disk/lru -> resident` prewarms from disk projection or WAL snapshot before
  reporting the table as resident-ready. Until prewarm completes, reads can serve
  from disk fallback and increment migration diagnostics.
- `resident -> lru/disk` releases memory only after the disk projection covers
  the current durable LSN. Volatile rows either expire by policy or are rejected
  before the migration starts.
- `disk/lru/resident -> actorPartition` derives HotStore partition keys from the
  schema key, pins HotStore partitions lazily, and keeps the disk projection
  readable throughout the transition.
- `actorPartition -> disk/lru/resident` drains or fences HotStore partition
  leases, waits for accepted durable writes to reach WAL and projection, then
  changes routing.
- `disk/lru/resident/actorPartition -> chatLog` requires a deterministic bucket
  and order definition. The ordered projection can be built in the background
  while older disk reads remain valid.
- `chatLog -> disk/lru/resident/actorPartition` stops new chat-log-specific
  activations, drains WAL batches, keeps the durable row projection, and changes
  future reads to the target table path.
- `object` is a separate body-storage contract. Moving object metadata between
  table shapes must not change object body ids, checksums, or retrieval semantics.

Rollback should be treated as another forward migration unless no incompatible
WAL facts have been accepted after the shape change. This keeps recovery and
replication replay monotonic.

### Introspection

`GET /v1/schema/storage-policy` is the stable place for clients, operators, and
the Admin UI to inspect the effective shape. It should report the requested
schema shape, effective runtime shape, migration status, active hot counts, and
disk fallback status. It should also expose memory policy and activation-index
diagnostics when available. The TypeScript SDK may expose this for diagnostics,
but generated typed table methods must keep the same behavior across shape
changes.

## TypeScript SDK Contract

The stable SDK is the application boundary. Applications should not talk to
IndexedDB, raw WebSocket frames, WAL files, or projection files directly.

### Client Construction

Stable base options:

```ts
export interface NextDbClientOptions {
  endpoint: string
  wsEndpoint?: string
  replicaEndpoints?: string[]
  authToken?: string
  adminToken?: string
  userId?: string
  sessionId?: string
  schemaVersion?: number
  cache?: NextDbLocalCache
  cacheNamespace?: string
  realtimeTransportKind?: "webSocket" | "jsonl" | "webTransport"
  realtimeTransport?: NextDbRealtimeTransportFactory
  connectionMetadata?: unknown
  offlineWrites?: boolean
  autoFlushPendingWrites?: boolean | PendingWriteAutoFlushOptions
  autoRestoreSubscriptions?: boolean
}
```

Compatibility commitment:

- `endpoint`, auth, schema pinning, cache ownership, transport selection, and
  subscription restore remain SDK-level concepts.
- New transports are added behind `NextDbRealtimeTransportFactory`.
- Generated typed clients wrap the same runtime client instead of replacing
  transport or cache semantics.

### Stable Operation Classes

The SDK should expose four operation classes with explicit acknowledgement
semantics:

```ts
db.signal(streamKey, event, options)
db.command(streamKey, command, options)
db.mutate(streamKey, mutation, options)
db.transaction(operations, options)
```

| Operation | Required acknowledgement | Persistence | Intended use |
| --- | --- | --- | --- |
| `signal` | accepted by connection or stream runtime | none | Presence, cursor, voice/video/game hints. |
| `command` | accepted by stream runtime | optional | Realtime state transition, may be lossy. |
| `mutate` | WAL committed according to durability | yes unless volatile | Durable application fact. |
| `transaction` | all participant durable acks plus transaction commit | yes | Cross-stream invariant. |

Existing helpers map into this model:

- `room(id).messages.send(..., "volatile")` is a signal/command-like volatile
  stream operation.
- `room(id).messages.send(..., "strict")` is a mutate operation.
- `table(name).upsert` and nested table writes are mutate operations.
- Record transactions are transaction operations scoped to record streams.

### Stream API

The generic stream API is the stable write surface:

```ts
export interface StreamHandle {
  key: string
  signal(event: StreamSignal, options?: SignalOptions): Promise<AcceptedAck>
  command(command: StreamCommand, options?: CommandOptions): Promise<AcceptedAck>
  mutate(mutation: StreamMutation, options?: MutateOptions): Promise<CommitAck>
  subscribe(listener: (event: DeliveryEvent) => void, options?: SubscriptionOptions): () => void
}
```

Domain helpers are aliases:

```ts
db.room(roomId)       -> db.stream(`room:${roomId}`)
db.channel(channelId) -> db.stream(`channel:${channelId}`)
db.table(table)       -> table-specific record helpers backed by stream keys
```

### View API

The generic view API is the stable read/subscription surface:

```ts
export interface ViewHandle<TSnapshot, TDiff> {
  key: string
  get(options?: FreshnessOptions): Promise<TSnapshot>
  subscribe(
    listener: (change: ViewSnapshot<TSnapshot> | ViewDiff<TDiff>) => void,
    options?: ViewSubscriptionOptions,
  ): () => void
  cached(): Promise<TSnapshot | undefined>
}
```

Stable view identity:

```text
viewKey = hash(viewType, params, authScope, schemaVersion)
```

The server may activate the owning `DomainActor`, pin the required HotStore
scope, attach local view state for the view key, maintain reference counts, and
drop that state after a grace period. The client only relies on `viewKey`,
snapshot, diff, and cursor semantics.

### Local Cache Contract

The SDK owns local cache migrations and exposes cache management APIs. Stable
cache concepts:

- cache namespace
- schema/version metadata
- durable cursors by room/user/table/nested table/object/view
- pending writes
- stored subscription intent
- cache lease/profile

Applications must not depend on the physical IndexedDB object store names. The
SDK may migrate them under `cacheFormatVersion`.

## Wire Protocol Contract

The realtime protocol is transport-neutral. WebSocket carries one JSON frame per
message. JSONL and stream transports carry one JSON frame per line. WebTransport
uses the same frame contract over bidirectional streams or datagrams where
supported.

### Frame Envelope

All future frames should support this envelope:

```json
{
  "type": "mutateStream",
  "requestId": "client-req-1",
  "protocolVersion": 1,
  "schemaVersion": 4,
  "clientId": "client-1",
  "sessionId": "session-1",
  "payload": {}
}
```

Existing frames which put fields at the top level remain valid. New frame types
should prefer `requestId` so clients can correlate acks/errors.

### Client Frames

Existing stable frame families:

- `subscribeRoom`, `unsubscribeRoom`
- `subscribeTable`, `unsubscribeTable`
- `subscribeNestedTable`, `unsubscribeNestedTable`
- `subscribeQuery`, `unsubscribeQuery`
- `subscribeUserEvents`, `unsubscribeUserEvents`
- `subscribeObjects`, `unsubscribeObjects`
- `updateConnectionMetadata`
- `subscribeConnectionEvents`, `unsubscribeConnectionEvents`
- `subscribeAggregateCount`, `unsubscribeAggregateCount`
- `subscribeAggregateSum`, `unsubscribeAggregateSum`
- `subscribeAggregatePresence`, `unsubscribeAggregatePresence`

Generic actor-native additions:

```ts
type ClientFrame =
  | { type: "subscribeView"; viewKey: string; spec: ViewSpec; afterLsn?: number; resultId?: string }
  | { type: "unsubscribeView"; viewKey: string }
  | { type: "signalStream"; streamKey: string; event: StreamSignal; requestId?: string }
  | { type: "commandStream"; streamKey: string; command: StreamCommand; requestId?: string }
  | { type: "mutateStream"; streamKey: string; mutation: StreamMutation; durability: Durability; requestId?: string; clientMutationId?: string }
  | { type: "transaction"; transactionId: string; operations: TransactionOperation[]; durability: "transactional"; requestId?: string }
```

These additions can coexist with the current room/table/query frames. The SDK
should prefer generic frames for new features and keep room/table frames as
compatibility helpers.

### Server Frames

Existing stable frame families:

- `hello`
- `subscribed`, `unsubscribed`
- `tableSubscribed`, `tableUnsubscribed`
- `querySubscribed`, `queryUnsubscribed`
- `queryResult`, `queryDiff`, `queryUnchanged`
- `aggregateCountSubscribed`, `aggregateCountUnsubscribed`,
  `aggregateCountUpdated`
- `aggregateSumSubscribed`, `aggregateSumUnsubscribed`,
  `aggregateSumUpdated`
- `aggregatePresenceSubscribed`, `aggregatePresenceUnsubscribed`,
  `aggregatePresenceUpdated`
- `event`, `events`
- `subscriptionCatchUp`
- `cacheInvalidated`
- `connectionMetadataUpdated`, `connectionEvent`, `connectionClosing`
- `error`

Generic actor-native additions:

```ts
type ServerFrame =
  | { type: "accepted"; requestId: string; streamKey?: string; acceptedAtMs: number }
  | { type: "committed"; requestId: string; streamKey?: string; lsn: number; durability: Durability }
  | { type: "transactionCommitted"; requestId: string; transactionId: string; lsn: number; participantLsns: Record<string, number> }
  | { type: "viewSubscribed"; viewKey: string; resultId: string }
  | { type: "viewUnsubscribed"; viewKey: string }
  | { type: "viewSnapshot"; viewKey: string; resultId: string; currentLsn: number; snapshot: unknown }
  | { type: "viewDiff"; viewKey: string; resultId: string; currentLsn: number; diff: unknown }
  | { type: "backpressure"; scope: "connection" | "stream" | "view" | "wal"; retryAfterMs?: number; reason: string }
```

`accepted` means the runtime has accepted the operation into an actor mailbox or
lossy stream. `committed` means the durable fact is in WAL according to the
requested durability.

### Durability and Acknowledgements

Stable durability modes:

| Mode | Accepted when | Committed when | Crash recovery |
| --- | --- | --- | --- |
| `volatile` | actor accepted or applied to memory | never | may be lost |
| `relaxed` | actor accepted into WAL batch | after batch write/sync policy | may lose last relaxed window if not synced |
| `strict` | after WAL write and required sync/replica ack | same as response | replayable |
| `transactional` | coordinator accepted transaction | after participant durable acks plus transaction fact | replayable as a transaction |

The protocol must never imply durable commit from `accepted`.

## HTTP Contract

HTTP remains the stable request/response API for environments where realtime is
not available.

Stable endpoint families:

```text
GET  /v1/health
GET  /v1/ready
GET  /v1/schema
GET  /v1/schema/history
POST /v1/mutate
GET  /v1/rooms/{roomId}/messages/latest
GET  /v1/records/{table}/{key}
POST /v1/records/{table}/{key}
DELETE /v1/records/{table}/{key}
GET  /v1/objects
POST /v1/objects
GET  /v1/objects/{objectId}/metadata
GET  /v1/objects/{objectId}/body
POST /v1/connect/jsonl
```

Future generic HTTP additions:

```text
POST /v1/streams/{streamKey}/signal
POST /v1/streams/{streamKey}/command
POST /v1/streams/{streamKey}/mutate
POST /v1/transactions
POST /v1/views/subscribe
POST /v1/views/query
```

HTTP response bodies must use the same acknowledgement objects as realtime
frames where possible.

## Persistence Contract

Only these durable files are part of the stable recovery contract:

- WAL frames and logical WAL records.
- Schema history files.
- Object metadata and object bodies.
- Export/import manifests and bundle manifests.

Derived projections are rebuildable:

- chat log buckets and indexes
- record disk projections
- secondary index manifests
- view snapshots
- actor snapshots
- SDK local cache

### WAL Frame Format

The physical WAL frame must be versioned independently of the logical record:

```text
magic
frameVersion
encoding
payloadLength
payloadBytes
```

Current frames use JSON payload bytes. Future binary encodings can be added only
by introducing a new `encoding` value while preserving the old reader.

### WAL Logical Record

Stable logical record shape:

```json
{
  "formatVersion": 1,
  "lsn": 42,
  "shard": 0,
  "shardEpoch": 1,
  "ownerNodeId": "node-a",
  "timestampMs": 1893456000000,
  "schemaVersion": 4,
  "durability": "strict",
  "streamKey": "room:general",
  "payload": {
    "type": "messageCreated",
    "message": {}
  },
  "checksum": "sha256:..."
}
```

Current records already carry `lsn`, `shard`, `shardEpoch`, `ownerNodeId`,
`timestampMs`, `schemaVersion`, `durability`, tagged `payload`, and `checksum`.
`formatVersion` and `streamKey` should be added as optional-forward fields in the
next WAL contract version:

- Missing `formatVersion` means `1`.
- Missing `streamKey` is derived from payload path or payload-specific keys.

### WAL Payload Rules

Payload `type` strings are permanent. Current stable facts:

- `messageCreated`
- `userEventPublished`
- `userUpserted`
- `objectCommitted`
- `objectDeleted`
- `recordUpserted`
- `recordDeleted`
- `recordTransactionCommitted`
- `schemaApplied`
- `actorReminderScheduled`
- `actorReminderCancelled`
- `actorReminderFired`
- `clientMutationRecorded`

Actor-native additions:

- `streamSignalAccepted` only if audit of volatile signal acceptance is enabled.
- `streamMutationCommitted`
- `transactionCommitted`
- `viewSnapshotCheckpointed` only for optional view warm-start, not truth.

Rules:

- WAL facts are append-only.
- WAL replay order is ascending `lsn` within shard and globally merged by `lsn`
  where a global view is required.
- `clientMutationId` maps to a replayable committed response.
- `schemaVersion` pins validation and replay interpretation.
- `checksum` covers stable record fields excluding the checksum field itself.

### Schema History

Schema history is a stable index for interpreting old WAL facts:

```text
schema/current.json
schema/history/v{schemaVersion}.json
```

Rules:

- History files are immutable once written.
- WAL records reference `schemaVersion`.
- Schema removal/rename requires an explicit migration plan and historical
  upcaster; additive schema evolution is preferred.

### Object Store

Object metadata is durable state. Object bodies are durable blobs referenced by
metadata.

Stable metadata fields:

```json
{
  "id": "object-id",
  "path": "objects/object-id",
  "contentType": "image/png",
  "byteSize": 123,
  "sha256": "hex",
  "createdAtMs": 1893456000000,
  "updatedAtMs": 1893456000000,
  "lsn": 42
}
```

Object body layout can change as long as metadata lookup, body retrieval, export,
replication, and checksum verification remain compatible.

### Snapshots and Checkpoints

Snapshots are optimization artifacts, not the primary truth.

Stable snapshot envelope:

```json
{
  "snapshotVersion": 1,
  "createdAtMs": 1893456000000,
  "uptoLsn": 42,
  "schemaVersion": 4,
  "runtimeKind": "actor",
  "sections": {
    "streams": {},
    "views": {},
    "recordHot": {}
  }
}
```

Rules:

- A runtime must be able to ignore an incompatible snapshot and rebuild from WAL.
- Snapshots must never contain facts absent from WAL unless the section declares
  itself volatile.
- View snapshots are warm-start hints and can be discarded.

## Actor Runtime Boundary

The stable external design intentionally hides actor placement and mailbox
details.

Internal actors can evolve:

```text
SessionActor
DomainActor
WalShardActor
TransactionActor
```

External contracts see:

```text
connection/session
streamKey
viewKey
wal shard
transactionId
```

This allows the runtime to split a hot stream into lanes, migrate actors, change
mailbox scheduling, or add supervision without changing the SDK or WAL.

## Migration Path From Current API

Current room/table APIs remain stable compatibility surfaces. New actor-native
APIs should be layered underneath them:

1. Introduce `streamKey` derivation internally for current message, user, object,
   and record WAL payloads.
2. Add `hello.protocolVersion` and `hello.capabilities`.
3. Add generic `subscribeView` while mapping existing `subscribeRoom` and
   `subscribeQuery` to view specs.
4. Add generic stream operations while keeping `sendMessage`, record writes, and
   object writes as typed helper APIs.
5. Add `formatVersion` and optional `streamKey` to WAL records with default
   readers for older records.
6. Add transaction frames and WAL `transactionCommitted` after single-stream
   mutation semantics are stable.

## Non-Goals

- Do not expose actor mailbox internals to clients.
- Do not make projections part of durable truth.
- Do not require SQL compatibility for the first stable contract.
- Do not require a specific transport. The frame contract is stable, not
  WebSocket itself.
- Do not treat `accepted` as durable commit.
