# NextDB Design

NextDB is a memory-first actor-native realtime database. Durable correctness is
owned by WAL, schema history, disk projections, and blob metadata. Runtime
performance is owned by virtual actors, the shared HotStore, and dynamic
activation indexes.

This document describes the target design, not only the current MVP.
For the accepted repositioning decisions and the phased implementation
roadmap, see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

## Core Principles

```text
durable truth      WAL + schema history + disk projection + blob metadata
runtime truth      HotStore snapshots and write-applied hot state
query acceleration HotStore activation indexes and optional local view indexes
behavior           Wasm modules executed inside domain actor mailboxes
client state       SDK-owned cache and subscription resume cursors
```

Rules:

- The database is memory-first, not memory-only.
- Activation scope is explicit and reference counted, not whole-cluster-global.
- WAL and disk projections remain sufficient for recovery, audit, replay, cold
  reads, and backup.
- HotStore state, activation indexes, view runtime state, SDK cache, and Wasm
  instance state are rebuildable unless marked volatile.
- Behavior code and data ownership are separated, but execution is colocated
  with the domain actor that owns the business ordering boundary.

## Durable Layers

Durable state consists of:

- WAL segments: write truth, event sourcing, audit, sync cursors, schema control
  facts, behavior/module update facts.
- Schema history: every schema, policy, table shape, memory policy, and behavior
  compatibility version needed to explain old WAL facts.
- Disk projections: rebuildable read models for rows, base ordering, partitions,
  chat/event logs, and durable secondary indexes.
- Blob metadata and blob files: lightweight binary storage for avatars,
  thumbnails, small attachments, and short media snippets.
- Module artifacts: Wasm behavior packages and ABI metadata.
- Shard map and placement metadata.

The system does not back up activation indexes, view runtime state, HotStore
query stats, volatile rows, connection state, or SDK cache.

## Memory-First Actor Runtime

The default table memory policy should be `onActivate`:

```ts
type TableMemoryPolicy =
  | { mode: "resident"; prewarm?: boolean; maxBytes?: number }
  | { mode: "onActivate"; maxBytes?: number; idleTtlMs?: number }
  | { mode: "windowed"; maxRows?: number; idleTtlMs?: number }
  | { mode: "diskOnly" }
```

Activation means:

```text
request/subscription/write touches scope
-> locate actor identity
-> pin HotStore shard or partition
-> hydrate hot state from projection/WAL snapshot
-> build or reuse activation indexes for observed query shapes
-> serve reads/writes from HotStore memory where possible
```

Passivation means:

```text
idle/memory pressure/quota violation
-> drain durable writes
-> optionally persist warm hints or actor snapshots
-> release HotStore pins
-> drop unreferenced activation indexes and hot state
-> keep WAL and disk projection as truth
```

Actor identities are stable even when physical actors are absent:

```text
SessionActor(userId, sessionId)
DomainActor(actorType, actorId)
WalShardActor(shardId)
TransactionActor(transactionId)
```

`DomainActor` is the primary runtime unit. It is organized by business
consistency boundary rather than by physical table: chat channel, game room,
user inbox, guild, document, tenant shard, or any application-defined domain.
Reducers and reads for that domain run in the same mailbox. Tables, partitions,
streams, and views are logical resources owned or touched by a domain actor, not
mandatory separate physical actor classes.

## Actor Responsibilities

| Actor | Owns | Does not own |
| --- | --- | --- |
| `SessionActor` | connection, authentication, transport, session metadata | durable table truth |
| `DomainActor` | reducer ordering, business invariants, view state, subscription diffs, WriteBehavior and ReadBehavior execution | HotStore row ownership, fsync, replication, cross-domain transaction decisions |
| `WalShardActor` | WAL batching, group commit, replication, ack policy | business logic |
| `TransactionActor` | multi-actor ack collection and commit status | row storage |

Virtual actors are execution and lifecycle managers, not replacements for WAL or
the shared HotStore.
The hot path should normally touch one `DomainActor` and one `WalShardActor`.
The same domain actor receives reducer calls, reads and updates HotStore through
snapshot/write handles, appends WAL through the WAL shard, recomputes affected
local views, and emits subscription diffs. Cross-domain views are built as
asynchronous projections over committed events and are not authoritative for
writes.

## Shared HotStore

HotStore is the runtime-wide memory-resident data layer. It is similar in spirit
to ETS, but with database semantics: typed rows, schema versions, WAL cursors,
MVCC snapshots, memory policies, and activation indexes.

Domain actors do not own rows. They own reducer ordering and business
invariants. They access shared HotStore through explicit handles:

```ts
type HotStoreHandle = {
  snapshotLsn: number
  readSet: HotReadSet
  writeLease?: HotWriteLease
  activationPins: ActivationPin[]
}
```

Sharing rules:

- Many domain actors may read the same HotStore shard through immutable
  snapshots.
- Writes are serialized by table partition, stream key, or declared write lease.
- A reducer that needs to modify shared rows must acquire the relevant write
  lease or route the command to the actor that owns the business invariant.
- Committed WAL facts are applied to HotStore shards in LSN order.
- Row values are stored once and shared by reference; actors should keep handles
  or view deltas, not private copies of large shared tables.

Concurrency model:

```text
read path:  DomainActor -> HotStore MVCC snapshot -> view/diff
write path: DomainActor -> validate lease/read LSN -> WAL append -> HotStore apply
```

This keeps reads mostly lock-free with RCU/MVCC snapshots while keeping writes
deterministic. The lock boundary is the HotStore shard or write lease, not every
actor that happens to use the data.

## Table Shapes

Every logical table has a durable storage shape and a memory policy. The shape is
a schema policy and can change through migration.

| Shape | Durable truth | Runtime state |
| --- | --- | --- |
| `disk` | WAL + disk projection | optional `onActivate` hot state |
| `lru` | WAL + disk projection | bounded hot rows and activation indexes |
| `resident` | WAL + disk projection | prewarmed or full hot table |
| `actorPartition` | WAL + partition projection | HotStore partition pinned by domain actors |
| `chatLog` | WAL + ordered log projection | HotStore live window pinned by domain actors |
| `blob` | blob metadata + blob files | optional metadata cache |

The durable default is still disk projection. The runtime default is memory-first
activation when a table, partition, stream, or view is touched.

## Activation Indexes

Activation indexes are dynamic indexes built only for active scopes.
They are the primary mechanism for avoiding permanent index explosion with many
predefined and user-defined tables.

```text
client subscribes query shape
-> route to owning DomainActor
-> DomainActor asks HotStore for activation index
-> miss: hydrate rows from projection
-> build in-memory index
-> update index from committed WAL/events
-> passivation drops index
```

Activation index metadata:

```ts
type ActivationIndexStatus = {
  queryShape: string
  coverage: "complete" | "windowed" | "predicateScoped"
  baseLsn: number
  indexedUntilLsn: number
  estimatedRows: number
  lastUsedAtMs: number
  complete: boolean
}
```

Persistent indexes are reserved for:

- primary keys and declared unique constraints;
- cold queries that must remain fast without actor activation;
- high-value query shapes promoted from repeated activation indexes;
- global integrity requirements.

Query planner order:

```text
1. HotStore activation index
2. HotStore hot row scan
3. durable primary/base projection
4. durable secondary index
5. bounded cold projection scan
6. QueryRequiresIndex
```

## Behavior Model

Developer experience can package read and write behavior together:

```ts
defineModule({
  tables,
  reducers,
  views,
  policies,
})
```

Runtime separates them:

| Behavior | Actor | Capability |
| --- | --- | --- |
| `WriteBehavior` | `DomainActor` reducer entrypoint | read required domain state, produce write set, request host WAL commit |
| `ReadBehavior` | `DomainActor` view/query entrypoint | read projections and HotStore snapshots, compute snapshot/diff |
| `PolicyBehavior` | host policy engine or constrained Wasm | decide row/field capability |

Wasm does not write storage directly. It returns host commands or write sets.
The host validates schema, policy, expected LSNs, blob references, and transaction
boundaries before appending WAL.

Hot update differs by behavior kind:

```text
ReadBehavior:
  hydrate new domain view state to target LSN
  send replacement snapshot or compatible diff
  drain old view state

WriteBehavior:
  pause domain actor mailbox
  drain in-flight command
  write WAL fence
  switch module version
  resume mailbox
```

## Query and Projection

Projection is an internal materialized read layer. Clients and behaviors query
through typed APIs or read plans, not through projection files.

```ts
type ReadPlan =
  | { kind: "point"; tableId: string; key: string }
  | { kind: "range"; tableId: string; indexId: string; lower?: unknown[]; upper?: unknown[]; limit: number }
  | { kind: "partition"; tableId: string; partitionKey: string; order?: string; limit: number }
  | { kind: "latest"; streamKey: string; tableId: string; limit: number }
  | { kind: "predicate"; tableId: string; terms: unknown[]; limit: number }
```

Read execution overlays memory over disk:

```text
disk projection rows
+ HotStore replacements
+ volatile rows when allowed
- hot deletes
= current result
```

The domain actor uses projections for initial snapshots and committed events for
diffs. Simple local views rerun bounded read plans inside the domain actor.
Optimized views maintain incremental state from event impact filters. Cross-
domain views read from asynchronous projections over committed WAL facts and
must route writes back to the owning domain actor.

## User-Defined Tables

User-defined tables are runtime schema, not schemaless JSON. Users can create
tables and fields through UI, but the accepted schema becomes a database
contract.

Field names are presentation. Field ids are storage identity:

```json
{
  "recordId": "rec_...",
  "tableId": "tbl_...",
  "schemaVersion": 12,
  "fields": {
    "fld_title": "Roadmap",
    "fld_status": "doing"
  }
}
```

Table modes:

| Mode | Use |
| --- | --- |
| `strict` | developer-defined typed tables |
| `flex` | Airtable/NocoDB-style user tables with schema fields plus optional custom JSON |
| `raw` | logs, webhook payloads, temporary semi-structured data |

Physical layout is adaptive:

| Layout | Use |
| --- | --- |
| `inlineSmall` | small tables loaded into the base/table actor and scanned in memory |
| `sharedPartition` | many tables grouped under a base/workspace partition |
| `dedicatedPartition` | large or hot tables with their own partition/shards |
| `streamLog` | append-heavy messages, events, feeds, game logs |

Because the system is memory-first, layout is not chosen only by row count. It is
chosen by activation scope, memory budget, write rate, subscription count, query
shape, and tenant quota.

## Blob Store

NextDB should keep Convex's file-storage shape, but scope it to lightweight
binary data rather than a full object storage product.

```text
generateBlobUploadUrl()
-> client uploads bytes directly
-> server streams bytes, computes sha256, stores blob file
-> _system.blobs metadata row is created
-> application stores BlobRef in user table
```

Blob metadata:

```ts
type BlobRef = {
  blobId: string
  sha256: string
  byteSize: number
  contentType?: string
}
```

WAL records metadata and references, not binary bodies. Blob files are included
in backup manifests with checksums. Reads are permission-checked by default;
short-lived signed URLs are an optimization, not the only access path.

## Authentication, Authorization, RLS, and Field Masks

Authentication creates a principal:

```text
user | service | anonymous | system
```

Authorization creates a permission scope:

```ts
type PermissionLevel = "none" | "read" | "write"
```

Extra capabilities are separate:

```text
createRecord
deleteRecord
manageSchema
managePermissions
exportData
share
```

Policies can exist at workspace, base, table, view, row, and field levels.
Effective permission is the minimum applicable level. Views can only narrow
permission, not elevate it.

RLS is query rewrite:

```text
effectiveFilter = userFilter AND rowPolicy
```

Column/field-level security is server-side field masking. Invisible fields are
not returned and are not written to SDK local cache. `permissionScope` is part of
view keys, cache keys, and subscription keys.

## Backup and Partitioning

Backup is continuous WAL archive plus periodic durable snapshot:

```text
sealed WAL segments -> backup store
projection snapshot -> backup manifest
blob files + checksums -> backup manifest
schema history + module artifacts + shard map -> backup manifest
```

Restore:

```text
restore schema/module/shard metadata
restore projection and blob snapshot
replay WAL from snapshot LSN
rebuild durable indexes
activate actors on demand
rebuild activation indexes on demand
```

Partitioning separates durable ownership from runtime activation:

```text
storage shard = WAL shard + projection shard + blob metadata shard
runtime actor = current owner of business ordering
HotStore shard = current owner of memory-resident rows and activation indexes
```

Default partition keys:

- user-defined tables: `workspaceId` or `baseId`;
- messages/events: `roomId`, `channelId`, or `streamKey`;
- user data: `userId`;
- global small tables: `tableId`;
- blobs: owner scope plus blob id or hash prefix.

High-performance queries should prefer one partition. Cross-partition queries are
explicitly expensive and should use scatter-gather, materialized views, or
precomputed projections.

## Performance Target

The hot path target is the same shape as SpacetimeDB's advantage:

```text
route once
enqueue one actor mailbox
execute behavior beside HotStore snapshots
batch host reads/writes across Wasm boundary
append WAL with group commit
apply HotStore memory state
fan out committed event
```

Expected performance profile:

- single-domain thin reducers can approach SpacetimeDB-class throughput;
- full product paths with RLS, field masks, view diffs, dynamic schema, and
  SDK cache invalidation trade some peak TPS for product semantics;
- memory-first actor scopes should outperform conventional app-server + database
  architectures for realtime collaborative workloads;
- activation indexes keep cold user-defined tables cheap while preserving hot
  view performance.
