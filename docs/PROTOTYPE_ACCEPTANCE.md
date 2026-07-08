# NextDB Prototype Acceptance Matrix

This document maps the original prototype goals to the current implementation
surface and the smoke evidence that protects it. It is an MVP acceptance matrix,
not a production-readiness certification.

## Acceptance Command

```sh
npm run test:full
```

The full suite builds Rust and TypeScript artifacts, compiles the TypeScript
behavior module to Wasm, runs the prototype acceptance smoke, exercises browser
Admin UI flows, and verifies restart, export/import, WAL, realtime, cache,
behavior, schema, and cluster paths.

For a local repeatable performance snapshot:

```sh
npm run benchmark:micro
npm run benchmark:local
npm run benchmark:flamegraph
```

`benchmark:micro` runs Criterion baselines for serialization, hashing, batch
construction, and fan-out framing. `benchmark:local` starts an isolated
temporary server and reports JSON metrics for strict, relaxed, and volatile
message writes, strict record upserts, key-order record list reads, object
puts, WAL record count, projection status, and health state.
`benchmark:flamegraph` runs the same local workload against a release server
started under `cargo flamegraph` and writes
`target/nextdb-server-flamegraph.svg`. Flamegraph acceptance is Linux-only.

For a local repeatable stability run:

```sh
npm run soak:local
```

`soak:local` continuously mixes durable and volatile writes, key-order and
secondary-index record reads, object puts, WebSocket subscription delivery,
periodic health/readiness sampling, sync wait, projection status, and WAL
accounting against an isolated temporary server.

For local release packaging:

```sh
npm run release:verify
```

`release:verify` builds the optimized server, Admin UI, SDK packages, and
example behavior module, creates a tar.gz release bundle with file checksums and
SBOM, verifies archive integrity/path safety/manifest hashes/SBOM contents, then
runs the packaged server through readiness, health, behavior loading, record
write, WAL integrity, and Admin static asset checks.

For final prototype completion auditing:

```sh
npm run completion:audit
```

`completion:audit` checks that original goals have acceptance evidence, key
scripts and smoke files exist, benchmark/soak/release commands are documented,
release manifest/SBOM artifacts are present, and known non-production boundaries
are explicit.

For a fast running-node acceptance check:

```sh
NEXTDB_ENDPOINT=http://127.0.0.1:3188 npm run test:prototype
```

`test:prototype` checks the integrated prototype surface: readiness, health,
schema declarations, loaded behavior modules, object writes/reads,
record/message writes, realtime channel state, export manifest, and WAL audit.

## Goal Coverage

| Original goal | Prototype status | Implementation surface | Smoke evidence |
| --- | --- | --- | --- |
| Data and behavior separation | Covered at prototype level | Wasm behaviors return host commands; server validates command capabilities/scopes before committing storage changes or runtime activation changes | `test:behavior-wasm`, `test:behavior-hot-reload`, `test:behavior-rust-wasm`, `test:behavior-idempotency`, `test:prototype` |
| Erlang ideas, Elixir-like syntax, Rust performance | Partially covered at prototype level | Rust server runtime, virtual actors, supervised runtime drain/restart path, AssemblyScript/TS behavior authoring, hot behavior epoch swap, and Rust behavior Wasm path | `cargo check`, `cargo build`, `test:runtime-restart`, `test:behavior-wasm`, `test:behavior-hot-reload`, `test:behavior-rust-wasm` |
| Runtime restart while serving | Covered at prototype level | Runtime drain, prepare-restart, actor snapshot, WAL replay, graceful shutdown snapshot, deterministic kill/restart/catch-up chaos path | `test:runtime-prepare`, `test:runtime-drain-connection`, `test:runtime-restart`, `test:runtime-chaos`, `test:actor-window` |
| Type system strongly bound to database fields | Covered at prototype level | Schema registry, schema validation, ObjectRef validation, schema version gates, generated TypeScript bindings | `test:schema-version`, `test:schema-history`, `test:schema-proposal`, `test:codegen`, `test:behavior-wasm`, `test:behavior-hot-reload`, `test:prototype` |
| Virtual actor tables with resident/LRU/disk behavior | Covered at prototype level | Room actors, hot windows, LRU/idle resident actor eviction, background room and durable record-hot idle passivation, record hot cache for schema storage classes, disk projections, room subscription activation, live-query record activation, and SDK/operator/behavior-triggered activation | `test:actor-window`, `test:actor-idle`, `test:lru-record-hot`, `test:record-hot-idle`, `test:volatile-record`, `test:volatile-overlay-restart`, `test:schema-actor-policy`, `test:behavior-wasm`, `test:behavior-rust-wasm` |
| WAL persistence plus event sourcing, audit, and tracing | Covered at prototype level | WAL group commit, sharded WAL, checksums, archive retention, audit WAL, trace/replay APIs | `test:wal-archive-retention`, `test:wal-integrity-corruption`, `test:wal-startup-corruption`, `test:wal-export-corruption`, `test:audit-trace`, `test:prototype` |
| Built-in object storage | Covered at prototype level | Object metadata/body store, range reads, ObjectRef index, object GC, object replication/repair surfaces | `test:object-range`, `test:export-import`, `test:cluster-object-repair`, `test:prototype` |
| Borrow from Convex and rustfs | Covered as design direction, not compatibility | Convex-like realtime/database SDK surface and local cache ownership; rustfs-like separate object body path with integrity and archive-object backup path | `test:cache`, `test:cache-control`, `test:live-query`, `test:export-import`, `test:object-range` |
| Realtime database syncing changes to clients | Covered at prototype level | WebSocket/JSONL connection layer, subscriptions, live queries, sync pull/wait, reconnect catch-up | `test:transport`, `test:nested-subscription`, `test:live-query`, `test:sync-wait`, `test:connection-auth` |
| Client SDK owns local cache management | Covered at prototype level | Memory/IndexedDB cache, cache leases, invalidation, cache profile enforcement, pending offline writes, local data status | `test:cache`, `test:cache-profile`, `test:cache-control`, `test:transport` |
| Polished management UI | Covered at prototype level | React/Vite Admin UI with health/readiness, WAL audit, schema/data explorer, object, behavior, realtime, cache, backup, and operations panels | `test:admin-ui` |
| Realtime channels for voice/video/game | Covered at signaling/control prototype level | Realtime channels, member/session ownership, presence, state, orphan state/sequence cleanup, signals, broadcasts, voice/video/game frame helpers | `test:realtime-channel`, `test:realtime-channel-sdk`, `test:runtime-limits` |

## Additional Prototype Guarantees

| Area | Current guarantee | Smoke evidence |
| --- | --- | --- |
| Readiness and operator drain | `/v1/ready` separates read, write, and realtime readiness; Admin UI shows and exercises drain/resume transitions | `test:runtime-prepare`, `test:runtime-drain-connection`, `test:admin-ui` |
| Backup and restore | Full, incremental, encrypted, archive-object, chain restore, and retention paths exist | `test:export-import`, `test:wal-export-corruption` |
| Cluster control | Shard ownership, handoff/failover planning, topology proposals, read quorum, WAL repair, and object repair are represented | `test:cluster-handoff`, `test:cluster-failover-election`, `test:cluster-read-quorum`, `test:cluster-wal-repair`, `test:cluster-object-repair` |
| Runtime limits and short chaos | High-risk payload paths reject oversized messages, records, objects, and volatile realtime frames before committing state; strict writes survive process kill and realtime subscriptions can catch up after restart | `test:runtime-limits`, `test:runtime-chaos` |
| Local benchmark harness | Repeatable isolated benchmark covers Criterion micro baselines, strict, relaxed, and volatile message writes, strict record upserts, key-order record list reads, object puts, WAL accounting, projection status, health state, and Linux flamegraph capture | `benchmark:micro`, `benchmark:local`, `benchmark:flamegraph` |
| Local soak harness | Repeatable isolated soak covers mixed durable/volatile writes, key-order and secondary-index record reads, object puts, live subscription delivery, periodic health/readiness sampling, sync wait, projection status, and WAL accounting over time | `soak:local` |
| Release packaging | Optimized server binary, Admin static assets, behavior modules, schema seed, docs, SBOM, manifest checksums, archive sidecar verification, path safety checks, and packaged-server smoke are produced locally | `release:verify` |
| Completion audit | Final local audit checks original-goal coverage, core scripts, smoke breadth, documented commands, release manifest/SBOM, and explicit non-production boundaries | `completion:audit` |

## Known Non-Production Scope

The prototype intentionally does not claim production completion for:

- Native WebTransport/HTTP3 server listener. The SDK and transport boundary exist,
  and the server exposes WebSocket plus a JSONL custom gateway.
- Production-grade distributed consensus. Cluster ownership, fencing, handoff,
  failover proposals, read quorum, and repair flows exist as prototype control
  surfaces, but not as a complete Raft/Paxos-style production system.
- Production benchmark and soak certification. Smoke tests cover correctness
  boundaries, `benchmark:local` provides a repeatable local snapshot, and
  `soak:local` provides a repeatable local stability run, but production
  performance characterization remains future work.
- Hardened multi-platform release workflow. `release:verify` covers local
  packaging, SBOM, artifact integrity, and packaged-server smoke, but signed
  artifacts, installers, provenance attestations, and multi-platform CI remain
  future work.
