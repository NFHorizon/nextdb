# Code Pointers

Use these as starting points. Inspect current code before editing.

## P5 Schema Replay Apply

Primary files:

- `crates/nextdb-server/src/api/schema.rs`
- `crates/nextdb-server/src/main.rs`
- `packages/nextdb-client/src/index.ts`
- `packages/nextdb-client/test/schema-background-replay-smoke.mjs`
- `packages/nextdb-client/test/schema-replay-smoke.mjs`

Key server symbols in `api/schema.rs`:

- `SchemaReplayApplyPhase`
- `SchemaReplayApplyStatus`
- `load_schema_replay_apply_status`
- `schema_replay_apply_status`
- `retry_schema_replay_apply`
- `resume_schema_replay_apply`
- `cancel_schema_replay_apply`
- `restart_schema_replay_apply`
- `start_schema_replay_apply`
- `mark_schema_replay_committing`
- `finish_schema_replay_apply`
- `apply_schema_candidate`
- `append_schema_applied_wal_record`

Key startup code:

- `crates/nextdb-server/src/main.rs`
  - startup calls `recover_schema_from_wal`
  - startup builds record projection and gets projection status
  - startup calls `load_schema_replay_apply_status`

Key SDK methods:

- `schemaReplayApplyStatus()`
- `resumeSchemaReplayApply()`
- `retrySchemaReplayApply()`
- `cancelSchemaReplayApply()`
- `applySchema(..., { backgroundReplay: true })`

## P3 Subscription Residency

Primary files to inspect:

- `crates/nextdb-server/src/actor.rs`
- `crates/nextdb-server/src/api/connections.rs`
- `crates/nextdb-server/src/sync_tests.rs`
- `packages/nextdb-client/src/index.ts`
- `packages/nextdb-client/test/actor-window-smoke.mjs`

Recently relevant concepts:

- 256 top-level record scope buckets.
- `record_actor_scope_bucket_key`.
- Table subscription retain/release helpers.
- Live query result-set scope retention.
- Scope subscription refcount and lingering tier metadata.

## P4 Behavior Runtime

Primary files to inspect:

- `crates/nextdb-server/src/behavior.rs`
- `crates/nextdb-server/src/api/behavior.rs`
- `crates/nextdb-server/src/api/runtime.rs`
- behavior SDK package under `packages/nextdb-behavior-sdk`

Relevant concepts:

- resident Wasm instance pool
- `on_activate` / `on_deactivate`
- v2 `handle_message` entrypoint with legacy fallback
- postcard / typedSchema ABI encoding
- durable `behaviorContinuation`
- `requestHostHttp`
- `replyTo` callback routing
- `BehaviorPublished` WAL fact and epoch swap

