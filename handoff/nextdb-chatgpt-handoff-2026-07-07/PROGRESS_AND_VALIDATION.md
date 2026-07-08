# Progress And Validation

## Recently Completed P3 Work

- Top-level full-table and key-range WebSocket subscriptions retain and release the subscribed table's full 256 top-level hash bucket scopes.
- Full-table and key-range subscriptions now affect actor-kernel scope subscription refcounts.
- Declared-index prefix subscriptions do not retain all table buckets at subscription time because value prefixes do not map to a stable key-scope subset.
- Disconnect cleanup releases normal table subscription scopes before nested/query scopes.
- TS actor kernel status types were expanded to include scope residency details.
- `actor-window-smoke` checks subscription refcount increases by at least 256 and returns to baseline after unsubscribe.

## Recently Completed P5 Work

The latest work has focused on background schema replay apply.

Implemented behavior:

- `applySchema(..., { allowBreakingReplay: true, backgroundReplay: true })` starts an async replay-backed schema apply after synchronous preflight.
- Replay status persists in `data/schema/schema-replay-status.json`.
- Status phases include `idle`, `running`, `committing`, `succeeded`, `failed`, and `cancelled`.
- `running` restart becomes a failed interrupted status that can be resumed.
- `committing` restart reconciles against recovered `SchemaApplied` WAL state:
  - if the WAL proves the target schema version is durable, status becomes `succeeded`;
  - recovered `schemaAuditLsn` and projection status are restored into status.
- `POST /v1/admin/schema/replay/resume` and `db.resumeSchemaReplayApply()` restart a failed interrupted replay.
- `POST /v1/admin/schema/replay/retry` and `db.retrySchemaReplayApply()` remain compatibility aliases for the same restart flow.
- `POST /v1/admin/schema/replay/cancel` and `db.cancelSchemaReplayApply()` cancel only pre-commit `running` jobs.
- `committing` jobs reject cancellation and are allowed to finish.
- `resumedFromRunId` records replay lineage.
- `resumeEligible` and `resumeReason` let admin tooling tell whether failed status can be resumed without parsing error strings.

## Verification Commands Recently Passed

These were run after the latest replay status-contract changes:

```bash
cargo fmt --check
cargo test -q -p nextdb-server schema_replay
npm run build
npm run test:schema-background-replay
cargo test -q --workspace
npm run test:schema-replay
npm run completion:audit
git diff --check
/Users/zhanghongpeng/.local/bin/codebase-memory-mcp cli index_repository '{"repo_path":"/Users/zhanghongpeng/Documents/nextdb","mode":"moderate","persistence":true}'
pgrep -fl nextdb-server || true
```

Observed result:

- All listed validation commands passed.
- `pgrep -fl nextdb-server || true` had no output after the smoke tests, so no server process was left running.

## Important Caveat

The passing tests prove the latest P5 replay status slices, not the entire P3/P4/P5 roadmap. `IMPLEMENTATION_PLAN.snapshot.md` still explicitly states that P3/P4/P5 remain target work.

