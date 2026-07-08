# Next Work Plan

Keep working from `IMPLEMENTATION_PLAN.snapshot.md`, not from this handoff alone.

## Priority 1: Continue P5 Background Replay Pipeline

The replay control plane is now much stronger, but the plan still says broader resume orchestration is incomplete.

Recommended next P5 slices:

1. Audit whether projection rebuild background status has parity with schema replay status.
   - Current schema replay status has `running/committing/succeeded/failed/cancelled`, `resumeEligible`, `resumeReason`, `resumedFromRunId`.
   - Projection rebuild status may still only expose a simpler `running/succeeded/failed` model.
   - Decide whether migration-as-replay needs cancel/resume/lineage semantics for projection rebuild too.

2. Audit startup recovery for partial projection rebuild artifacts.
   - Schema apply stages replacement projections before the `SchemaApplied` WAL commit.
   - Verify temporary/staged projection directories are cleaned or ignored on crash.
   - Add a smoke or Rust test if behavior is currently implicit.

3. Tighten status/API contract for failed replay jobs.
   - `resumeEligible` currently tells tooling whether a failed status is resumable.
   - Consider adding machine-readable `resumeBlockers` if preflight can fail after a failed status is loaded.

## Priority 2: P3 Subscription-Driven Residency Acceptance

P3 has many landed slices but remains incomplete against the acceptance criteria.

Recommended next P3 slices:

1. Audit "unsubscribed data occupies zero heat memory".
   - Confirm scope actors tier down from L3 to L1/L0 after subscription release and lingering grace.
   - Add a deterministic smoke or Rust test that activates/retains/releases a scope and asserts resident row memory drops.

2. Audit reconnect churn.
   - The plan says reconnect churn should not rehydrate because lingering should preserve warm scopes.
   - Add a smoke that subscribes, disconnects, reconnects within linger, and confirms no cold rehydrate path is taken.

3. Audit interval-router complexity.
   - Current docs mention connection-local range routing and prefix-best upper-bound cache.
   - Confirm whether this satisfies "per-shard interval-tree subscription router; O(log n) match per event" or still needs shard-global routing work.

## Priority 3: P4 ABI V2 Hardening

P4 has many completed pieces, but the top-level status still lists guest ABI v2 hardening and hot reload as target work.

Recommended next P4 slices:

1. Audit ABI v2 compatibility contract.
   - Ensure legacy `invoke` fallback and v2 `handle_message` behavior are both covered by tests.
   - Confirm postcard typed schema paths fail clearly on mismatched payloads.

2. Audit continuation edge cases.
   - Cycle detection, deadline inheritance, replyTo routing, and host HTTP idempotency are described as landed.
   - Add tests for nested continuation failure propagation if missing.

3. Audit hot reload acceptance.
   - Confirm `BehaviorPublished` WAL fact durability before epoch swap under sustained load.
   - Add a smoke that publishes during active invocation load and confirms no dropped connection/message.

## Suggested Validation Gates For Each Slice

Run a focused gate first, then the broader gate:

```bash
cargo fmt --check
cargo test -q -p nextdb-server <focused_filter>
npm run build
npm run <focused_smoke>
cargo test -q --workspace
npm run completion:audit
git diff --check
/Users/zhanghongpeng/.local/bin/codebase-memory-mcp cli index_repository '{"repo_path":"/Users/zhanghongpeng/Documents/nextdb","mode":"moderate","persistence":true}'
pgrep -fl nextdb-server || true
```

