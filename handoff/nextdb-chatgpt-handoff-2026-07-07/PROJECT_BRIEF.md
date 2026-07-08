# Project Brief

Repository: `/Users/zhanghongpeng/Documents/nextdb`

Long-term objective:

根据 `docs/IMPLEMENTATION_PLAN.md` 持续完成 NextDB 开发，逐项审计并验证 P3/P4/P5 等未完成要求。只有所有要求逐项审计并验证通过后，才能标记完成。

Current high-level status:

- P0/P1 groundwork is partially landed.
- P2 actor/runtime groundwork is substantially implemented.
- P3/P4/P5 are explicitly still target work.
- Recent work has focused on P5 migration-as-replay and background schema replay orchestration.

Current constraints and working rules:

- Use codebase-memory MCP for code discovery when available: `search_graph`, `trace_path`, `get_code_snippet`.
- Recent MCP direct calls often fail with `Transport closed`; use the CLI fallback shown in `README.md`.
- Most of the repo currently appears as untracked. Do not rely on `git diff` or `git diff --stat` for scope.
- For manual edits, use patch-style edits and keep changes narrow.
- Sync every meaningful implementation slice across server/runtime, TS SDK, smoke or Rust tests, and `docs/IMPLEMENTATION_PLAN.md`.
- Rebuild `target/debug/nextdb-server` before running long-lived Node smoke tests; stale binaries have caused false results before.

Current notable touched files:

- `docs/IMPLEMENTATION_PLAN.md`
- `crates/nextdb-server/src/api/schema.rs`
- `crates/nextdb-server/src/main.rs`
- `crates/nextdb-server/src/sync_tests.rs`
- `packages/nextdb-client/src/index.ts`
- `packages/nextdb-client/test/schema-background-replay-smoke.mjs`
- `.codebase-memory/artifact.json`
- `.codebase-memory/graph.db.zst`

