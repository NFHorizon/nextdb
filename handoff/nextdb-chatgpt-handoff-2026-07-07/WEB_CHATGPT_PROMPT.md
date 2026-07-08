# Prompt To Use In Web ChatGPT

You are helping continue development of NextDB in `/Users/zhanghongpeng/Documents/nextdb`.

Read the uploaded handoff files first:

1. `PROJECT_BRIEF.md`
2. `PROGRESS_AND_VALIDATION.md`
3. `NEXT_WORK_PLAN.md`
4. `CODE_POINTERS.md`
5. `IMPLEMENTATION_PLAN.snapshot.md`

Goal:

根据 `docs/IMPLEMENTATION_PLAN.md` 继续完成 NextDB 开发，逐项审计并验证 P3/P4/P5。不要把阶段性进展当成完成；只有逐项审计所有要求并验证通过后，才能认为长期目标完成。

Current state:

- P5 background schema replay apply has a stronger control plane:
  - persisted status
  - `running/committing/succeeded/failed/cancelled`
  - pre-commit cancel
  - resume/retry
  - `resumedFromRunId`
  - `resumeEligible`
  - `resumeReason`
  - startup reconciliation for `committing` status when `SchemaApplied` WAL proves the target schema is durable
- P3/P4/P5 still have incomplete work.

When proposing work:

- Ground every claim in current code or `IMPLEMENTATION_PLAN.snapshot.md`.
- Prefer one small, verifiable slice at a time.
- Include exactly which files to inspect/edit.
- Include validation commands.
- Do not claim completion of the full roadmap unless all P3/P4/P5 requirements have been audited and verified.

