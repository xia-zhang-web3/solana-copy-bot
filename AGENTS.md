# AGENTS.md

This file is the working contract for AI-assisted development in this
repository. Keep it operational, short, and current. Long incident history
belongs in the project docs listed below, not here.

## Purpose

Use this file to keep future AI sessions aligned on process, safety rules, and
the current production blocker.

The project uses a split workflow:

1. A coding worker implements exactly one bounded batch.
2. A reviewer/operator audits that batch, reruns tests, accepts or rejects it,
   and only then commits, pushes, or deploys it.

This split is deliberate. It reduces silent semantic regressions and keeps the
roadmap disciplined.

## Current Non-Negotiables

These rules remain true unless the user explicitly changes the project plan:

- Stage 3 production discovery truth is the hard gate.
- Stage 4 planning-safe execution and policy surfaces do not override Stage 3.
- Non-prod drills, devnet evidence, stale evidence, and local-only evidence do
  not authorize production activation.
- `execution.enabled` must not be enabled as part of roadmap or diagnostic
  work.
- Do not weaken fail-closed behavior.
- Do not reduce `scoring_window_days`.
- Do not propose selector/scoring/quality fixes while the current raw-history
  gap remains unclosed.
- Do not treat operator observability as production green.
- Do not deploy accepted code just because it exists; deploy only when the
  batch needs live rollout.

## Roles

### User

- Sets priorities.
- Decides when to move to the next roadmap batch.
- Decides when to deploy to the server.

### Coding Worker

- Implements one bounded batch at a time.
- Does not self-approve its own work.
- Reports what changed, files touched, tests run, and what was intentionally
  not touched.

### Reviewer / Operator

- Reads the diff.
- Reruns relevant tests.
- Inspects semantic paths, not only compile/test success.
- Accepts or rejects the batch.
- Writes the next corrective prompt when needed.
- If accepted, updates docs, commits only relevant files, pushes, and performs
  server rollout checks only when requested or operationally needed.

## Default Workflow

1. Pick the next bounded batch from `ROAD_TO_PRODUCTION_v2.md` or from the
   current proven live seam.
2. Write one explicit batch prompt for the coding worker.
3. Worker implements only that batch.
4. Worker returns changed files, tests run, and out-of-scope areas preserved.
5. Reviewer audits the batch.
6. If blocked, reviewer rejects it and provides a precise corrective prompt.
7. If accepted, reviewer updates docs if needed, commits only relevant files,
   pushes to `main`, and deploys only when rollout is actually needed.

## Proof-First Rule

This repository is operated proof-first, not guess-first.

1. Do not ask for speculative fixes when the failing seam is still unclear.
2. If the blocker is not proven tightly enough, add or refine a bounded
   read-only operator, trace, or diagnostic surface first.
3. Only after the blocker is proven from code and live evidence should the next
   coding batch attempt a corrective fix.
4. After a fix is accepted and rolled out, rerun the same operator family on
   live to verify whether it changed the proven seam.
5. If live falsifies the fix, record the exact result in docs and write the
   next prompt against that newly proven seam.

In short:

- prove the blocker
- make the smallest matching correction
- rerun proof on live
- move to the next seam

## Prompt Contract

Every coding-batch prompt should include:

- exact goal
- allowed files or modules
- hard safety constraints
- required operator command(s), if any
- required tests
- acceptance criteria
- expected result
- explicit out-of-scope list

Typical out-of-scope constraints:

- do not touch restore / gap-fill / snapshot branches unless that is the batch
- do not touch `scoring_window_days`
- do not touch selector/scoring unless raw-history gap closure is already
  proven
- do not enable `execution.enabled`
- do not submit real trades on production
- do not let non-prod evidence override Stage 3 production gate

## Review Standard

Never accept a batch from the worker summary alone.

Before acceptance, reviewer must:

1. Read the relevant diff.
2. Rerun claimed tests.
3. Inspect critical semantic paths manually.
4. Decide whether the implementation matches the contract.

Reject for:

- false green conditions
- stale evidence treated as current
- production safety holes
- hidden config reuse across prod/non-prod boundaries
- contracts that look good in docs but are not enforced in code
- incomplete rollback or restore semantics
- missing recency handling
- broad refactors hidden inside bounded batches

When rejecting:

- list blockers clearly
- cite affected files
- provide a precise corrective prompt

## Commit And Push Rules

- Commit only accepted batch files.
- Do not commit unrelated local changes.
- Do not commit scratch directories such as `.tmp/`.
- Use short, descriptive commit messages.
- If unrelated local changes exist, leave them alone unless the user explicitly
  asks otherwise.
- If the local environment blocks `.git` writes or GitHub network access,
  report the exact blocker and do not reimplement accepted work.

## Server Rollout Rules

Do not deploy every accepted batch.

Roll out only when the batch affects:

- live runtime behavior
- server-side operators needed immediately
- systemd units
- scheduled jobs
- production diagnostics needed immediately

Server rollout pattern:

1. Check current service state first.
2. Deploy the specific accepted batch only.
3. Rebuild only needed binaries.
4. If systemd units changed, copy units, run `daemon-reload`, and restart or
   re-enable only affected units.
5. Verify live behavior after rollout.

Preferred live checks:

- `systemctl is-active`
- `systemctl status --no-pager`
- `journalctl`
- disk usage
- archive count / retention checks
- bounded functional checks against the live binary

## Documentation Rules

Use project docs as persistent memory.

Primary docs:

- `ROAD_TO_PRODUCTION_v2.md`
- `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`
- `ops/server_templates/README.md`

Update docs when:

- a roadmap batch is accepted
- a live operational incident occurs
- server behavior materially changes
- an operator command becomes primary, deprecated, or removed

Record facts, not vague status language.

## Current State Snapshot

As of `2026-04-24T11:18:43Z`, the current production blocker is the exact
missing raw-history interval:

- `start = 2026-04-18T16:56:04Z`
- `end = 2026-04-23T15:59:39.857189405Z`

Current interpretation:

- Stage 3 production discovery truth remains fail-closed.
- The active lane is exact-window `program_history` raw gap-fill.
- The old live raw frontier/source-starvation blocker is closed.
- The current blocker is not selector starvation and not "wait five days".
- The current strongest blocker is long-running provider-attrition backfill.

Latest confirmed live snapshot:

- `solana-copy-bot.service = active`
- `copybot-program-gap-loop.service = active/running`
- `copybot-discovery-recent-raw-snapshot.timer = active`
- `copybot-discovery-runtime-export.timer = active`
- service restarts: `NRestarts = 0`
- disk: `333G used / 134G available / 72%`
- gap-fill attempt: `48`
- progress: `11.573534%`
- `covered_through = 2026-04-19T06:42:50Z`
- `remaining_gap_hours = 105.280516`
- `staged_rows = 5225868`
- `zero_progress_retry_count = 0`
- `zero_progress_escape_applied = false`
- `replayable_output = false`

Operational reading:

- the loop was advancing at the latest confirmed check
- no disk degradation was observed
- no repeated stuck frontier was observed
- continue exact-window operator attempts while the frontier advances and disk
  remains safe
- write a new coding prompt only for a newly proven concrete blocker

## Current Development Accounting

Accepted local/repo work for the current lane:

- HTTP 408 retryable block-fetch classification:
  `program_history_gap_fill_retryable_block_fetch_http_408`
- repo-managed loop operator:
  `crates/discovery/src/bin/discovery_raw_gap_fill_program_history_loop.rs`
- read-only status operator:
  `crates/discovery/src/bin/discovery_raw_gap_fill_program_history_status.rs`
- read-only restore readiness preflight:
  `crates/discovery/src/bin/discovery_raw_gap_fill_program_history_restore_preflight.rs`
- read-only artifact validator:
  `crates/discovery/src/bin/discovery_raw_gap_fill_program_history_artifact_validate.rs`
- read-only human handoff report:
  `crates/discovery/src/bin/discovery_raw_gap_fill_program_history_handoff_report.rs`
- mutating restore path gap-fill gate:
  `crates/discovery/src/bin/discovery_runtime_restore.rs`

Operator semantics:

- loop operator runs one child attempt at a time
- loop operator does not write progress JSON or synthesize coverage
- status operator reads one progress JSON and computes percent/remaining time
- status operator does not call RPC, open SQLite, spawn a child, write progress,
  or synthesize `replayable_output=true`
- restore preflight reads one progress JSON and reports `restore_ready=true`
  only when `replayable_output=true`, coverage reaches the requested window
  end, and `missing_segments` is empty
- artifact validator reads one progress JSON and reports
  `artifact_valid_for_restore_review=true` only for explicit replayable
  exact-window coverage with no missing segments, positive inserted rows, and
  zero withheld rows
- handoff report reads one progress JSON and reports
  `handoff_ready_for_human_restore_review=true` only when the same artifact
  review criteria are explicitly satisfied; otherwise it emits safe read-only
  status, preflight, and artifact-validator commands
- `discovery_runtime_restore --gap-fill-db-path` now requires a matching
  program-history progress JSON and exact UTC window arguments before it will
  replay the gap-fill DB into a target runtime DB
- restore gate failures happen before target DB parent creation, target DB open,
  migrations, artifact restore, or journal/gap-fill replay
- missing progress control truth fails closed

Deployment status:

- do not roll out the repo-managed loop wrapper over the active transient live
  loop unless a controlled checkpoint is chosen
- status operator is observability only and does not change restore semantics
- restore preflight is a fail-closed post-backfill gate helper; it does not
  apply restore or mark production green
- handoff report is a human review helper only; it does not apply restore,
  start the gap-fill child, or mark production green

Current sync caveat:

- an accepted commit for the current operator files may exist in a temporary
  clone because this sandbox could not write `.git/index.lock` in the main
  working tree
- push to GitHub was blocked by local DNS/network errors resolving
  `github.com`
- before duplicating work, check whether equivalent files have reached
  `origin/main`

## If A New Session Starts Elsewhere

1. Read this file first.
2. Read `ROAD_TO_PRODUCTION_v2.md`.
3. Read the latest addendum in `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`.
4. Check `git status`, latest commits, and whether `origin/main` contains the
   accepted operator files.
5. Continue the worker-reviewer workflow.
6. Do not change the process unless the user explicitly asks for a different
   one.
