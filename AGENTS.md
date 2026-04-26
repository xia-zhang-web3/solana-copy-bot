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

As of `2026-04-26T16:13:17Z`, the exact-window `program_history` backfill and
the broad explicit-missing repair scan have reached the requested raw-history
interval end, but Stage 3 remains fail-closed because explicit boundary missing
segments remain:

- `start = 2026-04-18T16:56:04Z`
- `end = 2026-04-23T15:59:39.857189405Z`

Current interpretation:

- Stage 3 production discovery truth remains fail-closed.
- The broad exact-window `program_history` backfill frontier has reached the
  requested end.
- The broad provider-blocked explicit missing root segment has been scanned and
  removed.
- The current active lane is targeted repair of remaining prefix/suffix
  boundary missing segments.
- The old live raw frontier/source-starvation blocker is closed.
- The current blocker is not selector starvation and not "wait five days".
- The current blocker is not broad backfill progress; it is missing-segment
  honesty.

Latest confirmed live snapshot:

- `solana-copy-bot.service = active`
- transient `copybot-gap-repair-loop-next.service` stopped after reaching
  `completed_with_explicit_missing_segments` with only boundary/non-target
  evidence remaining
- `copybot-discovery-recent-raw-snapshot.timer = active`
- `copybot-discovery-runtime-export.timer = active`
- service restarts: `NRestarts = 0`
- disk: `360G used / 108G available / 78%`
- gap-fill attempt: `2261`
- `covered_through = 2026-04-23T15:59:39Z`
- `next_batch_start_slot = 415159799`
- `current_phase = completed_with_explicit_missing_segments`
- `verdict = not_proven_due_to_provider_throttling`
- `reason = program_history_gap_fill_repair_explicit_missing_segments_non_target_segments_remain`
- `staged_rows = 45771784`
- `missing_segments_count = 8`
- `replayable_output = false`

Operational reading:

- do not run runtime restore from the current artifact
- do not restart the broad loop just because the artifact is not replayable
- repair only the explicit boundary missing segments with a bounded operator
  path
- production green requires `replayable_output=true` and no `missing_segments`

## Current Development Accounting

Accepted local/repo work for the current lane:

- HTTP 408 retryable block-fetch classification:
  `program_history_gap_fill_retryable_block_fetch_http_408`
- source-contract HTTP 408 / HTTP 503 retryable provider-failure
  classifications:
  `program_history_gap_fill_retryable_source_contract_http_408`,
  `program_history_gap_fill_retryable_source_contract_http_503`
- source-contract transport send-error retryable provider-failure
  classification:
  `program_history_gap_fill_retryable_source_contract_transport_send_error`
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
- Stage 4 operator emergency-stop CLI:
  `crates/app/src/bin/copybot_operator_emergency_stop.rs`
- targeted explicit-missing repair mode:
  `discovery_raw_gap_fill_program_history --repair-explicit-missing-segments`

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
- source-contract HTTP 408 / HTTP 503 remain incomplete/non-replayable and
  resume through `awaiting_next_attempt`; they do not mark coverage complete or
  promote the progress DB
- source-contract transport send errors have the same incomplete/non-replayable
  semantics and resume through `awaiting_next_attempt`
- successful HTTP 2xx JSON-RPC `result` payloads are not throttle evidence even
  if their body text contains throttle-like words
- `copybot_operator_emergency_stop` manages only
  `state/operator_emergency_stop.flag` or the explicitly configured
  `SOLANA_COPY_BOT_EMERGENCY_STOP_FILE` / `--path`
- emergency-stop status parsing matches runtime behavior: the first non-empty
  non-comment line is the reason, and an unreadable existing flag is treated as
  active fail-closed
- emergency-stop activation is atomic via temp-file rename, idempotent for the
  same reason, and requires `--force` to overwrite a different reason
- emergency-stop clear requires the exact
  `--confirm-clear CLEAR_OPERATOR_EMERGENCY_STOP` confirmation and does not
  enable execution or submit trades
- explicit-missing repair mode requires existing matching progress JSON +
  progress DB and objective proof that the base artifact reached the requested
  window end
- explicit-missing repair targets root provider-blocked missing segments first;
  once those roots are gone it can target explicit prefix/suffix boundary
  missing segments
- it does not scan synthetic full-window reasons such as
  `program_history_gap_fill_repair_explicit_missing_segments_non_target_segments_remain`
  directly
- retryable provider/source/budget attrition during repair stays
  non-replayable and persists repair resumability through
  `repair_explicit_missing_base_window_end_reached`
- missing segments are removed only after bounded re-scan proof; partial
  boundary evidence remains explicit fail-closed evidence
- when a repair scan completes the scanable part of a broad provider-blocked
  root segment, the root is replaced by narrower boundary missing evidence so
  future repair attempts do not retarget the same already-refined root
- boundary missing evidence is removed only after bounded repair scan proof;
  until then it remains explicit fail-closed evidence
- unchanged or non-narrower boundary evidence after a completed bounded scan is
  terminal incomplete evidence with reason
  `program_history_gap_fill_repair_explicit_missing_segments_irreducible_boundary_evidence_remains`,
  not a continuable provider-throttling loop
- `discovery_raw_gap_fill_program_history_irreducible_boundary_report` is a
  read-only decision-surface report only: it quantifies irreducible boundary
  residue, always keeps `restore_ready=false` and `production_green=false`, and
  does not weaken restore gates or fail-closed semantics

Deployment status:

- the broad repair live loop is currently stopped after proving only boundary
  evidence remains
- deploy the boundary-capable explicit-missing repair binary only after reviewer
  acceptance
- status operator is observability only and does not change restore semantics
- restore preflight is a fail-closed post-backfill gate helper; it does not
  apply restore or mark production green
- handoff report is a human review helper only; it does not apply restore,
  start the gap-fill child, or mark production green
- emergency-stop CLI is a manual safety surface only; it is not a Stage 3
  production-green signal and should not be deployed or used as an activation
  shortcut

Current sync status:

- current gap-fill / restore operator commits have reached `origin/main`
  through `21b56db`
- before duplicating work in a new session, still check whether equivalent
  files already exist on `origin/main`

## If A New Session Starts Elsewhere

1. Read this file first.
2. Read `ROAD_TO_PRODUCTION_v2.md`.
3. Read the latest addendum in `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`.
4. Check `git status`, latest commits, and whether `origin/main` contains the
   accepted operator files.
5. Continue the worker-reviewer workflow.
6. Do not change the process unless the user explicitly asks for a different
   one.
