# AGENTS.md

This file describes the working contract for AI-assisted development in this repository.

It is intentionally operational and specific to how this project is being run.

## Purpose

Use this file to keep future AI sessions consistent when work is moved to another machine or another session.

The project uses a two-step AI workflow:

1. A coding worker implements one bounded batch.
2. A reviewer/operator audits that batch, reruns tests, accepts or rejects it, and only then commits, pushes, or deploys it.

This split is deliberate. It reduces silent semantic regressions and keeps roadmap progress disciplined.

## Roles

There are three actors.

### User

- Sets priorities.
- Decides when to move to the next roadmap batch.
- Decides when to deploy to the server.

### Coding Worker

- Implements exactly one bounded batch at a time.
- Does not self-approve its own work.
- Reports what changed, what was tested, and what was intentionally not touched.

### Reviewer / Operator

- Reads the diff.
- Reruns relevant tests.
- Looks for semantic blockers, not just compile/test success.
- Rejects batches when the contract is wrong.
- Writes the next corrective prompt when needed.
- If accepted, updates docs, commits, pushes, and performs server rollout checks when requested.

## Default Workflow

This is the standard loop.

1. Pick the next bounded batch from `ROAD_TO_PRODUCTION_v2.md`.
2. Write one explicit batch prompt for the coding worker.
3. Worker implements only that batch.
4. Worker returns:
   - what changed
   - what files were touched
   - what tests were run
   - what was intentionally not touched
5. Reviewer audits the batch.
6. If blocked:
   - reviewer rejects it
   - reviewer provides an exact follow-up prompt for the missing/correctness issue
7. If accepted:
   - reviewer updates docs if needed
   - reviewer commits only the relevant files
   - reviewer pushes to `main`
   - reviewer deploys to the server only if the batch actually needs rollout

## Proof-First Rule

This repository is operated proof-first, not guess-first.

That means:

1. The auditor / reviewer does not ask for speculative fixes when the failing
   seam is still unclear.
2. If the current blocker is not yet proven tightly enough, the next batch
   should add or refine a bounded read-only operator / trace / diagnostic
   surface first.
3. Only after the blocker is proven from code and live evidence should the next
   coding batch attempt a corrective fix.
4. After a fix is accepted and rolled out, the same operator family should be
   rerun on live to verify whether the fix actually changed the proven seam.
5. If live falsifies the fix, record the exact result in docs and write the
   next prompt against that newly proven seam. Do not jump back to broad
   speculative rewrites.

In short:

- first prove the blocker
- then make the smallest corrective change that matches that proof
- then rerun proof on live
- then move to the next seam

## Prompt Style For Coding Batches

Batch prompts should be large enough to move the roadmap, but still bounded.

Each prompt should include:

- the exact roadmap goal
- the branches or areas that must not be touched
- hard safety constraints
- required operator command(s)
- required test coverage
- acceptance criteria
- expected result

Every prompt should explicitly say what is out of scope.

Typical out-of-scope constraints in this repo:

- do not touch restore / gap-fill / snapshot branches unless that is the batch
- do not touch `scoring_window_days`
- do not enable `execution.enabled`
- do not submit real trades on production
- do not let non-prod evidence override Stage 3 production gate

## Review Standard

Never accept a batch from the worker summary alone.

The reviewer must do all of the following before acceptance:

1. Read the relevant diff.
2. Rerun the claimed tests.
3. Inspect the critical semantic paths manually.
4. Decide whether the implementation matches the contract, not just whether tests pass.

The reviewer should reject for:

- false green conditions
- stale evidence being treated as current
- production safety holes
- hidden config reuse across prod/non-prod boundaries
- contracts that look good in docs but are not actually enforced in code
- incomplete rollback semantics
- missing recency handling

When rejecting a batch:

- list the blockers clearly
- cite the affected file(s)
- provide a precise corrective prompt

## Commit And Push Rules

The reviewer commits and pushes accepted batches.

Rules:

- commit only the files belonging to the accepted batch
- do not accidentally commit unrelated work
- do not commit scratch directories such as `.tmp/`
- use short, descriptive commit messages

If unrelated local changes exist, leave them alone unless the user explicitly asks otherwise.

## Server Rollout Rules

Do not deploy every accepted batch.

Roll out only when the batch affects:

- live runtime behavior
- server-side operators
- systemd units
- scheduled jobs
- production diagnostics needed immediately

Server work should follow this pattern:

1. Check current service state first.
2. Deploy the specific accepted batch only.
3. Rebuild only the needed binaries.
4. If systemd units changed:
   - copy unit files
   - `daemon-reload`
   - restart or re-enable only the affected unit
5. Verify live behavior after rollout.

For live verification, prefer explicit checks such as:

- `systemctl is-active`
- `systemctl status --no-pager`
- `journalctl`
- disk usage checks
- archive count / retention checks
- bounded functional checks against the live binary

## Documentation Rules

Use the project docs as the persistent memory layer.

Primary docs:

- `ROAD_TO_PRODUCTION_v2.md`
- `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`
- `ops/server_templates/README.md`

Update them when:

- a roadmap batch is accepted
- a live operational incident occurs
- server behavior materially changes
- an operator command becomes primary, deprecated, or removed

Use them to record facts, not vague status language.

## Project-Specific Decision Hierarchy

The following hierarchy must remain true unless the user explicitly changes the project plan.

1. Stage 3 production discovery truth is the hard gate.
2. Stage 4 planning-safe execution and policy surfaces do not override Stage 3.
3. Non-prod drills and devnet evidence do not authorize production activation.
4. Planning-safe green never means "turn on trading now".

In short:

- prod discovery truth first
- bounded launch dossier second
- non-prod rehearsal evidence third
- manual activation decision only after all of the above

## Current Working Style

The project currently operates like this:

- user asks for the next roadmap step
- reviewer writes the batch prompt
- coding worker implements it
- reviewer audits and accepts/rejects
- reviewer commits and pushes accepted work
- server rollout happens only when it is actually needed

This is the expected style for future sessions too.

## Practical Notes

- Keep answers concise and factual.
- Prefer exact operator surfaces over log archaeology.
- Prefer explicit bounded contracts over hidden defaults.
- Prefer one accepted architecture over multiple competing legacy paths.
- When a batch is accepted but not needed on the server yet, do not roll it out just because it exists.

## Current Server Facts

Snapshot as of `2026-04-23` after the recent-raw head-gap truthfulness
rollouts.

### Latest Stage 3 Update

- The production host is currently on commit `ab75d6d`.
- The main service was rebuilt and restarted:
  - `solana-copy-bot.service = active`
- The current active live blocker is no longer the replay `sol_leg` lane.
- Recent live cycles now repeatedly return:
  - `repair_state = skipped_recent_raw_journal_head_gap_no_repairable_rows`
  - `repair_reason = recent_raw_journal_has_no_rows_in_required_window_before_runtime_cursor`
- The same live cycles also show:
  - `journal_covers_runtime_cursor = true`
  - `runtime_window_first_cursor_signature = None`
  - `runtime_window_first_cursor_slot = None`
  - `runtime_window_first_cursor_ts = None`
  - `replay_batches_completed = 0`
  - `replay_rows_loaded = 0`
  - `replay_rows_inserted = 0`
  - `runtime_window_complete_after = false`
- The current live interval clue is:
  - `required_window_start` around `2026-04-18T07:30:53Z`
  - `replay_until_cursor_ts = 2026-04-17T17:33:19.708359647Z`
  - the derived repair interval is empty before first-batch replay can load
    rows
- Persisted publication truth on the runtime DB remains:
  - `publication_runtime_mode = fail_closed`
  - `publication_reason = raw_window_unusable_no_recent_published_universe`
- Interpretation:
  - Stage 3 is still blocked.
  - Replay corrective work was still useful because it proved replay is no
    longer the active live blocker.
  - The current live seam is now narrower and sits in recent-raw interval /
    frontier derivation.
  - The journal metadata says the runtime cursor is covered, but the derived
    replay interval still has no repairable rows.
  - The next bounded batch should target that recent-raw eligibility /
    interval-derivation contract, not replay resumability.

### Live Server

- Current host is `52.28.0.218`.
- Main service is healthy:
  - `solana-copy-bot.service = active`
  - `copybot-discovery-recent-raw-snapshot.timer = active/enabled`
  - `copybot-discovery-runtime-export.timer = active`

### Disk And Snapshot Path

- `/var/www/solana-copy-bot/state` is currently bounded, not re-inflating.
- Observed live slice:
  - `199G used / 268G avail / 43%`
  - `state/discovery_restore/recent_raw = 183G`
  - archive count is currently `0` because no fresh archive promotion has happened since the stall
- Emergency livelock fix `9387c65` is now deployed on the production host.
- The bounded `recent_raw` path is no longer resetting to zero on each timer tick:
  - first post-rollout run returned:
    - `state=deferred`
    - `staged_progress_resumed=false`
    - `staged_progress_preserved_for_retry=true`
    - `staged_progress_advanced=true`
    - `staged_row_count_after_attempt=483328`
  - second post-rollout run returned:
    - `state=deferred`
    - `staged_progress_resumed=true`
    - `staged_progress_preserved_for_retry=true`
    - `staged_progress_advanced=true`
    - `staged_row_count_before_attempt=483328`
    - `staged_row_count_after_attempt=753664`
- Current interpretation:
  - the snapshot path is now recovering via preserved staged progress
  - but `latest.sqlite` is still frozen at the old promoted frontier until a full resumed completion promotes a newer bounded snapshot

### Raw Window Progress

- The current promoted bounded snapshot is frozen at:
  - `covered_since = 2026-03-24 12:07:11 UTC`
  - `covered_through = 2026-03-26 07:33:59 UTC`
  - `last_batch_completed_at = 2026-03-26 07:35:10 UTC`
- That is only about `1d 19h 27m` of usable bounded raw coverage.
- The staged snapshot now shows resumed forward progress:
  - `created_at = 2026-03-26 22:12:34 UTC`
  - `row_count = 753664`
  - `covered_through = 2026-03-24 14:31:41 UTC`
  - `last_batch_completed_at = 2026-03-26 22:15:40 UTC`
- This is no longer a dead “reset every run” state.
- But it is also not yet green:
  - usable 5-day accumulation has not completed
  - `latest.sqlite` has not been replaced yet

### Discovery Ingestion

- Swap ingestion is alive.
- Example live growth sample observed on the server:
  - `rowid 16234032 -> 16235713`
  - over about `19s`
  - i.e. `+1681` rows
- Discovery cycles continue to complete normally.

### Stage 3 Current Interpretation

- Stage 3 is still blocked.
- This is now a production-critical blocker on the primary path.
- The runtime itself is still alive.
- The current blocker is no longer the old staged-snapshot reset problem and no
  longer the replay `sol_leg` seam.
- The current blocker is the recent-raw head-gap eligibility branch where:
  - metadata says the journal covers the runtime cursor
  - `runtime_window_first_cursor_*` is still `None`
  - the derived replay interval has no repairable rows
  - publication remains `fail_closed`

- Important observed fact:
  - the latest persisted `discovery_wallet_freshness_history` capture is still
    `2026-03-25 18:59:01 UTC`
  - no newer captures have been written since then

- Root cause interpretation from current code + logs:
  - the live service now truthfully proves that the current recent-raw repair
    branch is not replaying anything because there are no repairable rows in
    the derived interval before the runtime cursor
  - specifically, the active branch currently reaches:
    - `repair_state = skipped_recent_raw_journal_head_gap_no_repairable_rows`
    - `repair_reason = recent_raw_journal_has_no_rows_in_required_window_before_runtime_cursor`
  - practical result: live swap ingest continues, but Stage 3 publication truth
    remains fail-closed because runtime-window completeness does not advance

- Practical implication:
  - do not describe the server as “already healthy”; that is also false
  - the current priority is no longer generic snapshot convergence watching
  - shadow-trading readiness is still blocked until the recent-raw
    runtime-window eligibility seam is corrected
  - current operator priority is to watch for:
    - disappearance of
      `skipped_recent_raw_journal_head_gap_no_repairable_rows`
    - a repair branch that either finds replayable rows or exits through a more
      correct earlier predicate
    - a newer `discovery_strategy_state.updated_at`
    - a reason different from `raw_window_unusable_no_recent_published_universe`

## If A New Session Starts Elsewhere

A new AI session should:

1. Read this file first.
2. Read `ROAD_TO_PRODUCTION_v2.md`.
3. Check the current git status and latest commits.
4. Continue using the same worker-reviewer workflow.
5. Avoid changing the process unless the user explicitly asks for a different one.
