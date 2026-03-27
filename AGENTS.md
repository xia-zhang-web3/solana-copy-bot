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

Snapshot as of `2026-03-27`.

### Live Server

- Current host is `52.28.0.218`.
- Main service is healthy:
  - `solana-copy-bot.service = active`
  - `copybot-discovery-recent-raw-snapshot.timer = active/enabled`
  - `copybot-discovery-runtime-export.timer = active`

### Disk And Snapshot Path

- `/var/www/solana-copy-bot/state` is currently bounded, not re-inflating.
- Observed live slice:
  - `220G used / 247G avail / 48%`
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
- The bounded `recent_raw` accumulation path is now making preserved progress after
  rollout, but Stage 3 remains blocked until that staged progress completes and a
  newer `latest.sqlite` is promoted.

- Important observed fact:
  - the latest persisted `discovery_wallet_freshness_history` capture is still
    `2026-03-25 18:59:01 UTC`
  - no newer captures have been written since then

- Root cause interpretation from code + logs:
  - in-band Stage 3 captures are only persisted when `publish_due` and
    `runtime_mode == Healthy`
  - at `2026-03-25 19:00 UTC`, the runtime rolled into a new metrics bucket,
    invalidated the cached healthy summary, recomputed truth, and switched to
    `fail_closed`
  - the scoring source became
    `raw_window_incomplete_no_recent_published_universe`
  - separately, the standalone bounded raw snapshot service now repeatedly times
    out before it can finish cloning the growing source DB:
    - source DB grew to about `11.6G`
    - adaptive policy caps at `pages_per_step = 1024`
    - adaptive policy caps at `max_attempt_duration = 120000ms`
    - each attempt copies about `2.0M` pages out of about `2.72M` total, then
      returns `Deferred`
    - the old healthy `latest` surface is retained by design, so usable bounded
      raw coverage never advances
  - practical result: live swap ingest continues, but the 5-day bounded raw
    surface required for Stage 3 does not progress

- Practical implication:
  - do not describe the server as “already healthy”; that is also false
  - the dead livelock was fixed, and the bounded snapshot is progressing again
  - shadow-trading readiness is still blocked until the resumed staged snapshot
    finishes and Stage 3 can start collecting fresh healthy evidence again
  - current operator priority is to watch for:
    - continued `staged_progress_advanced=true`
    - eventual `state=written`
    - `archive_promoted=true`
    - a newer `latest.sqlite` frontier

## If A New Session Starts Elsewhere

A new AI session should:

1. Read this file first.
2. Read `ROAD_TO_PRODUCTION_v2.md`.
3. Check the current git status and latest commits.
4. Continue using the same worker-reviewer workflow.
5. Avoid changing the process unless the user explicitly asks for a different one.
