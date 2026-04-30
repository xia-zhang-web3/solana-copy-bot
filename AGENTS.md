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

Latest update as of `2026-04-29T19:27:41Z`:

- Stage 3 production discovery truth remains fail-closed.
- Raw-history recovery and the old `program_history` broad backfill lane are
  not the active blocker.
- Yellowstone request-first subscribe is working; source-open is not the active
  blocker.
- The current live seam is recent_raw journal safety/freshness on the
  observed-swap writer path.
- `3765b25` added recent_raw writer phase telemetry and proved the live stall
  was inside hot writer retention pruning: `prune_start` appeared without a
  matching `prune_end`.
- `cb659e8` (`Defer recent raw hot writer prune`) was accepted, pushed, and
  deployed by rebuilding only `copybot-app`.
- Pre-restart bounded catch-up committed `37,459` journal rows and reported
  `catch_up_complete=true`.
- Post-rollout live proof:
  - `solana-copy-bot.service = active`
  - `MainPID = 1603047`
  - `NRestarts = 0`
  - timers `copybot-discovery-recent-raw-snapshot.timer` and
    `copybot-discovery-runtime-export.timer` are active
  - disk: `377G used / 90G available / 81%`
  - `prune_start_count = 0` over the first 5-minute post-rollout window
  - `prune_skipped_count = 3623` with reason
    `recent_raw_journal_hot_writer_prune_deferred`
  - recent_raw journal tail was within about one slot of runtime tail:
    runtime `2026-04-29T19:27:21.650754072Z / 416501293`,
    journal `2026-04-29T19:27:21.424326107Z / 416501292`
- The hot writer now intentionally defers retention prune when
  `skip_prune_while_backlogged=true`; journal state still advances only after
  committed rows.
- This is not production green. The next proof step is to monitor sustained
  recent_raw freshness and only then decide whether a separate bounded prune
  operator/lane is needed outside the hot writer path.

Follow-up update as of `2026-04-29T20:03:24Z`:

- After the recent_raw hot-writer fix, one live restart occurred at
  `2026-04-29T19:40:40Z`.
- Restart reason:
  `observed swap writer terminal failure` caused by
  `failed to run discovery scoring rug finalize: failed to open discovery scoring rug finalize transaction: database is locked`.
- The next bounded fix was accepted and deployed:
  `835fd7b` (`Retry rug finalize sqlite locks`), touching only
  `crates/app/src/observed_swap_writer.rs`.
- Local reviewer checks passed:
  `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer -- --test-threads=1`,
  `cargo check -j 1 -p copybot-app --bin copybot-app`,
  `rustfmt --check --edition 2021 crates/app/src/observed_swap_writer.rs`,
  and `git diff --check -- crates/app/src/observed_swap_writer.rs`.
- Post-rollout snapshot:
  - `solana-copy-bot.service = active`
  - `MainPID = 1604802`
  - `NRestarts = 0` after manual restart
  - runtime and recent_raw journal tails both at
    `2026-04-29T20:03:23.039251401Z / 416506772`
  - `prune_start_count = 0`
  - `terminal_failure_count = 0`
  - no warning entries in the first post-rollout window
- The new retryable rug-finalize reason has not yet fired on live after
  rollout; keep monitoring before declaring that seam closed.

Follow-up update as of `2026-04-29T20:55:42Z`:

- The next observed-writer seam was accepted and deployed:
  `cf156af` (`Retry aggregate replay apply locks`), touching only
  `crates/app/src/observed_swap_writer.rs`.
- Implemented behavior:
  - SQLite busy/locked during aggregate startup/gap replay
    `apply_discovery_scoring_batch` is retryable and non-terminal.
  - Stable reason:
    `observed_swap_writer_discovery_scoring_replay_apply_sqlite_lock_retryable`.
  - On retryable replay-apply lock, the writer latches
    `discovery_scoring_materialization_gap_cursor` to the first failed replay
    row and leaves `covered_through` unchanged.
  - Unknown/non-lock apply errors remain terminal.
- Local reviewer checks passed:
  `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer -- --test-threads=1`,
  `cargo check -j 1 -p copybot-app --bin copybot-app`,
  `rustfmt --check --edition 2021 crates/app/src/observed_swap_writer.rs`,
  and `git diff --check -- crates/app/src/observed_swap_writer.rs`.
- Production rollout advanced the server checkout to `cf156af`, rebuilt only
  `copybot-app`, and restarted `solana-copy-bot.service`.
- Rollout incident:
  - the first restart entered a startup ABRT loop before app runtime handoff
  - failing stage:
    `sqlite_pragma_journal_mode_wal`
  - each process timed out at `30s` and aborted with `status=6/ABRT`
  - `NRestarts` reached `10`
  - runtime WAL was large:
    `live_runtime_20260324T134339Z.db-wal = 11G`
- Operator recovery:
  - stopped `solana-copy-bot.service`
  - ran SQLite-managed `PRAGMA wal_checkpoint(TRUNCATE)` as `copybot`
  - checkpoint returned `0|0|0`
  - runtime WAL/SHM were removed by SQLite and the recreated WAL was `9.6M`
  - read-only `PRAGMA journal_mode` returned `wal` quickly
- Post-recovery snapshot:
  - `solana-copy-bot.service = active`
  - `MainPID = 1606959`
  - `NRestarts = 0` after recovered restart
  - disk: `378G used / 89G available / 82%`
  - runtime and recent_raw journal tails both reached
    `2026-04-29T20:55:41.204062169Z / 416514723`
  - startup WAL timeout count: `0`
  - replay-apply retryable count: `0`
  - rug-finalize retryable count: `0`
  - terminal failure count: `0`
- Do not manually delete SQLite WAL/SHM files. If this recurs, stop the service
  and use SQLite-managed checkpoint/truncate before restarting.
- The replay-apply retryable reason has not yet fired after recovery; keep
  monitoring before declaring that seam closed.

Morning update as of `2026-04-30T07:56:51Z`:

- `solana-copy-bot.service = active`
- `MainPID = 1610242`
- `NRestarts = 3`
- current process started at `2026-04-30T06:47:00Z`
- runtime and recent_raw journal tails both reached
  `2026-04-30T07:56:47.989748492Z / 416615563`
- disk: `400G used / 67G available / 86%`
- runtime DB/WAL:
  - `live_runtime_20260324T134339Z.db = 83G`
  - `live_runtime_20260324T134339Z.db-wal = 8.3G`
- memory:
  - host RAM: `7.6GiB`
  - swap: `0B`
  - current service memory: `5.5G`
  - peak service memory: `7.4G`
- overnight failures:
  - `2026-04-30T01:03:41Z`: observed-writer terminal failure from
    `failed to run discovery scoring covered_through cursor update: failed to open discovery scoring covered_through cursor update transaction: database is locked`
  - `2026-04-30T04:14:12Z`: kernel OOM killed `copybot-app`, anon RSS about
    `7.3G`
  - `2026-04-30T06:46:58Z`: kernel OOM killed `copybot-app`, anon RSS about
    `7.3G`
- Current-process proof since `2026-04-30T06:47:00Z`:
  - replay-apply retryable lock reason fired `11` times and did not kill the
    service
  - discovery was aborted due to recent_raw journal backlog `5` times
  - observed-writer terminal failures: `0`
  - OOM kills: `0`
- Current interpretation:
  - `cf156af` is useful on live because replay-apply lock is now non-terminal.
  - The next blocker is runtime resource pressure around observed-writer /
    discovery rebuild: OOM risk, saturated pending request bursts, large WAL
    growth, and one remaining terminal SQLite lock in the
    `covered_through` cursor update path.
  - Do not move to selector/scoring fixes. The next coding prompt should stay
    bounded to observed-writer resource guard / covered-through lock
    retryability.

Resource-pressure follow-up as of `2026-04-30T08:26:27Z`:

- Accepted and deployed commit:
  `1309a55` (`Gate discovery on runtime memory pressure`)
- Touched only:
  - `crates/app/src/main.rs`
  - `crates/app/src/observed_swap_writer.rs`
- Implemented behavior:
  - scheduled and catch-up discovery are deferred when runtime memory pressure
    is unsafe, with reason
    `discovery_cycle_deferred_due_to_runtime_memory_pressure`
  - running discovery output is aborted/ignored when runtime memory pressure
    becomes unsafe, with reason
    `discovery_cycle_aborted_due_to_runtime_memory_pressure`
  - catch-up pending is preserved across memory-pressure deferral/abort
  - SQLite busy/locked during discovery scoring covered-through cursor update
    is retryable and non-terminal, with reason
    `observed_swap_writer_discovery_scoring_covered_through_update_sqlite_lock_retryable`
  - covered-through is not advanced on retryable covered-through update lock
  - unknown/non-lock covered-through update errors remain terminal
- Reviewer checks passed:
  `cargo test -j 1 -p copybot-app --bin copybot-app discovery_cycle_deferred_due_to_recent_raw_journal`,
  `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer -- --test-threads=1`,
  `cargo check -j 1 -p copybot-app --bin copybot-app`,
  `rustfmt --check --edition 2021 crates/app/src/main.rs crates/app/src/observed_swap_writer.rs`,
  and `git diff --check -- crates/app/src/main.rs crates/app/src/observed_swap_writer.rs`.
- Production rollout advanced the server checkout to `1309a55`, rebuilt only
  `copybot-app`, and restarted `solana-copy-bot.service`.
- Post-rollout snapshot after the first control window:
  - `solana-copy-bot.service = active`
  - `MainPID = 1611613`
  - `NRestarts = 0`
  - memory current: `2.7G`
  - memory peak: `3.8G`
  - runtime and recent_raw journal tails both reached
    `2026-04-30T08:26:25.274000063Z / 416620061`
  - runtime WAL remained large at `8.3G`
  - memory-pressure defer count: `0`
  - memory-pressure abort count: `0`
  - covered-through retryable lock count: `0`
  - observed-writer terminal failure count: `0`
  - OOM count: `0`
- The resource-pressure guard and covered-through retryable reason are deployed
  but have not yet fired in the first short control window.
- The large runtime WAL remains a separate maintenance/startup-risk seam.

Older historical snapshot follows.

As of `2026-04-28T20:02:16Z`, Stage 3 production discovery truth remains
fail-closed. Raw-history recovery and the one-shot aggregate-scoring
materialization are no longer the active blocker. The aggregate
materialization-gap latch seam was cleared by the frozen-target repair rollout.
The Yellowstone subscribe-open timeout seam was closed by the request-first
subscribe rollout, but production green is still blocked by live raw persistence
freshness: the source stream is receiving updates while the observed-swap writer
is saturated at its pending cap and the `observed_swaps` frontier is stale.

Current interpretation:

- Stage 3 production discovery truth is still red.
- The old long-running raw-history recovery blocker is closed.
- The old long-running `program_history` raw-history backfill lane is no longer
  the active production blocker.
- The aggregate-scoring materialization lane has been unblocked and run on
  production.
- `3ca9a53` cleared the live `materialization_gap_cursor` without fake-clearing
  coverage; aggregate write readiness is now true.
- The old Yellowstone subscribe-open timeout blocker is closed:
  `copybot_yellowstone_source_probe` now uses request-first
  `subscribe_with_request(Some(request))`, and live request-first probe returned
  `first_message_received` in `57ms`.
- Runtime was rebuilt and restarted with `8353a10`
  (`Use request-first Yellowstone subscribe`).
- The new active blocker is live raw persistence freshness after source resume:
  the source stream is receiving messages with `reconnect_count = 0`, but
  `observed_swaps` has not advanced past
  `2026-04-28T19:58:04.292842175Z`.
- Writer telemetry after rollout shows:
  `observed_swap_writer_pending_requests = 3968`,
  `observed_swap_writer_journal_queue_depth_batches = 64`,
  `observed_swap_writer_journal_overflow_depth_batches = 255`, and
  `observed_swap_writer_aggregate_queue_depth_batches = 1`.
- Ingestion telemetry after rollout shows:
  `grpc_message_total = 91896`,
  `grpc_transaction_updates_total = 91840`,
  `reconnect_count = 0`,
  `ws_notifications_dropped = 0`,
  `yellowstone_output_queue_depth = 0`.
- Do not reduce `scoring_window_days` or weaken fail-closed semantics to route
  around this result.

Latest confirmed live snapshot:

- `solana-copy-bot.service = active`
- service restarts: `NRestarts = 1`
- current `MainPID = 1576503`
- server repo and origin include:
  - `a566402` (`Bound scoring prepare market stats`)
  - `1722c36` (`Record aggregate scoring materialization result`)
  - `835b6d5` (`Raise aggregate writer enqueue budget`)
  - `f535503` (`Decouple observed swap writer startup gates`)
  - `3255576` (`Replay aggregate gaps behind coverage cursor`)
  - `f647d91` (`Prioritize aggregate gap repair`)
  - `3ca9a53` (`Freeze aggregate gap repair target`)
  - `8353a10` (`Use request-first Yellowstone subscribe`)
- `backfill_discovery_scoring` and `copybot-app` have both been rebuilt on
  production
- aggregate scoring backfill completed and marked coverage:
  - `covered_since = 2026-04-23T11:30:20.851433259Z`
  - `covered_through = 2026-04-28T11:28:26.020910509Z`
  - `covered_through_slot = 416210014`
  - `covered_through_signature =
    3RYyrPyHBSsve51YzYtiEVgDvCjYNMhWJQZSLyAr4ciJacSE2t2Y6J8wUoobdzP7KRd8zU2dSwHCK1NgPLd8X7ba`
- aggregate readiness after backfill:
  - `backfill_progress = null`
  - `backfill_active = false`
  - `materialization_gap_cursor = null`
  - `scoring_horizon_covered = true`
  - `covered_through_within_runtime_lag = true`
  - `storage_ready_for_runtime_gate = true`
  - remaining readiness blockers are config-only:
    `writes_disabled_by_config`, `reads_disabled_by_config`
- live scoring fact counts:
  - `wallet_scoring_days = 16717`
  - `wallet_scoring_buy_facts = 26593`
  - `wallet_scoring_open_lots = 25111`
  - `wallet_scoring_close_facts = 2123`
  - `wallet_scoring_carryover_lots = 0`
- read-only zero-universe report after aggregate materialization:
  - `publication.reason = raw_window_zero_publishable_universe`
  - `raw_window.persisted_raw_truth_sufficient = false`
  - `raw_window.persisted_raw_truth_reason = raw_window_zero_publishable_universe`
  - `raw_window.wallets_seen = 14351`
  - `persisted_metrics.metrics_rows = 14351`
  - `persisted_metrics.threshold_counts_proven = true`
  - `post_threshold_candidate_wallets = 0`
  - `score_distribution.max_score = 0.2406280107272889`
  - `open_position_distribution.wallets_with_open_position = 11376`
  - `selector_zero_universe_claimed = false`
  - `production_green = false`
- aggregate config rollout attempt:
  - `scoring_aggregates_write_enabled` and `scoring_aggregates_enabled` were
    temporarily set to `true`
  - `solana-copy-bot.service` was restarted and stayed `active`
  - with both flags enabled, aggregate readiness reported
    `writes_enabled = true`, `reads_enabled = true`,
    `covered_through_lag_seconds = 4195`,
    `effective_writes_ready = true`, `effective_reads_ready = false`
  - the read blocker was `covered_through_too_stale_for_runtime_gate`
  - `backfill_discovery_scoring` refused to run while aggregate writes/reads
    were enabled, with:
    `backfill requires discovery.scoring_aggregates_write_enabled=false in the target runtime config`
  - direct read-only SQL showed `observed_swaps` had no rows newer than
    `2026-04-28T11:28:26.020910509Z`
  - service logs showed observed-swap persistence deferral with
    `observed_swap_writer_pending_requests = 128` and aggregate queue depth `0`
- the aggregate config rollout was rolled back to `false` / `false` from
  `/etc/solana-copy-bot/live.server.toml.backup-20260428T114128Z-before-aggregate-enable`
- post-rollback service state:
  - `solana-copy-bot.service = active`
  - `MainPID = 1559057`
  - `NRestarts = 0`
  - aggregate readiness is back to config blockers plus stale coverage:
    `writes_disabled_by_config`, `reads_disabled_by_config`,
    `covered_through_too_stale_for_runtime_gate`
- enqueue-budget fix rollout:
  - commit `835b6d5` was deployed and `copybot-app` was rebuilt
  - local bounded checks passed:
    `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer_try_enqueue`,
    `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer_normal_try_enqueue_soft_limit`,
    `cargo check -j 1 -p copybot-app --bin copybot-app`
  - the fix changed the aggregate-enabled noncritical enqueue plateau from
    `128` to `3968`, preserving the discovery-critical reserve
  - live enablement with the new binary proved that the enqueue cap was raised:
    `observed_swap_writer_pending_requests = 3968`
  - the live run still did not write new raw rows:
    `observed_swap_writer_raw_batch_ms_p95 = 0`,
    `observed_swap_writer_observed_swaps_insert_ms_p95 = 0`
  - code inspection showed the raw writer blocks on downstream startup
    receivers before it processes any raw batch:
    `aggregate_startup_receiver.recv()` and `journal_startup_receiver.recv()`
  - aggregate coverage advanced only to
    `2026-04-28T13:28:52.149924946Z`, then stayed stale
  - aggregate flags were rolled back again to `false` / `false` from
    `/etc/solana-copy-bot/live.server.toml.backup-20260428T-enqueue-budget-before-aggregate-enable`
  - post-rollback service state:
    `solana-copy-bot.service = active`, `MainPID = 1561223`,
    `NRestarts = 0`
- startup-gate fix rollout:
  - commit `f535503` was deployed and `copybot-app` was rebuilt
  - local bounded checks passed:
    `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer`,
    `cargo check -j 1 -p copybot-app --bin copybot-app`
  - live enablement with aggregate flags proved raw persistence now moves:
    `observed_swap_writer_raw_batch_ms_p95` and
    `observed_swap_writer_observed_swaps_insert_ms_p95` became non-zero
  - aggregate coverage became fresh:
    `covered_through = 2026-04-28T15:43:10.093919453Z`,
    `covered_through_lag_seconds = 97`
  - readiness still failed closed with
    `write_blockers = ["materialization_gap_latched"]` and
    `read_blockers = ["materialization_gap_latched"]`
  - latched gap cursor:
    `2026-04-28T15:40:04.136715225Z / 416248013 /
    ZSoPwzqpMLMBypvWdLhf1jJXwaLPJbPcAv9siEED1s8uZTz5RP9mkE9byvJvkwpCkuczkCudjfED4Us3hpTbk3w`
  - code inspection showed current `run_aggregate_gap_replay` starts from
    `covered_through`, so once hot-path aggregate writes advance beyond the
    latched gap, idle replay cannot observe the exact gap row and cannot clear
    the latch honestly
  - aggregate flags were rolled back again to `false` / `false` from
    `/etc/solana-copy-bot/live.server.toml.backup-20260428T-startup-gates-before-aggregate-enable`
  - post-rollback service state:
    `solana-copy-bot.service = active`, `MainPID = 1562846`,
    `NRestarts = 0`
  - with aggregate flags disabled on the new binary, raw observed-swap writes
    still move: `observed_swap_writer_pending_requests = 0`,
    `observed_swap_writer_raw_batch_ms_p95 = 652`
- behind-cursor gap replay rollout:
  - commit `3255576` was deployed and `copybot-app` was rebuilt
  - local bounded checks passed:
    `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer`,
    `cargo check -j 1 -p copybot-app --bin copybot-app`
  - live enablement with aggregate flags proved the exact latched gap row
    exists in `observed_swaps`:
    `2026-04-28T16:06:58.496536802Z / 416252112 /
    2RHt6BMikxRyB2BfgGCADZ9mC6ioYfoPsrfexW2qcyw4mVXPZhvPk46GBStaMXzsvrUMF4K9PnkDNpHSxhEDxpR8`
  - `covered_through` was already past the latch:
    `2026-04-28T16:07:30.401379572Z / 416252192 /
    3fHGZRbKC7p2A2Xdvejx3SbxaSypeXRCtpz5uHczzo55zujZRKSuUd9rQiV7s8NYsyE5cV7oMctUfrUWW5o7iDB8`
  - readiness remained fail-closed with
    `materialization_gap_latched`
  - raw writer pressure returned under aggregate-enabled live traffic:
    `observed_swap_writer_pending_requests = 3968`,
    `observed_swap_writer_journal_queue_depth_batches = 64`,
    `observed_swap_writer_journal_overflow_depth_batches = 255`
  - interpretation: the replay primitive is correct but not sufficient because
    gap repair is still scheduled through idle replay and does not complete
    under continuous live traffic/journal pressure
  - aggregate flags were rolled back again to `false` / `false` from
    `/etc/solana-copy-bot/live.server.toml.backup-20260428T-gap-replay-before-aggregate-enable`
  - post-rollback service state:
    `solana-copy-bot.service = active`, `MainPID = 1564245`,
    `NRestarts = 0`
  - with aggregate flags disabled, raw writes resumed:
    `observed_swap_writer_pending_requests = 0`,
    `observed_swap_writer_raw_batch_ms_p95 = 680`
- prioritized gap-repair rollout:
  - commit `f647d91` was deployed and `copybot-app` was rebuilt
  - local bounded checks passed:
    `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer`,
    `cargo check -j 1 -p copybot-app --bin copybot-app`
  - live enablement kept raw pressure lower initially and aggregate coverage
    fresh:
    `covered_through = 2026-04-28T16:39:40.740669023Z`,
    `covered_through_lag_seconds = 62`
  - readiness still failed closed with
    `materialization_gap_latched`
  - the latched exact row exists in `observed_swaps`:
    `2026-04-28T16:37:26.107417860Z / 416256738 /
    5mRLCtXnhH9cQsZBWyZfdRM99nDNjDieeU6aX13m2tVawZzGZ9w4HUieChRFobxgGHuFh2JeeVUoDyrfy88qC2gz`
  - under continued live load, journal pressure saturated:
    `observed_swap_writer_journal_queue_depth_batches = 64`,
    `observed_swap_writer_journal_overflow_depth_batches = 246`
  - raw pending began growing again:
    `observed_swap_writer_pending_requests = 2739` then `2415`
  - interpretation: gap repair scheduling improved, but the remaining blocker
    is shared observed-swap writer pressure from recent_raw journal backlog
    while aggregate repair remains latched
  - aggregate flags were rolled back again to `false` / `false` from
    `/etc/solana-copy-bot/live.server.toml.backup-20260428T-prioritized-gap-repair-before-aggregate-enable`
  - post-rollback service state:
    `solana-copy-bot.service = active`, `MainPID = 1565708`,
    `NRestarts = 0`
  - with aggregate flags disabled, raw writes resumed:
    `observed_swap_writer_pending_requests = 0`,
    `observed_swap_writer_raw_batch_ms_p95 = 549`
- frozen-target gap-repair rollout:
  - commit `3ca9a53` was deployed and `copybot-app` was rebuilt
  - local bounded checks passed:
    `cargo test -j 1 -p copybot-app --bin copybot-app observed_swap_writer`,
    `cargo check -j 1 -p copybot-app --bin copybot-app`,
    `rustfmt --check --edition 2021 crates/app/src/observed_swap_writer.rs`,
    `git diff --check -- crates/app/src/observed_swap_writer.rs`
  - aggregate flags were enabled from backup
    `/etc/solana-copy-bot/live.server.toml.backup-20260428-frozen-gap-target-before-aggregate-enable-2`
  - `execution.enabled` remained `false`
  - post-rollout service state:
    `solana-copy-bot.service = active`, `MainPID = 1566861`,
    `NRestarts = 0`
  - aggregate readiness after rollout:
    `materialization_gap_cursor = null`,
    `effective_writes_ready = true`,
    `effective_reads_ready = false`,
    `read_blockers = ["covered_through_too_stale_for_runtime_gate"]`,
    `covered_through_lag_seconds = 2401`
  - writer telemetry stayed clean:
    `observed_swap_writer_pending_requests = 0`,
    `observed_swap_writer_journal_queue_depth_batches = 0`,
    `observed_swap_writer_journal_overflow_depth_batches = 0`,
    `observed_swap_writer_aggregate_queue_depth_batches = 0`
  - latest observed raw cursor stayed at
    `2026-04-28T16:42:13.769796192Z / 416257461 /
    3HXJDhf3LWPvEnST8vpq4H1PhzYwnLbHTypoExasJpxfPKDvxcobWJrBMafaxi5pkY3DvX8thm94jTovBZpNHGm`
  - direct SQL showed `0` `observed_swaps` rows at or after
    `2026-04-28T17:00:00Z`
  - logs showed repeated Yellowstone subscription-open timeout failures, and
    the first timeout appeared on the previous PID before this rollout
  - aggregate flags were left enabled because readiness remains fail-closed
    while stale, queues are clean, and the next useful proof is source resume
    behavior
- Yellowstone source probe and request-first rollout:
  - commits `f0c2d43`, `f99f42c`, and `aad8a15` added/corrected read-only
    `copybot_yellowstone_source_probe`
  - operator-only debug binary was built on production; main service was not
    restarted
  - live default result:
    `connect_completed = true`,
    `subscribe_started = true`,
    `subscribe_completed = false`,
    `reason_class = yellowstone_subscription_open_timeout`,
    `production_green = false`
  - docs-parity mode results:
    `transaction-filter`, `slots-only`, `blocks-meta`, and `empty-then-send`
    all completed connect and then timed out at subscribe-open before request
    send
  - terminal gRPC proof from production host showed the QuickNode endpoint,
    token, and add-on were valid:
    `SubscribeReplayInfo` returned `grpc-status = 0`, and raw `/Subscribe`
    returned `HTTP/2 200` plus protobuf bytes when the first request frame was
    sent immediately
  - commit `8353a10` changed runtime and probe to request-first subscribe
    open
  - live request-first probe result:
    `connect_completed = true`,
    `subscribe_completed = true`,
    `subscribe_send_completed = true`,
    `initial_request_sent_during_subscribe_open = true`,
    `first_message_received = true`,
    `reason_class = yellowstone_first_message_received`
  - production release build of `copybot-app` completed in `47m38s`
  - controlled service restart succeeded, then systemd performed one automatic
    fail-closed restart after aggregate-writer startup catch-up hit:
    `failed to open discovery scoring batch transaction: database is locked`
  - post-restart service remained `active`, `MainPID = 1576503`,
    `NRestarts = 1`

Operational reading:

- do not restart old raw-history gap-fill loops for the current blocker
- do not run restore/gap-fill work as the next step unless new raw-history
  evidence appears
- do not mark production green from operator observability alone
- next batch should target the proven observed-swap writer/journal overflow
  freshness seam after source resume; do not go back to provider/add-on,
  raw-history gap-fill, selector thresholds, or `scoring_window_days`

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
- aggregate-scoring stage diagnostics:
  `crates/storage/src/bin/backfill_discovery_scoring.rs`,
  `crates/storage/src/discovery_scoring.rs`
- bounded private scoring prepare market stats:
  `token_market_stats_on_conn` no longer runs lifetime token `MIN(ts)` or
  lifetime `COUNT(DISTINCT wallet_id)` scans in the private aggregate
  materialization path
- observed-swap writer aggregate runtime enablement fixes:
  - enqueue budget raised for aggregate-enabled noncritical writes while
    preserving discovery-critical reserve
  - downstream startup gates no longer block raw `observed_swaps` persistence
  - aggregate gap replay can resume from a latched gap behind
    `covered_through`
  - aggregate repair is prioritized while a latch exists
  - aggregate repair now freezes a bounded target cursor so it can clear an
    observed exact gap without chasing a moving live tail

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
- `backfill_discovery_scoring` requires exact resume cursor semantics after a
  partial run; it refuses non-idempotent replay without `--reset`, seeded reset,
  or exact `--resume-*`
- aggregate-scoring coverage was marked only after `completed_source_exhausted`
  and durable checkpoints; partial probe runs did not mark coverage
- bounded production backfill used `--mark-covered`, but coverage was actually
  written only on full source exhaustion

Deployment status:

- aggregate flags are currently enabled on production, but readiness remains
  fail-closed because `covered_through` is stale while Yellowstone subscription
  opens time out
- do not call this production green until live raw resumes and
  `effective_reads_ready = true` is proven by the read-only readiness operator
- if source resumes and aggregate writer queues/gap degrade again, roll back
  using the latest aggregate-enable config backup and record the exact blocker
- emergency-stop CLI remains a manual safety surface only; it is not a Stage 3
  production-green signal

Current sync status:

- current accepted code commits have reached `origin/main` through `3ca9a53`
- server checkout was advanced to `3ca9a53` during the frozen-target rollout;
  still check `git status` and `git log -1` at session start

## If A New Session Starts Elsewhere

1. Read this file first.
2. Read `ROAD_TO_PRODUCTION_v2.md`.
3. Read the latest addendum in `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`.
4. Check `git status`, latest commits, and whether `origin/main` contains the
   accepted operator files.
5. Continue the worker-reviewer workflow.
6. Do not change the process unless the user explicitly asks for a different
   one.
