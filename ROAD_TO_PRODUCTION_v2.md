# ROAD TO PRODUCTION v2

Date: 2026-03-17
Status: Active

## 0. Why v2 exists

МЫ СУКА ОБОССАЛИСЬ С ЭТИМ И УСАДИЛИ ХОЗЯИНА ПРОЕКТА НА ДЕНЬГИ

### What went wrong

1. We put aggregate/backfill recovery into the production critical path even though the actual business need was fresh wallets and current PnL from the active raw window.
2. We treated exact historical aggregate lineage as a must-have runtime dependency instead of an optional offline accounting path.
3. We let discovery wallet selection depend on bootstrap, aggregate readiness, and recovery state instead of making current `observed_swaps` the runtime source of truth.
4. We ran long offline validation attempts on the production host and twice lost host manageability before even reaching committed seed install.
5. We spent time and money validating recovery semantics while live execution was still disabled and wallet selection was still not trustworthy.
6. We let the roadmap and decision surface fragment into too many overlapping documents, which made the real blocker harder to see.
7. We over-invested recent engineering effort into aggregate/backfill hardening instead of restoring production-usable wallet selection.

## 1. Purpose

This is the only production roadmap that should be used from now on.

It replaces the old mixed roadmap and removes aggregate/backfill recovery from the critical path.

## 2. Current factual state

### 2.1 What is already working

1. The main runtime is still `copybot-app`: [crates/app/src/main.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/app/src/main.rs).
2. Live ingestion is already built around Yellowstone gRPC:
   - [configs/live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml)
   - [configs/prod.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/prod.toml)
3. Incoming swaps are persisted into `observed_swaps` in SQLite.
4. Shadow pipeline exists and can process followed wallets.
5. Execution runtime code exists, but is not enabled in current live config.

### 2.2 What is not working (updated 2026-03-17)

1. Discovery wallet selection logic is fixed in code, startup SQLite no longer stalls before runtime, and the bounded persisted rebuild now makes observable forward progress on live; however, the latest live rollout still has not completed Stage 1 to a healthy published universe on cold start.
   - current live runtime remains `fail_closed` because there is no valid recent published universe and the bounded cold-start rebuild has not completed yet
   - `raw_window_persisted_stream` and `active_follow_wallets > 0` are still not confirmed on live
2. Aggregate/backfill recovery is not a safe operational path and is removed from the critical path.
3. Current live config still has:
   - `discovery.scoring_aggregates_write_enabled = false`
   - `discovery.scoring_aggregates_enabled = false`
   - `execution.enabled = false`

### 2.3 What the current code already proves (updated 2026-03-17)

1. Stage 1 runtime contract was deployed and live on `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`:
   - bootstrap/aggregate/backfill no longer block runtime wallet selection
   - `healthy / degraded / fail_closed` modes are propagated through discovery and app
   - publication truth is persisted separately from bootstrap/aggregate recovery truth
   - persisted-stream fallback (`build_wallet_snapshots_from_persisted_stream`) is the active cold-start path when RAM cache is cap-truncated
2. The persisted-stream path correctly engaged on the earlier live rollout (`0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`), which proved the old one-shot cold-start rebuild was the active fallback path.
3. The later rollout on `96606b83880cb1b942de67f61c5ecdb459fe4139` did not reach discovery/runtime logs at all:
   - after restart at `2026-03-17 18:44:55 UTC`, logs showed only `configuration loaded`
   - there was no `sqlite migrations applied`
   - there was no startup WAL checkpoint log
   - there was no `recomputing discovery snapshots from persisted observed_swaps stream`
4. The next rollout on `3fac9afdafbeb3e4ca2c66486124a8683d281f02` validated the startup SQLite follow-up on live:
   - after restart at `2026-03-17 19:46:56 UTC`, startup emitted exact per-stage progress logs and reached `app runtime loop started`
   - startup WAL checkpoint was explicitly `skipped/deferred`
   - there was no silent `active/running` hang after `configuration loaded`
5. The same `3fac9afdafbeb3e4ca2c66486124a8683d281f02` rollout also validated the bounded/resumable persisted rebuild contract on live:
   - runtime resumed an existing persisted rebuild checkpoint instead of restarting from zero
   - rebuild cycles yielded back to the scheduler with bounded partial progress
   - discovery cycles completed while rebuild remained in progress
   - there was no repeated `discovery cycle still running, skipping scheduled trigger`
   - runtime stayed correctly `fail_closed` with `active_follow_wallets = 0` and no false `healthy` because no valid recent published universe existed yet
6. The next rollout on `aed70c91906321e3e80b1a14614454a9db740026` validated the canonical migration parity follow-up on live:
   - after restart at `2026-03-17 20:59:52 UTC`, startup again emitted the full expected stage sequence and reached `app runtime loop started`
   - runtime resumed an existing legacy `CollectBuyMints` checkpoint and emitted the explicit canonical safe-prefix migration log instead of silently continuing the old raw-cursor state
   - once the frozen metrics bucket changed, runtime correctly discarded the stale persisted rebuild state with `restart_reason = metrics_window_start_changed` and started a fresh canonical frozen rebuild
   - the fresh rebuild then advanced in bounded `CollectBuyMints` cycles with a monotonic direct distinct-mint token cursor and completed discovery cycles in between
7. Current live blocker is now narrower:
   - startup SQLite path boundedness / observability is validated on live
   - persisted rebuild boundedness / resumability is validated on live
   - upgrade-path migration / canonical repair is validated on live
   - the remaining blocker is whether a fresh canonical bounded rebuild can reach healthy completion within one metrics bucket on live-size state without a recent published universe
8. Current working tree now lands the chosen fix for that blocker in code:
   - `CollectBuyMints` now persists exact canonical buy-mint membership counts, not only the token cursor / mint vector
   - `metrics_window_start_changed` no longer blindly discards a carry-forwardable canonical rebuild; it now carries forward only the exact mint-membership state through bounded expired-head + new-tail reconciliation
   - bucket-sensitive `ResolveTokenQuality` / `Replay` / `PublishPending` state is reset on rollover, so no stale `healthy` or stale publish can leak across the metrics bucket boundary
   - the runtime contract remains bounded / resumable / observable, but useful canonical progress no longer dies just because the next metrics bucket starts before healthy completion
9. Current working tree still keeps the bounded/resumable persisted rebuild fix intact:
   - frozen rebuild horizon (`window_start`, `horizon_end`, `metrics_window_start`) is captured once per rebuild
   - rebuild progress is persisted separately from `discovery_runtime_cursor`
   - cold-start rebuild advances in bounded chunks across cycles and restarts instead of monopolizing one long cycle
10. Current working tree now also makes the startup SQLite path observable and bounded:
   - startup SQLite bootstrap now emits per-stage `started / waiting / completed / timed_out` progress for connection open, PRAGMAs, and `schema_migrations` bootstrap via the explicit startup bootstrap path
   - startup migrations, heartbeat, alert cursor, and app-loop handoff emit explicit startup progress logs
   - startup WAL checkpoint is removed from the startup critical path and now emits an explicit deferred/skipped outcome instead of blocking startup

### 2.4 Current verdict (updated 2026-03-17)

The project is not blocked by bootstrap, aggregate, or backfill.

The project is not blocked by ingestion.

The startup SQLite silent-hang blocker is no longer the current blocker.

Stage 1 is still `partial` after live rollout `eba671f2215e9114065799be2792262abbb1d2b1`.

Current working diagnosis: startup SQLite observability/boundedness is validated on live, bounded/resumable persisted rebuild behavior is validated on live, canonical migration/repair is validated on live, carry-forward across `metrics_window_start_changed` is validated on live, and the retryable SQLite lock / observed-swap writer restart path is now also validated on live. The old process-restart blocker is therefore closed. The remaining blocker had moved again to later-phase convergence: the rebuild could repeatedly reach `Replay`, but it still had not completed to a healthy `raw_window_persisted_stream` publish before the next metrics bucket rollover. Each rollover correctly carried forward canonical `CollectBuyMints` membership state, but bucket-sensitive `Replay` state was reset for the new target window and healthy publication still had not landed. The current code change targets that exact limiter with a replay-phase throughput split: exact wallet activity/accounting is buffered first, then bounded replay streams only SOL-leg swaps through a dedicated indexed cursor path, and legacy in-progress replay checkpoints are rewound only at the replay-phase boundary onto that new contract. Stage 1 is still not operationally deployable until the next live rollout proves that this replay-phase throughput fix actually lands healthy publication.

Do not start Stage 2 yet.

### 2.5 Server state (updated 2026-03-18)

- Deployed commit: `eba671f2215e9114065799be2792262abbb1d2b1`
- Service: `solana-copy-bot.service active`, stable on the same `MainPID` through the latest observed windows
- `execution.enabled = false`
- Latest observed restart: none on the current deploy (`NRestarts = 0` through `2026-03-18 13:54 UTC`)
- `active_follow_wallets = 0` during the observed validation window
- Observed during validation window:
  - startup emitted exact stage logs for sqlite open / pragmas / schema bootstrap / migrations / heartbeat / app-loop handoff
  - startup reached `app runtime loop started`
  - `startup_sqlite_wal_checkpoint` was explicitly `skipped/deferred`
  - the current deploy did not emit `observed swap writer is no longer running; restarting app to avoid silent stale ingestion`
  - retry counters now grow without process death: by `2026-03-18 13:54 UTC`, live counters reached `sqlite_busy_error_total = 7` and `sqlite_write_retry_total = 7` while `MainPID` remained stable and `NRestarts = 0`
  - Yellowstone output pressure stayed low on the current deploy (`yellowstone_output_queue_depth` mostly `0`, briefly `1`; `yellowstone_output_queue_fill_ratio` near `0.0`), so the old pressure regime did not reappear in the observed windows
  - carry-forward across metrics bucket rollover is now validated repeatedly on live on the current deploy as well: at `12:00 UTC`, `12:30 UTC`, and `13:30 UTC` runtime emitted `carrying forward exact canonical buy-mint membership progress across metrics bucket rollover` and resumed through `reconcile_expired_head` / `reconcile_new_tail` instead of restarting from zero
  - rebuild repeatedly progressed back to `Replay` after rollover reconciliation; by `2026-03-18 13:54 UTC` it had reached `rebuild_chunks_completed = 385`, `rebuild_prepass_rows_processed = 4860355`, and `rebuild_replay_rows_processed = 1944665`
  - there was still no completed `raw_window_persisted_stream` promotion and no `active_follow_wallets > 0` during the observed window
  - runtime stayed `fail_closed` with `scoring_source = raw_window_incomplete_no_recent_published_universe`
  - there was no false `healthy`
  - there was no repeated `discovery cycle still running, skipping scheduled trigger`
  - a secondary operational signal appeared at `2026-03-18 13:07:22 UTC`: `shadow risk infra stop activated` with reason `no_ingestion_progress_for=20m` while Yellowstone queue depth stayed `0`; this is noted for follow-up but is not yet the primary Stage 1 blocker
- Rollout reports:
  - [ops/server_reports/2026-03-17_1758_stage1_discovery_runtime_contract_rollout_report.md](ops/server_reports/2026-03-17_1758_stage1_discovery_runtime_contract_rollout_report.md) — first Stage 1 deploy (`2eb5c30`), confirmed bootstrap path removed but fail_closed due to cap-truncated warm load
  - [ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md](ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md) — persisted-stream follow-up (`0c58aba`), confirmed correct path engaged but first cycle did not complete in 6+ minutes
  - rollout `96606b8` recorded the earlier startup stall before discovery/runtime
  - rollout `3fac9af` validated startup SQLite observability/deferred WAL checkpoint and bounded persisted rebuild progress, but not yet healthy publication
  - rollout `aed70c` validated canonical safe-prefix migration / repair on live and showed fresh canonical `CollectBuyMints` progress after `metrics_window_start_changed`, but still did not yet reach healthy publication during the observed window
  - rollout `52e1e8a` validated carry-forward across metrics bucket rollover on live and showed discovery reaching `Replay`, but exposed a later process-stability blocker: restarts under `database is locked` + Yellowstone output queue saturation before healthy completion
  - rollout `eba671f` validated the retryable-lock writer fix on live: the service stayed up with growing SQLite retry counters, carry-forward still worked, and rebuild repeatedly returned to `Replay`, but healthy publication still did not land

### 2.6 Live data scale (observed)

- `live_copybot.db`: 117 GB
- `live_copybot.db-wal`: 71 GB
- `live_copybot.db-shm`: 101 MB
- `observed_swaps_retention_days`: 7
- `scoring_window_days`: 5
- `max_window_swaps_in_memory`: 100,000

## 3. Production principles

1. Production discovery must run from current raw data, not from offline historical replay.
2. `observed_swaps` over the active scoring window is the runtime source of truth.
3. `wallet_metrics`, trusted snapshots, and follow snapshots are cache/publication artifacts, not recovery state machines.
4. No long offline backfills on the production host.
5. No `seeded-reset`, `seed install`, `covered_since`, or `backfill_progress` dependency in the runtime selection path.
6. Fail-close is allowed only when there is no trustworthy current raw window and no still-valid recent published universe.

## 4. Discovery v2 target

### 4.1 Runtime source of truth

Runtime discovery should compute wallet ranking directly from `observed_swaps` within the scoring window.

Use the existing direct raw-window builder path as the base:

1. [crates/discovery/src/lib.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/lib.rs#L2310)
2. [crates/discovery/src/lib.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/lib.rs#L2357)

### 4.2 What must leave the critical path

These concepts must stop being required for normal runtime selection:

1. aggregate scoring readiness
2. `wallet_scoring_*` tables
3. `covered_since`
4. `covered_through_cursor`
5. `backfill_progress`
6. `seed_boundary_install_*`
7. trusted bootstrap as the mandatory startup gate for normal raw-window operation

### 4.3 New runtime modes

#### Healthy

Conditions:

1. full raw scoring window is present in `observed_swaps`
2. discovery can recompute current ranking directly

Behavior:

1. recompute wallet metrics from raw window
2. publish current top-N
3. update followlist
4. allow shadow/copy progression

#### Degraded

Conditions:

1. raw scoring window is temporarily incomplete or startup warm state is still catching up
2. there is a recent previously published follow universe

Behavior:

1. keep last published follow universe
2. do not rotate wallets yet
3. do not claim fresh strategy evaluation
4. alert operator

#### Fail-closed

Conditions:

1. raw scoring window is unusable
2. there is no valid recent published follow universe

Behavior:

1. clear runtime follow universe
2. stop new strategy progression
3. keep the rest of runtime healthy and observable

## 5. Immediate implementation plan

This section is the code-first work order.

No new roadmap documents are needed before Stage 1 lands in code.

### Stage 1. Remove aggregate recovery from runtime discovery

Status: **partial after live rollout `eba671f2215e9114065799be2792262abbb1d2b1`; startup SQLite boundedness/observability is validated on live, bounded/resumable persisted rebuild is validated on live, canonical migration/repair is validated on live, carry-forward across `metrics_window_start_changed` is validated on live, retryable SQLite lock handling in the observed-swap writer is validated on live, and code now contains a replay-phase throughput fix (`wallet-stats + SOL-leg replay`) for the later-phase completion blocker, but healthy completion still remains pending next live rollout validation**

Goal:

Make discovery choose wallets from the current raw window without aggregate scoring gates.

What is done in Stage 1 so far:

1. Removed startup dependence on `trusted_selection_bootstrap_pending`, aggregate readiness, and persisted trusted snapshots.
2. Made raw-window recompute (`PreparedCycleState::Recompute`) the default runtime selection path.
3. Added `PreparedCycleState::PersistedRecompute` fallback: when RAM cache is cap-truncated but persisted `observed_swaps` covers the scoring window, discovery scores from the persisted stream instead of failing closed.
4. Separated publication truth from bootstrap/aggregate recovery truth in `discovery_strategy_state`.
5. Propagated `healthy / degraded / fail_closed` runtime mode through discovery, app startup, and shadow consumption.
6. Hardened: coverage check requires left-boundary + in-window swap presence; empty-scan guard prevents false healthy; degraded eligible_wallets sourced from last healthy metrics bucket; bucket-stale published universes rejected.
7. Replaced the old one-shot cold-start persisted rebuild with a bounded four-phase design:
   - `CollectBuyMints` prepass now pages the direct distinct SOL-buy mint set instead of replaying the full raw window: it runs under the same page/time budgets, uses `INDEXED BY idx_observed_swaps_token_in_out_ts`, persists its own resumable mint cursor in the rebuild payload, and records exact per-mint buy counts for canonical carry-forward
   - legacy raw-cursor checkpoints are migrated onto the new prepass by recovering the maximal safe lexicographic mint prefix under the covering-index distinct query; any unsafe tail mints are intentionally re-enumerated later in token order so `ResolveTokenQuality` still sees the exact canonical one-shot mint ordering
   - `ResolveTokenQuality` resolves token quality for the frozen mint set in bounded chunks with its own resumable progress index and RPC budget telemetry
   - `Replay` replays the same frozen horizon with the same streaming scoring semantics in bounded pages/time and persists a resumable phase cursor plus streaming state payload
   - `PublishPending` keeps the durable checkpoint alive until healthy publication/trusted-state persistence succeeds, so a failed publish resumes from the completed rebuild instead of restarting from zero
8. Added a dedicated durable rebuild progress contract in storage that is explicitly separate from `discovery_runtime_cursor`:
   - `discovery_runtime_cursor` still tracks normal live delta fetch
   - persisted rebuild progress stores its own phase, frozen horizon, checkpoint cursor, processed-row/page counters, chunk count, and serialized replay state
9. Partial no-fallback rebuild cycles now make bounded durable progress without burning the publish cadence; if rebuild is still incomplete and there is no valid published universe, runtime can remain `fail_closed` while the next cycle resumes from the persisted checkpoint.
10. Rebuild completion now forces the recovered publish instead of waiting for the next normal publish interval, so a successful bounded cold start can immediately promote the healthy `raw_window_persisted_stream` universe.
11. `metrics_window_start_changed` no longer blindly discards a carry-forwardable canonical rebuild:
   - if the rebuild has exact canonical `CollectBuyMints` membership state, runtime carries forward only that state into the new target bucket
   - rollover applies bounded expired-head reconciliation plus bounded new-tail reconciliation before resuming fresh canonical mint discovery on the new target window
   - bucket-sensitive `ResolveTokenQuality` / `Replay` / `PublishPending` state is intentionally reset, so stale publishable snapshots never cross the bucket boundary
   - invalid future horizons still restart fresh, and old checkpoints without exact canonical mint-membership state still conservatively restart instead of pretending they are safe to carry forward
12. Completion keeps semantic parity with the previous one-shot persisted rebuild by freezing the same horizon and replaying the same streaming scoring logic; carry-forward parity is enforced by direct canonical target-window set comparisons plus end-to-end one-shot-vs-bounded regression coverage.
13. Startup SQLite is now observable and bounded before discovery/runtime:
   - the explicit startup SQLite bootstrap path reports exact startup stage progress for connection open, `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, and `schema_migrations` bootstrap
   - startup migrations / heartbeat / alert cursor / app-loop handoff report explicit start/finish/waiting/failure outcomes
   - required startup SQLite stages now treat timeout as a fatal startup outcome: SQLite startup syscalls are not cancellable in-process, so the process aborts explicitly on timeout instead of returning control beside a stuck worker
14. Startup WAL checkpoint is no longer part of the startup critical path:
   - startup explicitly reports the WAL checkpoint as skipped/deferred
   - startup correctness no longer depends on waiting for a checkpoint attempt to finish
15. Live rollout `3fac9afdafbeb3e4ca2c66486124a8683d281f02` validated the new startup/runtime behavior:
   - startup stage telemetry appeared exactly as designed and reached `app runtime loop started`
   - persisted rebuild resumed from its stored checkpoint instead of restarting from zero
   - rebuild yielded bounded progress back to the scheduler and discovery cycles completed while rebuild remained partial
   - runtime stayed correctly `fail_closed` without a false `healthy` because no valid recent published universe existed yet
16. Current code now closes the fresh-bucket-reset blocker in design:
   - exact canonical `CollectBuyMints` progress can now survive `metrics_window_start_changed`
   - later bucket-sensitive phases are rebuilt for the new target window instead of publishing stale truth
   - live no longer needs the entire fresh canonical rebuild to finish inside the same metrics bucket just to avoid losing all useful cold-start progress
17. Live rollout `52e1e8a61612b3e8d95fa808bb25c32a23f39438` validated the carry-forward fix on the real server:
   - rollover no longer reset the rebuild to zero
   - runtime reached `CollectBuyMints`, `ResolveTokenQuality`, and `Replay`
   - discovery cycles kept completing and there was still no false `healthy`
18. The exact process-restart path is now fixed in code:
   - the fatal path was not discovery semantics; it was raw observed-swap persistence
   - retryable `database is locked` failures in the raw observed-swap writer were previously treated as terminal raw-batch failures, which latched writer terminal failure and made the main app loop return `Err` with `observed swap writer is no longer running; restarting app to avoid silent stale ingestion`
   - retryable busy/locked raw-batch failures are now handled as runtime pressure: the writer stays alive, keeps retrying the same batch, and emits explicit retry/recovery telemetry instead of dying
   - irrelevant observed swaps now use deferred non-blocking enqueue with a local pending retry slot in the app loop, so Yellowstone queue pressure no longer has to turn into a process restart just because the writer queue is temporarily saturated
   - bounded rebuild / carry-forward / publication truth contracts remain unchanged: the fix only changes pressure handling and fatal propagation around raw observed-swap persistence
19. Live rollout `eba671f2215e9114065799be2792262abbb1d2b1` validated the restart-path fix on the real server:
   - the service remained on a single `MainPID` with `NRestarts = 0` across repeated observation windows
   - SQLite retry counters increased on live without reviving the old fatal path (`sqlite_busy_error_total = 7`, `sqlite_write_retry_total = 7` by `2026-03-18 13:54 UTC`)
   - carry-forward across `metrics_window_start_changed` continued to work from `Replay`
   - rebuild repeatedly returned to `Replay` after rollover reconciliation, so later-phase convergence is now the visible limiter instead of process death
20. Replay later-phase throughput is now reduced structurally without changing publication truth:
   - the optimized replay contract first buffers exact wallet-level activity/accounting across the frozen window in bounded/resumable chunks
   - once that prepass is complete, later-phase streaming replay reads only SOL-leg swaps through `idx_observed_swaps_sol_leg_ts_slot_signature` instead of replaying every non-SOL row through token/rug/position state
   - `Replay` still freezes the same horizon, uses the same canonical mint set and token-quality cache, and publishes only after the full target-window replay completes
   - legacy in-progress `Replay` checkpoints are repaired onto the new optimized replay contract by rewinding replay-local state only; canonical mint membership and token-quality progress are preserved

Code hotspots touched:

1. `crates/discovery/src/lib.rs` — runtime branching, bounded persisted-stream path, bucket-roll carry-forward contract, publication cadence handling, tests
2. `crates/storage/src/market_data.rs` — bounded window scan with cursor/budget, grouped buy-mint count pages, plus durable rebuild-state persistence
3. `crates/storage/src/lib.rs` — persisted rebuild progress types plus startup SQLite open telemetry/watchdog
4. `crates/storage/src/migrations.rs` — startup open+migration bootstrap path
5. `crates/app/src/main.rs` — startup stage orchestration, deferred WAL checkpoint, app-loop handoff telemetry, non-blocking irrelevant observed-swap backpressure handling
6. `crates/app/src/observed_swap_writer.rs` — retryable raw observed-swap lock handling, writer pressure recovery, tests
7. `crates/core-types/src/lib.rs` — serde support for persisted rebuild payload types

Exit criteria (all closed in code and tests):

1. discovery can publish top-N from current `observed_swaps` — done
2. no aggregate tables are required for runtime wallet selection — done
3. restart does not require offline recovery — done
4. restart with a full raw window and no trusted bootstrap snapshot still publishes from raw data — done

Mandatory Stage 1 tests (all green):

1. cold start + sufficient `observed_swaps` + no trusted snapshot → healthy, top-N published
2. cold start + cap-truncated RAM + complete persisted `observed_swaps` → healthy via persisted stream
3. cold start + incomplete raw window + recent published universe → degraded
4. cold start + stale persisted history + recent published universe → degraded
5. cold start + unusable raw window + no published universe → fail-closed
6. cold start + stale persisted history + no published universe → fail-closed
7. large persisted-stream rebuild does not monopolize a cycle; bounded progress row is persisted and observable
8. bounded rebuild resumes across cycles from its own checkpoint instead of restarting from zero
9. bounded rebuild resumes after process restart from persisted rebuild state
10. bounded rebuild completes to `healthy` with `scoring_source = raw_window_persisted_stream`
11. bounded rebuild matches one-shot persisted-stream scoring semantics across chunk boundaries
12. carry-forward across `metrics_window_start_changed` preserves the exact canonical target-window mint set and does not miss new tail mints that sort before the old token cursor
13. carry-forwarded `CollectBuyMints` resume survives process restart and continues bounded progress instead of resetting from zero
14. publish-pending bucket rollover does not publish stale `healthy`; it resets into a bounded carry-forward rebuild for the new target window
15. cold-start rebuild can still converge to `healthy` after a metrics bucket roll without losing the carried-forward canonical prepass state
16. startup SQLite bootstrap emits explicit stage progress for open / PRAGMA / schema bootstrap / migrations
17. a blocked startup step emits `waiting` progress and then an explicit timeout instead of hanging silently
18. deferred startup WAL checkpoint leaves the store usable and does not block startup
19. retryable `database is locked` during raw observed-swap persistence no longer latches writer terminal failure or forces a process restart when a safe pressure path exists
20. irrelevant observed swaps report bounded backpressure and stay retryable/deduped without blocking the runtime event loop
21. optimized replay preserves one-shot semantics while reducing later-phase heavy replay work to SOL-leg swaps after an exact wallet-activity/accounting prepass
22. optimized replay resumes after process restart and legacy in-progress replay checkpoints rewind only replay-local state onto the new contract

Remaining operational blocker:

Live startup validation is closed. Carry-forward across bucket rollover is also validated on live. The observed-swap writer restart path under retryable SQLite lock contention is now also validated on live. Stage 1 remains partial only because the rebuild still has not completed to healthy publication before later metrics bucket rollovers: live shows `Replay` making bounded progress and then being reset back into bucket-sensitive carry-forward reconciliation for the next target window before `raw_window_persisted_stream` is published. The current code change addresses that exact replay-phase limiter by splitting replay into exact wallet-activity/accounting buffering plus SOL-leg-only later-phase streaming.

Current working diagnosis:

the old deploy `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c` proved that a one-shot first-cycle persisted rebuild was too slow / insufficiently bounded for live-size state; the later deploy `96606b83880cb1b942de67f61c5ecdb459fe4139` exposed an earlier blocker on startup SQLite open/migration boundedness; deploy `3fac9afdafbeb3e4ca2c66486124a8683d281f02` validated both the startup fix and bounded/resumable rebuild behavior on live; deploy `aed70c91906321e3e80b1a14614454a9db740026` proved the next blocker was bucket-boundary reset of a still-incomplete fresh canonical rebuild; deploy `52e1e8a61612b3e8d95fa808bb25c32a23f39438` validated that carry-forward fixed that blocker but exposed a later operational failure mode; and deploy `eba671f2215e9114065799be2792262abbb1d2b1` validated that retryable raw observed-swap `database is locked` events no longer force writer death or process restart under the observed live windows. The remaining blocker then moved again: the rebuild could survive into `Replay` and across rollovers, but `Replay` still had not completed to a healthy `raw_window_persisted_stream` publish before the next metrics bucket rollover reset bucket-sensitive state for the new target window. The current code change selects the throughput branch of that fix: exact wallet-level activity/accounting is buffered first, then later-phase replay streams only SOL-leg swaps under the same bounded/resumable contract.

Immediate next operational step before Stage 2:

validate the replay-phase throughput fix on the next rollout:

1. the new `Replay` contract must complete to a healthy `raw_window_persisted_stream` publish before a later metrics bucket rollover resets bucket-sensitive state again
2. live logs must show the new replay telemetry:
   - exact wallet-activity/accounting progress
   - replay mode = `wallet_stats_then_sol_leg`
   - bounded SOL-leg replay progress after the wallet prepass completes
3. discovery must still complete cycles while the optimized replay path is in progress
4. `active_follow_wallets > 0` must appear once healthy publication lands
5. there is still no false `healthy` and no empty published universe

See section 2.5 for observed server state and section 2.6 for live data scale.

### Stage 2. Stabilize wallet publication contract

Goal:

Make wallet selection understandable and restart-safe.

Work:

1. Define one published follow universe per cycle.
2. Persist last successful published selection timestamp.
3. Persist selection state as publication truth, not bootstrap archaeology.
4. Add explicit `healthy / degraded / fail_closed` runtime visibility.
5. Keep degraded mode bounded and operator-visible.

Exit criteria:

1. after restart the bot either recomputes fresh top-N or clearly stays degraded/fail-closed
2. wallet selection behavior is explainable without bootstrap archaeology

### Stage 3. Validate real wallet freshness

Goal:

Prove that the new selection path produces current wallets again.

Work:

1. compare published top-N against live raw data over several cycles
2. verify followlist churn is real again
3. verify shadow signals appear from current selected wallets
4. do this validation on the live runtime path, not by re-opening aggregate recovery as the blocker

Exit criteria:

1. active follow wallets are current
2. wallet rotation resumes without aggregate recovery

### Stage 4. Controlled execution activation

Goal:

Only after discovery is trustworthy again, move toward real-money execution.

Work:

1. keep `execution.enabled = false` until Stage 3 is done
2. validate adapter/execution path separately
3. enable controlled tiny-live only after wallet selection is healthy

Exit criteria:

1. trustworthy wallet selection is already restored
2. execution is enabled only on top of that restored discovery path

## 6. What is explicitly out of scope

1. further aggregate/backfill recovery work on the production host
2. `seeded-reset` as a production dependency
3. plain/seeded horizon reset as a runtime operating model
4. any more long offline validation runs on prod
5. reopening aggregate recovery as the primary blocker before Stage 1 wallet-selection work is shipped
6. writing replacement planning documents instead of landing the Stage 1 code change

## 7. Operator rules

1. Do not run aggregate backfills on the production host.
2. Do not enable live execution while discovery is still fail-closed or degraded without an explicit bounded policy.
3. Do not use aggregate readiness as a proxy for runtime wallet truth.
4. Treat `observed_swaps` retention as the only discovery data that must stay current for normal operation.
5. Do not spend more prod time validating aggregate semantics while execution is still disabled and wallet selection is still untrusted.

## 8. Execution rollout stages (deferred)

The old `ROAD_TO_PRODUCTION.md` (git HEAD version) contains detailed Stages C.5 through H for execution rollout:

- **Stage C.5** — Devnet dress rehearsal
- **Stage D** — Jito primary route, tip strategy, route fallback policy
- **Stage E** — Live risk enforcement, breakeven sizing, tiny-live limits
- **Stage F** — Staged rollout (dry-run → tiny → limited → standard), KPI gates
- **Stage G** — Controlled live stabilization (7-14 days), reconcile discipline
- **Stage H** — Standard live handover, on-call ownership, runbook completeness

These become relevant after Stage 3 here is done. Until then they are parked.

## 9. Legacy documents removed from active planning

The following documents were intentionally removed from the active working set and should not be recreated as the primary plan:

1. `SELECTION_BOOTSTRAP_REDESIGN_PLAN_2026-03-15.md`
2. `STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md`
3. `TEMP_CONSOLIDATED_AUDIT_2026-03-05.md`
4. `URGENT_DISK_CAPACITY_HOTFIX_2026-03-08.md`
5. `YELLOWSTONE_GRPC_MIGRATION_PLAN.md`

Their useful conclusions are already absorbed here:

1. Yellowstone is already the intended live ingestion source.
2. Bootstrap/control-plane truthfulness mattered, but it is not the active production path anymore.
3. Aggregate recovery is not a safe runtime dependency.
4. Operational incidents on prod must not be repeated just to prove recovery semantics.

## 10. Execution log

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / replay throughput follow-up: zero-progress legacy Replay checkpoint repair`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo test -p copybot-discovery --lib persisted_stream_replay_zero_progress_legacy_checkpoint_upgrades_to_optimized_mode_stage1 -- --nocapture`
  - broader replay/discovery coverage rerun before commit
- Done:
  - fixed the rollout-relevant upgrade edge case where a persisted legacy `Replay` checkpoint with zero local replay progress could bypass repair and still execute the old full-window replay path
  - legacy `Replay` checkpoints now upgrade onto `wallet_stats_then_sol_leg` even when `replay_rows_processed = 0`, `phase_cursor = None`, and replay-local maps are still empty
  - the repair remains narrow:
    - it preserves canonical mint membership
    - it preserves token-quality cache/progress
    - it resets only replay-local state onto the optimized replay contract
  - added a targeted regression for the exact zero-progress legacy replay checkpoint path
- In progress:
  - live rollout validation of the replay throughput fix still remains
- Blocked:
  - Stage 1 is still operationally blocked until a live rollout proves healthy publication lands before later metrics bucket rollover
- Acceptance criteria closed:
  - rollout-relevant legacy replay checkpoints no longer miss the optimized replay path because they happened to persist at the phase boundary before processing the first replay row
- Acceptance criteria remaining:
  - next live rollout must emit a completed healthy discovery cycle with `scoring_source = raw_window_persisted_stream`
  - next live rollout must reach `active_follow_wallets > 0`
  - next live rollout must not regress startup observability, writer stability, or `CollectBuyMints` carry-forward

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / replay-phase throughput fix after stable live replay reached`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `crates/storage/src/lib.rs`
  - `migrations/0039_observed_swaps_sol_leg_ts_index.sql`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo test -p copybot-storage --lib observed_sol_leg_swap_cursor_query_filters_and_resumes_in_order -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_replay_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
  - broader package tests are rerun after this change before commit
- Done:
  - fixed the later-phase blocker exposed by live deploy `eba671f2215e9114065799be2792262abbb1d2b1` with a throughput design instead of a semantic shortcut
  - `Replay` now has two bounded/resumable substeps under one durable contract:
    - exact wallet-activity/accounting buffering across the frozen window
    - later-phase replay of SOL-leg swaps only through `idx_observed_swaps_sol_leg_ts_slot_signature`
  - this keeps publication truth unchanged:
    - no partial replay state is publishable
    - no stale `healthy` is allowed across bucket boundaries
    - final snapshots still come from the same frozen horizon and canonical mint set
  - legacy in-progress `Replay` checkpoints are upgraded safely by rewinding replay-local state only onto the new optimized replay contract while preserving canonical mint membership and token-quality progress
  - added targeted regressions for:
    - SOL-leg cursor paging/resume
    - structural replay work reduction on a large noisy fixture
    - optimized replay restart-resume
    - legacy replay checkpoint repair onto the new contract
- In progress:
  - live rollout validation of the replay throughput fix
- Blocked:
  - Stage 1 is still operationally blocked until the next server rollout proves that healthy publication now lands before a later metrics bucket rollover
- Acceptance criteria closed:
  - replay no longer spends later-phase heavy scoring work on non-SOL rows
  - optimized replay remains bounded and resumable
  - optimized replay preserves one-shot semantic parity and keeps publication truth unchanged
  - legacy replay upgrade path is explicit and tested
- Acceptance criteria remaining:
  - next live rollout must emit a completed healthy discovery cycle with `scoring_source = raw_window_persisted_stream`
  - next live rollout must reach `active_follow_wallets > 0`
  - next live rollout must not regress startup observability, writer stability, or `CollectBuyMints` carry-forward
- Remaining risks:
  - if even the optimized wallet-stats + SOL-leg replay still cannot finish before the next bucket rollover on real live state, the next step will need semantically-valid replay carry-forward rather than more generic throughput tuning
- Next action:
  - deploy this build
  - verify live logs now show `rebuild_replay_mode = wallet_stats_then_sol_leg`, replay wallet-stats progress, SOL-leg replay progress, and eventual healthy completion

- Date: 2026-03-18
- Commit SHA: `eba671f2215e9114065799be2792262abbb1d2b1`
- Stage / substep: `Stage 1 / live rollout validation of retryable-lock restart fix + replay progression`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - service restarted successfully and remained stable with `ActiveState=active`, `SubState=running`, `Result=success`, `ExecMainStatus=0`, `MainPID=17027`, `NRestarts=0`
  - startup again emitted the full expected stage log sequence and reached `app runtime loop started`
  - `startup_sqlite_wal_checkpoint` remained explicitly `skipped/deferred`
  - the old fatal path did not reappear:
    - there was no `observed swap writer is no longer running; restarting app to avoid silent stale ingestion`
    - there was no `status=1/FAILURE`
    - there was no process restart during the observed windows
  - retryable SQLite contention now shows as live counters without process death:
    - `sqlite_busy_error_total` grew to `7`
    - `sqlite_write_retry_total` grew to `7`
    - the same process stayed alive throughout
  - Yellowstone queue pressure stayed low in the observed windows, so the old saturated `yellowstone_output_queue_fill_ratio=1.0` regime did not recur
  - carry-forward across `metrics_window_start_changed` was validated again on the current deploy at `12:00 UTC`, `12:30 UTC`, and `13:30 UTC`
  - rebuild repeatedly progressed back into `Replay`; by `2026-03-18 13:54 UTC` live logs showed:
    - `rebuild_chunks_completed = 385`
    - `rebuild_prepass_rows_processed = 4860355`
    - `rebuild_replay_rows_processed = 1944665`
    - `rebuild_unique_buy_mints = 46326`
  - discovery cycles continued to complete and there was still no false `healthy`
- In progress:
  - healthy completion to `raw_window_persisted_stream`
- Blocked:
  - even though restart-free runtime stability is now validated, live still has not completed to `raw_window_persisted_stream`
  - `Replay` repeatedly remains in progress across later metrics bucket rollovers, after which bucket-sensitive state is reset for the new target window and rebuild returns through carry-forward reconciliation
  - `active_follow_wallets` remains `0`
- Acceptance criteria closed:
  - retryable SQLite lock handling no longer forces the old observed-swap writer restart path on the current live deploy
  - carry-forward still works while the process remains stable
  - discovery cycles continue completing without `discovery cycle still running, skipping scheduled trigger`
- Acceptance criteria remaining:
  - live must still emit a completed healthy discovery cycle with `scoring_source = raw_window_persisted_stream`
  - live must still reach `active_follow_wallets > 0`
  - the later-phase rebuild path must no longer get trapped in rollover-driven `Replay -> carry-forward reconciliation -> Replay` without healthy publication
- Remaining risks:
  - if replay-phase throughput/convergence is the real limiter, the next fix will need to target `Replay` rather than `CollectBuyMints` or writer stability
  - `shadow risk infra stop activated` was observed once with `no_ingestion_progress_for=20m` while Yellowstone queue depth stayed `0`; this is noted as a secondary operational signal but is not yet the primary Stage 1 blocker
- Next action:
  - fix replay-phase completion semantics and/or replay throughput without regressing the now-validated writer stability and carry-forward contracts
  - validate on the next rollout that healthy publication lands before a later rollover resets bucket-sensitive state again

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / runtime stability under retryable SQLite lock contention + Yellowstone output queue saturation`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/app/src/main.rs`
  - `crates/app/src/observed_swap_writer.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo test -p copybot-app --bin copybot-app observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app enqueue_irrelevant_observed_swap_reports_pending_backpressure_without_forgetting_signature -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app observed_swap_writer_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
- Done:
  - identified the exact fatal path from the live `52e1e8a61612b3e8d95fa808bb25c32a23f39438` rollout:
    - raw observed-swap writer treated retryable `database is locked` failures as terminal raw-batch failure
    - that latched writer terminal failure
    - the app loop then deliberately returned `Err` once `observed_swap_writer.ensure_running()` or a write path saw the dead writer, producing the `status=1/FAILURE` restart
  - changed the raw observed-swap writer so retryable busy/locked raw-batch failures are no longer fatal:
    - the writer keeps the batch in-flight
    - logs bounded retry/recovery telemetry
    - resumes normal flow once the lock clears
  - changed irrelevant observed-swap persistence in the app loop from inline commit wait to deferred bounded enqueue:
    - irrelevant swaps now use non-blocking `try_enqueue`
    - if the writer queue is saturated, the app loop keeps a single pending irrelevant swap and retries it on a short interval
    - during that pressure window the runtime event loop keeps servicing discovery / heartbeat / risk / maintenance ticks instead of stalling inside a write await
  - preserved carry-forward / bounded rebuild / publication truth semantics; the fix changes pressure handling only, not discovery scoring or publication semantics
  - added regression coverage proving:
    - retryable raw observed-swap lock no longer causes terminal writer failure
    - irrelevant observed-swap backpressure is surfaced as a pending bounded retry state without forgetting dedupe state or stalling the runtime thread
    - startup and persisted rebuild regressions remain green
- In progress:
  - live rollout validation of the pressure/stability fix on the real server
- Blocked:
  - none in code for the restart-loop blocker; only live validation remains
- Acceptance criteria closed:
  - retryable `database is locked` on the affected raw observed-swap path no longer implies whole-process restart in code
  - Yellowstone/output-queue pressure now has a controlled runtime path instead of forcing a writer-death restart
  - carry-forward and bounded persisted rebuild regressions remain green
  - startup observability / fatal-timeout / deferred-WAL regressions remain green
- Acceptance criteria remaining:
  - next live rollout must confirm the service stays alive through the previously observed `database is locked` + `yellowstone_output_queue_fill_ratio=1.0` regime
  - next live rollout must confirm discovery keeps completing cycles under that pressure
  - next live rollout must still reach healthy completion with `scoring_source = raw_window_persisted_stream`
  - next live rollout must confirm `active_follow_wallets > 0`
- Remaining risks:
  - if live pressure now shifts from process restart to extremely long sustained queue saturation, the next blocker may become throughput/ingestion shedding rather than fatal propagation
  - relevant observed swaps still use the commit-ack path by design; the current fix specifically removes the live fail-closed restart path where swaps are irrelevant and the service previously died before discovery could finish
- Next action:
  - deploy this runtime-pressure fix
  - confirm the service no longer exits with `status=1/FAILURE` after retryable raw observed-swap lock contention
  - confirm discovery still reaches `Replay` / healthy completion without losing carry-forward progress

- Date: 2026-03-18
- Commit SHA: `52e1e8a61612b3e8d95fa808bb25c32a23f39438`
- Stage / substep: `Stage 1 / live rollout validation of carry-forward across metrics buckets`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - service restarted successfully and emitted the full expected startup stage sequence
  - startup again reached `app runtime loop started`
  - `startup_sqlite_wal_checkpoint` remained explicitly `skipped/deferred`
  - one expected conservative restart path was observed from a legacy checkpoint without exact canonical buy-mint membership state
  - after that, rebuild progressed beyond `CollectBuyMints`: live logs showed prepass completion, token-quality completion, and later `rebuild_phase = replay`
  - carry-forward across `metrics_window_start_changed` was validated repeatedly on live; useful canonical membership progress was preserved and runtime resumed through `reconcile_expired_head` / `reconcile_new_tail` instead of restarting from zero
  - a rollover from `Replay` back to bucket-sensitive `CollectBuyMints` reconciliation was also validated as expected, without stale `healthy` publication
  - discovery cycles continued to complete and there was still no false `healthy`
- In progress:
  - healthy completion to `raw_window_persisted_stream`
- Blocked:
  - the process auto-restarted twice before healthy completion
  - immediately before both exits logs showed `database is locked: Error code 5` and Yellowstone output queue saturation (`yellowstone_output_queue_depth=2048`, `yellowstone_output_queue_fill_ratio=1.0`)
- Acceptance criteria closed:
  - carry-forward across metrics bucket rollover is validated on live
  - bucket-boundary progress is no longer lost
  - rebuild can now reach `ResolveTokenQuality` and `Replay` on live
  - stale `healthy` is still prevented
- Acceptance criteria remaining:
  - Stage 1 still needs stable runtime execution without restart loops under live SQLite / ingestion pressure
  - Stage 1 still needs eventual healthy publication with `scoring_source = raw_window_persisted_stream`
  - Stage 1 still needs `active_follow_wallets > 0`
- Remaining risks:
  - SQLite lock contention and/or observed-swap writer backpressure may now be the primary operational blocker rather than rebuild semantics
  - Yellowstone queue saturation can force process instability before discovery finishes the carried-forward rebuild
- Next action:
  - fix the `database is locked` + Yellowstone output queue saturation restart path without regressing the now-validated carry-forward rebuild contract

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / semantically-valid carry-forward across metrics bucket boundary for bounded cold-start rebuild`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app`
  - full `copybot-discovery` and full `copybot-app` were rerun outside sandbox because the quality-cache / alert tests bind localhost fake servers that sandboxed runs cannot open
- Done:
  - kept the bounded/resumable persisted rebuild contract, but changed bucket-roll semantics: `metrics_window_start_changed` no longer blindly discards an exact canonical rebuild
  - `CollectBuyMints` now persists exact per-mint SOL-buy counts alongside the canonical mint set and the direct distinct-mint cursor
  - added bounded carry-forward reconciliation for the new target bucket:
    - expired-head reconciliation subtracts qualifying buys that left the scoring window
    - new-tail reconciliation adds qualifying buys that entered after the previous frozen horizon
    - once reconciliation finishes, fresh canonical mint discovery resumes from the persisted token cursor if the old prepass was still partial
  - bucket-sensitive `ResolveTokenQuality` / `Replay` / `PublishPending` state is reset on rollover, so stale publishable snapshots cannot leak across the bucket boundary
  - publish-pending rollover now returns to bounded carry-forward rebuild instead of publishing stale `healthy`
  - added explicit carry-forward telemetry for `collect_buy_mints_mode`, source/target bounds, and reconciliation cursors
  - added storage coverage for grouped buy-mint count pages
  - added regression coverage proving:
    - exact canonical `CollectBuyMints` progress survives `metrics_window_start_changed`
    - new tail mints that sort before the old token cursor are not missed
    - carried-forward `CollectBuyMints` resumes after restart
    - a carried-forward cold-start rebuild can still converge to `healthy`
    - stale publish-pending snapshots are not published as `healthy` after bucket roll
- In progress:
  - live rollout validation of the new carry-forward contract on the real server
- Blocked:
  - none in code for the old bucket-boundary reset blocker; only live validation remains
- Acceptance criteria closed:
  - exact canonical progress no longer dies just because the next metrics bucket starts before healthy completion
  - bounded/resumable/observable contracts remain intact
  - stale `healthy` publication is still prevented
  - restart-resume and canonical migration/repair regressions remain green
- Acceptance criteria remaining:
  - next live rollout must confirm `metrics_window_start_changed` now carries forward canonical cold-start progress instead of restarting from zero
  - next live rollout must confirm eventual completed discovery cycle with `scoring_source = raw_window_persisted_stream`
  - next live rollout must confirm `active_follow_wallets > 0`
- Remaining risks:
  - legacy pre-existing checkpoints that do not yet contain exact canonical buy-mint membership state still conservatively restart instead of pretending they are safe to carry forward
  - if live later proves a new throughput limiter after the carried-forward `CollectBuyMints` phase, the next blocker will move to a later bounded phase rather than disappear
- Next action:
  - deploy this carry-forward build
  - confirm live logs now show carry-forward / reconciliation telemetry instead of repeated discard-on-bucket-boundary resets
  - confirm the rebuild can bridge bucket rollover and still reach a healthy `raw_window_persisted_stream` publish

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / CollectBuyMints throughput follow-up on bounded cold-start rebuild`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-storage --lib observed_buy_mint_count_query_counts_safe_sorted_prefix_for_cursor_migration -- --nocapture`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-discovery --lib persisted_stream_collect_buy_mints_migrates_legacy_raw_cursor_to_safe_prefix_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_collect_buy_mints_legacy_migration_preserves_canonical_order_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild_repairs_noncanonical_quality_checkpoint_before_resume_stage1 -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app`
  - full `copybot-discovery` and full `copybot-app` were rerun outside sandbox because existing localhost fake-server tests are blocked by sandbox socket restrictions
- Done:
  - replaced the `CollectBuyMints` prepass raw-swap replay scan with direct paged distinct SOL-buy mint extraction, so completion work scales with the frozen mint set instead of the entire raw window
  - forced the prepass query onto `idx_observed_swaps_token_in_out_ts`, avoiding the planner path that preferred `idx_observed_swaps_token_in_ts` and a temp B-tree for distinct mint extraction
  - moved `CollectBuyMints` resume state onto its own payload cursor (`collect_buy_mints_cursor_token`) instead of overloading the replay cursor contract
  - tightened legacy checkpoint migration so the new token-sorted prepass keeps only the maximal safe lexicographic prefix and re-enumerates any unsafe tail mints in canonical order instead of carrying forward a mixed chronological/token-sorted mint vector
  - repaired non-canonical persisted quality/replay checkpoints by rewinding them onto canonical mint order before resume, so post-upgrade parity cannot depend on whichever mint order an older checkpoint happened to serialize
  - expanded rebuild telemetry with `collect_buy_mints` cursor token, cycle unique-mint growth, and per-cycle throughput
  - added regression coverage proving that large noisy windows now complete the mint prepass based on unique buy mints rather than all observed swap rows
  - revalidated the startup-fix regression surface so the fatal-timeout/deferred-WAL startup contract still holds
- In progress:
  - server rollout validation on live-size `observed_swaps`
- Blocked:
  - Stage 1 still needs a live rollout to prove that the new `CollectBuyMints` query path converges fast enough to reach a healthy `raw_window_persisted_stream` publish
- Acceptance criteria closed:
  - startup-fix behavior remains covered in regression tests
  - bounded/resumable rebuild contract remains covered in regression tests
  - `CollectBuyMints` no longer scales its bounded progress with the full raw window on the large noisy fixture
  - legacy in-progress `CollectBuyMints` checkpoints are migrated forward onto a safe sorted prefix and then continue from a persisted token cursor instead of reusing a semantically unsafe mixed-order mint set
- Acceptance criteria remaining:
  - live must confirm materially faster `CollectBuyMints` convergence on the persisted window
  - live must reach a completed healthy discovery cycle with `scoring_source = raw_window_persisted_stream`
  - live must reach `active_follow_wallets > 0`
- Remaining risks:
  - real live convergence still depends on the cardinality of unique buy mints in the frozen window, so rollout validation must confirm that the direct distinct-mint path is fast enough in production
  - the current follow-up does not change replay semantics, so any remaining latency after `CollectBuyMints` would move to a later bounded phase rather than disappear
- Next action:
  - deploy this throughput follow-up
  - confirm live logs move out of `CollectBuyMints` materially faster than before
  - confirm eventual healthy publication with `raw_window_persisted_stream` and `active_follow_wallets > 0`

- Date: 2026-03-17
- Commit SHA: `aed70c91906321e3e80b1a14614454a9db740026`
- Stage / substep: `Stage 1 / live rollout validation of canonical migration parity + fresh canonical rebuild convergence`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - service restarted successfully and remained stable with `ActiveState=active`, `SubState=running`, `Result=success`, `ExecMainStatus=0`, `NRestarts=0`
  - startup again emitted the full expected stage log sequence and reached `app runtime loop started`
  - `startup_sqlite_wal_checkpoint` remained explicitly `skipped/deferred`
  - runtime resumed an existing `CollectBuyMints` rebuild checkpoint and emitted the explicit canonical safe-prefix migration log instead of silently continuing the legacy raw cursor
  - runtime later discarded the stale persisted rebuild state with `restart_reason = metrics_window_start_changed` and started a fresh canonical rebuild from a new frozen metrics bucket
  - the fresh canonical `CollectBuyMints` prepass advanced with a monotonic token cursor (`2AZ... -> 2Kp... -> 2TF...`), bounded unique-mint growth, bounded time-budget yields, and completed discovery cycles in between
  - the later validation slice proved that this fresh canonical rebuild still did not complete before the next metrics bucket boundary: it remained in `CollectBuyMints` up to `rebuild_chunks_completed = 30` / `rebuild_prepass_rows_processed = 11436`, then was discarded at `2026-03-17 21:30:52 UTC` with `restart_reason = metrics_window_start_changed`
  - after the boundary reset, the next fresh canonical rebuild again remained in `CollectBuyMints` during the observed window (`rebuild_chunks_completed = 5`, `rebuild_prepass_rows_processed = 2004`)
  - there was no repeated `discovery cycle still running, skipping scheduled trigger`
  - there was no false `healthy`
- In progress:
  - fresh canonical rebuild completion on live-size state inside one metrics bucket
- Blocked:
  - a fresh canonical rebuild still does not reach `scoring_source = raw_window_persisted_stream` before the next metrics bucket reset
  - a fresh canonical rebuild still does not reach `active_follow_wallets > 0` before the next metrics bucket reset
  - live now proves that the current fresh canonical rebuild remains in `CollectBuyMints` long enough to be discarded by the next `metrics_window_start_changed`
- Acceptance criteria closed:
  - startup SQLite fix remains validated on live
  - bounded/resumable rebuild remains validated on live
  - upgrade-path migration onto canonical mint order is validated on live
  - discovery cycles continue to complete while rebuild is in progress
- Acceptance criteria remaining:
  - a fresh canonical bounded rebuild must complete before the next metrics bucket reset
  - live must converge to a healthy publish with `scoring_source = raw_window_persisted_stream`
  - live must reach `active_follow_wallets > 0`
- Remaining risks:
  - live has now confirmed the risk: if a fresh canonical rebuild cannot finish before the next `metrics_window_start_changed`, runtime keeps discarding partial progress at the bucket boundary and never reaches healthy publication
  - the current bottleneck is still `CollectBuyMints`; later phases are not yet the operational limiter on live
- Next action:
  - implement a code fix for fresh canonical rebuild completion throughput and/or bucket-roll completion semantics, with `CollectBuyMints` as the first target
  - validate on the next rollout that a fresh canonical rebuild exits `CollectBuyMints` and reaches healthy publication before the next metrics bucket reset

- Date: 2026-03-17
- Commit SHA: `3fac9afdafbeb3e4ca2c66486124a8683d281f02`
- Stage / substep: `Stage 1 / live rollout validation of startup SQLite follow-up + bounded persisted rebuild resume`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - service restarted successfully and remained stable with `ActiveState=active`, `SubState=running`, `Result=success`, `ExecMainStatus=0`, `NRestarts=0`
  - startup emitted the full expected stage log sequence after `configuration loaded`
  - startup reached `app runtime loop started`
  - `startup_sqlite_wal_checkpoint` was explicitly `skipped/deferred`
  - the earlier silent startup hang after `configuration loaded` is no longer present on live
  - runtime resumed a persisted rebuild checkpoint from `collect_buy_mints` instead of restarting from zero
  - bounded rebuild progress was visible on live (`rebuild_chunks_completed`, `rebuild_prepass_rows_processed`, phase cursor, elapsed time, page-budget yield)
  - discovery cycles completed while rebuild remained partial
  - there was no repeated `discovery cycle still running, skipping scheduled trigger`
  - there was no false `healthy`
- In progress:
  - cold-start bounded persisted rebuild completion on live-size state without a recent published universe
- Blocked:
  - live did not yet reach `scoring_source = raw_window_persisted_stream`
  - live did not yet reach `active_follow_wallets > 0`
  - observed runtime remained `fail_closed` with `scoring_source = raw_window_incomplete_no_recent_published_universe` while rebuild stayed in `CollectBuyMints`
- Acceptance criteria closed:
  - startup SQLite observability/boundedness fix is validated on live
  - startup no longer hangs silently before discovery/runtime
  - bounded/resumable persisted rebuild behavior is validated on live
  - rebuild makes checkpointed forward progress across cycles on live
  - discovery cycles complete again while rebuild is in progress
- Acceptance criteria remaining:
  - live must converge to a healthy completed publish with `scoring_source = raw_window_persisted_stream`
  - live must reach `active_follow_wallets > 0`
- Remaining risks:
  - current `CollectBuyMints` throughput may require too many bounded cycles to complete on the live-size scoring window
  - with no recent published universe available, runtime remains correctly `fail_closed` until completion, so operational usability is still blocked on rebuild convergence time
- Next action:
  - optimize/shorten wall-clock completion of the bounded cold-start rebuild, especially the `CollectBuyMints` phase
  - deploy the throughput follow-up and confirm live eventually publishes `raw_window_persisted_stream` and reaches `active_follow_wallets > 0`

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / bounded + resumable persisted-stream cold-start rebuild`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/core-types/src/lib.rs`
  - `crates/discovery/src/lib.rs`
  - `crates/discovery/src/quality_cache.rs`
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild`
  - `cargo test -p copybot-discovery --lib cold_start_truncated_in_memory_with_complete_persisted_observed_swaps_publishes_healthy_stage1`
  - `cargo test -p copybot-discovery --lib raw_window`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-storage --lib`
- Done:
  - explicitly recorded that Stage 1 became `partial` again after live rollout `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`
  - replaced the old one-shot persisted rebuild with a bounded four-phase cold-start rebuild (`CollectBuyMints` + `ResolveTokenQuality` + `Replay` + `PublishPending`)
  - froze rebuild horizon per attempt (`window_start`, `horizon_end`, `metrics_window_start`) so every bounded cycle works against the same semantic window
  - introduced a dedicated persisted rebuild progress contract that is separate from normal `discovery_runtime_cursor`
  - persisted rebuild progress now survives both cycle boundaries and full process restarts
  - added progress telemetry for processed rows, processed pages, chunk count, elapsed time, rebuild cursor/checkpoint, frozen horizon, and partial vs completed outcomes
  - bounded token-quality resolution so the pre-replay quality stage can no longer monopolize a cycle on a large unique-mint set
  - resume now invalidates only semantically invalid checkpoints; longer same-bucket restarts keep the persisted rebuild state instead of restarting from zero
  - completion now clears the durable rebuild checkpoint only after publish/trusted-state writes succeed, so a publish failure resumes from `PublishPending` instead of restarting the full rebuild
  - partial no-fallback rebuild cycles no longer burn `last_publish_at`, so the first healthy bounded completion can publish immediately instead of waiting for the next nominal publish tick
  - persisted-stream no-fallback cycles now force followlist deactivation when they report `fail_closed`, while legacy cap-truncation suppression behavior remains unchanged outside that path
  - preserved scoring semantic parity with the old one-shot persisted rebuild; bounded replay uses the same streaming scoring logic and is covered by direct equivalence tests
  - partial no-fallback rebuild cycles remain bounded and `fail_closed` when necessary, but no longer consume publish cadence before the eventual healthy publish
- In progress:
  - rollout validation on live-size `observed_swaps`
- Blocked:
  - production still needs a new rollout to prove that the bounded rebuild completes under live-size state without reintroducing a hanging cycle
- Acceptance criteria closed:
  - cold-start persisted rebuild no longer monopolizes one cycle in code/tests
  - rebuild progress is resumable across cycles and after restart
  - completion path reaches `healthy` with `scoring_source = raw_window_persisted_stream` in regression coverage
  - no-fallback path remains bounded and progress-making instead of hanging
  - semantic equivalence across chunk boundaries is covered in tests
- Acceptance criteria remaining:
  - live must emit completed discovery cycles again instead of endlessly logging `discovery cycle still running, skipping scheduled trigger`
  - live must emit `scoring_source = raw_window_persisted_stream`
  - live must reach `active_follow_wallets > 0`
  - live must avoid both false `healthy` and empty published universe
- Remaining risks:
  - this is still unvalidated against the live-size SQLite state on the server
  - legacy aggregate/bootstrap dead code and warnings remain intentionally out of the Stage 1 runtime fix
- Next action:
  - deploy this bounded rebuild follow-up
  - on rollout, verify rebuild progress logs advance chunk-by-chunk, a completed cycle appears, `scoring_source = raw_window_persisted_stream` appears, `active_follow_wallets > 0`, and no hanging cycle remains

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / startup SQLite boundedness + observability before discovery runtime`
- Status: `done in code; pending server rollout validation`
- Code changed:
  - `crates/app/src/main.rs`
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/migrations.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::inline_startup -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::skipped_inline_startup_step_reports_started_and_skipped -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
  - `cargo test -p copybot-discovery --lib cold_start_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib`
- Done:
  - recorded the latest live fact for deploy `96606b83880cb1b942de67f61c5ecdb459fe4139`: service stayed `active/running` but emitted only `configuration loaded` and never reached discovery/runtime logs
  - localized the current blocker to startup SQLite bootstrap on live-size `live_copybot.db` / `live_copybot.db-wal`, earlier than persisted-stream rebuild validation
  - added explicit startup progress telemetry for config validation, sqlite open, sqlite PRAGMAs, `schema_migrations` bootstrap, migrations scan/apply, startup heartbeat, alert cursor, and app-loop handoff
  - required startup SQLite stages now emit `started / waiting / completed / failed / timed_out`; timeout is enforced as a fatal startup abort because the underlying SQLite startup syscalls are not cancellable in-process
  - removed startup WAL checkpoint from the critical startup path and replaced it with an explicit deferred/skipped startup outcome
  - preserved the bounded/resumable persisted-stream rebuild fix and revalidated its regression suite after the startup work
- In progress:
  - server rollout validation on live-size DB+WAL
- Blocked:
  - none in code; operational validation still required on the server
- Acceptance criteria closed:
  - startup no longer relies on silent sqlite bootstrap calls with no stage visibility
  - a heavy required startup step now produces an explicit progress trail and timeout/failure outcome
  - startup WAL checkpoint no longer blocks the path to discovery/runtime startup
  - bounded/resumable persisted-stream rebuild coverage remains green
- Acceptance criteria remaining:
  - next live rollout must confirm startup reaches discovery/runtime logs or exits with an explicit startup-stage failure on live-size DB+WAL
  - once startup reaches discovery/runtime again, live must separately validate `raw_window_persisted_stream` completion and `active_follow_wallets > 0`
- Remaining risks:
  - the latest server state may still need offline DB/WAL maintenance if `Connection::open` itself exceeds the new startup budget on the 117G/71G live database pair
  - startup WAL checkpoint is now explicitly out of the critical path, so WAL growth still needs separate operational attention after runtime startup is restored
- Next action:
  - deploy this startup SQLite follow-up before any further persisted-stream rebuild validation
  - on rollout, verify exact startup stage logs appear after `configuration loaded`, then verify discovery/runtime logs appear; only after that re-check `scoring_source = raw_window_persisted_stream`

- Date: 2026-03-17
- Commit SHA: `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`
- Stage / substep: `Stage 1 / operational rollout validation of persisted-stream follow-up`
- Status: `partial`
- Code changed:
  - none in this step; this was a server validation of the already-built follow-up artifact
- Tests run:
  - live server rollout validation recorded in `ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md`
- Done:
  - exact follow-up artifact deployed successfully on live
  - service remained stable with `NRestarts = 0`
  - startup entered the correct persisted-stream path (`recomputing discovery snapshots from persisted observed_swaps stream`)
  - no return to bootstrap-only fail-close behavior
  - no false empty `healthy`
  - no `shadow_risk_universe_stop` after restart
- In progress:
  - first persisted-stream rebuild on live-size state
- Blocked:
  - first persisted-stream cycle did not complete within the observed window
  - scheduler emitted repeated `discovery cycle still running, skipping scheduled trigger`
- Acceptance criteria closed:
  - persisted-stream follow-up is deployed and active on live
  - the correct runtime path is engaged under cap-truncated warm load
- Acceptance criteria remaining:
  - cold-start persisted-stream rebuild must become bounded/resumable enough to complete on live-size state
  - live must emit a completed discovery cycle with `scoring_source = raw_window_persisted_stream`
  - live must leave `active_follow_wallets = 0`
- Remaining risks:
  - current operational blocker is latency / boundedness of the first persisted-stream rebuild on live-size state
  - dead aggregate/bootstrap code and warning noise still exist but are not the blocker here
- Next action:
  - do not start Stage 2
  - implement bounded/resumable persisted-stream rebuild with progress telemetry and cycle-level forward progress


- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / auditor follow-up for stale persisted history guard + persisted-stream observability`
- Status: `done`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/app/src/main.rs`
- Tests run:
  - `cargo test -p copybot-discovery cold_start_stale_persisted_history_with_recent_published_universe_enters_degraded_stage1`
  - `cargo test -p copybot-discovery cold_start_stale_persisted_history_without_recent_published_universe_fail_closes_stage1`
  - `cargo test -p copybot-discovery cold_start_truncated_in_memory_with_complete_persisted_observed_swaps_publishes_healthy_stage1`
  - `cargo test -p copybot-app risk_guard_observe_discovery_cycle_persists_cap_truncation_context_for_persisted_stream_scoring`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-app`
- Done:
  - persisted-stream runtime fallback no longer treats `MIN(ts) <= window_start` as sufficient coverage by itself; it now also requires at least one persisted `observed_swaps` row inside the current scoring window
  - stale persisted history can no longer produce a false `healthy` / `raw_window_persisted_stream` cycle with an empty published universe
  - runtime now defensively checks `observed_swaps_loaded` from the persisted stream scan and falls back to `degraded` or `fail_closed` instead of silently publishing an empty healthy universe if the scan returns zero rows
  - persisted-stream recompute now emits explicit start/finish logs around the SQLite stream scan so cold-start work is observable instead of looking hung
  - `raw_window_persisted_stream` is now included in raw-window cap-truncation telemetry propagation for universe-stop risk events
  - new regression coverage proves stale persisted history degrades when a recent published universe exists and fail-closes when it does not
- In progress:
  - none on the Stage 1 runtime path
- Blocked:
  - none
- Acceptance criteria closed:
  - live-like cap-truncated warm load no longer fail-closes when persisted `observed_swaps` covers the scoring window
  - runtime no longer publishes a false healthy empty universe when persisted history is stale outside the scoring window
  - raw-window persisted-stream runtime path remains observable in both discovery logs and risk telemetry
- Acceptance criteria remaining:
  - none for Stage 1 in code; only rollout confirmation remains operationally
- Remaining risks:
  - production still needs post-rollout confirmation that cap-truncated live startup now reaches `healthy` with `scoring_source = raw_window_persisted_stream`
  - aggregate/bootstrap dead code and related warnings remain intentionally parked outside the Stage 1 runtime path
- Next action:
  - deploy this follow-up and confirm live no longer reports false `fail_closed` or false empty-healthy publication under cap pressure or stale persisted history

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / persisted observed_swaps runtime fallback for cap-truncated warm load`
- Status: `done`
- Code changed:
  - `crates/discovery/src/lib.rs`
- Tests run:
  - `cargo test -p copybot-discovery cold_start_truncated_in_memory_with_complete_persisted_observed_swaps_publishes_healthy_stage1`
  - `cargo test -p copybot-discovery cold_start_incomplete_raw_window_with_recent_published_universe_enters_degraded_stage1`
  - `cargo test -p copybot-discovery cold_start_truncated_in_memory_with_incomplete_persisted_observed_swaps_and_no_recent_published_universe_fail_closes_stage1`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-app`
- Done:
  - Stage 1 was only partial after the first server rollout: the new runtime contract was live, but `fail_closed` still triggered because cap-truncated warm load treated incomplete RAM history as missing raw truth
  - normal runtime discovery now falls back to persisted `observed_swaps` when the in-memory cache is truncated but persisted raw history still covers the scoring window
  - persisted-stream recompute uses the existing raw-truth snapshot builder and publishes a normal healthy top-N universe instead of degrading solely because the RAM cache hit its swap cap
  - degraded mode is now reserved for the real fallback case: persisted raw window is still incomplete, but a valid recent published universe exists
  - fail-closed is now reserved for the real terminal case: persisted raw truth is unusable and no valid recent published universe exists
  - discovery summary/telemetry now exposes `raw_window_persisted_stream` as the scoring source for this runtime path
  - new Stage 1 regression coverage proves all three cap-truncated cold-start branches plus the new scoring-source contract
- In progress:
  - none on the Stage 1 runtime path
- Blocked:
  - none
- Acceptance criteria closed:
  - live-like cap-truncated warm load no longer fail-closes when persisted `observed_swaps` covers the scoring window
  - runtime wallet selection no longer depends on fitting the full scoring window inside the RAM cache
  - fail-close remains limited to the absence of usable raw truth plus absence of a valid recent published universe
- Acceptance criteria remaining:
  - none for Stage 1 in code; only rollout confirmation remains operationally
- Remaining risks:
  - this closes the confirmed live blocker in code and tests, but production still needs post-rollout confirmation that the persisted-stream path is the one being exercised under live cap pressure
  - aggregate/bootstrap dead code and related warnings remain intentionally parked outside the Stage 1 runtime path
- Next action:
  - roll out and confirm live discovery leaves `fail_closed` by switching to `healthy` with `scoring_source = raw_window_persisted_stream` under cap-truncated warm load

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / auditor follow-up hardening for degraded universe counts + bucket-valid published fallback`
- Status: `done`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/app/src/main.rs`
  - `crates/storage/src/discovery.rs`
  - `crates/storage/src/lib.rs`
- Tests run:
  - `cargo test -p copybot-storage recent_published_follow_wallets_rejects_bucket_stale_published_universe`
  - `cargo test -p copybot-discovery cold_start_incomplete_raw_window_with_recent_published_universe_enters_degraded_stage1`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-app`
- Done:
  - degraded runtime no longer self-references `eligible_wallets` to the active follow set; it now reconstructs the last healthy eligible pool from the published `wallet_metrics` bucket
  - cold-start/runtime fallback to the published universe now requires both wall-clock recency and bucket-valid `last_published_window_start`
  - live/prod config was re-verified to keep `observed_swaps_retention_days >= scoring_window_days`, so the short-retention permanent-degraded finding is not a current production blocker
  - Stage 1 degraded regression coverage now proves `active_follow_wallets = 15` can coexist with `eligible_wallets = 80` from the last published universe
- In progress:
  - none on Stage 1 runtime path
- Blocked:
  - none
- Acceptance criteria closed:
  - discovery publishes top-N from current `observed_swaps`
  - aggregate tables are no longer required for runtime wallet selection
  - cold start no longer requires offline trusted-bootstrap recovery
  - cold start with full raw window and no trusted bootstrap snapshot publishes from raw data
  - degraded mode keeps the last published universe without silently turning it into a false universe-breach condition
  - bucket-stale published universes are no longer accepted as valid startup/runtime fallback truth
- Acceptance criteria remaining:
  - none for Stage 1
- Remaining risks:
  - `recent_published_follow_wallets` still reads publication state and followlist in separate queries; the practical race is low because discovery mutates followlist single-threadedly, but the helper is not a transactional snapshot API
  - telemetry still has at least one stale legacy reason string (`trusted_selection_unavailable`) outside the Stage 1 correctness path
  - aggregate/watchdog dead code and related warnings remain and should be removed or explicitly parked before Stage 2 cleanup
- Next action:
  - clear remaining Stage 1 cleanup debt, then move to Stage 2 publication-contract hardening with the degraded/healthy/fail_closed contract already enforced

- Date: 2026-03-17
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / raw-window primary runtime selection + publication truth startup contract`
- Status: `done`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/app/src/main.rs`
  - `crates/app/src/task_spawns.rs`
  - `crates/storage/src/discovery.rs`
  - `crates/storage/src/lib.rs`
- Tests run:
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib`
  - `cargo test -p copybot-app`
- Done:
  - raw-window recompute is the default runtime publish path again
  - aggregate/bootstrap/backfill no longer block normal runtime wallet selection
  - cold start no longer requires trusted bootstrap restoration before raw-window publish
  - runtime degraded fallback now uses the recent published follow universe instead of persisted trusted snapshot bootstrap
  - explicit `healthy / degraded / fail_closed` runtime mode is propagated through discovery and app startup/runtime consumption
  - publication truth is persisted separately from trusted/bootstrap recovery truth inside discovery strategy state
  - followlist mutation is tied to publish cadence so the persisted active set remains the last published universe
  - mandatory Stage 1 scenarios were added and are green in `copybot-discovery`
- In progress:
  - none on Stage 1 runtime path
- Blocked:
  - none
- Acceptance criteria closed:
  - discovery publishes top-N from current `observed_swaps`
  - aggregate tables are no longer required for runtime wallet selection
  - cold start no longer requires offline trusted-bootstrap recovery
  - cold start with full raw window and no trusted bootstrap snapshot publishes from raw data
- Acceptance criteria remaining:
  - none for Stage 1
- Remaining risks:
  - some legacy aggregate/bootstrap/watchdog tests are intentionally ignored because they assert the superseded pre-Stage-1 contract
  - aggregate/watchdog dead code and related warnings remain and should be removed or explicitly parked before Stage 2 cleanup
- Next action:
  - trim or quarantine obsolete aggregate/bootstrap runtime code paths and then move to Stage 2 publication contract hardening
