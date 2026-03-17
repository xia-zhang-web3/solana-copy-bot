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

1. Discovery wallet selection enters the correct runtime path but the first persisted-stream scoring cycle has not completed on live yet.
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
2. The persisted-stream path correctly engages on live (confirmed in logs: `recomputing discovery snapshots from persisted observed_swaps stream`).
3. The first persisted-stream cycle did not complete in the observed window (6+ minutes).
4. Current working diagnosis from the second rollout: the next blocker is latency / boundedness of the first persisted-stream rebuild on live-size state, not bootstrap, aggregate, or ingestion.
5. Current working tree replaces the old one-shot persisted rebuild with a bounded/resumable design:
   - frozen rebuild horizon (`window_start`, `horizon_end`, `metrics_window_start`) is captured once per rebuild
   - rebuild progress is persisted separately from `discovery_runtime_cursor`
   - cold-start rebuild advances in bounded chunks across cycles and restarts instead of monopolizing one long cycle

### 2.4 Current verdict (updated 2026-03-17)

The project is not blocked by bootstrap, aggregate, or backfill.

The project is not blocked by ingestion.

The first discovery cycle on live has not completed yet.

Stage 1 is `partial` again after live rollout `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`.

Current working diagnosis: live is still running the old unbounded cold-start persisted-stream rebuild from `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`, while the current working tree now contains the bounded/resumable replacement that needs rollout validation.

Do not start Stage 2 yet.

### 2.5 Server state (updated 2026-03-17)

- Deployed commit: `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`
- Service: `solana-copy-bot.service active`, no crash loop
- `execution.enabled = false`
- Discovery enters persisted-stream path on startup but first cycle has not completed in observed window
- `active_follow_wallets = 0` (pending first cycle completion)
- Observed during validation window:
  - `%CPU ≈ 17.5`, `RSS ≈ 121148 KiB`
  - `sqlite_busy_error_total = 0`
  - `yellowstone_output_queue_fill_ratio` mostly 0.0
  - repeated `discovery cycle still running, skipping scheduled trigger` every 60s
  - no `scoring_source = raw_window_persisted_stream` appeared (cycle did not finish)
  - no `shadow_risk_universe_stop` after restart
- Rollout reports:
  - [ops/server_reports/2026-03-17_1758_stage1_discovery_runtime_contract_rollout_report.md](ops/server_reports/2026-03-17_1758_stage1_discovery_runtime_contract_rollout_report.md) — first Stage 1 deploy (`2eb5c30`), confirmed bootstrap path removed but fail_closed due to cap-truncated warm load
  - [ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md](ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md) — persisted-stream follow-up (`0c58aba`), confirmed correct path engaged but first cycle did not complete in 6+ minutes

### 2.6 Live data scale (observed)

- `live_copybot.db`: 116 GB
- `live_copybot.db-wal`: 4.4 GB
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

Status: **partial after live rollout `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`; bounded/resumable rebuild fix is now in code and pending rollout validation**

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
   - `CollectBuyMints` prepass scans the frozen horizon in bounded pages/time and persists a resumable phase cursor
   - `ResolveTokenQuality` resolves token quality for the frozen mint set in bounded chunks with its own resumable progress index and RPC budget telemetry
   - `Replay` replays the same frozen horizon with the same streaming scoring semantics in bounded pages/time and persists a resumable phase cursor plus streaming state payload
   - `PublishPending` keeps the durable checkpoint alive until healthy publication/trusted-state persistence succeeds, so a failed publish resumes from the completed rebuild instead of restarting from zero
8. Added a dedicated durable rebuild progress contract in storage that is explicitly separate from `discovery_runtime_cursor`:
   - `discovery_runtime_cursor` still tracks normal live delta fetch
   - persisted rebuild progress stores its own phase, frozen horizon, checkpoint cursor, processed-row/page counters, chunk count, and serialized replay state
9. Partial no-fallback rebuild cycles now make bounded durable progress without burning the publish cadence; if rebuild is still incomplete and there is no valid published universe, runtime can remain `fail_closed` while the next cycle resumes from the persisted checkpoint.
10. Rebuild completion now forces the recovered publish instead of waiting for the next normal publish interval, so a successful bounded cold start can immediately promote the healthy `raw_window_persisted_stream` universe.
11. Resume validates semantic checkpoint validity before reusing it: if the frozen metrics bucket moved or the stored horizon is invalid for the current wall clock, runtime discards the old rebuild state and starts a new frozen attempt. Longer restarts within the same metrics bucket keep the checkpoint and continue from it.
12. Completion keeps semantic parity with the previous one-shot persisted rebuild by freezing the same horizon and replaying the same streaming scoring logic; parity is enforced by direct one-shot-vs-bounded regression coverage.

Code hotspots touched:

1. `crates/discovery/src/lib.rs` — runtime branching, bounded persisted-stream path, publication cadence handling, tests
2. `crates/storage/src/market_data.rs` — bounded window scan with cursor/budget plus durable rebuild-state persistence
3. `crates/storage/src/lib.rs` — persisted rebuild progress types
4. `crates/core-types/src/lib.rs` — serde support for persisted rebuild payload types

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

Remaining operational blocker:

Live rollout validation is still pending for the new bounded/resumable rebuild.

Current working diagnosis:

the old deploy `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c` proved that a one-shot first-cycle persisted rebuild is too slow / insufficiently bounded for live-size state; the new code changes that path to a checkpointed chunked rebuild that must now be validated on live.

Immediate next operational step before Stage 2:

roll out the bounded/resumable persisted-stream rebuild and confirm:

1. completed discovery cycles resume instead of hanging behind `discovery cycle still running, skipping scheduled trigger`
2. rebuild progress logs show processed rows/pages, chunk count, elapsed time, checkpoint cursor, frozen horizon, and partial/completed outcome
3. live eventually emits a completed cycle with `scoring_source = raw_window_persisted_stream`
4. `active_follow_wallets > 0`
5. there is no false `healthy` and no empty published universe

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
