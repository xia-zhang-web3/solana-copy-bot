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

1. Stage 1 runtime contract is deployed and live on `0c58aba`:
   - bootstrap/aggregate/backfill no longer block runtime wallet selection
   - `healthy / degraded / fail_closed` modes are propagated through discovery and app
   - publication truth is persisted separately from bootstrap/aggregate recovery truth
   - persisted-stream fallback (`build_wallet_snapshots_from_persisted_stream`) is the active cold-start path when RAM cache is cap-truncated
2. The persisted-stream path correctly engages on live (confirmed in logs: `recomputing discovery snapshots from persisted observed_swaps stream`).
3. The first persisted-stream cycle did not complete in the observed window (6+ minutes).
4. Current working diagnosis from the second rollout: the next blocker is latency / boundedness of the first persisted-stream rebuild on live-size state, not bootstrap, aggregate, or ingestion.

### 2.4 Current verdict (updated 2026-03-17)

The project is not blocked by bootstrap, aggregate, or backfill.

The project is not blocked by ingestion.

The first discovery cycle on live has not completed yet.

Current working diagnosis: the cold-start persisted-stream rebuild is not yet bounded/resumable enough for live-size state.

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

Status: **code complete, deployed, operationally blocked by first-cycle scan latency**

Goal:

Make discovery choose wallets from the current raw window without aggregate scoring gates.

What was done (4 code iterations, 2 server rollouts):

1. Removed startup dependence on `trusted_selection_bootstrap_pending`, aggregate readiness, and persisted trusted snapshots.
2. Made raw-window recompute (`PreparedCycleState::Recompute`) the default runtime selection path.
3. Added `PreparedCycleState::PersistedRecompute` fallback: when RAM cache is cap-truncated but persisted `observed_swaps` covers the scoring window, discovery scores from `build_wallet_snapshots_from_persisted_stream` instead of failing closed.
4. Separated publication truth from bootstrap/aggregate recovery truth in `discovery_strategy_state`.
5. Propagated `healthy / degraded / fail_closed` runtime mode through discovery, app startup, and shadow consumption.
6. Hardened: coverage check requires left-boundary + in-window swap presence; empty-scan guard prevents false healthy; degraded eligible_wallets sourced from last healthy metrics bucket; bucket-stale published universes rejected.

Code hotspots touched:

1. `crates/discovery/src/lib.rs` — runtime branching, persisted-stream path, publication state, tests
2. `crates/app/src/main.rs` — startup boot path, runtime mode consumption, cap-truncation telemetry
3. `crates/app/src/task_spawns.rs` — runtime_mode propagation
4. `crates/storage/src/discovery.rs` — publication truth persistence
5. `crates/storage/src/lib.rs` — publication state types, bucket validity check

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

Remaining operational blocker:

The first persisted-stream discovery cycle has not completed on live in the observed window (6+ minutes).

Current working diagnosis:

the first persisted-stream rebuild is too slow / insufficiently bounded for live-size state and needs a resumable or chunked runtime design.

Immediate next coding step before Stage 2:

implement bounded/resumable persisted-stream rebuild with explicit progress telemetry and checkpointed forward progress across cycles.

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
