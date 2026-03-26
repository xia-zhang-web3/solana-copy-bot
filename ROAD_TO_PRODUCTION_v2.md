# ROAD TO PRODUCTION v2

Date: 2026-03-17
Status: Active historical roadmap with 2026-03-24 live bootstrap-degraded addendum

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

This remains the primary historical roadmap for Stage 1 runtime findings,
server investigations, and code-validation history.

As of 2026-03-23, it is no longer the standalone runtime-restore plan.

For the current runtime-restore contract and incident-response direction, use
[`DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`](/Users/blacktower/Documents/solana-copy-bot/DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md).

What remains valid in this document:

- the Stage 1 runtime contract findings and code-level conclusions
- the startup / SQLite / persisted-stream investigation history
- the operational safety findings about host pressure and clone strategy
- the proof that aggregate/backfill must not sit in the production boot path

What is no longer valid as the current runtime-recovery plan:

- reading this file as an instruction to keep pushing long-running aggregate /
  backfill replay until runtime comes back
- treating multi-hour replay with multi-day ETA as an acceptable restore path
- treating the bounded seeded replay loop as the chosen closure path for the
  current runtime incident

This addendum exists because the old wording in section `2.4 Current verdict`
can otherwise mislead a reader into thinking the historical aggregate replay
investigation is still the active restore plan, which it is not.

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

### 2.3 What the current code already proves (updated 2026-03-22)

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
9. Current working tree now also lands the next stale-reconcile convergence fix in code:
   - stale grouped-delta `reconcile_expired_head` / `reconcile_new_tail` no longer rebuild the entire `unique_buy_mints` vector from `buy_mint_counts` on every grouped page
   - `buy_mint_counts` remains authoritative, while `unique_buy_mints` is now maintained incrementally in exact canonical sorted order as counts are added/removed
   - this removes the per-page `O(total unique mints)` rebuild cost from stale reconcile, keeps exact carry-forward truth available after every bounded page, and does so without adding any new startup-critical migration or storage dependency
10. Current working tree now also narrows the remaining stale expired-head SQL page contract:
   - stale `reconcile_expired_head` now discovers exact candidate mint batches directly from the expired delta window, instead of walking the whole carried-forward canonical membership set just to find which mints were touched by the expired head
   - those exact expired-head candidate batches are persisted and then drained through capped exact count sub-batches, so bounded cycles keep making deterministic progress toward the next carry-forward checkpoint without reopening the same expired-head work
11. Current working tree still keeps the bounded/resumable persisted rebuild fix intact:
   - frozen rebuild horizon (`window_start`, `horizon_end`, `metrics_window_start`) is captured once per rebuild
   - rebuild progress is persisted separately from `discovery_runtime_cursor`
   - cold-start rebuild advances in bounded chunks across cycles and restarts instead of monopolizing one long cycle
12. Current working tree now also makes the startup SQLite path observable and bounded:
   - startup SQLite bootstrap now emits per-stage `started / waiting / completed / timed_out` progress for connection open, PRAGMAs, and `schema_migrations` bootstrap via the explicit startup bootstrap path
   - startup migrations, heartbeat, alert cursor, and app-loop handoff emit explicit startup progress logs
   - startup WAL checkpoint is removed from the startup critical path and now emits an explicit deferred/skipped outcome instead of blocking startup
13. Current working tree now also closes the exact seeded-boundary durability gap in the offline aggregate tool (`8e748dc`):
   - pre-seed `boundary_build` no longer relies on an in-memory-only lot builder until `seed_boundary_installed`
   - boundary replay is forced through the checkpointable SQL path, so `backfill_progress` can be persisted before seed install
   - seed snapshot export now comes from already materialized SQL state instead of only from transient builder memory
   - targeted regression coverage exists for checkpoint-before-seed-install and crash-before-seed-install resume behavior
14. Current working tree now also removes the offline-tool startup confounder that masked that fix on the server (`777a1c8`):
   - `backfill_discovery_scoring` now uses the same startup-safe SQLite bootstrap contract as app/runtime instead of raw `run_migrations(...)`
   - pending optional `0039_observed_swaps_sol_leg_ts_index.sql` is explicitly deferred during offline backfill validation instead of stalling before the first boundary log
   - the tool now emits early startup telemetry (`backfill_tool_start`, `sqlite_startup_progress`, `backfill_sqlite_bootstrap_complete`) so an "empty log" no longer hides where execution stopped
15. Later stopped-host validation on the existing offline clone proved the checkpointable boundary SQL path on real data:
   - `8fdffd9` first emitted durable `event=checkpoint_persisted`, `event=batch_committed phase=boundary_build`, and `summary outcome=stopped_due_to_batch_budget`
   - `7b6ab59` then moved the same path to explicit `boundary_lot_sql`, drove `prepare_ms` down to `0` during pre-seed bounded slices, and advanced the persisted cursor on every server-validated resume slice
   - the bounded near-boundary chain reached the exact boundary-adjacent persisted cursor `2026-03-08T22:15:51.242661287Z | 405123180 | 3AbrgKcriCUKPrdpztgnm1jj2BxfRPkP6xA6sjeReL1AtSBKghNKkZjTvzLdMtVWHxpzgWX5m7HLChDcXpwia5nL`
16. Commit `edf90a7caa4e455ca0f3e46d8bdeb3148d8fee02` then validated seeded install end-to-end on the real clone:
   - `event=seed_boundary_exported`, `event=seed_boundary_installed`, and `event=seed_boundary_stop_requested` were all observed on `--stop-after-seed-install`
   - the durable install marker was written with `seed_lot_count = 2011931`
   - the follow-up normal seeded resume emitted `event=seed_boundary_resume_from_persisted_progress` and advanced post-seed replay to `2026-03-08T22:17:02.935800892Z | 405123360 | 5G2cGVUFuDDDPXCuqpaUaeVfUQXxNWE6xxeoX1NCEwgzfycTTYaJDiUYNW5UuL3zqD2j2qWuk3FUVMG1AkbsUC3x`
17. Commit `87a705296e3643f64cca4d7c3797c68b89b0bca4` was a useful post-seed dead end:
   - bounded `replay_after_seed` was switched to the replay builder by policy
   - local tests immediately diverged from the old SQL-path expectations
   - stopped-host validation on the same clone then timed out before the first `builder_replay_ready` / `checkpoint_persisted` / `summary` for `phase=replay_after_seed`
18. Current working tree now targets the same narrowed blocker with a safer bootstrap:
   - bounded `replay_after_seed` still attempts the replay builder after committed seed install
   - but builder bootstrap no longer needs to preload all `wallet_scoring_open_lots` for that path
   - open lots are now loaded lazily per touched `wallet_id + token`, with the goal of preserving builder replay semantics without reintroducing the full-table bootstrap stall on the real clone
19. Commit `c3e057fc504e1941e385ed25c872fa8f0722ac44` then validated that lazy bootstrap on the real clone:
   - bounded `replay_after_seed` repeatedly emitted `event=builder_replay_ready`, `event=checkpoint_persisted`, and builder `event=batch_committed`
   - repeated bounded chains advanced the persisted cursor from `2026-03-08T22:17:02.935800892Z | 405123360 | 5G2cGVUFuDDDPXCuqpaUaeVfUQXxNWE6xxeoX1NCEwgzfycTTYaJDiUYNW5UuL3zqD2j2qWuk3FUVMG1AkbsUC3x` to `2026-03-08T22:56:27.923141336Z | 405129333 | 3o16UWsam85kXYhsj594snQNaKSqpPWoHFb8bdMCuFHkDfnPnTi1gbCaKHvmMae36QBkZDoxo8jVx4cqE1MuBaGf`
   - the remaining dominant cost is no longer builder bootstrap or SQL prepare; it is the expensive final rug-finalize tail on bounded post-seed runs
20. Commit `90ada0d0fe4f8603d1f708bd570a12be662633ba` then validated that newest narrowed blocker on the real clone:
   - bounded builder replay after seed now explicitly defers `final_rug_finalize` at `run_complete`
   - the defer is limited to incomplete bounded `replay_after_seed` builder runs, so full completion and non-builder paths keep the old finalize contract
   - stopped-host validation emitted `event=final_rug_finalize_skipped phase=run_complete reason=bounded_post_seed_builder_path`, emitted no final rug-finalize event, and advanced the persisted cursor again to `2026-03-08T23:00:45.980276955Z | 405129982 | 24yuNmrtkJ8JNPhxC2FoGT4QfA2SzWX2TyzukE5MqQske7ABNZqF289ua36A8DiQyRpaNtbwGevviyvJQ1tnevGz`
21. Commit `ae688b7fb84cead93185f6f5bbd50ac32f59f452` then operationalized that confirmed path into one bounded seeded loop:
   - `tools/discovery_aggregate_backfill_loop.sh` now preserves `seeded-reset` across chained bounded runs and can enforce an outer timeout per slice
   - the first stopped-host wrapper validation on the same clone ran `24` bounded slices with `mode = seeded-reset`, ended at `summary verdict = max_runs_exhausted`, and advanced the persisted cursor from `2026-03-08T23:00:45.980276955Z | 405129982 | 24yuNmrtkJ8JNPhxC2FoGT4QfA2SzWX2TyzukE5MqQske7ABNZqF289ua36A8DiQyRpaNtbwGevviyvJQ1tnevGz` to `2026-03-09T00:30:25.487279814Z | 405143661 | sAKJBVmLgfR5g9dMwTViWTSFqc4u5or33rhZwJBYHiiZ8v7jGBfZMZNWY5aQeVXNnf4g93pyPTVTRHKWYWD1yP6`
   - every wrapper slice emitted `event=seed_boundary_resume_from_persisted_progress`, `event=builder_replay_ready`, durable builder `checkpoint_persisted` / `batch_committed`, `event=final_rug_finalize_skipped phase=run_complete reason=bounded_post_seed_builder_path`, and `summary outcome=stopped_due_to_runtime_budget`
22. Current working tree now targets the remaining operator gap rather than another replay semantic gap:
   - the bounded seeded path is now code-proven through boundary install, post-seed builder replay, and deferred final finalize
   - the remaining risk is manual operator orchestration, not the replay contract itself
   - the loop wrapper keeps the persisted SQLite cursor as the single source of truth without manual cursor handoff

### 2.4 Current verdict (updated 2026-03-24)

The project is no longer on the dead long-running replay path.

The startup SQLite silent-hang blocker is no longer the current blocker.

Stage 1 is still `partial`.

The historical aggregate / backfill investigation below remains useful as
evidence and postmortem material, but it is no longer the chosen runtime
restore path for the current incident.

The current conclusions are:

1. giant replay remains dead as a runtime-restore path:
   - if a recovery path has already spent roughly `9.5` hours and still
     projects remaining time on the order of `14` days, that path is dead for
     runtime restore
   - it may still contain useful tooling evidence, but it is not an incident
     closure plan
2. the valid runtime-restore direction now lives in
   [`DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`](/Users/blacktower/Documents/solana-copy-bot/DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md):
   - broken runtime DB is disposable
   - restore truth is external to runtime DB
   - restore must come from `runtime artifact` + `recent raw journal`
   - stale publication truth must not silently turn into trading-ready truth
3. that new restore stack is now actually deployed on the live server:
   - runtime repo / live build is now on commit `d2a6253`
   - `solana-copy-bot.service` is running again
   - live discovery is no longer stuck at `active_follow_wallets = 0`
4. the live server is still not healthy:
   - current runtime state is `bootstrap_degraded_publication_truth`
   - `runtime_mode = bootstrap_degraded`
   - `scoring_source = bootstrap_degraded_publication_truth_raw_window_degraded`
   - `active_follow_wallets = 15`
   - `execution.enabled = false`
   - copy trading / shadow trading remain fail-closed; this is not a trading-ready recovery
5. the new restore chain is proven operationally on the server:
   - live `runtime artifact` baseline exists
   - live `recent raw journal` sidecar exists
   - a fresh-DB restore drill from those surfaces completed successfully
   - live was then cut over onto a new fresh runtime DB restored from those surfaces
   - the old `live_copybot.db` was removed from the active path and then deleted from disk
   - the resulting live verdict is still only `bootstrap_degraded`, not `healthy`
6. what remains open right now:
   - runtime has not yet exited from `bootstrap_degraded` to `healthy`
   - raw coverage is still insufficient for a real trading-ready restore
   - address-scoped bounded gap-fill has now been tested against both the
     current QuickNode path and a separate Helius-specific path and still did
     not close the missing recent-raw window
   - QuickNode-first program-history validation no longer dies instantly in
     `429`; `Phase A / Phase B` validation is now code-proven and live-proven:
     - `phase_a` returned `viable_enough_for_phase_b`
     - `phase_b --max-slots-to-scan 1081575` returned `viable`
     - therefore QuickNode program-history source viability is no longer the
       main uncertainty
   - the new production `program-scoped gap-fill` path is now deployed too:
     - live config includes `[program_history_gap_fill]`
     - the first live run exposed an operator-path bug:
       relative `output_dir` resolved under `/etc/solana-copy-bot`, not under
       `/var/www/solana-copy-bot`
     - after fixing that live config path to an absolute state path, the tool
       now returns its own bounded terminal JSON and persists resumable
       `in_progress.*` state on the server
     - however, the current practical tuning is still not enough for closure:
       bounded live attempts are making forward progress, but only under
       `not_proven_due_to_cost_budget`, with no replayable output yet
     - so the current blocker is no longer source validation or snapshot
       durability; it is practical completion / throughput of the real
       program-history gap-fill run
7. what remains valid from the aggregate investigation in this file:
   - same-host hot clone under live load is unsafe
   - stopped-host / offline investigation is operationally safe
   - the seeded-boundary and startup-tooling fixes are real code findings
   - the aggregate tooling notes below remain valid as historical debug context
8. what is no longer valid as current runtime plan:
   - continuing the bounded seeded replay loop until readiness as the chosen
     runtime restore path
   - treating manual cursor-by-cursor replay orchestration as acceptable
     incident closure
   - treating multi-hour / multi-day replay as “good enough for now” restore

Recommended operational posture now:

1. keep the bot running only in the current safe `bootstrap_degraded` posture
2. keep `execution.enabled = false`
3. do not resume the old long-running replay as the runtime restore path
4. do not call the current server state “recovered” in the healthy sense
5. use this file for Stage 1 findings and investigation history
6. use `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md` for the active restore
   contract and the remaining incident-closure work

### 2.5 Server state (updated 2026-03-25)

- Live runtime repo / deployed build: `d2a6253`
- Historical stopped-host investigation checkout preserved below:
  - old offline tooling checkpoint before the last investigation:
    `02f887a3a37ad57cf09578c9105d1f11d08744d8`
  - latest stopped-host aggregate validation checkout:
    `ae688b7fb84cead93185f6f5bbd50ac32f59f452`
- Current live host config relevant to restore:
  - `scoring_window_days = 5`
  - `metric_snapshot_interval_seconds = 3600`
  - `scoring_aggregates_write_enabled = false`
  - `scoring_aggregates_enabled = false`
  - `execution.enabled = false`
  - live config paths were converted to absolute state paths during rollout
- Current live service state:
  - `solana-copy-bot.service -> active`
  - `copybot-adapter.service -> active`
  - `copybot-executor.service -> active`
- Current live runtime DB:
  - active runtime DB path is now
    `/var/www/solana-copy-bot/state/live_runtime_20260324T134339Z.db`
  - legacy `/var/www/solana-copy-bot/state/live_copybot.db` was deleted from disk
- Current live discovery state:
  - `runtime_state = bootstrap_degraded_publication_truth`
  - `runtime_mode = bootstrap_degraded`
  - `scoring_source = bootstrap_degraded_publication_truth_raw_window_degraded`
  - `active_follow_wallets = 15`
  - `published_wallet_count = 15`
  - the current bootstrap universe was manually bridged from the latest
    `wallet_metrics` top-15 snapshot at
    `last_published_window_start = 2026-03-19T12:00:00Z`
- Current live restore surfaces:
  - runtime artifact baseline exists at
    `/var/www/solana-copy-bot/state/discovery_restore/artifacts/latest.json`
  - recent raw journal sidecar exists at
    `/var/www/solana-copy-bot/state/discovery_recent_raw.db`
  - recent raw snapshot baseline exists at
    `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/latest.sqlite`
  - latest successful recent-raw snapshot on the server is now:
    - `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/discovery_recent_raw_20260324T200613Z.sqlite`
    - `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/discovery_recent_raw_20260324T200613Z.json`
- Current timer state:
  - `copybot-discovery-runtime-export.timer -> active/enabled`
  - `copybot-discovery-recent-raw-snapshot.timer -> active/enabled`
  - runtime artifact export is already running on cadence
  - recent raw snapshot timer was returned to `enabled` after the next rollout
    completed a full fresh large-journal snapshot successfully
- Current provider posture:
  - QuickNode remains the active production path in live config
  - `[recent_raw_gap_fill]` in `/etc/solana-copy-bot/live.server.toml` still
    points at the existing QuickNode HTTP endpoint
  - `[program_history_validation]` now also has explicit QuickNode pacing knobs:
    - `max_requests_per_second = 60`
    - `retry_429_max_attempts = 6`
    - `retry_429_backoff_ms = 500`
  - `[program_history_gap_fill]` is also live in the same config
  - its `output_dir` had to be corrected on the server to the absolute path:
    `/var/www/solana-copy-bot/state/discovery_restore/gap_fill_program_history`
  - no Helius endpoint was left active in live config
  - Helius was tested only via explicit CLI override / separate bin runs and
    was not adopted as the active server contract
- Current server-side restore drill result:
  - fresh DB restore from live artifact + live recent raw snapshot succeeds
  - `journal_available = true`
  - `journal_replayed = true`
  - `journal_covers_artifact_cursor = true`
  - the later live cutover replayed `534244` rows into the fresh runtime DB
  - `raw_coverage_satisfied = false`
  - final verdict = `bootstrap_degraded`
- Current bounded gap-fill findings on the real server:
  - required missing window remained:
    `2026-03-19T13:43:40.230377748Z -> 2026-03-24T12:07:11.775090344Z`
  - generic address-scoped gap-fill on the live QuickNode path produced:
    - `scanned_signatures = 63000`
    - `fetched_rows = 95`
    - `inserted_rows = 95`
    - `gap_fill_covered_since = 2026-03-19T13:43:51Z`
    - `gap_fill_covered_through_cursor = 2026-03-21T09:01:20Z`
    - `sufficient_for_healthy_restore = false`
  - separate Helius-specific gap-fill using
    `discovery_raw_gap_fill_helius` and `getTransactionsForAddress`
    produced:
    - `scanned_items = 46071`
    - `scanned_pages = 485`
    - `fetched_rows = 95`
    - `inserted_rows = 95`
    - the same `gap_fill_covered_since` and
      `gap_fill_covered_through_cursor`
    - `sufficient_for_healthy_restore = false`
  - conclusion: provider swap / address-history fetch strategy did not improve
    coverage on the real incident window
- Current program-history validation finding on the real server after
  throttling adaptation:
  - `discovery_program_history_source_validate` no longer failed immediately
    with QuickNode `429`
  - the same live missing window was rerun with local pacing / retry knobs
    capped below the QuickNode `125 req/s` ceiling
  - the process stayed alive and kept making progress, but still did not emit a
    terminal JSON verdict before bounded `timeout 900`
  - current conclusion: self-inflicted `429` storm was removed, but the
    QuickNode block-history validation path is still operationally expensive on
    the live incident window
- Current recent-raw snapshot rerun finding on the real server after throughput
  hardening and practical-completion rollout:
  - the earlier rerun on `f19360f` exited honestly as bounded `deferred`
  - the follow-up rollout on `4148969` then completed a full live snapshot
  - observed live result:
    - `state = written`
    - `latest_surface_status = healthy`
    - `latest_surface_action = refreshed_from_source`
    - `terminal_reason = written`
    - `source_total_bytes = 1917220544`
    - `snapshot_pages_per_step = 1024`
    - `snapshot_max_attempt_duration_ms = 120000`
    - `attempt_duration_ms = 7927`
    - `backup_step_count = 343`
    - `backup_copied_page_count = 350752`
    - `backup_total_page_count = 350752`
    - `snapshot_bytes = 1436680192`
  - current conclusion: the recent-raw snapshot path is now operationally
    closed on the live server and the timer is back in service
- Current program-history gap-fill rerun finding on the real server after the
  resumable live-gap-fill rollout:
  - the repo / server rollout to `d2a6253` succeeded and the new
    `discovery_raw_gap_fill_program_history` binary was rebuilt on the server
  - the first direct live rerun immediately exposed the relative `output_dir`
    miswire above; the tool tried to create state under
    `/etc/solana-copy-bot/state/...` and failed with `Permission denied`
  - after fixing `program_history_gap_fill.output_dir` to an absolute state
    path, the tool no longer needed an outer shell timeout to stop
  - practical live result with bounded overrides:
    - command used:
      `discovery_raw_gap_fill_program_history --max-slot-batches-per-attempt 64 --max-blocks-to-fetch 200 --json`
    - attempt `1` returned its own terminal JSON with:
      - `verdict = not_proven_due_to_cost_budget`
      - `current_phase = awaiting_next_attempt`
      - `attempt_number = 1`
      - `next_batch_start_slot = 407461317`
      - `staged_rows = 8385`
      - `replayable_output = false`
    - attempt `2` returned its own terminal JSON again with cumulative forward
      progress:
      - `verdict = not_proven_due_to_cost_budget`
      - `attempt_number = 2`
      - `cumulative_across_attempts = true`
      - `next_batch_start_slot = 407461517`
      - `staged_rows = 15573`
      - `replayable_output = false`
  - current conclusion:
    - the new resumable contract is real on the live server
    - forward progress is real on the live server
    - but the current practical tuning is still far too slow for incident
      closure, because the real window advances only a few hundred slots per
      long attempt and still produces no replayable output
- Additional provider compare-runs completed on `2026-03-25`:
  - QuickNode remains the fastest provider actually observed on this exact
    `program_history_gap_fill` contract, but it is still not practical enough
    for timely recovery and intermittently returns `503` on long historical
    runs
  - Alchemy compare-run on the same bounded `200`-block contract completed
    cleanly but was slower:
    - first attempt:
      - `resolve_slot_bounds_ms = 54557`
      - `attempt_block_fetch_ms = 26784`
      - `attempt_frontier_advanced_slots = 200`
    - second attempt with reused bounds:
      - `attempt_block_fetch_ms = 30075`
      - `attempt_frontier_advanced_slots = 200`
    - conclusion: more stable than QuickNode on this small bounded run, but
      slower and not a better recovery source on the current tool contract
  - ANKR compare-run failed immediately as a non-archive source:
    - `verdict = non_viable_source_contract`
    - provider returned:
      `Block 407480552 cleaned up, does not exist on node. First available block: 408632723`
    - conclusion: this endpoint does not retain the historical depth needed
      for the incident window
  - Infura compare-run did not return a bounded terminal JSON even after more
    than two minutes on the same `200`-block attempt and produced no progress
    files
    - conclusion: unusable on the current gap-fill contract
- Mac Studio local compare-run completed on `2026-03-25`:
  - a local run was executed on an `M3 Ultra / 96 GB` host using:
    - the same QuickNode HTTP endpoint
    - the same live incident window
    - the same current gap-fill progress state copied from the server
  - a minimal local runtime DB was created with the same
    `discovery_recent_raw_restore_state`, so the comparison isolated provider
    / fetch cost from the server host itself
  - observed local steady-state result on the same bounded `200`-block attempt:
    - `attempt_block_fetch_ms = 24618`
    - `attempt_frontier_advanced_slots = 200`
  - observed live server steady-state result on the same bounded `200`-block
    attempt remained better:
    - `attempt_block_fetch_ms = 18874`
    - `attempt_frontier_advanced_slots = 200`
  - conclusion: the hypothesis that the production host is the primary
    bottleneck is not supported; the dominant constraint remains provider-side
    historical `getBlock` throughput / reliability, not local CPU or RAM
- Current decision on this branch:
  - active provider / gap-fill compare-testing is now paused
  - the branch is considered operationally explored enough to conclude:
    - snapshots are healthy and protect accumulated raw progress
    - discovery restore tooling is no longer the main blocker
    - the remaining blocker is the practical cost of provider-side historical
      `getBlock` recovery on the incident window
  - engineering focus should return to the main delivery plan while live
    ingestion continues to accumulate the missing raw window in the background
- Server build / stability check on `2026-03-25`:
  - the live repo remained on commit `c3ad5d8`
  - an explicit server-side build check succeeded without stopping the bot:
    - `~/.cargo/bin/cargo build --release -p copybot-discovery --bin discovery_raw_gap_fill_program_history --bin discovery_status`
  - both before and after the build:
    - `solana-copy-bot.service = active`
    - `copybot-discovery-recent-raw-snapshot.timer = active`
  - a direct live-write verification was then run against the server:
    - service/timer state at the same check:
      - `solana-copy-bot.service = active`
      - `copybot-discovery-recent-raw-snapshot.timer = active`
      - `copybot-discovery-runtime-export.timer = active`
    - live app logs remained healthy in the observed window:
      - `grpc_message_total` and `grpc_transaction_updates_total` were still increasing
      - `swaps_seen` was still increasing
      - live ingestion telemetry reported `rpc_429 = 0` and `rpc_5xx = 0`
      - no writer-death or process-crash signal appeared in the sampled `journalctl` window
    - direct SQLite verification on `observed_swaps` confirmed recent-raw writes were still advancing:
      - at `2026-03-25T13:44:28Z` the latest row was:
        - `rowid = 8270426`
        - `ts = 2026-03-25T13:43:40.433157287+00:00`
        - `slot = 408776640`
      - at `2026-03-25T13:45:38Z` the latest row was:
        - `rowid = 8276618`
        - `ts = 2026-03-25T13:44:57.514693140+00:00`
        - `slot = 408776834`
      - observed delta over `70s`:
        - `+6192` rows
        - slot frontier moved from `408776640` to `408776834`
        - latest raw timestamp moved forward by `77s`
    - snapshot timer also remained operational:
      - latest sampled timer-triggered run completed with `status=0/SUCCESS` at `2026-03-25 13:42:54 UTC`
      - snapshot metadata from that run reported:
        - `row_count = 8233319`
        - `covered_through_cursor.ts_utc = 2026-03-25T13:36:30.059096747Z`
        - `covered_through_cursor.slot = 408775557`
  - conclusion from this check:
    - the server is currently stable
    - the recent-raw journal is currently still being written
    - the snapshot/export timers are currently still functioning
    - this is a real-time health slice, not a guarantee that nothing can break later
- Business meaning of the current server state:
  - the dead multi-day replay path is no longer the active plan
  - discovery is alive on a new fresh runtime DB and holding `15` wallets instead of zero
  - the old `live_copybot.db` has been fully removed and is no longer a dependency
  - the old offline aggregate clone
    `/var/www/solana-copy-bot/state/live_copybot.aggregate_clone_offline_20260321.db`
    has now also been removed from disk
  - copy trading / shadow trading are still not opening positions
  - the server is stabilized, but the incident is not yet closed in the
    trading-ready sense
  - the remaining live blocker is now extremely narrow:
    practical completion of program-history gap-fill on the real incident
    window
  - however, the active decision is to stop spending additional calendar time
    on this gap-fill/provider branch for now and let live accumulation continue
    while the team returns to the main roadmap
  - until that branch is intentionally reopened, `ROAD_TO_PRODUCTION_v2.md`
    is again the primary document for ongoing development, while
    `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md` remains the incident /
    restore record

- Historical stopped-host investigation record preserved below.
- Aggregate backfill status: **blocked, but materially narrowed**:
  - same-host hot clone under active live load was aborted as unsafe after pressure spiked to:
    - `observed_swap_writer_pending_requests = 4224`
    - `sqlite_busy_error_total = 469`
    - `sqlite_write_retry_total = 399`
    - host IO wait `wa = 90.5%`
  - same-host cold clone with the service intentionally stopped succeeded
  - offline coarse wrapper run on `a25c1e5` on the old tool path still produced no first durable checkpoint
  - offline budget-aware bounded retry on `02f887a` on the old tool path still produced no first durable checkpoint
  - offline direct unbounded phase-1 `seeded-reset --stop-after-seed-install` on the old tool path also failed to reach:
    - `boundary_batch_buffered`
    - `seed_boundary_installed`
    - or any durable change to `backfill_progress`
  - the next server-side investigation proved the "empty log" confounder on the same existing clone:
    - optional migration `0039_observed_swaps_sol_leg_ts_index.sql` was still pending on the clone
    - the offline tool was therefore able to stall before boundary replay unless it used the startup-safe SQLite bootstrap path
  - commit `8e748dc` fixed the real seeded-boundary correctness gap in code:
    - `boundary_build` is checkpointable via SQL replay before `seed_boundary_installed`
    - seed snapshot is exported from materialized state
  - commit `777a1c8` then made the offline tool operationally testable on the server:
    - `0039` is deferred instead of blocking startup
    - early startup/boundary-entry telemetry is emitted
  - on the same existing offline clone, server-side bounded runs after those two fixes now show repeated durable pre-seed progress:
    - initial persisted cursor before the new validation:
      - `2026-03-08T21:06:45.726139749Z | 405112624 | 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d`
    - first diagnostic micro-run after `777a1c8`:
      - `2026-03-08T21:06:45.726202249Z | 405112624 | oeZav89P6QLScbxZHKDKvA1GWey4rjxrwGhxCbLk241EdsTWyptJoPbDpyngBw4NGhahotNaoWGNFtyz78R6iMU`
    - next bounded resume run:
      - `2026-03-08T21:06:46.017484291Z | 405112625 | 5GRJdWm4fFyq2tyLTP6M5uhE9tzNb1hUWC6WSKjBtFhsd7NQvUpwcHvzq31dYoziyA5fsb1nTwGPxXKcM5KPx1X3`
    - bounded slice chain on the same clone:
      - slice 1 -> `2026-03-08T21:06:46.466356720Z | 405112626 | 4x7pnDytijahmva6NFCpcBT7VJDj8qfmzXzuo9VhpW8XVyKWr4U5vAQYR52h8Hdq4AMBNuvnjdpJjYEAJz5FsojU`
      - slice 2 -> `2026-03-08T21:06:47.179787272Z | 405112628 | PVn4Cd3vWt6dECAaHa6PLr4RXGsj8RUEgEqN8XKBn1NpB9eoGGeMMTrjVZPbxQhLM2uDsPjrUfUGV4iorPH8zug`
      - slice 3 -> `2026-03-08T21:06:47.579812100Z | 405112629 | 2xFAChfen6yLGeFaLh4X36FNmRZbq1cbsfatBEKqGwqJ3QYVTTJ32MNZ33gyNhUkHd8TmnEYBF1Aa3ZLgx5xHY5S`
  - what is still missing after those successful resume slices:
    - no emitted `event=batch_committed phase=boundary_build`
    - no emitted `summary outcome=...`
    - no `seed_boundary_install_*`
    - throughput remains too low for this path to be considered operationally closed
- Observed data scale: `SELECT COUNT(*) FROM observed_swaps WHERE ts >= datetime('now', '-5 days')` returned `48,386,266`; the failed `3`-day bridge was expected to shrink this to roughly `~29M`, but the bridge still did not close Stage 1 durably
- Observed during validation window:
  - corrected raw replay fast path was validated before the bridge experiments:
    - live deploy of `70e959d` stayed healthy overnight
    - replay ran through the sampled window with:
      - `rebuild_replay_wallet_stats_fast_path_pages_processed = 534`
      - `rebuild_replay_wallet_stats_fast_path_wallets_processed = 480,600`
      - `rebuild_replay_wallet_stats_fallback_pages_processed = 0`
      - `rebuild_replay_wallet_stats_fallback_wallets_processed = 0`
    - this proved the corrected `wallet_activity_days` fast path is not the remaining blocker
  - same-host hot clone was disproven, but cold clone was proven:
    - hot clone under live load materially increased pressure and was aborted
    - cold clone on the same host succeeded once the service was intentionally stopped
    - exact persisted resume lineage before offline work:
      - `backfill_progress.start_ts = 2026-03-03T17:05:37Z`
      - `backfill_progress.cursor.ts_utc = 2026-03-08T21:06:45.726139749Z`
      - `backfill_progress.cursor.slot = 405112624`
      - `backfill_progress.cursor.signature = 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d`
    - clone path used for the offline runs:
      - `/var/www/solana-copy-bot/state/live_copybot.aggregate_clone_offline_20260321.db`
    - the original three offline aggregate attempts on the old tool path failed before the first durable seeded-boundary checkpoint
    - the follow-up validation sequence on the same clone after `8e748dc` + `777a1c8` materially changed the state:
      - `0039` was confirmed pending and then correctly deferred by the new offline-tool startup-safe path
      - `8fdffd9` surfaced the first clean `checkpoint_persisted / batch_committed / summary` boundary slice
      - `61719f1` was a useful dead end: server validation showed the in-memory boundary lot builder stalled before the first boundary event, and read-only inspection measured `wallet_scoring_open_lots = 1,710,424` rows with `717,988` distinct wallet-token buckets
      - `7b6ab59` then validated the real fix for pre-seed replay on the clone: `boundary_lot_sql_ready`, `checkpoint_persisted`, `batch_committed`, and `summary` all appeared with `prepare_ms = 0`, and the persisted cursor was advanced through repeated bounded resume slices up to the exact near-boundary cursor `2026-03-08T22:15:51.242661287Z | 405123180 | 3AbrgKcriCUKPrdpztgnm1jj2BxfRPkP6xA6sjeReL1AtSBKghNKkZjTvzLdMtVWHxpzgWX5m7HLChDcXpwia5nL`
      - `edf90a7caa4e455ca0f3e46d8bdeb3148d8fee02` then validated the next closure step on the same clone:
        - `event=seed_boundary_exported` and `event=seed_boundary_installed` were observed with `seed_lot_count = 2011931`
        - `event=seed_boundary_stop_requested` and `summary outcome=stopped_after_seed_install` proved a clean durable stop immediately after committed seed install
        - the follow-up normal resume emitted `event=seed_boundary_resume_from_persisted_progress` and advanced replay-after-seed to `2026-03-08T22:17:02.935800892Z | 405123360 | 5G2cGVUFuDDDPXCuqpaUaeVfUQXxNWE6xxeoX1NCEwgzfycTTYaJDiUYNW5UuL3zqD2j2qWuk3FUVMG1AkbsUC3x`
      - `87a705296e3643f64cca4d7c3797c68b89b0bca4` then disproved the first naive post-seed builder follow-up on the same clone:
        - `event=seed_boundary_resume_from_persisted_progress` still appeared, so the committed seed marker contract remained intact
        - but no `event=builder_replay_ready`, no post-seed `checkpoint_persisted`, no builder `batch_committed`, and no `summary outcome=...` were emitted before the outer timeout fired
        - the persisted cursor therefore remained unchanged at `2026-03-08T22:17:02.935800892Z | 405123360 | 5G2cGVUFuDDDPXCuqpaUaeVfUQXxNWE6xxeoX1NCEwgzfycTTYaJDiUYNW5UuL3zqD2j2qWuk3FUVMG1AkbsUC3x`
      - `c3e057fc504e1941e385ed25c872fa8f0722ac44` then validated the lazy builder recovery on that same clone:
        - one initial bounded run emitted `event=builder_replay_ready`, three durable builder checkpoints, and builder `batch_committed` lines with no outer timeout
        - a later 5-run chain repeated the same builder events on every run and advanced the cursor to `2026-03-08T22:38:03.568827634Z | 405126550 | 2biGRk8Yozn3vor5rMKscdWZf6EdurhirSyRYYtchAy4ovTERdvNUUM36pTeK83y4CrZwRnQUanfxvkzLfSqDwyy`
        - the step-up chain with `max-batches-per-run = 10` then advanced further to `2026-03-08T22:56:27.923141336Z | 405129333 | 3o16UWsam85kXYhsj594snQNaKSqpPWoHFb8bdMCuFHkDfnPnTi1gbCaKHvmMae36QBkZDoxo8jVx4cqE1MuBaGf`
      - `90ada0d0fe4f8603d1f708bd570a12be662633ba` then removed the next tail blocker on the same clone:
        - bounded post-seed builder replay emitted `event=final_rug_finalize_skipped phase=run_complete reason=bounded_post_seed_builder_path`
        - no `event=final_rug_finalize phase=run_complete` was emitted on that validation run
        - the persisted cursor advanced again to `2026-03-08T23:00:45.980276955Z | 405129982 | 24yuNmrtkJ8JNPhxC2FoGT4QfA2SzWX2TyzukE5MqQske7ABNZqF289ua36A8DiQyRpaNtbwGevviyvJQ1tnevGz`
      - `ae688b7fb84cead93185f6f5bbd50ac32f59f452` then operationalized the confirmed seeded path into one wrapper-driven recovery flow:
        - the first wrapper validation used `mode = seeded-reset`, `max_runs = 24`, `outer_timeout_seconds = 420`, `max_runtime_seconds = 180`, `max_batches_per_run = 10`, `batch_size = 10000`
        - `summary.txt` ended with `verdict = max_runs_exhausted`, which is expected until `coverage_marked` is reached
        - `next_resume.env` stayed on `MODE = seeded-reset`, so committed seed-marker restart semantics remained intact across runs
        - the first wrapper run advanced the persisted cursor further to `2026-03-09T00:30:25.487279814Z | 405143661 | sAKJBVmLgfR5g9dMwTViWTSFqc4u5or33rhZwJBYHiiZ8v7jGBfZMZNWY5aQeVXNnf4g93pyPTVTRHKWYWD1yP6`
      - latest stopped-host wrapper chain on the same clone and same commit is now complete as a bounded partial-progress chain, not as a recovery success:
        - service remained intentionally stopped throughout and is still inactive: `systemctl is-active solana-copy-bot.service -> inactive`
        - report directory: `/tmp/aggregate-seeded-loop-20260323T121242Z`
        - wrapper parameters were:
          - `mode = seeded-reset`
          - `start_ts = 2026-03-08T22:15:51.246588877Z`
          - `max_runs = 96`
          - `outer_timeout_seconds = 420`
          - `max_runtime_seconds = 180`
          - `max_batches_per_run = 10`
          - `batch_size = 10000`
        - wrapper completion artifact now exists:
          - `summary.txt` ended with `verdict = max_runs_exhausted`
          - no active `aggregate-seeded-loop` or `backfill_discovery_scoring` process remained at the verification snapshot
        - latest completed readiness artifact:
          - `/tmp/aggregate-seeded-loop-20260323T121242Z/096_post_run_aggregate_readiness_status.json`
          - `covered_since = null`
          - `covered_through_cursor = null`
          - `backfill_resume_required = true`
          - `effective_reads_ready = false`
          - `effective_writes_ready = false`
        - latest persisted resume lineage exported by the wrapper:
          - `MODE = seeded-reset`
          - `START_TS = 2026-03-08T22:15:51.246588877Z`
          - `RESUME_TS = 2026-03-09T10:00:27.660051087Z`
          - `RESUME_SLOT = 405230735`
          - `RESUME_SIGNATURE = 3o4fybEub2fugAF4yRX74ouW4gvKfvWNghHkRrjPv5iobpbJCoLiZbWeHAPP7fidGnu4ANofJN76CtMFwBahnYcR`
        - direct SQLite inspection on the clone matched that exact partial-progress state:
          - `backfill_progress_start_ts = 2026-03-08T22:15:51.246588877+00:00`
          - `backfill_progress_cursor_ts = 2026-03-09T10:00:27.660051087+00:00`
          - `backfill_progress_cursor_slot = 405230735`
          - `backfill_progress_cursor_signature = 3o4fybEub2fugAF4yRX74ouW4gvKfvWNghHkRrjPv5iobpbJCoLiZbWeHAPP7fidGnu4ANofJN76CtMFwBahnYcR`
        - the last bounded run itself still showed real forward progress rather than semantic failure:
          - `096_backfill.log` emitted `event=builder_replay_ready`
          - six durable `checkpoint_persisted` / `batch_committed` slices were written in `phase=replay_after_seed`
          - last run advanced the cursor from `2026-03-09T09:54:33.626020485Z | 405229838 | 5wrDgPUfmhXQU3tBoQu964m9Szt7SJHc1wDsj283rFUGP84nPgnDwz53A27n7XN4SrfeF5C2sHM5misTkYpf93xC`
            to `2026-03-09T10:00:27.660051087Z | 405230735 | 3o4fybEub2fugAF4yRX74ouW4gvKfvWNghHkRrjPv5iobpbJCoLiZbWeHAPP7fidGnu4ANofJN76CtMFwBahnYcR`
          - run-level stop reason remained bounded-runtime exhaustion, not a new semantic crash:
            - `summary outcome=stopped_due_to_runtime_budget`
            - `coverage_marked=false`
      - the remaining blocker is now operational completion, not a newly discovered replay semantic gap:
        - pre-seed checkpointability is already proven on the clone
        - committed `seed_boundary_install_*` is already proven on the clone
        - post-seed builder replay and deferred final-finalize are already proven on the clone
        - the wrapper loop chains `seeded-reset` progress correctly without manual cursor handoff, but still does not finish within the current bounded run budget
        - what is still missing is simply end-to-end completion to:
          - `coverage_marked`
          - `backfill_resume_required = false`
          - `effective_reads_ready = true`
          - `effective_writes_ready = true`
  - raw-bridge experiment sequence is now complete:
    - initial `scoring_window_days = 3` restart stayed pinned behind stale persisted rebuild state from the old `5`-day window
    - clearing only `discovery_persisted_rebuild_state` correctly restarted a fresh 3-day rebuild
    - that fresh restart reached:
      - bounded observed-swaps prepass completion
      - bounded token-quality completion
      - `Replay`
    - but the decisive next bucket still failed:
      - latest decisive sample `2026-03-21T19:11:35.899184Z`
      - `rebuild_phase = collect_buy_mints`
      - `rebuild_collect_buy_mints_mode = reconcile_new_tail`
      - `rebuild_replay_rows_processed = 0`
      - `rebuild_replay_sol_leg_access_path` not emitted
      - `scoring_source = raw_window_incomplete_no_recent_published_universe`
      - `active_follow_wallets = 0`
      - stale warning returned near the end of that decisive window
- Rollout reports:
  - [ops/server_reports/2026-03-17_1758_stage1_discovery_runtime_contract_rollout_report.md](ops/server_reports/2026-03-17_1758_stage1_discovery_runtime_contract_rollout_report.md) — first Stage 1 deploy (`2eb5c30`), confirmed bootstrap path removed but fail_closed due to cap-truncated warm load
  - [ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md](ops/server_reports/2026-03-17_1839_stage1_persisted_stream_followup_rollout_report.md) — persisted-stream follow-up (`0c58aba`), confirmed correct path engaged but first cycle did not complete in 6+ minutes
  - rollout `96606b8` recorded the earlier startup stall before discovery/runtime
  - rollout `3fac9af` validated startup SQLite observability/deferred WAL checkpoint and bounded persisted rebuild progress, but not yet healthy publication
  - rollout `aed70c` validated canonical safe-prefix migration / repair on live and showed fresh canonical `CollectBuyMints` progress after `metrics_window_start_changed`, but still did not yet reach healthy publication during the observed window
  - rollout `52e1e8a` validated carry-forward across metrics bucket rollover on live and showed discovery reaching `Replay`, but exposed a later process-stability blocker: restarts under `database is locked` + Yellowstone output queue saturation before healthy completion
  - rollout `eba671f` validated the retryable-lock writer fix on live: the service stayed up with growing SQLite retry counters, carry-forward still worked, and rebuild repeatedly returned to `Replay`, but healthy publication still did not land
  - rollout `2072123` regressed earlier in startup: the new `0039_observed_swaps_sol_leg_ts_index.sql` partial-index migration ran inside fatal `sqlite_migrations_apply`, timed out at 120s, aborted the process, and prevented any runtime validation of the replay fix
  - rollout `1093a55` validated the startup-safe 0039 deferral contract and showed `rebuild_replay_mode = wallet_stats_then_sol_leg` on live, but rebuild still remained in `CollectBuyMints` through the observed windows, so replay access-path telemetry and healthy publication were still not reached
  - rollout `bc9f6d7` validated the grouped-delta `CollectBuyMints` reconcile improvement on live by reaching `Replay` before rollover and emitting reconcile token-cursor telemetry, but also exposed the next rollover-specific blocker: if the boundary lands while runtime is still in `reconcile_expired_head`, the next slice restarts as a new `fresh_scan`
  - rollout `5e8d71b` validated the stale-frozen-target resume contract on live: boundary during `reconcile_expired_head` no longer resets to a fresh scan, but also revealed the new remaining blocker that stale reconcile still converges too slowly to the next exact checkpoint and `Replay` re-entry
  - rollout `94847aa` preserved that stale-resume contract on live, but still did not show faster return to `Replay`; the convergence blocker remained
  - rollout `7d28c76` moved the same stale path farther on live: runtime advanced into `reconcile_new_tail`, preserved progress across another boundary there, and kept bounded cursor lineage, but still did not re-enter `Replay`
  - rollout `dd8c6e5` preserved the stale new-tail boundary contract on live, but showed a narrower stall: runtime stayed on the same new-tail cursor with zero processed rows and did not re-enter `Replay`
  - rollout `8d99e32` validated that the narrowed-slice / exact single-token fallback breaks the pinned zero-row stale-new-tail stall on live: cursor and slice-end lineage advanced with non-zero processed rows across later windows, but runtime still did not yet re-enter `Replay`
  - rollout `c01487f` validated that stale `reconcile_new_tail` now persists an exact pending batch across bounded cycles / repeated bucket rollovers instead of reopening the same tail candidate slice each cycle, but runtime still did not yet re-enter `Replay`
  - rollout `492c7e3` validated that stale expired-head candidate discovery was itself a meaningful part of the remaining bottleneck: with the existing time-first `idx_observed_swaps_token_in_ts` path, post-boundary stale expired-head cycles now exhausted `page_budget` with `rebuild_cycle_rows_processed = 160` in `~1.2-1.6s`, no pressure symptoms appeared, and the remaining limiter narrowed to overall duty cycle / time-to-`Replay`
  - rollout `9b5f8cd` validated the first guarded catch-up scheduler step on live: exact carry-forward checkpoints were reached much faster, but runtime still had not yet re-entered `Replay`
  - rollout `8efd6c4` validated the relaxed catch-up gate / moderate writer-backlog threshold on live and confirmed `fresh_scan / time_budget` catch-up behavior under safe conditions, but the old `CollectBuyMints -> Replay` blocker still was not yet fully closed in the earlier observed window
  - rollout `423fd51` validated the next step: `fresh_scan / time_budget` catch-up is now live, runtime re-entered `Replay` for the first time in this fix series, and overnight replay survived later bucket rollover and returned quickly to `Replay`; the remaining limiter is now replay wallet-stats prepass throughput before SOL-leg replay and healthy publication
  - rollout `371403c` validated that folding replay wallet-stats `active_days` into the main summary query does not regress startup, replay re-entry, or exact carry-forward from `Replay`, but it still did not materially improve replay wallet-stats enough to finish the prepass before rollover; Stage 1 remained blocked before SOL-leg replay, and the next operational experiment is hourly live buckets via `c6e0af8`

### 2.6 Live data scale (observed)

- `live_copybot.db`: ~140 GB
- `live_copybot.db-wal`: ranged from `0` during the stopped-service cold clone up to ~`2.0 GB` during later live runtime
- `live_copybot.db-shm`: ~3.4 MB
- `observed_swaps_retention_days`: 7
- `scoring_window_days`: failed emergency bridge used `3`; stabilized stopped host is reverted to `5`
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

The stopped-host seeded aggregate recovery loop is background operational work, not a reason to pause coding. Engineering work must continue in parallel and must not reintroduce recovery into the runtime critical path.

### Stage 1. Remove aggregate recovery from runtime discovery

Status: **partial after live rollout `7d28c7607d380cd4711de24b49ec325c2302a1c6`; startup SQLite boundedness/observability is validated on live, bounded/resumable persisted rebuild is validated on live, canonical migration/repair is validated on live, carry-forward across `metrics_window_start_changed` is validated on live, retryable SQLite lock handling in the observed-swap writer is validated on live, startup-safe deferral for `0039_observed_swaps_sol_leg_ts_index.sql` is validated on live, `wallet_stats_then_sol_leg` replay wiring is validated on live, grouped-delta `CollectBuyMints` reconcile is validated on live as a pre-replay improvement, stale-frozen-target resume across bucket boundaries is validated on live, and current working tree now contains the follow-up stale new-tail convergence fix needed to get runtime back into `Replay` materially faster**

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
21. Stale grouped-delta reconcile now avoids rebuilding canonical mint membership from scratch on every page:
   - `buy_mint_counts` remains the authoritative exact carried-forward membership source during `reconcile_expired_head` / `reconcile_new_tail`
   - `unique_buy_mints` is maintained incrementally in canonical sorted order as counts are added or removed, so partial stale pages still keep exact carry-forward truth
   - this removes the per-page `O(total unique mints)` mint-vector rebuild from stale reconcile without adding any new startup migration, index dependency, or publication lie

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
23. startup-deferred optional performance migrations do not block `sqlite_migrations_apply`, and replay remains correct before and after the deferred index becomes available
24. stale grouped-delta reconcile maintains exact canonical membership incrementally without rebuilding the full mint vector on every page, so stale resume remains rollback-safe while removing per-page `O(total unique mints)` work
25. stale `reconcile_expired_head` now pages by capped exact candidate-token batches from the canonical membership set, instead of asking SQLite to aggregate the entire remaining token range in one query under live `fetch_limit = 20000`
26. stale `reconcile_new_tail` now uses the same candidate-batch idea for the live carry-forward tail:
   - it first pages exact distinct SOL-buy mint candidates inside `(source_horizon_end, target_horizon_end]`
   - it then asks SQLite for grouped counts only inside that bounded token slice instead of the entire remaining tail range
   - completion is detected from candidate-batch exhaustion, so exact checkpoint detection no longer depends on one wide grouped query finishing over the whole tail
   - no new startup-critical migration or index contract is introduced; the fix stays on the existing `idx_observed_swaps_token_in_out_ts` access path
27. stale `reconcile_new_tail` now also breaks zero-row unchanged-cursor stalls without lying about membership truth:
   - when a grouped-count slice times out before the first row, runtime persists a narrower exact token-slice cap instead of retrying the same wide slice forever
   - repeated retries keep narrowing that exact slice across bounded cycles until progress becomes cheap enough to make
   - once the narrowed slice reaches a single candidate mint, runtime switches to an exact single-token occurrence count query and can advance the stale cursor without skipping any truthful work
   - the temporary narrowed-slice cap is stored only in the persisted rebuild payload, so there is still no new startup-critical migration or index rollout dependency
28. stale `reconcile_new_tail` now persists the exact candidate mint batch itself and counts only those remaining mints, instead of re-opening the same tail slice every cycle:
   - candidate discovery still uses bounded `DISTINCT token_out` pagination on `(source_horizon_end, target_horizon_end]`
   - once a candidate batch is found, the batch is stored in the persisted rebuild payload and resumed verbatim across bounded cycles and repeated bucket rollovers
   - count work for that batch now runs against the exact candidate mint set, not a token range, so runtime stops paying repeated `DISTINCT` + range-grouped cost before reaching the next exact checkpoint
   - zero-row stall escape remains intact: an empty exact-batch attempt still narrows the slice, and a single surviving mint still falls back to exact single-token counting
   - no new startup-critical migration or deferred-index contract is introduced; the fix stays on the existing `idx_observed_swaps_token_in_out_ts` access path

Remaining operational blocker:

The previous live blocker around startup-safe replay-index rollout is no longer the current blocker: deploy `1093a5556e82f8adb6ec73bb51e73d62b8d9ac02` reached runtime, deferred `0039` explicitly, and showed `wallet_stats_then_sol_leg` in live telemetry. Deploy `bc9f6d7d946a34b1f854680c9c53a9c117cde735` then validated that the grouped-delta reconcile fix moved progress farther: runtime now reaches `Replay` before rollover. Deploy `5e8d71beadc9e5adabce357c65f2f8a2785b1a6d` then validated that the stale-frozen-target resume fix closed the boundary-loss bug: in-progress `reconcile_expired_head` no longer restarts as a new `fresh_scan` at bucket rollover. Deploy `94847aaf1eda6f19b04c0988ea15e60646757e9d` preserved that semantic fix but still did not return runtime to `Replay`. Deploy `7d28c7607d380cd4711de24b49ec325c2302a1c6` then showed that the candidate-batch expired-head fix moved the stale path farther again: runtime was already in `reconcile_new_tail` and stayed there across another boundary without fresh restart. Deploy `dd8c6e5c2798347808e375573edc7105da4f35e4` then showed that the first candidate-batch new-tail fix still was not enough: runtime remained on the same stale new-tail cursor with zero processed rows across many bounded cycles. Deploy `8d99e324bfd1d0312d98a0cfa3f179243cbec35e` then validated that persisted slice narrowing and exact single-token fallback break that pinned zero-row stall on live: narrowed-slice telemetry is emitted, new-tail cursor/slice lineage advances, and `rebuild_cycle_rows_processed` is non-zero again. Deploy `c01487f2453c02ef25164ebb41ee0e0312c72782` then validated that persisted exact pending-batch state survives bounded cycles and boundary handling as intended, so runtime no longer appears to reopen the same stale tail candidate slice on every cycle. Deploy `661ca3f7e73e4f451cce68c4e2df50766157f31` then validated the next throughput step on live as well: exact sub-batch counting carried stale `reconcile_new_tail` to an exact carry-forward checkpoint, runtime exited stale `reconcile_new_tail`, and the next observed boundary already landed in stale `reconcile_expired_head`. Deploy `08c65adc0377537dce0644c82fe14c8321e12093` then validated the analogous stale expired-head contract on live: exact pending-batch telemetry appeared, expired-head cursor and pending batches advanced with non-zero rows, and later windows again showed runtime reaching stale `reconcile_new_tail` without any fresh restart. Deploy `492c7e38c90317b75d5f3a822c429a0fe7d20ac6` then validated that stale expired-head candidate discovery was itself a meaningful part of the remaining bottleneck: with the existing time-first `idx_observed_swaps_token_in_ts` path, post-boundary stale expired-head cycles now exhaust `page_budget` with `rebuild_cycle_rows_processed = 160` in `~1.2-1.6s`, while pressure, lock, and restart symptoms remain absent. Stage 1 is still partial because the remaining blocker observed on live is now narrower again: stale `CollectBuyMints` still does not converge back into `Replay` quickly enough on live-size state, but the active limiter now appears to be overall bounded page/cadence duty cycle after truthful candidate discovery rather than the old stale expired-head candidate-discovery SQL geometry. The next code change should therefore preserve the existing exact-truth / stale-resume / no-false-healthy contract while increasing effective catch-up duty cycle only for safely bounded partial rebuilds that remain `fail_closed`.

Current working diagnosis:

the old deploy `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c` proved that a one-shot first-cycle persisted rebuild was too slow / insufficiently bounded for live-size state; the later deploy `96606b83880cb1b942de67f61c5ecdb459fe4139` exposed an earlier blocker on startup SQLite open/migration boundedness; deploy `3fac9afdafbeb3e4ca2c66486124a8683d281f02` validated both the startup fix and bounded/resumable rebuild behavior on live; deploy `aed70c91906321e3e80b1a14614454a9db740026` proved the next blocker was bucket-boundary reset of a still-incomplete fresh canonical rebuild; deploy `52e1e8a61612b3e8d95fa808bb25c32a23f39438` validated that carry-forward fixed that blocker but exposed a later operational failure mode; deploy `eba671f2215e9114065799be2792262abbb1d2b1` validated that retryable raw observed-swap `database is locked` events no longer force writer death or process restart under the observed live windows; deploy `2072123e7ba90a9133494be0d70023d0c9b2cc4b` then regressed before runtime because the new heavy partial-index migration ran synchronously inside fatal `sqlite_migrations_apply`; deploy `1093a5556e82f8adb6ec73bb51e73d62b8d9ac02` validated the startup-safe deferred rollout contract for `0039` and confirmed that `wallet_stats_then_sol_leg` is live; deploy `bc9f6d7d946a34b1f854680c9c53a9c117cde735` validated that grouped-delta reconcile moves the rebuild far enough to reach `Replay` before rollover; deploy `5e8d71beadc9e5adabce357c65f2f8a2785b1a6d` validated that boundary during `reconcile_expired_head` no longer causes effective fresh restart; deploy `94847aaf1eda6f19b04c0988ea15e60646757e9d` showed that the later incremental-membership optimization still was not enough to re-enter `Replay`; deploy `7d28c7607d380cd4711de24b49ec325c2302a1c6` showed that the candidate-batch expired-head fix was enough to move runtime onward into stale `reconcile_new_tail`, but still not enough to return to `Replay`; deploy `dd8c6e5c2798347808e375573edc7105da4f35e4` then showed that the first candidate-batch new-tail fix still was not enough because stale `reconcile_new_tail` could remain stuck on one cursor with zero rows processed; and deploy `8d99e324bfd1d0312d98a0cfa3f179243cbec35e` then showed that this correctness stall was closed, but still did not re-enter `Replay`. The current blocker is now narrower and purely operational: stale `reconcile_new_tail` keeps exact truth, survives rollover, and now preserves exact pending-batch state across cycles, but still does not reach the next exact checkpoint and `Replay` quickly enough. The next code change should therefore target throughput after exact pending-batch persistence, not the already-closed zero-row unchanged-cursor semantics.

Immediate next operational step before Stage 2:

roll out a follow-up fix for overall stale `CollectBuyMints` time-to-`Replay` and then re-validate the later replay path on the next rollout:

1. stale frozen-target `reconcile_new_tail` must continue to avoid the old zero-row unchanged-cursor stall seen on deploy `dd8c6e5c2798347808e375573edc7105da4f35e4`
2. runtime must continue to preserve in-progress reconcile across boundaries without falling back to a new `fresh_scan`
3. after the now-live-validated exact pending-batch contracts for both stale `reconcile_expired_head` and stale `reconcile_new_tail`, runtime must materially shorten overall stale `CollectBuyMints` convergence on live-size state
4. once stale `CollectBuyMints` returns to an exact checkpoint on the current bucket, runtime must roll back into `Replay` without false `healthy`
5. runtime must then re-enter `Replay` and emit the first actual replay slice, emitting:
   - `rebuild_replay_mode = wallet_stats_then_sol_leg`
   - `rebuild_replay_sol_leg_access_path = ts_cursor_fallback` until the deferred index is applied
   - `rebuild_replay_sol_leg_access_path = sol_leg_partial_index` after the deferred index is applied offline
6. discovery must still complete cycles while the optimized replay path is in progress
7. the new `Replay` contract must then complete to a healthy `raw_window_persisted_stream` publish
8. `active_follow_wallets > 0` must appear once healthy publication lands
9. there is still no false `healthy` and no empty published universe

See section 2.5 for observed server state and section 2.6 for live data scale.

### Parallel workstream while stopped-host seeded recovery runs (added 2026-03-23)

Status: **active**

Why this exists:

1. The current stopped-host seeded loop is making real forward progress on the offline clone, but it has already consumed too much calendar time to justify freezing development behind it.
2. Even a successful aggregate recovery would restore an operational baseline; it would not by itself fix the bad discovery architecture if normal runtime truth still depends on historical recovery semantics.
3. Coding work must therefore proceed now, in parallel, under the assumption that aggregate recovery is a background ops track and not the long-term runtime model.

Rules for this workstream:

1. Do not block coding on the stopped-host loop finishing.
2. Do not make `seeded-reset`, aggregate readiness, `covered_since`, `covered_through_cursor`, or offline recovery state a normal runtime dependency again.
3. Do not require production-host validation as the only proof that a code change is correct.
4. Do not add startup-critical migrations, large new indexes, or heavyweight startup scans just to make recovery or runtime behavior look simpler.
5. Keep `execution.enabled = false` and keep aggregate reads/writes disabled while this workstream is landing.

Work packages to hand to the coder:

1. Package A. Quarantine remaining recovery-state coupling from runtime discovery
   - audit `copybot-app`, `copybot-discovery`, and runtime-facing `copybot-storage` reads for any remaining decision that still depends on offline recovery semantics instead of current raw truth or recent publication truth
   - rename or split ambiguous state reads where publication truth and recovery truth are still mixed together conceptually
   - remove or quarantine legacy bootstrap / trusted-selection wording that no longer matches the current runtime contract
   - required evidence:
     - changed files are limited to runtime/discovery/storage code, not server-only scripts
     - targeted tests prove the same `healthy / degraded / fail_closed` outcomes still hold after restart
   - accepted first sub-slice on `2026-03-23`:
     - startup/runtime publication fallback now reads recent publication truth through discovery/runtime helpers instead of treating current followlist residue as runtime truth
     - degraded restart path now restores the last published universe instead of preserving stale followlist residue across restart
     - runtime aggregate-read path is removed as a runtime source-of-truth decision; restart stays on `raw_window`, recent publication truth, or `fail_closed`
     - aggregate-ready state no longer overrides restart outcomes that should remain `healthy`, `degraded`, or `fail_closed` under the Stage 1 runtime contract
   - accepted verification for that sub-slice:
     - `cargo test -p copybot-discovery --lib aggregate_ready_state_ -- --nocapture`
     - `cargo test -p copybot-discovery restart_with_recent_published_universe_replaces_stale_followlist_residue_stage1`
     - `cargo test -p copybot-app startup_recent_published_universe_ignores_stale_followlist_residue`
     - `cargo test -p copybot-discovery`
   - package status after accepted sub-slice:
     - accepted for the current Package A scope
     - aggregate-ready state is no longer allowed to override runtime restart truth
     - remaining cleanup on legacy helpers is non-blocking and does not reopen Package A

2. Package B. Make publication truth the only restart-safe runtime control plane
   - create one explicit runtime-facing read path for:
     - latest published universe metadata
     - freshness / age of that publication
     - last known runtime mode
   - runtime restart decisions must read that publication truth directly rather than inferring truth from recovery state tables
   - required evidence:
     - one clear code path owns publication-state reads
     - tests cover restart with:
       - fresh valid published universe
       - stale published universe
       - no published universe
   - next package to assign:
     - make the exact published wallet set a first-class control-plane object instead of reconstructing it from published `wallet_metrics` under current ranking/config
     - make restart/publication helpers read that object directly
     - preserve the accepted Package A invariant that aggregate/recovery state is not runtime truth
   - reason this is Package B and not Package A:
     - the remaining work is no longer about removing aggregate/recovery runtime coupling
     - it is about making publication truth itself exact, restart-safe, and directly readable as a control-plane object
   - accepted on `2026-03-23`:
     - exact published wallet membership is now persisted in publication state as a first-class control-plane object
     - startup/restart/degraded runtime paths now read exact recent publication truth directly instead of reconstructing the universe from published `wallet_metrics` plus current ranking/config
     - healthy publish writes the exact published wallet set into publication state
     - degraded/fail-closed publication-state updates preserve the last exact published set instead of replacing it with a reconstructed runtime view
   - accepted verification for Package B:
     - `cargo test -p copybot-discovery recent_runtime_publication_truth_rejects_stale_exact_published_universe`
     - `cargo test -p copybot-discovery restart_with_recent_published_universe_uses_exact_wallet_set_when_current_ranking_drifted_stage1`
     - `cargo test -p copybot-discovery restart_with_recent_published_universe_replaces_stale_followlist_residue_stage1`
     - `cargo test -p copybot-discovery --lib aggregate_ready_state_ -- --nocapture`
     - `cargo test -p copybot-app startup_recent_published_universe_ignores_stale_followlist_residue`
     - `cargo test -p copybot-discovery`
   - package status after acceptance:
     - accepted for the current Package B scope
     - Package A remains closed
     - remaining telemetry cleanup does not reopen Package B
   - next package to assign:
     - Package C
     - build a deterministic local perf harness for stale `CollectBuyMints`, bounded `Replay`, and other currently expensive discovery phases so throughput work stops depending on the production clone as the primary debugger

3. Package C. Add a reproducible local perf harness for the current discovery bottlenecks
   - add a deterministic fixture, integration test, or bench command for the current slow paths:
     - stale `CollectBuyMints` convergence
     - bounded `Replay`
     - post-seed builder replay, where applicable outside server-only tooling
   - the goal is to measure cursor advance, rows processed, and phase timings without using the production clone as the primary debugger
   - required evidence:
     - one documented command runs locally against fixture data
     - output includes enough timing / progress detail to compare before vs after changes
   - immediate assignment focus:
     - prefer one narrow first slice that produces a single documented local command and deterministic output for the current stale `CollectBuyMints` / bounded `Replay` bottlenecks
   - accepted on `2026-03-23`:
     - one standard local harness command now exists for the current narrow bottlenecks:
       - stale `CollectBuyMints` convergence on an exact carry-forward checkpoint
       - bounded `Replay` on a deterministic local noise fixture
     - the harness lives in `copybot-discovery` as a library module plus a small binary wrapper
     - the harness prints a stable JSON report with scenario metadata, phase, rows/pages processed, chunk count, cursor/progress markers, and timing fields
     - the harness runs entirely on local deterministic fixture data and does not depend on the production clone
   - accepted verification for Package C:
     - `cargo test -p copybot-discovery standard_harness_reports_both_discovery_bottleneck_scenarios -- --nocapture`
     - `cargo test -p copybot-discovery stale_collect_buy_mints_harness_exposes_new_tail_progress_markers -- --nocapture`
     - `cargo test -p copybot-discovery bounded_replay_harness_exposes_wallet_stats_or_phase_cursor_progress -- --nocapture`
     - `RUSTFLAGS='-Awarnings' cargo run -p copybot-discovery --quiet --bin discovery_perf_harness`
     - `cargo test -p copybot-discovery`
   - package status after acceptance:
     - accepted for the current Package C scope
     - Package A remains closed
     - Package B remains closed
   - next package to assign:
     - Package D
     - add one compact operator-visible discovery status surface so the current runtime / restart / rebuild state can be understood without reading logs by hand

4. Package D. Improve operator-visible discovery status without opening logs by hand
   - add one script or binary mode that summarizes the current discovery runtime state from SQLite/runtime state into a compact operator view
   - minimum required fields:
     - runtime mode
     - scoring source
     - `active_follow_wallets`
     - latest publication timestamp / age
     - persisted rebuild phase if present
     - bounded rebuild or recovery cursor if present
   - required evidence:
     - output clearly distinguishes:
       - healthy runtime truth
       - degraded fallback truth
       - fail-closed with rebuild in progress
       - offline aggregate recovery progress
   - accepted on `2026-03-23`:
     - one compact operator-visible discovery status surface now exists as a dedicated `copybot-discovery` binary backed by a small library classifier
     - the command reads persisted/runtime state directly from SQLite and does not depend on log scraping
     - output exposes current runtime truth, publication truth, persisted bounded rebuild state, and offline aggregate recovery state in one operator view
     - the output distinguishes healthy runtime truth, degraded recent-publication fallback, fail-closed with rebuild in progress, and separate offline aggregate recovery progress
   - accepted verification for Package D:
     - `cargo test -p copybot-discovery --bin discovery_status -- --nocapture`
     - `cargo test -p copybot-discovery`
     - `git diff --check`
   - package status after acceptance:
     - accepted for the current Package D scope
     - Package A remains closed
     - Package B remains closed
     - Package C remains closed
   - follow-up package landed:
     - Package E
     - post-recovery cutover checks are now exposed directly without wiring aggregate coverage back into runtime selection

5. Package E. Prepare the post-recovery cutover without making recovery the architecture
   - codify the exact checks that must pass before any future re-enable step:
     - healthy publication from runtime discovery
     - `active_follow_wallets > 0`
     - no false `healthy`
     - clear separation between runtime publication truth and offline recovery state
   - this package may add helper checks, assertions, or status surfaces, but must not wire aggregate readiness back into runtime selection
   - required evidence:
     - code or scripts expose those checks directly
     - no new runtime branch depends on aggregate coverage markers
   - accepted on `2026-03-23`:
     - one dedicated read-only cutover readiness surface now exists as a `copybot-discovery` binary backed by a small classifier
     - the command returns one explicit verdict, `ready` or `not_ready`, plus blocker reasons and separated fact sections for `runtime_truth`, `publication_truth`, and `offline_recovery`
     - offline aggregate recovery is used only as an operator readiness signal and is not wired back into runtime truth or runtime selection
     - the cutover view makes it explicit that “offline recovery progress” is not the same thing as “healthy runtime truth”
   - accepted verification for Package E:
     - `cargo test -p copybot-discovery --bin discovery_cutover_readiness -- --nocapture`
     - `cargo test -p copybot-discovery`
     - `RUSTFLAGS='-Awarnings' cargo run -p copybot-discovery --quiet --bin discovery_cutover_readiness -- --config <temp-fixture> --now 2026-03-23T12:00:00Z`
     - `git diff --check`
   - package status after acceptance:
     - accepted for the current Package E scope
     - Package A remains closed
     - Package B remains closed
     - Package C remains closed
     - Package D remains closed
     - the current A-E parallel workstream is complete for its planned package set

Review contract for every slice in this workstream:

1. The coder must state which package the slice belongs to.
2. Every slice must include targeted tests or a deterministic local verification command.
3. I will reject any slice that:
   - reintroduces aggregate recovery as runtime truth
   - relies on prod-only validation to prove semantics
   - adds heavy startup work to paper over unclear state ownership
   - mixes unrelated ops cleanup with runtime behavior changes
4. Preferred slice size is one package or one narrow sub-slice at a time.
5. Each accepted slice must close with:
   - files changed
   - tests/commands run
   - which review-contract bullets it satisfies

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

Operator surface:

1. `discovery_wallet_freshness_audit` is the exact Stage 3 runtime check:
   it compares exact publication truth, exact active follow truth, and current raw-truth top-N on the same bounded scoring window without reopening aggregate/offline recovery as runtime truth.

Acceptance update (`2026-03-25`):

1. The first Stage 3 operator surface is now landed in code:
   - shared classifier / comparator logic lives in
     `crates/discovery/src/wallet_freshness_audit.rs`
   - runnable operator command lives in
     `crates/discovery/src/bin/discovery_wallet_freshness_audit.rs`
2. The surface now reports one explicit verdict:
   - `fresh_current`
   - `drifting_but_acceptable`
   - `stale_publication_truth`
   - `insufficient_raw_truth`
   - `fail_closed_no_publication_truth`
3. The accepted correctness follow-up for this Stage 3 slice is also landed:
   - stale raw tail no longer counts as “current raw truth”
   - `fail_closed` publication state with a preserved exact published set is
     audited as exact publication truth rather than being downgraded to
     “no publication truth”
4. Accepted verification for this Stage 3 slice:
   - `cargo test -p copybot-discovery --bin discovery_wallet_freshness_audit`
   - `cargo test -p copybot-discovery --lib wallet_freshness_audit -- --nocapture`
5. What this closes:
   - operators now have one exact runtime-facing command to compare:
     - exact publication truth
     - exact active follow truth
     - current raw-truth top-N
   - the project no longer needs log archaeology to answer “do the currently
     selected wallets still look current?”
6. The multi-cycle Stage 3 validation layer is now landed in code:
   - persisted point-in-time captures:
     `crates/discovery/src/bin/discovery_wallet_freshness_capture.rs`
     retained only for explicit manual/debug spot checks
   - recent-history verdict over persisted captures:
     `crates/discovery/src/bin/discovery_wallet_freshness_report.rs`
   - these persist exact publication truth vs active follow vs current raw
     top-N, plus shadow-signal evidence for the current selected wallets
7. New exact operator commands:
   - primary read path:
     `discovery_wallet_freshness_report --config <live.server.toml> --limit 5`
   - manual/debug spot-check only:
     `discovery_wallet_freshness_capture --config <live.server.toml>`
8. What remains before Stage 4:
   - run the Stage 3 history captures on the live runtime path for several
     cycles
   - require the recent-history verdict to validate the live selection as
     current before revisiting execution activation
9. What this still does not close by itself:
   - Stage 3 live validation itself still has to be accumulated on the live
     runtime over several captures
   - Stage 4 execution activation remains blocked until that live history says
     the selected wallets are truly current and alive
10. Accepted correctness follow-up for the history layer is also landed:
    - stale persisted captures are now excluded from the validation verdict
    - `discovery_wallet_freshness_report` is recent-cycle-aware via an explicit
      recency horizon, so old stored captures remain historical evidence but no
      longer validate the current selection by themselves
11. Accepted verification for the Stage 3 history layer:
    - `cargo test -p copybot-discovery --lib wallet_freshness_audit -- --skip quality_cache::tests::resolve_token_quality_for_mints_returns_error_on_fatal_cache_write_failure`
    - `cargo test -p copybot-discovery --bin discovery_wallet_freshness_capture`
    - `cargo test -p copybot-discovery --bin discovery_wallet_freshness_report`
12. Practical meaning of the new Stage 3 surfaces:
    - point-in-time freshness is now checked by `discovery_wallet_freshness_audit`
    - multi-cycle validation now accumulates in-band during the normal
      refresh/publication path and is read by:
      - `discovery_wallet_freshness_report`
    - `discovery_wallet_freshness_capture` remains only as a manual/debug deep
      spot-check command
    - Stage 4 should not be revisited until recent live captures, inside the
      explicit recency horizon, validate the current published selection
13. Accepted operational follow-up for Stage 3 evidence accumulation:
    - the primary accumulation path is now in-band inside the normal
      discovery refresh/publication cycle in `solana-copy-bot.service`
    - each publish-due refresh reuses already computed exact publication truth,
      active follow truth, current raw top-N, and exact selected-wallet
      shadow/raw evidence, then appends one persisted Stage 3 capture
    - this replaces the standalone timer as the primary operational path,
      because the standalone job duplicated an expensive raw-truth build against
      the live runtime DB
    - operators should inspect in-band capture accumulation in
      `journalctl -u solana-copy-bot.service`, especially:
      `wallet_freshness_capture_state`,
      `wallet_freshness_capture_reason`,
      `wallet_freshness_capture_id`,
      `wallet_freshness_capture_captured_at`
    - operators should validate recent Stage 3 evidence with:
      `discovery_wallet_freshness_report --config <live.server.toml> --limit 5`
    - the standalone systemd capture service/timer were removed after the
      in-band architecture was accepted
    - the standalone manual/debug command remains available for explicit
      spot checks with:
      `discovery_wallet_freshness_capture --config <live.server.toml> --recent-cycles 1 --shadow-evidence-lookback-seconds 960 --json`
    - this Stage 3 evidence path still does not change `execution.enabled`,
      restore, gap-fill, snapshot, or scoring behavior
14. Live rollout status for the in-band Stage 3 path (`2026-03-25`):
    - commit `b279c4e` was rolled out to the live server
    - the primary Stage 3 accumulation path is now the in-band refresh /
      publication cycle inside `solana-copy-bot.service`
    - the old standalone wallet-freshness capture timer/service are no longer
      part of the primary accumulation architecture
15. The rollout exposed a separate operational issue on the live host:
    - `/var/www/solana-copy-bot/state` had reached `100%` usage
    - the immediate failure mode was SQLite WAL startup failure on
      `sqlite_pragma_journal_mode_wal`
    - root cause was archive growth under
      `/var/www/solana-copy-bot/state/discovery_restore/recent_raw`
      rather than the Stage 3 in-band capture logic itself
16. Emergency operator action taken on the live server:
    - recent-raw archive history was pruned while preserving the `latest`
      snapshot surface and a short tail of recent archives
    - the live runtime DB surface was restored and `solana-copy-bot.service`
      was brought back to `active`
    - live `journal_snapshot_retention` was reduced from `144` to `24` to
      prevent the same disk-full failure mode from recurring on the current
      host
    - the follow-up repo fix now stages recent-raw archive promotion under temp
      names, enforces full `{sqlite,json,wal,shm}` retention on every scheduled
      invocation, and raises the service outer timeout to `10min` so systemd no
      longer kills the finalize/prune phase mid-run
17. Current Stage 3 live status after recovery:
    - `discovery_wallet_freshness_report --config /etc/solana-copy-bot/live.server.toml --json --limit 10`
      now shows that in-band evidence is accumulating on the live path:
      - `captures_loaded = 7`
      - `captures_within_recent_horizon = 7`
      - first persisted capture:
        `2026-03-25T17:44:01.543908175+00:00`
      - latest persisted capture:
        `2026-03-25T18:48:01.541388757+00:00`
      - current verdict still remains:
        - `insufficient_evidence`
        - `reason = recent_captures_include_missing_publication_truth`
    - this means the Stage 3 plumbing is now live and honest, but discovery
      truth itself is still fail-closed / incomplete until the raw window fully
      covers the required scoring horizon
18. Stage 3 evidence footprint and interference check (`2026-03-25`):
    - persisted Stage 3 evidence is currently tiny relative to the live runtime
      DB:
      - `discovery_wallet_freshness_history` row count: `7`
      - approximate JSON payload: `18.75 KiB`
      - SQLite table footprint via `dbstat`: `32.0 KiB`
    - recent raw accumulation continues in parallel while in-band capture is
      enabled:
      - `observed_swaps` advanced from:
        - `rowid = 10151243`
        - `ts = 2026-03-25T18:52:27.697467155+00:00`
        - `slot = 408823514`
      - to:
        - `rowid = 10155054`
        - `ts = 2026-03-25T18:53:02.879398891+00:00`
        - `slot = 408823587`
      - over roughly `35s`, i.e. `+3811` rows while the service continued to
        log normal discovery cycles and in-band capture cadence
    - practical reading:
      - Stage 3 evidence storage is negligible
      - it is not the thing filling disk
      - current discovery/raw accumulation is still progressing alongside it

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

Operator note:

- Stage 4 preparation now has a dedicated readiness/preflight surface:
  `copybot_execution_readiness_audit --config /etc/solana-copy-bot/live.server.toml --json`
- This command does not enable execution and does not submit real trades.
- It separates static contract validity from live reachability and answers
  whether the current execution-side wiring is ready for a later dry-run
  rehearsal or still blocked on adapter / signer / route / policy wiring.

Acceptance update (`2026-03-25`):

1. The first Stage 4 preparation surface is now landed in code:
   - runnable operator command:
     `crates/app/src/bin/copybot_execution_readiness_audit.rs`
   - shared static contract classifier:
     `crates/app/src/config_contract.rs`
2. The audit now separates:
   - `config_valid`
   - `connectivity_valid`
   - `adapter_contract_valid`
   - `signer_contract_valid`
   - `policy_contract_valid`
   - `route_contract_valid`
   - `ready_for_dry_run`
   - `blocked_for_activation`
3. The audit uses a safe adapter `action=simulate` probe and RPC reachability
   checks only:
   - it does not enable `execution.enabled`
   - it does not submit real trades
4. Accepted verification for this Stage 4 preparation slice:
   - `cargo test -p copybot-app --bin copybot_execution_readiness_audit`
   - `cargo test -p copybot-app validate_execution_runtime_contract -- --nocapture`
5. Practical meaning:
   - while Stage 3 accumulates live evidence, the team can now validate
     execution-side wiring explicitly instead of guessing from ad-hoc shell
     checks
   - this still does not authorize activation; Stage 3 remains the gate before
     any future tiny-live rehearsal discussion

Acceptance update (`2026-03-25`, Stage 4 dry-run rehearsal trail):

1. Stage 4 now also has a persisted dry-run rehearsal surface:
   - run and persist one safe rehearsal:
     `copybot_execution_dry_run_rehearsal --config /etc/solana-copy-bot/live.server.toml --json`
   - inspect recent rehearsal trail:
     `copybot_execution_dry_run_rehearsal --config /etc/solana-copy-bot/live.server.toml --history --limit 10 --json`
2. The rehearsal stays pre-activation only:
   - `execution.enabled` remains unchanged
   - no real trades are submitted
   - only RPC preflight/read checks and adapter `action=simulate` are used
3. Persisted rehearsal history now records:
   - deterministic intent summary (`route`, `token`, `notional_sol`)
   - exact route/policy envelope from current execution config
   - RPC preflight result and adapter simulate classification
   - policy echo result, blockers, warnings, and overall verdict
4. Accepted verification for this Stage 4 slice:
   - `cargo test -p copybot-app --bin copybot_execution_dry_run_rehearsal`
   - `cargo test -p copybot-app --bin copybot_execution_readiness_audit`
5. Practical meaning:
   - while Stage 3 is still accumulating live discovery evidence, the team can
     now accumulate execution-side dry-run rehearsal evidence without ad-hoc
     shell archaeology
   - good rehearsal history still does not override the Stage 3 discovery gate

Acceptance update (`2026-03-25`, consolidated pre-activation gate):

1. Operators now have a single planning-safe gate surface:
   - `copybot_pre_activation_gate_report --config /etc/solana-copy-bot/live.server.toml --json`
2. This command reuses existing truth instead of inventing a fourth ad-hoc
   decision path:
   - Stage 3 recent-cycle truth from `discovery_wallet_freshness_report`
   - Stage 4 readiness truth from `copybot_execution_readiness_audit`
   - Stage 4 persisted rehearsal history from
     `copybot_execution_dry_run_rehearsal --history`
   - explicit tiny-live bounded policy truth from
     `copybot_tiny_live_policy_audit`
3. Top-level verdict semantics are now explicit:
   - `pre_activation_gates_green`
   - `blocked_by_stage3`
   - `blocked_by_stage4_readiness`
   - `blocked_by_dry_run_history`
   - `blocked_by_tiny_live_policy`
   - `insufficient_recent_evidence`
4. Hierarchy remains strict:
   - Stage 3 stays the primary gate
   - Stage 4 cannot override a non-green Stage 3
   - dry-run rehearsal history and tiny-live policy boundedness are additional
     lower-layer blockers, not overrides
   - `pre_activation_gates_green` is still planning-safe only and does not
     enable execution by itself
5. Accepted verification for this consolidation slice:
   - `cargo test -p copybot-app --bin copybot_pre_activation_gate_report`
   - `cargo test -p copybot-app --bin copybot_execution_readiness_audit`
   - `cargo test -p copybot-app --bin copybot_execution_dry_run_rehearsal`

Acceptance update (`2026-03-25`, tiny-live policy audit package):

1. Stage 4 preparation now also has an explicit bounded policy surface:
   - `copybot_tiny_live_policy_audit --config /etc/solana-copy-bot/live.server.toml --json`
2. This command is still pre-activation only:
   - `execution.enabled` remains unchanged
   - no real trades are submitted
   - no Stage 3 truth is used as a substitute for execution-side boundedness
3. The audit compares the current `execution` / `risk` / `shadow` envelope
   against an explicit `[tiny_live_policy]` block in config, instead of
   relying on hidden repo defaults.
4. Important verdict semantics are now explicit:
   - `tiny_live_policy_bounded`
   - `tiny_live_policy_too_open`
   - `tiny_live_policy_incomplete`
   - `tiny_live_policy_route_risk_unbounded`
   - `tiny_live_policy_fee_risk_unbounded`
5. Practical meaning:
   - later tiny-live discussion can rely on an explicit bounded policy
     contract, not ad-hoc operator judgment
   - this still does not authorize activation and does not override the Stage 3
     discovery gate

Acceptance update (`2026-03-26`, tiny-live activation plan package):

1. Stage 4 preparation now also has a planning-only activation package:
   - `copybot_tiny_live_activation_plan --config /etc/solana-copy-bot/live.server.toml --json`
2. The command reuses accepted truth surfaces instead of creating a parallel
   activation checklist:
   - `copybot_pre_activation_gate_report`
   - `copybot_tiny_live_policy_audit`
   - `copybot_tiny_live_guardrail_audit`
   - current execution/risk/shadow config truth
3. It renders an explicit bounded future activation overlay plus an explicit
   rollback delta back to the current safe state, plus the future
   rollback-trigger envelope from the accepted guardrail audit. It does not
   enable `execution.enabled`, write the live config, restart services, or
   submit trades.
4. Important top-level verdicts:
   - `activation_plan_ready_when_stage_gate_allows`
   - `blocked_by_pre_activation_gate`
   - `blocked_by_policy_contract`
   - `blocked_by_guardrail_contract`
   - `activation_overlay_incomplete`
   - `rollback_plan_incomplete`
   - `service_restart_contract_incomplete`
5. Checks:
   - `cargo test -p copybot-app --bin copybot_tiny_live_activation_plan`
   - `cargo test -p copybot-app --bin copybot_pre_activation_gate_report`
   - `cargo test -p copybot-app --bin copybot_tiny_live_guardrail_audit`

Acceptance update (`2026-03-26`, tiny-live guardrail package):

1. Stage 4 preparation now also has a planning-only guardrail surface:
   - `copybot_tiny_live_guardrail_audit --config /etc/solana-copy-bot/live.server.toml --json`
2. This command defines the future post-activation rollback/monitoring
   envelope explicitly without enabling execution or sending trades.
3. It is intentionally separate from the bounded tiny-live activation policy:
   - policy answers what bounded future activation envelope is acceptable
   - guardrails answer which live failure/degradation conditions must force
     rollback
4. Important verdicts:
   - `tiny_live_guardrails_bounded`
   - `tiny_live_guardrails_incomplete`
   - `tiny_live_guardrails_too_open`
   - `tiny_live_guardrails_rollback_contract_incomplete`
   - `tiny_live_guardrails_monitoring_contract_incomplete`
5. Checks:
   - `cargo test -p copybot-config --lib`
   - `cargo test -p copybot-app --bin copybot_tiny_live_guardrail_audit`

Acceptance update (`2026-03-25`, devnet dress-rehearsal package):

1. Stage 4 now also has a first-class non-production dress-rehearsal surface:
   - run and persist one devnet rehearsal:
     `copybot_devnet_dress_rehearsal --config /etc/solana-copy-bot/devnet.server.toml --route jito --token So11111111111111111111111111111111111111112 --notional-sol 0.01 --json`
   - inspect recent persisted devnet rehearsal trail:
     `copybot_devnet_dress_rehearsal --config /etc/solana-copy-bot/devnet.server.toml --history --limit 10 --json`
2. The command is hard-guarded against production-like config profiles:
   - it refuses production-like `system.env` values
   - it uses `execution.rpc_devnet_http_url` as the rehearsal RPC target
   - it now also requires explicit non-production adapter endpoints in
     `execution.submit_adapter_devnet_http_url` /
     `execution.submit_adapter_devnet_fallback_http_url`, and refuses any
     devnet endpoint that reuses the normal adapter URL set
   - the same refusal contract applies to `--history`
   - it does not enable `execution.enabled` and does not submit real trades on
     production
3. The devnet package reuses accepted Stage 4 truth instead of inventing new
   execution logic:
   - readiness/preflight truth from `copybot_execution_readiness_audit`
   - bounded policy truth from `copybot_tiny_live_policy_audit`
   - safe simulate/preflight execution truth from
     `copybot_execution_dry_run_rehearsal`
4. Persisted history is explicitly environment-labeled, so non-production dress
   rehearsal evidence does not leak into the production pre-activation trail.
5. Practical meaning:
   - while Stage 3 keeps accumulating live production evidence, the team can now
     rehearse the accepted execution-side contract on non-production
     infrastructure
   - a green devnet dress rehearsal still does not authorize production
     activation and does not override the Stage 3 gate

Acceptance update (`2026-03-26`, devnet activation-and-rollback drill package):

1. Stage 4 now also has a first-class non-production drill over the accepted
   bounded launch dossier:
   - run and persist one drill:
     `copybot_devnet_activation_drill --config /etc/solana-copy-bot/devnet.server.toml --route jito --token So11111111111111111111111111111111111111112 --notional-sol 0.01 --json`
   - inspect recent persisted drill history:
     `copybot_devnet_activation_drill --config /etc/solana-copy-bot/devnet.server.toml --history --limit 10 --json`
2. The drill stays hard-guarded against production-like profiles and does not
   touch production activation state.
3. It reuses the accepted bounded launch dossier instead of inventing another
   activation checklist:
   - `copybot_tiny_live_activation_plan`
   - `copybot_devnet_dress_rehearsal`
   - `copybot_tiny_live_guardrail_audit`
4. It applies the activation overlay only to a derived non-prod config, then
   validates the rollback overlay back to the original safe-mode contract and
   persists explicit activation/rollback drill history with environment labels.
5. Important verdicts:
   - `devnet_activation_drill_green`
   - `devnet_activation_drill_blocked_by_launch_dossier`
   - `devnet_activation_drill_blocked_by_non_prod_contract`
   - `devnet_activation_drill_blocked_by_guardrails`
   - `devnet_rollback_drill_failed`
   - `devnet_activation_drill_refused_for_prod_profile`
6. Checks:
   - `cargo test -p copybot-app --bin copybot_devnet_activation_drill`

Acceptance update (`2026-03-26`, consolidated devnet readiness report):

1. Stage 4 non-production evidence now also has a single consolidated read-only
   operator surface:
   - `copybot_devnet_readiness_report --config /etc/solana-copy-bot/devnet.server.toml --json`
2. The command is non-prod only, reuses persisted drill history, and does not
   rerun heavy rehearsal logic by default.
3. It summarizes two accepted evidence layers together:
   - recent `copybot_devnet_dress_rehearsal` history
   - recent `copybot_devnet_activation_drill` history
4. Important top-level verdicts:
   - `devnet_readiness_green`
   - `devnet_readiness_insufficient_recent_evidence`
   - `devnet_readiness_blocked_by_dress_rehearsal_history`
   - `devnet_readiness_blocked_by_activation_drill_history`
   - `devnet_readiness_stale_history`
   - `devnet_readiness_refused_for_prod_profile`
5. Practical meaning:
   - operators no longer need to manually join separate non-prod history
     surfaces
   - stale or missing non-prod evidence can no longer look green by accident
   - a green non-prod readiness report still does not authorize production
     activation and does not override the Stage 3 gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_devnet_readiness_report`

Acceptance update (`2026-03-26`, final activation checklist report):

1. The repo now also has one final production-facing synthesis surface for
   later tiny-live discussions:
   - `copybot_activation_checklist_report --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --json`
2. This command is still read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not rerun heavy drills by default
   - it does not submit trades
3. It reuses accepted surfaces instead of inventing yet another activation
   decision path:
   - `copybot_pre_activation_gate_report`
   - `copybot_tiny_live_activation_plan`
   - `copybot_tiny_live_guardrail_audit`
   - `copybot_devnet_readiness_report`
4. Important top-level verdicts:
   - `activation_checklist_blocked_by_prod_stage3`
   - `activation_checklist_blocked_by_prod_gate`
   - `activation_checklist_blocked_by_launch_dossier`
   - `activation_checklist_blocked_by_non_prod_readiness`
   - `activation_checklist_discussion_ready_but_not_authorized`
   - `activation_checklist_refused_for_prod_profile_mismatch`
5. Practical meaning:
   - operators no longer need to manually stitch prod and non-prod evidence
     together before future activation discussion
   - stale or blocked non-prod evidence can no longer look discussion-ready by
     accident
   - even a discussion-ready verdict still does not authorize production
     activation and does not override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_checklist_report`
   - `cargo test -p copybot-app --bin copybot_devnet_readiness_report`
   - `cargo test -p copybot-app --bin copybot_tiny_live_activation_plan`

Acceptance update (`2026-03-26`, activation decision packet export):

1. The repo now also has one final archival-ready decision-packet export:
   - `copybot_activation_decision_packet --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --json --output /var/www/solana-copy-bot/state/activation_decision_packet/latest.json`
2. This command is still read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not rerun heavy drills by default
   - it does not submit trades
3. It reuses the accepted final checklist instead of inventing another
   approval path, and adds durable export semantics:
   - final checklist verdict / blockers / warnings
   - prod and non-prod config paths
   - execution state
   - nested prod/non-prod summaries
   - optional operator note
   - build/git metadata when available
   - redacted config fingerprints for later review
4. Important packet verdicts:
   - `decision_packet_blocked`
   - `decision_packet_discussion_ready_but_not_authorized`
   - `decision_packet_refused_for_profile_mismatch`
5. Practical meaning:
   - operators can now preserve the exact bounded decision state as one
     reviewable artifact instead of screenshots or ad-hoc shell notes
   - even a discussion-ready packet still does not authorize production
     activation and does not override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_decision_packet`
   - `cargo test -p copybot-app --bin copybot_activation_checklist_report`

Acceptance update (`2026-03-26`, activation runbook generator):

1. The repo now also has one final planning-only operator handoff surface:
   - `copybot_activation_runbook --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --json --output /var/www/solana-copy-bot/state/activation_runbook/latest.json --markdown-output /var/www/solana-copy-bot/state/activation_runbook/latest.md`
2. This command is still read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not rerun heavy drills by default
   - it does not submit trades
3. It reuses the accepted decision packet and launch dossier rather than
   inventing another decision layer, and produces a human-usable handoff
   artifact with:
   - preflight checks and explicit blockers
   - exact bounded activation overlay steps
   - post-change verification commands
   - rollback triggers and rollback procedure
   - explicit not-authorized disclaimer
4. Important runbook verdicts:
   - `runbook_blocked`
   - `runbook_discussion_ready_but_not_authorized`
   - `runbook_refused_for_profile_mismatch`
5. Practical meaning:
   - operators can now export both an archival decision packet and a
     human-usable runbook from the same accepted planning truth
   - even a discussion-ready runbook still does not authorize production
     activation and does not override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_runbook`
   - `cargo test -p copybot-app --bin copybot_activation_decision_packet`

Acceptance update (`2026-03-26`, activation decision history + diff surface):

1. The repo now also has one final artifact-analysis surface over exported
   activation decision packets:
   - history mode:
     `copybot_activation_decision_history_report --history-dir /var/www/solana-copy-bot/state/activation_decision_packet/archive --json`
   - diff mode:
     `copybot_activation_decision_history_report --compare /var/www/solana-copy-bot/state/activation_decision_packet/archive/older.json /var/www/solana-copy-bot/state/activation_decision_packet/archive/newer.json --json`
2. This command is still read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not mutate packet artifacts
   - it does not rerun heavy prod or non-prod drills by default
   - it does not submit trades
3. It works from exported decision-packet artifacts rather than re-running the
   underlying readiness logic, and now answers:
   - latest verdict progression over time
   - blocked vs discussion-ready packet counts
   - blocker additions/removals between packets
   - prod/non-prod config fingerprint drift
   - invalid artifact detection that cannot yield false green history
4. Important verdicts:
   - `decision_history_latest_blocked`
   - `decision_history_latest_discussion_ready`
   - `decision_history_insufficient_packets`
   - `decision_history_compare_ready`
   - `decision_history_invalid_artifact`
5. Practical meaning:
   - operators no longer need to diff packet JSON files by hand to understand
     readiness progression
   - malformed artifacts are now surfaced explicitly instead of being able to
     hide behind a false green latest packet
   - this is still artifact analysis only and does not authorize production
     activation or override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_decision_history_report`

Acceptance update (`2026-03-26`, activation artifact archive index + retention preview):

1. The repo now also has one read-only archive-management surface over
   exported activation artifacts:
   - index/report mode:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --json`
   - retention-plan mode:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --retention-plan --keep-latest 10 --json`
2. This command is still read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not rerun heavy prod or non-prod logic
   - it does not delete artifacts in this batch
   - it does not submit trades
3. It now gives operators a bounded archive-health and retention-preview
   surface:
   - packet/runbook archive indexing and pairing
   - malformed artifact detection
   - latest artifact summaries
   - safe retention preview over the latest N packet-backed generations
4. Important verdicts:
   - `archive_health_ok`
   - `archive_health_missing_pairings`
   - `archive_health_invalid_artifacts_present`
   - `archive_retention_plan_ready`
   - `archive_retention_plan_insufficient_artifacts`
5. Practical meaning:
   - operators no longer need to inspect activation artifact archives by hand
   - future cleanup can be added later on top of an already explicit preview
     surface instead of starting with direct deletion
   - this is still archive analysis only and does not authorize production
     activation or override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_archive`

Acceptance update (`2026-03-26`, activation artifact archive retention apply executor):

1. The same activation artifact archive surface now also has a bounded cleanup
   executor built on the already accepted preview logic:
   - preview:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --retention-plan --keep-latest 10 --json`
   - apply:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --retention-apply --keep-latest 10 --json`
2. Cleanup semantics are conservative by default:
   - invalid or malformed artifacts block cleanup
   - orphan runbook generations and orphan markdown are left untouched
   - only packet-backed generations outside the latest `keep-latest` set are
     removed
   - apply mode never touches files outside the requested archive dir
3. Important cleanup verdicts:
   - `archive_cleanup_applied`
   - `archive_cleanup_blocked_by_invalid_artifacts`
   - `archive_cleanup_nothing_to_do`
   - `archive_cleanup_failed_partial`
4. Practical meaning:
   - operators can now preview and then apply bounded archive cleanup through
     one first-class tool instead of manual file deletion
   - cleanup uses the exact same generation-selection logic as preview, so the
     plan and the apply step cannot silently diverge
   - this is still archive maintenance only and does not authorize production
     activation or override the Stage 3 prod gate
5. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_archive`

Acceptance update (`2026-03-26`, activation artifact manifest generator + verifier):

1. The repo now also has an explicit integrity layer over exported activation
   artifacts:
   - generate:
     `copybot_activation_artifact_manifest --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --generate-manifest --output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --json`
   - verify:
     `copybot_activation_artifact_manifest --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --verify-manifest /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --json`
2. Manifest contents are explicit and bounded:
   - artifact paths are stored relative to the archive root
   - packet/runbook artifact hashes use SHA-256
   - generation identity is anchored by decision-packet timestamp plus
     prod/non-prod config fingerprints
   - manifest metadata carries generation count, file count, and tool/build
     version for later review
3. Verify mode now detects:
   - missing artifact files
   - changed artifact hashes
   - unexpected extra recognized artifact files
   - generation membership drift
   - malformed manifest or invalid current archive state
4. Important manifest verdicts:
   - `artifact_manifest_generated`
   - `artifact_manifest_verified`
   - `artifact_manifest_drift_detected`
   - `artifact_manifest_invalid`
   - `artifact_manifest_missing_files`
5. Practical meaning:
   - operators can now snapshot activation artifact integrity and later verify
     archive drift/corruption explicitly instead of relying on ad hoc parsing
   - this still does not enable production execution, authorize activation, or
     override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_manifest`

Acceptance update (`2026-03-26`, activation artifact bundle export + verifier):

1. The repo now also has a portable review/transfer layer for exactly one
   selected packet-backed activation artifact generation:
   - export:
     `copybot_activation_artifact_bundle --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --export-bundle --generation 2026-03-26T12:00:00Z --output /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
   - verify:
     `copybot_activation_artifact_bundle --verify-bundle /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
2. Export semantics are explicit and bounded:
   - exactly one packet-backed generation is selected by timestamp or full
     generation id
   - only that generation's decision packet, runbook json, and runbook
     markdown are copied into the bundle
   - the bundle carries its own metadata and SHA-256 file hash coverage
3. Verify mode now detects:
   - malformed bundle metadata
   - missing or tampered bundled files
   - unexpected extra bundled files
   - generation identity / membership drift inside the bundle
4. Important bundle verdicts:
   - `artifact_bundle_exported`
   - `artifact_bundle_verified`
   - `artifact_bundle_invalid`
   - `artifact_bundle_drift_detected`
   - `artifact_bundle_generation_not_found`
5. Practical meaning:
   - operators can now export one bounded activation generation as a portable
     review bundle and verify it elsewhere without copying the whole archive
   - this still does not enable production execution, authorize activation, or
     override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_bundle`

Acceptance update (`2026-03-26`, activation artifact provenance report):

1. The repo now also has a final provenance-oriented surface across the three
   accepted artifact layers:
   - archive generations
   - manifest files
   - bundle manifests
   Command:
   `copybot_activation_artifact_provenance_report --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --json`
2. Lineage is correlated by the accepted generation identity:
   - decision-packet timestamp
   - prod config fingerprint
   - non-prod config fingerprint
3. The report now makes it explicit:
   - which archive generations have manifest coverage
   - which archive generations have bundle coverage
   - which manifest or bundle references point to missing archive generations
   - which malformed lineage artifacts block a trustworthy provenance result
4. Important provenance verdicts:
   - `artifact_provenance_complete`
   - `artifact_provenance_incomplete`
   - `artifact_provenance_invalid_artifacts_present`
   - `artifact_provenance_inconsistent_lineage`
5. Practical meaning:
   - operators no longer need to mentally stitch together archive, manifest,
     and bundle worlds by hand
   - this is still artifact lineage only and does not authorize production
     activation or override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_provenance_report`

Acceptance update (`2026-03-26`, activation artifact publish pipeline):

1. The repo now also has a one-shot publish pipeline for one complete review
   generation:
   `copybot_activation_artifact_publish --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --bundle-output-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
2. This command is still bounded and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not delete or rewrite unrelated archive generations
   - it does not submit trades
3. In one pass it now reuses the accepted artifact chain to:
   - export one decision packet
   - export one runbook json and markdown artifact
   - place them into a deterministic archive generation directory
   - optionally write a fresh archive manifest snapshot
   - optionally export a portable review bundle for that exact generation
4. Path safety is explicit:
   - generation directory naming is deterministic from packet timestamp plus
     prod/non-prod config fingerprints
   - existing generation targets are not silently overwritten
   - archive publish is blocked if the current archive state is already
     invalid
5. Important publish verdicts:
   - `artifact_publish_succeeded`
   - `artifact_publish_blocked_by_checklist`
   - `artifact_publish_blocked_by_invalid_archive_state`
   - `artifact_publish_partial_manifest_skipped`
   - `artifact_publish_failed`
6. Practical meaning:
   - operators no longer need to hand-orchestrate packet export, runbook
     export, archive placement, manifest snapshotting, and optional bundle
     export as separate steps
   - this is still artifact publication only and does not authorize
     production activation or override the Stage 3 prod gate
7. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_publish`

Acceptance update (`2026-03-26`, activation artifact channel/latest-pointer manager):

1. The repo now also has an explicit channel/latest-pointer surface over
   published activation artifacts:
   - report:
     `copybot_activation_artifact_channel --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --report --json`
   - promote:
     `copybot_activation_artifact_channel --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --promote --generation 2026-03-26T12:00:00+00:00|prod_fp|non_prod_fp --allow-overwrite --json`
   - verify:
     `copybot_activation_artifact_channel --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --verify --json`
2. Channel state is explicit JSON metadata under `--channel-dir`:
   - default channel name is `current_review`
   - selected generation identity is recorded by decision-packet timestamp plus
     prod/non-prod config fingerprints
   - packet/runbook paths and optional manifest/bundle references are stored as
     explicit metadata rather than hidden pointer conventions
3. Promote mode is bounded and conservative:
   - it refuses to point at a non-existent packet-backed generation
   - it refuses to point through invalid archive state
   - it does not silently overwrite existing channel metadata without
     `--allow-overwrite`
   - it does not rewrite archive generations or delete anything
4. Verify mode now detects:
   - missing target generations
   - missing packet/runbook files
   - invalid channel metadata
   - manifest/bundle reference drift when those references are present
5. Important channel verdicts:
   - `artifact_channel_ok`
   - `artifact_channel_missing_target`
   - `artifact_channel_inconsistent`
   - `artifact_channel_promoted`
   - `artifact_channel_refused_without_overwrite`
   - `artifact_channel_invalid_metadata`
6. Practical meaning:
   - operators no longer need to edit JSON by hand to declare which review
     generation is the current/latest one under discussion
   - channel verify now gives one explicit trust check over that latest review
     pointer without touching production execution state
   - this is still artifact management only and does not authorize production
     activation or override the Stage 3 prod gate
7. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_channel`

Acceptance update (`2026-03-26`, activation artifact release flow):

1. The repo now also has one bounded artifact release flow over publish plus
   optional channel promotion:
   - publish only:
     `copybot_activation_artifact_release --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --bundle-output-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
   - publish plus promote:
     `copybot_activation_artifact_release --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --bundle-output-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --promote-channel --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --allow-channel-overwrite --json`
2. The new flow reuses the accepted publish and channel surfaces rather than
   duplicating artifact logic:
   - it writes at most one new packet-backed review generation
   - it may optionally promote `current_review` channel metadata under explicit
     `--channel-dir`
   - it does not rewrite unrelated archive generations or delete anything
3. Partial-success semantics stay explicit:
   - if publish does not complete cleanly, channel promotion is not attempted
   - if publish succeeds but channel promotion is blocked, the report remains
     non-green while still surfacing the published generation
   - no raw post-publish channel error is allowed to masquerade as a clean
     release
4. Important release verdicts:
   - `artifact_release_published`
   - `artifact_release_published_and_promoted`
   - `artifact_release_publish_failed`
   - `artifact_release_channel_promote_blocked`
   - `artifact_release_failed`
5. Practical meaning:
   - operators no longer need to run publish and channel promotion as separate
     manual steps
   - this is still artifact workflow only and does not authorize activation or
     override the Stage 3 prod gate
6. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_release`

Acceptance update (`2026-03-26`, activation artifact release history ledger):

1. The repo now also has one read-only history and diff surface over exported
   activation artifact release reports:
   - history summary:
     `copybot_activation_artifact_release_history --history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --json`
   - compare mode:
     `copybot_activation_artifact_release_history --compare /var/www/solana-copy-bot/state/activation_artifacts/releases/release-older.json /var/www/solana-copy-bot/state/activation_artifacts/releases/release-newer.json --json`
2. The new surface is built on persisted release artifacts rather than rerunning
   publish, channel, checklist, or drill flows:
   - it summarizes publish-only vs published-and-promoted releases
   - it shows when channel promotion stayed blocked
   - it shows how the current review generation changed over time
3. Invalid or malformed release artifacts stay blocking:
   - they are surfaced explicitly as invalid inputs
   - they do not yield false healthy release history
4. Practical meaning:
   - operators no longer need to diff release JSON by hand to understand
     publish/promote progression
   - this is still artifact/release analysis only and does not authorize
     activation or override the Stage 3 prod gate
5. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_release_history`

Acceptance update (`2026-03-26`, activation release artifact archive publisher):

1. The repo now also has a deterministic persisted archive flow for release
   artifacts themselves:
   - publish one persisted release artifact:
     `copybot_activation_artifact_release_publish_report --publish --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --json`
   - publish plus update latest pointer:
     `copybot_activation_artifact_release_publish_report --publish --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --persist-latest-pointer --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --allow-latest-pointer-overwrite --json`
   - verify latest pointer:
     `copybot_activation_artifact_release_publish_report --verify-latest --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
2. The new surface is built on the accepted release and release-history logic
   instead of inventing another release parser:
   - persisted release artifacts are valid inputs for
     `copybot_activation_artifact_release_history`
   - latest pointer verification reuses the same backward-compatible release
     artifact parsing contract
3. Operational meaning:
   - operators no longer need ad hoc paths or ad hoc latest metadata for release
     JSON outputs
   - latest release pointer is explicit, verifiable, and overwrite-safe
   - legacy artifacts with missing deterministic timestamps are surfaced
     honestly rather than hidden behind a false healthy pointer
4. This remains artifact/release management only:
   - it does not change review-generation archive contents
   - it does not enable execution
   - it does not mutate live config
   - it does not authorize activation or override the Stage 3 prod gate
5. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_release_publish_report`

Acceptance update (`2026-03-26`, activation release provenance report):

1. The repo now also has a provenance-oriented surface for the release side:
   - `copybot_activation_artifact_release_provenance_report --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --json`
2. The new surface correlates three release-side inputs without rerunning heavy
   prod/non-prod logic:
   - persisted release artifacts in the deterministic release archive
   - latest-pointer metadata and target verification
   - release history inputs from a history dir or explicit release artifact set
3. Operational meaning:
   - operators no longer need to mentally stitch together release archive,
     latest pointer, and release history coverage
   - dangling latest pointers, missing history coverage, malformed release
     artifacts, and legacy timestamp ambiguity are surfaced explicitly
   - ambiguous legacy timestamp lineage does not get a false clean-green
     provenance verdict
4. This remains release artifact analysis only:
   - it does not mutate release archive contents
   - it does not rewrite latest-pointer metadata
   - it does not enable execution or authorize activation
5. Checks:
   - `cargo test -p copybot-app --bin copybot_activation_artifact_release_provenance_report`

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

- Date: 2026-03-21
- Commit SHA: `70e959df677f35347fd25b2a1ed91481b6d90769`
- Stage / substep: `Stage 1 / production host stabilization after raw-bridge failure`
- Status: `completed`
- Code changed:
  - none in the binary; this was an operational stabilization step on the production host
- Tests run:
  - service stop / process audit on the production host
- Done:
  - `solana-copy-bot.service` was stopped and left inactive
  - `scoring_window_days` in `/etc/solana-copy-bot/live.server.toml` was reverted from `3` back to `5`
  - other config invariants remained unchanged:
    - `metric_snapshot_interval_seconds = 3600`
    - `scoring_aggregates_write_enabled = false`
    - `scoring_aggregates_enabled = false`
  - exact stabilization checks passed:
    - `systemctl is-active solana-copy-bot.service -> inactive`
    - no active `copybot-app`
    - no active `backfill_discovery_scoring`
    - no active `sqlite3 .*live_copybot`
- Acceptance criteria closed:
  - production is no longer wasting Yellowstone / gRPC tokens on a fail-closed non-publishing runtime
  - temporary raw-bridge config drift is removed
- Blocked:
  - discovery remains unavailable until the aggregate/backfill blocker is fixed and revalidated offline
- Next action:
  - deploy the aggregate/backfill checkpointability fix and rerun offline cold-clone seeded-reset validation

- Date: 2026-03-21
- Commit SHA: `70e959df677f35347fd25b2a1ed91481b6d90769`
- Stage / substep: `Stage 1 / emergency raw 3-day bridge after clearing stale persisted rebuild state`
- Status: `failed`
- Code changed:
  - none in this step on the binary; this was a live config + persisted-state operational experiment on the already-deployed runtime
- Tests run:
  - live restart on `solana-copy-bot.service`
  - live runtime observation through the decisive next bucket after fresh restart
- Done:
  - the stale persisted rebuild row was dumped before cleanup and showed the expected frozen old-window shape:
    - `phase = collect_buy_mints`
    - `window_start = 2026-03-18T14:41:17.794541987+00:00`
    - `metrics_window_start = 2026-03-18T14:00:00+00:00`
    - `prepass_rows_processed = 217344`
    - `prepass_pages_processed = 6697`
    - `replay_rows_processed = 0`
    - `replay_pages_processed = 0`
  - only `discovery_persisted_rebuild_state` was cleared:
    - `DELETE FROM discovery_persisted_rebuild_state WHERE id = 1;`
    - post-delete `SELECT COUNT(*) FROM discovery_persisted_rebuild_state; -> 0`
  - config remained intentionally narrowed for the bridge:
    - `scoring_window_days = 3`
    - `metric_snapshot_interval_seconds = 3600`
    - `scoring_aggregates_write_enabled = false`
    - `scoring_aggregates_enabled = false`
  - the restart fixed the stale-freeze branch exactly as intended:
    - `metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint` disappeared immediately after restart
    - first sample at `2026-03-21T16:11:56.825005Z` showed fresh `collect_buy_mints / fresh_scan`
    - `completed bounded discovery persisted observed_swaps prepass; switching to bounded token-quality resolution` appeared at `2026-03-21T17:54:37.322320Z`
    - `completed bounded discovery token-quality resolution; switching to replay` appeared at `2026-03-21T17:54:39.161897Z`
    - `rebuild_phase = replay` appeared at `2026-03-21T17:54:46.060821Z`
    - replay wallet-stats then advanced:
      - `2026-03-21T17:55:01.209681Z -> rebuild_replay_wallet_stats_rows_processed = 25075`
      - `2026-03-21T17:55:17.404683Z -> rebuild_replay_wallet_stats_rows_processed = 55257`
      - `2026-03-21T17:59:00.608178Z -> rebuild_replay_wallet_stats_rows_processed = 130568`
- Acceptance criteria closed:
  - the 3-day bridge was given a fair fresh-start test instead of being judged through stale persisted state from the prior 5-day run
  - stale persisted rebuild state is now proven to be a separate operational hazard that must be cleared when changing the scoring window
- Blocked:
  - the bridge still failed on the decisive next bucket:
    - latest decisive sample at `2026-03-21T19:11:35.899184Z` had returned to `collect_buy_mints / reconcile_new_tail`
    - `rebuild_replay_wallet_stats_complete = false`
    - `rebuild_replay_wallet_stats_rows_processed = 0`
    - `rebuild_replay_rows_processed = 0`
    - `rebuild_replay_sol_leg_access_path` still never appeared
    - `scoring_source` remained `raw_window_incomplete_no_recent_published_universe`
    - `active_follow_wallets = 0`
    - stale warning returned near the end of the decisive window
- Acceptance criteria remaining:
  - raw `Replay wallet-stats` would still need to finish durably before rollover and enter `SOL-leg` / publication to make raw the closing path
- Remaining risks:
  - this bridge can re-enter `Replay`, but still does not survive the next bucket as a durable path to publication
  - leaving the runtime up in this state continues to consume live ingestion resources without producing trusted output
- Next action:
  - stop treating the raw bridge as a viable closing track
  - stabilize the host by stopping the bot if it remains fail-closed
  - move engineering effort back to the aggregate/backfill blocker

- Date: 2026-03-21
- Commit SHA: `70e959df677f35347fd25b2a1ed91481b6d90769`
- Stage / substep: `Stage 1 / emergency raw 3-day bridge initial rollout`
- Status: `failed`
- Code changed:
  - none in this step on the binary; this was a live config-only operational bridge experiment
- Tests run:
  - live config rollout validation on `solana-copy-bot.service`
- Done:
  - live config was changed only by:
    - `scoring_window_days = 5 -> 3`
  - other invariants stayed unchanged:
    - `metric_snapshot_interval_seconds = 3600`
    - `scoring_aggregates_write_enabled = false`
    - `scoring_aggregates_enabled = false`
  - service restarted cleanly:
    - exact start timestamp `2026-03-21 14:41:12 UTC`
    - `MainPID = 90076`
    - `NRestarts = 0`
- Acceptance criteria closed:
  - the config-only bridge was validated under production runtime conditions instead of by arithmetic alone
- Blocked:
  - the bridge did not even reach `Replay` because runtime remained frozen behind stale persisted rebuild state from the old 5-day run:
    - first sample already showed `collect_buy_mints / reconcile_expired_head`
    - after two buckets `rebuild_phase = replay` still never appeared
    - `restart_reason = metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint` returned
    - all replay counters stayed `0`
  - no publication signals appeared:
    - no `raw_window_persisted_stream`
    - `active_follow_wallets = 0`
- Acceptance criteria remaining:
  - if the bridge was to be judged fairly, stale persisted rebuild state first had to be cleared and the runtime restarted fresh
- Remaining risks:
  - changing `scoring_window_days` without clearing stale persisted rebuild state can keep runtime pinned to the old window
- Next action:
  - dump the persisted rebuild row, clear only `discovery_persisted_rebuild_state`, and rerun the bridge from a fresh restart

- Date: 2026-03-21
- Commit SHA: `02f887a3a37ad57cf09578c9105d1f11d08744d8`
- Stage / substep: `Stage 1 / offline seeded-reset phase-1 seed install attempt`
- Status: `failed`
- Code changed:
  - none in this step on the live runtime; this was an offline cold-clone validation of the rebuilt `backfill_discovery_scoring` binary
- Tests run:
  - fresh cold clone creation while `solana-copy-bot.service` remained `inactive`
  - direct unbounded `seeded-reset --stop-after-seed-install` run on the clone
- Done:
  - a fresh cold clone was recreated successfully with the service still stopped
  - exact `SEED_START_TS` was derived from clone readiness:
    - `SEED_START_TS = 2026-03-16T13:45:58.613850438Z`
  - phase 1 was then run directly, without the bounded wrapper:
    - `stdbuf -oL -eL backfill_discovery_scoring ... --seeded-reset --stop-after-seed-install --mark-covered --batch-size 10000 --sleep-ms 0`
  - the process itself was alive and doing work:
    - CPU remained non-zero
    - `/proc/<pid>/io` kept growing
    - the clone DB was being read and written
- Acceptance criteria closed:
  - the team now has a clean separation between "bounded wrapper" behavior and the raw phase-1 seeded-reset install path
  - the lack of progress was confirmed against a fresh cold clone, not against a partial or live-contended clone
- Blocked:
  - phase 1 still did not reach any durable seeded-boundary milestone:
    - `/tmp/seed_install_offline.log` stayed empty
    - no `boundary_batch_buffered`
    - no `seed_boundary_installed`
    - clone post-state still showed:
      - `covered_since = null`
      - `backfill_resume_required = true`
      - `backfill_progress.start_ts = 2026-03-03T17:05:37Z`
      - no new seed-boundary marker
  - because phase 1 never reached a durable seed install, phase 2 was intentionally not started
- Acceptance criteria remaining:
  - land a first durable checkpoint before seed install
  - then land a durable `seed_boundary_installed`
- Remaining risks:
  - the aggregate blocker is now clearly before the first durable seed-boundary checkpoint, not just in the bounded wrapper
- Next action:
  - debug and fix the pre-seed boundary build path in code

- Date: 2026-03-21
- Commit SHA: `02f887a3a37ad57cf09578c9105d1f11d08744d8`
- Stage / substep: `Stage 1 / offline budget-aware bounded aggregate retry`
- Status: `failed`
- Code changed:
  - runtime-budget-aware bounded replay patch was built into `backfill_discovery_scoring`
- Tests run:
  - fresh cold clone creation while `solana-copy-bot.service` remained `inactive`
  - bounded wrapper rerun against the offline clone with the same exact persisted resume cursor
- Done:
  - repo on the server was updated to `02f887a3a37ad57cf09578c9105d1f11d08744d8`
  - `backfill_discovery_scoring` was rebuilt
  - old offline clone was deleted and recreated cold with the service still off
- Acceptance criteria closed:
  - the exact previous failure mode was re-tested after the runtime-budget patch instead of being inferred
- Blocked:
  - the exact offline bounded-resume scenario still repeated the old failure mode:
    - first run still lasted `42m+`
    - `001_backfill.log` stayed empty
    - `next_resume.env` stayed unchanged
    - final readiness still showed:
      - `covered_since = null`
      - `backfill_resume_required = true`
      - unchanged March 8 `backfill_progress` cursor
    - wrapper ended `verdict = run_failed`
- Acceptance criteria remaining:
  - a first durable checkpoint still had to land before any bounded recovery loop could be considered viable
- Remaining risks:
  - this was no longer credibly an operator-profile problem; the blocker had moved to the binary's pre-checkpoint execution path itself
- Next action:
  - separate the first seed-install attempt from the bounded wrapper and debug the pre-seed path directly

- Date: 2026-03-21
- Commit SHA: `a25c1e5fa1ab766d8f866520e6292c02fd7df361`
- Stage / substep: `Stage 1 / same-host live clone attempt under active production load`
- Status: `failed`
- Code changed:
  - none in this step on the live runtime; this was an operational clone attempt using the existing server state
- Tests run:
  - same-host `sqlite3 .backup` clone attempt while the production service was still running
- Done:
  - stale `.bak*` backup artifacts were deleted to free enough disk space for a same-host clone
  - the clone copy itself did start and the clone file grew as expected
- Acceptance criteria closed:
  - the original capacity blocker ("no room for a clone") was removed by deleting stale backup artifacts
- Blocked:
  - clone creation on the same host while the service was live materially degraded runtime pressure:
    - `observed_swap_writer_pending_requests = 4224`
    - `sqlite_busy_error_total = 469`
    - `sqlite_write_retry_total = 399`
    - host IO wait `wa = 90.5%`
  - the clone and partial clone file were intentionally aborted and deleted
- Acceptance criteria remaining:
  - any same-host clone path must not materially degrade live runtime pressure
- Remaining risks:
  - same-host hot clone is operationally unsafe even if there is enough disk space
- Next action:
  - do not repeat hot clone under live load
  - only perform cold clone with the service intentionally stopped

- Date: 2026-03-21
- Commit SHA: `a25c1e5fa1ab766d8f866520e6292c02fd7df361`
- Stage / substep: `Stage 1 / offline aggregate clone and first bounded recovery attempt`
- Status: `failed`
- Code changed:
  - none in this step on the live runtime; this was an offline operator attempt using the rebuilt aggregate recovery tooling
- Tests run:
  - cold offline clone creation on the production host with the service intentionally stopped
  - aggregate recovery wrapper run against the offline clone
- Done:
  - production repo was updated to `a25c1e5fa1ab766d8f866520e6292c02fd7df361`
  - release binaries `backfill_discovery_scoring` and `aggregate_readiness_status` were built successfully
  - live config remained stabilized:
    - `metric_snapshot_interval_seconds = 3600`
    - `scoring_aggregates_write_enabled = false`
    - `scoring_aggregates_enabled = false`
  - the production service was intentionally stopped and left off before clone work:
    - `systemctl is-active solana-copy-bot.service -> inactive`
  - a cold clone was created successfully at:
    - `/var/www/solana-copy-bot/state/live_copybot.aggregate_clone_offline_20260321.db`
  - source readiness captured the exact persisted resume lineage before offline work:
    - `backfill_progress.start_ts = 2026-03-03T17:05:37Z`
    - `backfill_progress.cursor.ts_utc = 2026-03-08T21:06:45.726139749Z`
    - `backfill_progress.cursor.slot = 405112624`
    - `backfill_progress.cursor.signature = 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d`
  - the wrapper report directory and artifacts were captured:
    - `/tmp/aggregate-backfill-loop-offline/summary.txt`
    - `/tmp/aggregate-backfill-loop-offline/final_aggregate_readiness_status.json` or equivalent final readiness JSON artifact
    - `/tmp/aggregate-backfill-loop-offline/next_resume.env`
- Acceptance criteria closed:
  - the team now has a safe way to create a cold SQLite clone on the same host, but only with the production service intentionally stopped
  - the exact source resume lineage was captured before offline recovery
  - the wrapper correctly did not invent new lineage or fake progress when the first offline run failed to reach a checkpoint
- Blocked:
  - the first offline recovery attempt used too-coarse bounded-run settings and failed before the first durable checkpoint:
    - `--batch-size 10000`
    - `--max-runtime-seconds 1800`
    - no first persisted progress update after `42m+`
    - `covered_since = null`
    - `materialization_gap_cursor = null`
    - `backfill_progress` remained unchanged at the original March 8 cursor
    - `next_resume.env` remained unchanged
    - wrapper ended `verdict = run_failed`
  - because the first durable checkpoint never landed, aggregate readiness did not advance:
    - `effective_writes_ready = false`
    - `effective_reads_ready = false`
- Acceptance criteria remaining:
  - land a first durable checkpoint on the offline clone
  - then continue bounded resumable progress until `covered_since != null`, `materialization_gap_cursor = null`, and `backfill_resume_required = false`
  - only after that re-evaluate any production tail catch-up or aggregate cutover step
- Remaining risks:
  - if the first offline checkpoint cannot be reached even with much smaller bounded slices, the blocker is no longer operator orchestration but a code-path problem before the first durable commit inside `backfill_discovery_scoring`
  - keeping the production service down for too long still has business cost, so offline recovery should now move in small deterministic slices instead of another coarse long run
- Next action:
  - keep the service down for now, keep the existing offline clone, and retry the first checkpoint as a direct micro-slice on that clone:
    - `--batch-size 250`
    - `--max-batches-per-run 1`
    - `--max-runtime-seconds 120`
  - if that lands a first durable checkpoint, continue with the operator loop using small bounded slices (for example `250 x 4`)
  - if even the micro-slice does not produce a first checkpoint, stop treating this as an operator-profile issue and debug `backfill_discovery_scoring` before the first commit

- Date: 2026-03-21
- Commit SHA: `70e959df677f35347fd25b2a1ed91481b6d90769`
- Stage / substep: `Stage 1 / live rollout validation of corrected wallet_activity_days fast path`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - deploy/startup stayed healthy on live:
    - `app runtime loop started` appeared at `2026-03-20T21:02:59.903726Z`
    - `sqlite_migrations_deferred` still emitted deferred `0039_observed_swaps_sol_leg_ts_index.sql`
    - `sqlite_migrations_apply` completed immediately with `applied = 0`
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - service stayed on the same `MainPID = 70143` overnight with `NRestarts = 0`
  - the corrected `wallet_activity_days` fast path was proven active on live:
    - first replay sample after deploy appeared at `2026-03-20T21:06:02.881379Z`
    - latest replay sample in the overnight window appeared at `2026-03-21T06:32:43.833556Z`
    - `rebuild_replay_wallet_stats_fast_path_pages_processed = 534`
    - `rebuild_replay_wallet_stats_fast_path_wallets_processed = 480,600`
    - `rebuild_replay_wallet_stats_fallback_pages_processed = 0`
    - `rebuild_replay_wallet_stats_fallback_wallets_processed = 0`
    - full observed replay window stayed on the fast path with zero fallback pages
  - replay remained alive and bounded through the window:
    - `rebuild_replay_wallet_stats_rows_processed = 19,803,724`
    - `rebuild_replay_wallet_stats_pages_processed = 621`
    - `rebuild_budget_exhausted_reason = time_budget`
    - no restart loop
    - no writer-death path
    - no `metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint`
- Acceptance criteria closed:
  - the corrected `wallet_activity_days` fast path is now validated on live and does not silently degrade into the raw fallback path
  - config invariants stayed aligned with the stabilized live state (`scoring_aggregates_write_enabled = false`, `scoring_aggregates_enabled = false`)
  - Stage 1 remains clearly beyond the old `CollectBuyMints -> Replay` blocker
- Blocked:
  - even with the corrected fast path, replay wallet-stats still did not finish:
    - `rebuild_replay_wallet_stats_complete = false`
    - `rebuild_replay_rows_processed = 0`
    - `rebuild_replay_sol_leg_access_path` is still not emitted
    - no `raw_window_persisted_stream`
    - `active_follow_wallets = 0`
  - `discovery_runtime_mode` remained `fail_closed`
  - `scoring_source` remained `raw_window_incomplete_no_recent_published_universe`
- Acceptance criteria remaining:
  - a future rollout must still emit `completed bounded replay wallet-stats prepass; switching to SOL-leg replay`
  - a future rollout must emit `rebuild_replay_sol_leg_access_path`
  - a future rollout must land `scoring_source = raw_window_persisted_stream`
  - a future rollout must land `active_follow_wallets > 0`
- Remaining risks:
  - the corrected fast path materially lowers one hot-path cost, but the raw replay wallet-stats prepass is still not sufficient to reach SOL-leg/publication inside the current live runtime contract
  - `discovery cycle still running, skipping scheduled trigger` continued through the night
  - `shadow risk infra stop activated` continued through the night
  - retry counters remained non-zero (`sqlite_busy_error_total = 261`, `sqlite_write_retry_total = 225`) even though they only crept slowly in the sampled window
- Next action:
  - stop treating raw replay micro-optimizations as the main solution path; keep the raw path only as a bridge and move the permanent-solution work to aggregate backfill / resume, with any further raw-path runtime experiments treated as temporary operational bridges only

- Date: 2026-03-20
- Commit SHA: `824c9174cbed61a28ec50dadedea121f7cf39720`
- Stage / substep: `Stage 1 / live rollout validation of aggregate discovery runtime cutover`
- Status: `failed`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - startup stayed healthy on live:
    - `app runtime loop started` appeared at `2026-03-20T14:45:14.355456Z`
    - `sqlite_migrations_deferred` still emitted deferred `0039_observed_swaps_sol_leg_ts_index.sql`
    - `sqlite_migrations_apply` completed immediately with `applied = 0`
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
  - the rollout correctly proved the aggregate readiness gate would not switch prematurely:
    - effective config on deploy was `metric_snapshot_interval_seconds = 3600`, `scoring_aggregates_write_enabled = true`, `scoring_aggregates_enabled = true`
    - `covered_since = null`
    - `backfill_resume_required = true`
    - `effective_reads_ready = false`
    - `scoring_source = aggregates` never appeared
  - no healthy publication landed:
    - `active_follow_wallets = 0`
    - `discovery_published = false`
- Acceptance criteria closed:
  - aggregate cutover is now proven to require completed historical backfill / readiness on live
  - simply enabling aggregate writes/reads in config does not switch runtime scoring when `effective_reads_ready = false`
- Blocked:
  - no new backfill progress was observed:
    - latest persisted backfill cursor remained `2026-03-08T21:06:45.726139749Z / 405112624 / 2wsWtL4...`
    - `covered_since` stayed unset
    - `materialization_gap_cursor = null`
  - runtime stayed on the old fail-closed raw path:
    - `scoring_source = raw_window_incomplete_no_recent_published_universe`
    - aggregate cutover never occurred
  - the rollout materially worsened live pressure:
    - `observed_swap_writer_pending_requests = 4096`
    - `sqlite_busy_error_total = 2700`
    - `sqlite_write_retry_total = 2025`
    - `sqlite_wal_size_bytes = 2084221512`
  - runtime regressed into a frozen stale-window loop:
    - repeated `restart_reason = metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint`
    - frozen `persisted_horizon_end = 2026-03-20 15:00:14.356172037 UTC`
    - stale `collect_buy_mints / reconcile_expired_head` persisted through the observed window
- Acceptance criteria remaining:
  - stabilize live by disabling aggregate writes/reads again
  - keep runtime on the raw path until bounded aggregate historical backfill makes `effective_reads_ready = true`
  - only then re-attempt aggregate runtime cutover
- Remaining risks:
  - aggregate writes without readiness/backfill can add material live pressure even though aggregate reads never switch on
  - leaving this rollout enabled risks continued writer backlog, retry growth, and stale frozen-window runtime behavior
- Next action:
  - immediately run a live stabilization pass on the same build with aggregate writes/reads disabled in config and validate that runtime returns to raw `Replay`

- Date: 2026-03-20
- Commit SHA: `824c9174cbed61a28ec50dadedea121f7cf39720`
- Stage / substep: `Stage 1 / live stabilization pass with aggregate writes and reads disabled`
- Status: `partial`
- Code changed:
  - none in this step; this was a manual live config stabilization pass on the already-deployed artifact
- Tests run:
  - live server stabilization validation on `solana-copy-bot.service`
- Done:
  - live config after restart was stabilized to:
    - `metric_snapshot_interval_seconds = 3600`
    - `scoring_aggregates_write_enabled = false`
    - `scoring_aggregates_enabled = false`
  - startup stayed healthy on live:
    - `app runtime loop started` appeared at `2026-03-20T19:00:53.878122Z`
    - `sqlite_migrations_deferred` still emitted deferred `0039_observed_swaps_sol_leg_ts_index.sql`
    - `sqlite_migrations_apply` completed immediately with `applied = 0`
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
  - the frozen stale-window loop cleared after restart:
    - the last `metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint` warning was observed at `2026-03-20T19:02:08.627799Z`
    - by `2026-03-20T19:02:08.748703Z` runtime had resumed a fresh target with `rebuild_horizon_end = 2026-03-20 19:02:08.613360747 UTC`
  - runtime returned to raw `Replay`:
    - `2026-03-20T19:14:11.898709Z` completed bounded discovery persisted observed_swaps prepass
    - `2026-03-20T19:14:14.021608Z` completed bounded discovery token-quality resolution and switched to replay
    - replay then advanced from `1,499,035` at `2026-03-20T19:15:58Z` to `5,998,380` at `2026-03-20T19:23:28Z`
    - sampled replay wallet-stats rate was about `600k rows/min`
  - live pressure improved materially:
    - `observed_swap_writer_pending_requests` fell from the pre-restart `4096` baseline to mostly `0-45`
    - `writer_aggregate_queue_depth_batches` stayed `0`
    - `sqlite_busy_error_total` and `sqlite_write_retry_total` were only `4 / 4` at `2026-03-20T19:22:53.879944Z`
- Acceptance criteria closed:
  - aggregate writes/reads were correctly identified as the main source of the extra live pressure introduced by the previous rollout
  - live no longer remained stuck in the frozen stale-window loop after stabilization
  - runtime returned to bounded raw-path progress and then back into `Replay`
- Blocked:
  - the original raw replay blocker remains:
    - `rebuild_replay_wallet_stats_complete = false`
    - `rebuild_replay_rows_processed = 0`
    - `rebuild_replay_sol_leg_access_path` is still not emitted
    - `scoring_source = raw_window_incomplete_no_recent_published_universe`
    - no healthy publication landed
- Acceptance criteria remaining:
  - either raw replay must eventually finish wallet-stats and enter SOL-leg on the stabilized config, or the team must fix aggregate backfill / resume so readiness can become true and aggregate cutover can replace the raw replay treadmill
  - do not re-enable aggregate writes/reads on the production host before that readiness path is proven
- Remaining risks:
  - occasional writer-pressure spikes (`1325`, `1852`) and rare overlap warnings still appear
  - the stabilized raw replay path is operationally better but still fail-closed and still blocked before healthy publication
- Next action:
  - keep live in the stabilized raw configuration, align repo config with that live state, and move the main engineering effort to fixing aggregate backfill / durable resume off the critical production path

- Date: 2026-03-20
- Commit SHA: `371403cd6bb642035464b07a33a909bf780a5d62`
- Stage / substep: `Stage 1 / live rollout validation of replay wallet-stats active-day summary fold`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - startup stayed healthy on live:
    - `app runtime loop started` appeared at `2026-03-20T11:37:20.068827Z`
    - `sqlite_migrations_deferred` still emitted deferred `0039_observed_swaps_sol_leg_ts_index.sql`
    - `sqlite_migrations_apply` completed immediately with `applied = 0`
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
  - process stability held throughout the observed window:
    - `MainPID = 59562`
    - `NRestarts = 0`
    - no restart loop
    - no writer-death path
    - no shadow-risk stop
    - no false `healthy`
  - replay resumed immediately after deploy and survived the next boundary:
    - first replay resume appeared at `2026-03-20T11:37:26.413208Z`
    - `2026-03-20T12:00:21.396588Z` carried forward exact canonical buy-mint state with `rebuild_previous_phase = "replay"`
    - runtime then traversed bounded `collect_buy_mints`, completed token-quality resolution, and re-entered `Replay` by `2026-03-20T12:01:50.426340Z`
  - replay wallet-stats still did not finish:
    - `2026-03-20T11:37:26Z -> 8,593,984`
    - `2026-03-20T11:43:20Z -> 12,975,126`
    - `2026-03-20T11:59:43Z -> 21,826,756`
    - carry-forward reset at `2026-03-20T12:00:21Z`
    - `2026-03-20T12:01:50Z -> 220,393`
    - `2026-03-20T12:04:01Z -> 2,307,493`
    - sampled pre-boundary replay rate was about `579k rows/min`
    - sampled post-boundary recovery reached about `956k rows/min`
- Acceptance criteria closed:
  - the replay wallet-stats active-day summary fold did not regress startup, replay re-entry, or exact carry-forward from `Replay`
  - Stage 1 remains clearly beyond the old `CollectBuyMints -> Replay` blocker
  - the current blocker is confirmed to sit inside replay wallet-stats completion before SOL-leg replay
- Blocked:
  - `rebuild_replay_wallet_stats_complete = false`
  - `rebuild_replay_rows_processed = 0`
  - `rebuild_replay_sol_leg_access_path` is still not emitted
  - `discovery cycle still running, skipping scheduled trigger` appeared repeatedly, showing the replay wallet-stats cycle itself is now heavy enough to overlap the normal cadence
  - no healthy `raw_window_persisted_stream` publication landed yet
- Acceptance criteria remaining:
  - next operational change must prove that replay wallet-stats can finish before rollover
  - next rollout must emit `completed bounded replay wallet-stats prepass; switching to SOL-leg replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - replay wallet-stats still resets on rollover before SOL-leg replay starts
  - mild retry counters and repeated overlap warnings imply the replay wallet-stats cycle is now wall-clock heavy even after the latest SQL fold
  - further raw-window replay micro-optimizations are likely to have diminishing returns
- Next action:
  - deploy config-only rollout `c6e0af8` to extend `metric_snapshot_interval_seconds` from `1800` to `3600` and validate whether replay wallet-stats can complete inside an hourly bucket without further semantic changes

- Date: 2026-03-20
- Commit SHA: `423fd519c09ccac3d40a5cd9565ab27a1394ce96`
- Stage / substep: `Stage 1 / live rollout validation of fresh-scan catch-up and replay re-entry`
- Status: `partial`
- Code changed:
  - none in this step; this was a live server rollout validation of the already-built artifact
- Tests run:
  - live server rollout validation on `solana-copy-bot.service`
- Done:
  - service stayed healthy overnight on the same process:
    - `MainPID = 48007`
    - `NRestarts = 0`
    - no writer-death path
    - no overlap regression
    - no false `healthy`
  - the widened guarded catch-up behavior is now validated on live:
    - `fresh_scan / time_budget` slices emitted `discovery_persisted_stream_catch_up_requested = true`
    - immediate retrigger passed at moderate writer backlog (`writer_pending_requests = 14..19`)
    - deferred retrigger now appears only at higher backlog (`writer_pending_requests = 166..191`) under the explicit threshold `128`
    - later overnight rollover still showed immediate catch-up at `writer_pending_requests = 56`, confirming the gate no longer requires an empty writer queue
  - the old `CollectBuyMints -> Replay` blocker was broken on live:
    - runtime first re-entered `Replay` at `2026-03-19T20:58:06.841826Z`
    - boundary at `2026-03-19T21:00:06.931230Z` carried forward exact canonical buy-mint membership progress from `replay`
    - the overnight boundary at `2026-03-20T05:30:08.661621Z` again carried forward from `replay`
    - after that boundary runtime briefly traversed `reconcile_expired_head -> reconcile_new_tail -> fresh_scan` and was already back in `Replay` by `2026-03-20T05:30:25.950155Z`
  - by the morning slice, replay still remained active:
    - `rebuild_phase = replay`
    - `rebuild_replay_mode = wallet_stats_then_sol_leg`
    - `rebuild_replay_wallet_stats_rows_processed = 900000`
    - `rebuild_replay_rows_processed = 0`
    - `latest rebuild_cycle_rows_processed = 100000`
    - `rebuild_replay_sol_leg_access_path` had still not yet been emitted
- Acceptance criteria closed:
  - guarded `fresh_scan / time_budget` catch-up is now validated on live
  - the previous `CollectBuyMints -> Replay` convergence blocker is no longer the dominant live blocker
  - replay can now survive bucket rollover and quickly return from carry-forward reconciliation back into `Replay`
- Blocked:
  - replay still remains in wallet-stats prepass on live-size state
  - just before the `2026-03-20 05:30 UTC` boundary, `rebuild_replay_wallet_stats_rows_processed` had already reached `3,000,000`, yet SOL-leg replay still had not started
  - `rebuild_replay_rows_processed = 0`
  - `rebuild_replay_sol_leg_access_path` is still not emitted
  - no healthy `raw_window_persisted_stream` publication landed yet
- Acceptance criteria remaining:
  - next code change must materially shorten replay wallet-stats time-to-SOL-leg without regressing carry-forward or the current safety gates
  - next rollout must emit `completed bounded replay wallet-stats prepass; switching to SOL-leg replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - replay wallet-stats progress is bucket-sensitive and is reset on rollover, so a prepass that cannot finish within one bucket can still livelock even though `Replay` is now re-entered correctly
  - overnight replay-heavy runtime introduced mild retry counters (`sqlite_busy_error_total = 8`, `sqlite_write_retry_total = 7`), so any additional replay catch-up must stay behind the existing writer / queue safety gates
- Next action:
  - extend guarded catch-up narrowly to `Replay` only while `replay_wallet_stats_complete = false`, then re-rollout to validate the transition into SOL-leg replay

- Date: 2026-03-19
- Commit SHA: `492c7e38c90317b75d5f3a822c429a0fe7d20ac6`
- Stage / substep: `Stage 1 / live rollout validation of time-first stale buy-mint candidate discovery`
- Status: `partial`
- Code changed:
  - `crates/storage/src/market_data.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo test -p copybot-storage observed_buy_mint_page_query -- --nocapture`
  - `cargo test -p copybot-discovery --lib live_like_cycle_advances_exact_token_batches_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_collect_buy_mints_carry_forward_reconcile_reduces_work_on_large_noise_fixture_stage1 -- --nocapture`
- Done:
  - startup stayed healthy on live:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` appeared at `2026-03-19T18:20:11.230943Z`
  - process stability held:
    - `MainPID = 44695`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
    - `sqlite_busy_error_total = 0`
    - `sqlite_write_retry_total = 0`
    - `yellowstone_output_queue_depth = 0`
    - `yellowstone_output_queue_fill_ratio = 0.0`
  - stale-resume boundary contract remained intact on live:
    - boundary at `2026-03-19 18:30 UTC` emitted the explicit stale-resume warning for `collect_buy_mints / reconcile_expired_head`
    - `rebuild_started_at` stayed unchanged
    - `rebuild_chunks_completed` kept advancing
    - no effective fresh restart pattern appeared
  - the time-first stale candidate path produced a strong positive intermediate throughput signal:
    - post-boundary stale `reconcile_expired_head` cycles exhausted `page_budget`, not `time_budget`
    - bounded cycles processed `rebuild_cycle_rows_processed = 160`, matching the current `32 x 5` exact-count/page caps
    - stale expired-head cursor lineage advanced quickly within `~1.2-1.6s` bounded cycles (`H4q... -> HaP...`)
    - pending-batch telemetry stayed live and non-zero instead of pinning flat at zero
- Acceptance criteria closed:
  - the time-first stale candidate access-path change did not regress startup, stale-resume, or runtime stability
  - the previous stale expired-head candidate-discovery SQL geometry is no longer the clearly dominant live blocker
  - the rollout materially narrowed the remaining blocker from SQL candidate discovery toward bounded page/cadence duty cycle
- Blocked:
  - runtime still remained in stale `collect_buy_mints / reconcile_expired_head`
  - `rebuild_replay_wallet_stats_rows_processed = 0`
  - `rebuild_replay_rows_processed = 0`
  - `rebuild_replay_sol_leg_access_path` was still not emitted
  - no healthy `raw_window_persisted_stream` publication landed yet
- Acceptance criteria remaining:
  - next code change must materially shorten overall stale `CollectBuyMints` time-to-`Replay` without weakening exact-truth or stale-resume semantics
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the remaining blocker now appears to be effective catch-up duty cycle for bounded partial rebuilds rather than the old stale expired-head candidate-discovery SQL geometry
  - an overly aggressive cadence fix could reintroduce pressure/lock contention if it is not gated narrowly to safe partial `fail_closed` rebuilds
- Next action:
  - implement a narrow conditional catch-up scheduler for safely bounded partial persisted rebuilds and then re-rollout to validate `Replay` re-entry without pressure regressions

- Date: 2026-03-18
- Commit SHA: `working tree (pending commit)`
- Stage / substep: `Stage 1 / stale expired-head candidate-batch convergence fix after live deploy 94847aa`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_expired_head_live_like_cycle_advances_exact_token_batches_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib`
- Done:
  - fixed the remaining stale expired-head hot path in code:
    - stale `reconcile_expired_head` no longer queries the entire remaining token range in one grouped page under live `fetch_limit = 20000`
    - each stale expired-head page now takes the next capped exact token batch directly from the authoritative canonical membership set and bounds the grouped count query to that token range
    - the cursor now advances by exact candidate-token batch end when a page completes, so bounded progress no longer depends on SQLite discovering all remaining groups before yielding
  - exact carry-forward truth is preserved:
    - partial stale reconcile pages still keep `buy_mint_counts` and `unique_buy_mints` aligned
    - stale-resume eligibility across repeated bucket boundaries still holds
    - no false `healthy` or stale publish path was introduced
  - startup-safe migration behavior did not change:
    - no new migration was added
    - no startup-critical index or helper dependency was introduced
- Acceptance criteria closed:
  - the semantic stale-resume contract remains intact while stale expired-head pages do less SQL work
  - stale grouped-delta reconcile remains bounded, restart-resumable, and rollout-safe
  - the new live-like regression proves one cycle now advances capped exact token batches instead of depending on a single huge grouped query
  - no startup / writer / replay regression was introduced in the validated suites
- Acceptance criteria remaining:
  - next live rollout must show stale `reconcile_expired_head` reaching the next exact checkpoint materially faster
  - next live rollout must re-enter `Replay`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`

- Date: 2026-03-18
- Commit SHA: `5e8d71beadc9e5adabce357c65f2f8a2785b1a6d`
- Stage / substep: `Stage 1 / live rollout validation of stale frozen-target reconcile resume`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup still reached runtime cleanly:
    - `sqlite_migrations_deferred` emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed without hang/timeout/abort
    - `app runtime loop started` appeared
  - process stability held:
    - `MainPID = 26880`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - boundary-time stale-resume contract is now validated on live:
    - at `2026-03-18 20:00 UTC`, runtime emitted the explicit stale-resume warning and info logs for `collect_buy_mints / reconcile_expired_head`
    - `rebuild_started_at` stayed unchanged
    - `rebuild_chunks_completed` advanced `60 -> 61`
    - reconcile cursor lineage continued instead of resetting
    - at `2026-03-18 21:00 UTC`, the same stale-resume contract was observed again
- Acceptance criteria closed:
  - boundary during in-progress grouped-delta reconcile no longer causes effective fresh restart
  - stale frozen-target resume is explicit in telemetry and keeps bounded progress
  - startup-safe 0039 deferral and process stability did not regress
- Blocked:
  - stale `reconcile_expired_head` is still converging too slowly to the next exact checkpoint
  - by the latest observed window runtime was still in `collect_buy_mints / reconcile_expired_head` with:
    - `rebuild_chunks_completed = 123`
    - `rebuild_prepass_rows_processed = 20645`
    - `rebuild_replay_wallet_stats_rows_processed = 0`
    - `rebuild_replay_rows_processed = 0`
  - runtime therefore had still not re-entered `Replay`, emitted `rebuild_replay_sol_leg_access_path`, or reached healthy publication
- Acceptance criteria remaining:
  - next code change must materially shorten stale `reconcile_expired_head` time-to-exact-checkpoint
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the new remaining blocker is no longer semantic boundary loss; it is slow convergence of stale frozen-target `reconcile_expired_head` on live-size state
- Next action:
  - optimize stale `reconcile_expired_head` convergence and then re-rollout to validate replay access-path telemetry and healthy publication

- Date: 2026-03-18
- Commit SHA: `94847aaf1eda6f19b04c0988ea15e60646757e9d`
- Stage / substep: `Stage 1 / live rollout validation of stale reconcile checkpoint convergence fix`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` appeared at `2026-03-18T21:27:16.220977Z`
  - process stability held:
    - `MainPID = 28694`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - at `2026-03-18 21:30 UTC`, runtime emitted the explicit stale-resume warning and info logs for `collect_buy_mints / reconcile_expired_head`
    - `rebuild_started_at` stayed `2026-03-18 19:00:41 UTC`
    - `rebuild_chunks_completed` advanced `150 -> 151`
    - reconcile cursor lineage continued (`Vff... -> axU...`) instead of resetting
- Blocked:
  - the intended faster convergence was not yet validated:
    - runtime still remained in `collect_buy_mints / reconcile_expired_head`
    - `rebuild_collect_buy_mints_reconcile_new_tail_cursor_token` was still not observed
    - `rebuild_replay_wallet_stats_rows_processed = 0`
    - `rebuild_replay_rows_processed = 0`
    - `rebuild_replay_sol_leg_access_path` was still not emitted
  - runtime therefore still had not re-entered `Replay`, emitted access-path telemetry, or progressed to healthy publication
- Acceptance criteria closed:
  - startup-safe 0039 deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
- Acceptance criteria remaining:
  - next code change must materially shorten stale `reconcile_expired_head` time-to-exact-checkpoint
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer semantic boundary loss; it remains slow stale reconcile convergence on live-size state
- Next action:
  - further optimize stale `reconcile_expired_head` convergence and re-rollout specifically for `Replay` re-entry and access-path telemetry

- Date: 2026-03-19
- Commit SHA: `c01487f2453c02ef25164ebb41ee0e0312c72782`
- Stage / substep: `Stage 1 / stale reconcile_new_tail exact-batch persistence throughput fix`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `crates/storage/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib observed_buy_mint_exact_batch_count_query_counts_only_requested_tokens -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_new_tail_pending_exact_batch_survives_rollover_and_finishes_batch_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_replay_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure -- --nocapture`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib`
- Done:
  - fixed the remaining stale new-tail throughput gap after deploy `8d99e324bfd1d0312d98a0cfa3f179243cbec35e`: runtime no longer has to rediscover the same stale tail candidate slice on every bounded cycle before it can continue counting
  - changed stale `CollectBuyMintsMode::ReconcileNewTail` to persist the exact candidate mint batch itself:
    - bounded candidate discovery still uses `DISTINCT token_out` over `(source_horizon_end, target_horizon_end]`
    - once a batch is found, the remaining candidate mints are stored in the persisted rebuild payload
    - later cycles and repeated bucket rollovers resume that exact batch verbatim instead of rerunning the same candidate discovery work
  - replaced stale new-tail range-grouped recount with exact candidate-batch counting:
    - count work now runs only for the remaining candidate mints in the persisted batch
    - partial progress drops the processed prefix from that batch and keeps the remainder for the next bounded cycle
    - zero-row recovery still narrows the slice, and a single-mint batch still falls back to exact single-token counting
  - added storage coverage for exact batch counting and a discovery regression showing that a partially processed exact stale new-tail batch survives bucket rollover and finishes the remaining batch without rediscovering candidates
  - kept exact truth intact:
    - `buy_mint_counts` remains authoritative membership state
    - `unique_buy_mints` remains canonical and exact
    - stale-resume across repeated bucket rollovers still resumes the same frozen target instead of restarting fresh
    - no new startup-critical migration or deferred-index contract was introduced
- Acceptance criteria closed:
  - stale `reconcile_new_tail` remains bounded and resumable across cycles
  - stale `reconcile_new_tail` remains resumable across repeated bucket rollovers
  - the new helper state stays in persisted rebuild payload only; startup-safe migration behavior does not change
- Acceptance criteria remaining:
  - next rollout must show runtime re-entering `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - live-size stale new-tail may still need one more throughput iteration if exact batch counting alone is not enough to reach `Replay`
  - repeated `shadow risk infra stop activated` remains secondary unless the next rollout shows it is coupled to the same convergence window
- Next action:
  - roll out the persisted exact-batch stale new-tail fix and validate `Replay` re-entry plus first emitted `rebuild_replay_sol_leg_access_path`

- Date: 2026-03-19
- Commit SHA: `c01487f2453c02ef25164ebb41ee0e0312c72782`
- Stage / substep: `Stage 1 / live rollout validation of stale reconcile_new_tail exact-batch persistence throughput fix`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` was observed on the rollout
  - process stability held:
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - boundary at `2026-03-19 11:00 UTC` landed during stale `collect_buy_mints / reconcile_new_tail`
    - runtime emitted the explicit stale-resume logs and did not restart fresh
    - `rebuild_started_at` stayed unchanged on the frozen target
    - `rebuild_chunks_completed` advanced instead of resetting
  - the new exact pending-batch contract appears live-valid:
    - runtime surfaced `rebuild_collect_buy_mints_reconcile_new_tail_pending_mints` telemetry
    - stale `reconcile_new_tail` no longer appeared to reopen the same tail candidate slice every bounded cycle
    - exact pending-batch state persisted across the observed boundary window instead of falling back to fresh candidate discovery
- Blocked:
  - runtime still did not re-enter `Replay`
  - `rebuild_replay_sol_leg_access_path` was still not emitted
  - `raw_window_persisted_stream` and healthy publication still did not appear
  - the remaining blocker is now time-to-exact-checkpoint / time-to-`Replay` after exact pending-batch persistence is already in place
- Acceptance criteria closed:
  - startup-safe `0039` deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
  - exact pending-batch stale new-tail state now survives bounded cycles / boundary handling without reopening the same candidate slice
- Acceptance criteria remaining:
  - next code change must materially shorten stale `reconcile_new_tail` time-to-exact-checkpoint
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer stale slice rediscovery; it is throughput/convergence after exact pending-batch persistence
- Next action:
  - optimize stale `reconcile_new_tail` time-to-exact-checkpoint / `Replay` re-entry from the persisted exact pending-batch path and then re-rollout

- Date: 2026-03-19
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / stale reconcile_new_tail exact-subbatch throughput fix`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_new_tail_pending_exact_batch_survives_rollover_and_finishes_batch_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_new_tail_exact_subbatches_reduce_live_like_timeout_pressure_stage1 -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
- Done:
  - identified the remaining stale new-tail throughput gap after deploy `c01487f2453c02ef25164ebb41ee0e0312c72782`: runtime kept truthful exact pending-batch state, but each bounded cycle still recounted the whole remaining pending batch before it could make live-size progress toward the next exact checkpoint
  - added `STALE_RECONCILE_EXACT_COUNT_BATCH_CAP = 32` and changed stale `CollectBuyMintsMode::ReconcileNewTail` to count only an active exact sub-batch from the persisted pending batch in each bounded cycle
  - kept the pending batch persisted and truthful while draining it incrementally:
    - only the current exact sub-batch is counted in a bounded cycle
    - partial progress drops the processed prefix and leaves the remaining pending batch for later cycles
    - runtime no longer needs to recount the entire remaining pending batch on every cycle before moving forward
  - added targeted regression coverage proving that exact sub-batches reduce live-like timeout pressure without changing stale-resume, exact membership truth, or startup-safe `0039` deferral behavior
- Acceptance criteria closed:
  - stale `reconcile_new_tail` remains bounded and resumable across cycles
  - stale `reconcile_new_tail` continues to preserve exact pending-batch truth across repeated bucket rollovers
  - startup-safe `0039` deferral and startup test coverage remained green on the touched path
- Acceptance criteria remaining:
  - next live rollout must show stale `reconcile_new_tail` reaching an exact checkpoint and exiting the stale new-tail path
  - next live rollout must then re-enter `Replay`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - live-size stale new-tail may still need one more throughput iteration if exact sub-batching alone is not enough to reach `Replay`
- Next action:
  - roll out the stale new-tail exact-subbatch throughput fix and validate exact checkpoint exit, `Replay` re-entry, and access-path telemetry on live

- Date: 2026-03-19
- Commit SHA: `661ca3f7e73e4f451cce68c4e2df50766157f31`
- Stage / substep: `Stage 1 / live rollout validation of stale reconcile_new_tail exact-subbatch throughput fix`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` was observed on the rollout
  - process stability held:
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - boundary at `2026-03-19 11:00 UTC` still emitted stale-resume logs without restarting fresh
    - the next observed boundary at `2026-03-19 12:00 UTC` also stayed healthy and landed in stale `reconcile_expired_head`
    - `rebuild_started_at` stayed unchanged on the frozen target
    - `rebuild_chunks_completed` advanced instead of resetting
  - the new exact-subbatch stale-new-tail contract appears live-valid:
    - runtime surfaced `rebuild_collect_buy_mints_reconcile_new_tail_pending_mints` telemetry before the exact checkpoint
    - stale `reconcile_new_tail` no longer appeared to recount the whole remaining pending batch every bounded cycle
    - runtime emitted the exact carry-forward log at `2026-03-19 11:50:28 UTC`
    - runtime then exited stale `collect_buy_mints / reconcile_new_tail`
    - by the observed `2026-03-19 12:00 UTC` boundary, runtime was already in stale `collect_buy_mints / reconcile_expired_head`
- Blocked:
  - runtime still did not re-enter `Replay`
  - `rebuild_replay_sol_leg_access_path` was still not emitted
  - `raw_window_persisted_stream` and healthy publication still did not appear
  - the remaining blocker moved later again: after stale new-tail exit, runtime still has not yet returned from stale `reconcile_expired_head` into `Replay`
- Acceptance criteria closed:
  - startup-safe `0039` deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
  - stale `reconcile_new_tail` now reaches an exact carry-forward checkpoint and exits the stale new-tail path on live
- Acceptance criteria remaining:
  - next code change must materially shorten the later stale `reconcile_expired_head` / return-to-`Replay` path
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer stale new-tail pending-batch throughput; it is now the later stale `reconcile_expired_head` / post-checkpoint path back into `Replay`
- Next action:
  - optimize the stale `reconcile_expired_head` / `Replay` convergence path and then re-rollout specifically for access-path telemetry and healthy publication

- Date: 2026-03-19
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / stale reconcile_expired_head exact pending-batch convergence fix`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_expired_head_pending_exact_batch_survives_rollover_and_finishes_batch_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_expired_head_exact_subbatches_reduce_live_like_timeout_pressure_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_expired_head_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
- Done:
  - identified the remaining post-`661ca3f...` hot path: stale `ReconcileExpiredHead` still walked the carried-forward canonical membership set just to discover which mints actually had buys inside the expired delta window, so later stale cycles kept paying that candidate-discovery cost even after stale new-tail had already exited successfully
  - replaced that later stale expired-head path with a direct expired-delta exact pending-batch contract:
    - when a stale expired-head page is opened, runtime now asks SQLite for the next distinct mint batch directly from the expired delta window instead of paging over the full carried-forward membership set
    - that exact expired-head candidate batch is then persisted in rebuild state and drained via capped exact count sub-batches, updating the expired-head cursor as each exact prefix is fully accounted
    - once the persisted batch is empty, runtime opens the next direct expired-delta candidate batch instead of recounting the same later stale work again
  - kept exact truth intact:
    - `buy_mint_counts` remains the authoritative membership source
    - `unique_buy_mints` remains canonical and exact
    - stale-resume across repeated bucket boundaries still resumes the same frozen target instead of restarting fresh
    - no new startup-critical migration or deferred-index contract was introduced
- Acceptance criteria closed:
  - stale `reconcile_expired_head` remains bounded and resumable across cycles
  - stale `reconcile_expired_head` remains resumable across repeated bucket rollovers
  - there is now targeted regression coverage for persisted expired-head exact pending-batch resume plus live-like exact sub-batch throughput
  - startup-safe `0039` deferral, writer stability, grouped-delta reconcile, stale frozen-target resume, stale new-tail exact pending-batch persistence, stale new-tail exact sub-batch throughput, and `wallet_stats_then_sol_leg` regressions remain green
- Acceptance criteria remaining:
  - next live rollout must re-enter `Replay`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must then still land `raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - if live still does not re-enter `Replay`, the blocker will move farther down the post-checkpoint path rather than remain hidden behind repeated stale expired-head recount work
- Next action:
  - rollout this stale expired-head exact pending-batch / exact sub-batch fix and validate `Replay` re-entry, `rebuild_replay_sol_leg_access_path`, and then healthy publication on live

- Date: 2026-03-19
- Commit SHA: `08c65adc0377537dce0644c82fe14c8321e12093`
- Stage / substep: `Stage 1 / live rollout validation of stale reconcile_expired_head exact pending-batch convergence fix`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` was observed on the rollout
  - process stability held:
    - `MainPID` stayed stable during the observed windows
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - the `2026-03-19 13:00 UTC`, `15:30 UTC`, and `17:00 UTC` boundaries all emitted stale-resume logs without restarting fresh
    - later windows showed runtime boundary handling in both stale `reconcile_expired_head` and stale `reconcile_new_tail`
    - `rebuild_started_at` stayed unchanged on the frozen target
    - `rebuild_chunks_completed` advanced instead of resetting
  - the new exact pending-batch stale-expired-head contract appears live-valid:
    - runtime surfaced `rebuild_collect_buy_mints_reconcile_expired_head_pending_mints` telemetry on live
    - stale `reconcile_expired_head` cursor lineage advanced materially with non-zero `rebuild_cycle_rows_processed`
    - no new operational pin of same expired-head cursor + same pending batch + zero rows appeared in the observed windows
    - later windows showed runtime again in stale `reconcile_new_tail`, with `rebuild_collect_buy_mints_reconcile_new_tail_pending_mints` draining and no fresh restart there either
  - pressure signals rose but did not regress stability:
    - `sqlite_busy_error_total` and `sqlite_write_retry_total` increased without crashing the process
    - `ws_notifications_backpressured` and `runtime_pressure` gating were observed
    - writer-failure / abort paths still did not appear
- Blocked:
  - runtime still did not re-enter `Replay`
  - `rebuild_replay_sol_leg_access_path` was still not emitted
  - `raw_window_persisted_stream` and healthy publication still did not appear
  - the remaining blocker is no longer one isolated stale reconcile subpath; stale `CollectBuyMints` as a whole still does not converge back into `Replay` quickly enough on live-size state
- Acceptance criteria closed:
  - startup-safe `0039` deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
  - stale `reconcile_expired_head` exact pending-batch telemetry appeared and advanced without a new zero-row pin
  - runtime again showed truthful bounded progress in stale `reconcile_new_tail` without fresh restart
- Acceptance criteria remaining:
  - next code change must materially shorten overall stale `CollectBuyMints` time-to-`Replay`
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer expired-head or new-tail in isolation; it is now overall stale `CollectBuyMints` convergence back into `Replay`
  - recurring `runtime_pressure` / backlog gating may be coupled to that later convergence path and should be evaluated in the next fix if evidence continues to accumulate
- Next action:
  - optimize overall stale `CollectBuyMints` time-to-`Replay`, then re-rollout specifically for replay access-path telemetry and healthy publication

- Date: 2026-03-19
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / stale reconcile_new_tail zero-row stall fix`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `crates/storage/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib observed_buy_mint_occurrence_count_query_respects_exclusive_time_bounds -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_new_tail_zero_row_timeout_narrows_slice_and_escapes_stall_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure -- --nocapture`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib`
- Done:
  - identified the exact remaining live stall after deploy `dd8c6e5c2798347808e375573edc7105da4f35e4`: stale `ReconcileNewTail` could fetch a candidate token slice successfully, then time out in the grouped-count query before the first row, leaving `rebuild_cycle_rows_processed = 0`, `rebuild_prepass_rows_processed` flat, and the stale cursor unchanged
  - replaced that stall behavior with a persisted narrowed-slice contract:
    - when the grouped-count slice times out before the first row, runtime persists a narrower exact token-slice end instead of retrying the same wide slice forever
    - subsequent bounded cycles keep narrowing that exact slice until it becomes cheap enough to make progress
    - once the narrowed slice reaches a single candidate mint, runtime switches to an exact single-token occurrence count query and can advance the stale cursor without skipping truthful work
  - added explicit storage coverage for the exact single-token count helper and a deterministic discovery regression that reproduces the zero-row unchanged-cursor stall, persists the narrowed slice through restart, and proves that the next cycle escapes the stall
  - kept exact truth intact:
    - `buy_mint_counts` remains authoritative membership state
    - `unique_buy_mints` remains canonical and exact
    - stale-resume across repeated bucket rollovers still resumes the same frozen target instead of restarting fresh
    - no new startup-critical migration or deferred-index contract was introduced
- Acceptance criteria closed:
  - stale `reconcile_new_tail` remains bounded and resumable across cycles
  - stale `reconcile_new_tail` remains resumable across repeated bucket rollovers
  - there is now targeted regression coverage for the zero-row unchanged-cursor stall and its persisted narrowed-slice recovery
  - startup-safe `0039` deferral, writer stability, grouped-delta reconcile, stale frozen-target resume, and `wallet_stats_then_sol_leg` regressions remain green
- Acceptance criteria remaining:
  - next live rollout must re-enter `Replay`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must then still land `raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - if live still does not re-enter `Replay`, the remaining blocker will move farther down the post-checkpoint path rather than remain hidden behind stale new-tail zero-row retries
- Next action:
  - rollout this stale new-tail zero-row stall fix and validate `Replay` re-entry, `rebuild_replay_sol_leg_access_path`, and then healthy publication on live

- Date: 2026-03-19
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / stale reconcile_new_tail candidate-batch convergence fix`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `crates/storage/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib observed_buy_mint_page_query_respects_exclusive_time_bounds -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_new_tail_live_like_cycle_advances_exact_token_batches_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure -- --nocapture`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib`
- Done:
  - identified the remaining stale hot path after deploy `7d28c7607d380cd4711de24b49ec325c2302a1c6`: `CollectBuyMintsMode::ReconcileNewTail` was still issuing one broad grouped-count query over the whole remaining tail token range under live `fetch_limit = 20000`
  - replaced that broad grouped tail scan with exact candidate-token batch paging:
    - first page the distinct SOL-buy mint candidates inside `(source_horizon_end, horizon_end]` after the persisted stale cursor
    - then ask SQLite for grouped counts only inside that bounded candidate token slice
    - advance the stale new-tail cursor by exact candidate-batch boundaries instead of waiting for one wide grouped query to consume the whole remaining tail
  - added explicit storage coverage for the new exclusive time-bound distinct mint page helper, so stale new-tail keeps the correct `(source_horizon_end, horizon_end]` semantics
  - added a live-like stale new-tail regression that proves one bounded cycle now advances exact token batches instead of depending on a giant grouped tail query
  - kept exact truth intact:
    - `buy_mint_counts` remains authoritative membership state
    - `unique_buy_mints` remains canonical and exact
    - stale-resume across repeated bucket rollovers still resumes the same frozen target instead of restarting fresh
    - no new startup-critical migration or deferred-index contract was introduced
- Acceptance criteria closed:
  - stale `reconcile_new_tail` remains bounded and resumable across cycles
  - stale `reconcile_new_tail` remains resumable across repeated bucket rollovers
  - large live-like stale boundary coverage now demonstrates exact candidate-batch advancement instead of one wide grouped tail query
  - startup-safe `0039` deferral, writer stability, grouped-delta reconcile, stale frozen-target resume, and `wallet_stats_then_sol_leg` regressions remain green
- Acceptance criteria remaining:
  - next live rollout must re-enter `Replay`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must then still land `raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - if live still does not re-enter `Replay`, the remaining blocker will move farther down the post-checkpoint path instead of staying hidden behind stale new-tail grouped work
- Next action:
  - rollout this stale new-tail candidate-batch fix and validate `Replay` re-entry, `rebuild_replay_sol_leg_access_path`, and then healthy publication on live

- Date: 2026-03-19
- Commit SHA: `8d99e324bfd1d0312d98a0cfa3f179243cbec35e`
- Stage / substep: `Stage 1 / live rollout validation of stale new-tail zero-row stall escape`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` appeared at `2026-03-19T07:09:41.849523Z`
  - process stability held:
    - `MainPID = 33735`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - observed boundaries at `2026-03-19 07:30 UTC`, `08:30 UTC`, `09:00 UTC`, `09:30 UTC`, and `10:00 UTC` all emitted explicit stale-resume logs without restarting fresh
    - `rebuild_started_at` stayed unchanged on the frozen target
    - `rebuild_chunks_completed` advanced instead of resetting
  - the zero-row unchanged-cursor stale-new-tail stall moved forward materially:
    - narrowed-slice telemetry was emitted repeatedly
    - bounded stale new-tail cursor lineage advanced across multiple exact steps instead of remaining pinned on one token
    - bounded stale new-tail slice-end lineage advanced as the grouped slice narrowed
    - `rebuild_cycle_rows_processed` returned to non-zero values across the later observed windows (`9`, `9`, `8`, then `34`, `33`, `36`, then `28`, `28`, `23`, then `23`, `4`)
    - by `2026-03-19 08:28-08:30 UTC`, runtime was observed in stale `collect_buy_mints / reconcile_expired_head` with non-zero processed rows and a moving expired-head cursor; this is a strong inference from the observed state rather than a directly captured exact carry-forward log
- Blocked:
  - runtime still did not re-enter `Replay`
  - `rebuild_replay_sol_leg_access_path` was still not emitted
  - `raw_window_persisted_stream` and healthy publication still did not appear
  - later windows showed runtime back in stale `collect_buy_mints / reconcile_new_tail`, still making bounded truthful progress but not yet reaching the next exact checkpoint
- Acceptance criteria closed:
  - startup-safe `0039` deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
  - the pinned stale new-tail zero-row unchanged-cursor stall is no longer the active blocker on live
- Acceptance criteria remaining:
  - next code change must materially shorten stale `reconcile_new_tail` time-to-exact-checkpoint
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer an unchanged-cursor correctness stall; it is now operational throughput/convergence from narrowed stale `reconcile_new_tail` to the next exact checkpoint and `Replay`
  - `shadow risk infra stop activated` was observed more than once while Yellowstone queue depth remained near zero; this remains a secondary signal, not yet the primary Stage 1 blocker
- Next action:
  - optimize stale `reconcile_new_tail` time-to-exact-checkpoint / `Replay` re-entry and then re-rollout specifically for access-path telemetry and healthy publication

- Date: 2026-03-19
- Commit SHA: `dd8c6e5c2798347808e375573edc7105da4f35e4`
- Stage / substep: `Stage 1 / live rollout validation of stale new-tail candidate-batch convergence fix`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` appeared at `2026-03-19T06:09:13.412653Z`
  - process stability held:
    - `MainPID = 31784`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - boundary at `2026-03-19 06:30 UTC` landed during `collect_buy_mints / reconcile_new_tail`
    - runtime emitted the explicit stale-resume warning/info logs and did not restart fresh
    - `rebuild_started_at` stayed unchanged on the frozen target
    - `rebuild_chunks_completed` advanced instead of resetting
- Blocked:
  - the intended new-tail batching still did not return runtime to `Replay`
  - the observed stall was narrower than before:
    - `rebuild_collect_buy_mints_reconcile_new_tail_cursor_token` remained pinned at `92cRC6kV5D7TiHX1j56AbkPbffo9jwcXxSDQZ8Mopump`
    - `rebuild_cycle_rows_processed = 0`
    - `rebuild_prepass_rows_processed = 21582` stayed flat
    - `rebuild_replay_wallet_stats_rows_processed = 0`
    - `rebuild_replay_rows_processed = 0`
    - `rebuild_replay_sol_leg_access_path` was still not emitted
  - runtime therefore still had not re-entered `Replay`, emitted access-path telemetry, or progressed to healthy publication
- Acceptance criteria closed:
  - startup-safe 0039 deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
- Acceptance criteria remaining:
  - next code change must break the stale new-tail zero-row unchanged-cursor stall
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer just slow convergence; it is an operational stall on one stale new-tail token slice
- Next action:
  - debug and fix the stale new-tail zero-row unchanged-cursor stall, then re-rollout specifically for `Replay` re-entry, access-path telemetry, and healthy publication

- Date: 2026-03-19
- Commit SHA: `7d28c7607d380cd4711de24b49ec325c2302a1c6`
- Stage / substep: `Stage 1 / live rollout validation of stale expired-head candidate-batch convergence fix`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup stayed healthy:
    - `sqlite_migrations_deferred` still emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed immediately
    - `startup_sqlite_wal_checkpoint` remained explicitly deferred
    - `app runtime loop started` appeared at `2026-03-18T21:54:27.439487Z`
  - process stability held:
    - `MainPID = 29606`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - stale-resume boundary contract remained intact on live:
    - boundary at `2026-03-18 22:00 UTC` landed during `collect_buy_mints / reconcile_new_tail`
    - boundary at `2026-03-18 22:30 UTC` again landed during `collect_buy_mints / reconcile_new_tail`
    - both times runtime emitted the explicit stale-resume warning/info logs and did not restart fresh
    - `rebuild_started_at` stayed unchanged on the frozen target
    - `rebuild_chunks_completed` advanced instead of resetting
  - the stale path moved farther than before:
    - runtime was already in `reconcile_new_tail` at rollout start
    - bounded new-tail cursor lineage advanced across multiple exact steps (`4Tr... -> ... -> 57o9... -> 8ft... -> 8mej...`)
- Blocked:
  - runtime still did not re-enter `Replay`
  - `rebuild_replay_sol_leg_access_path` was still not emitted
  - `raw_window_persisted_stream` and healthy publication still did not appear
- Acceptance criteria closed:
  - startup-safe 0039 deferral did not regress
  - process stability did not regress
  - stale-resume during boundary remained live-validated
  - stale expired-head hotspot no longer appears to be the dominant blocker, because runtime progressed onward into `reconcile_new_tail`
- Acceptance criteria remaining:
  - next code change must materially shorten stale `reconcile_new_tail` time-to-exact-checkpoint
  - next rollout must re-enter `Replay`
  - next rollout must emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the blocker is no longer stale expired-head convergence; it is now stale new-tail convergence on live-size state
- Next action:
  - optimize stale `reconcile_new_tail` convergence and then re-rollout specifically for `Replay` re-entry, access-path telemetry, and healthy publication

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / stale frozen-target resume for in-progress grouped-delta reconcile`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-discovery --lib persisted_stream_reconcile_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_replay_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
- Done:
  - identified the exact cause of the live `fresh_scan` restart after `bc9f6d7`: `state_can_carry_forward_metrics_rollover()` only accepted exact `FreshScan` state, so a boundary during `reconcile_expired_head` / `reconcile_new_tail` fell through to the old discard-and-start-fresh branch
  - replaced that discard behavior with an explicit stale-metrics-window resume contract for in-progress grouped-delta reconcile
  - runtime now keeps bounded progress on the stale frozen target until the next exact carry-forward checkpoint exists, then performs the normal metrics-bucket carry-forward from exact state
  - exact membership truth is preserved because mixed reconcile state is never published and is never re-labeled as current-bucket truth before the exact checkpoint is reached
  - added targeted regressions for stale-bucket `reconcile_expired_head`, stale-bucket `reconcile_new_tail`, and a noisy live-like boundary case that previously would have restarted to `fresh_scan`
- Acceptance criteria closed:
  - in-progress `reconcile_expired_head` now survives metrics bucket rollover by the chosen semantic contract
  - in-progress `reconcile_new_tail` now survives metrics bucket rollover by the chosen semantic contract
  - large noisy stale-bucket reconcile no longer restarts operationally as a fresh scan
  - startup-safe 0039 deferral, replay-mode regressions, writer retry stability, and no-stale-publish regressions remain green
- Acceptance criteria remaining:
  - next live rollout must show boundary-time reconcile progress surviving without returning to `fresh_scan`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - if live still stalls before the first replay access-path slice after this fix, the remaining blocker will move to another bounded later-phase convergence gap rather than disappear silently
- Next action:
  - rollout this stale-frozen-target reconcile-resume fix and verify that boundary-time `reconcile_expired_head` / `reconcile_new_tail` progress is preserved operationally until the next exact carry-forward checkpoint

- Date: 2026-03-18
- Commit SHA: `bc9f6d7d946a34b1f854680c9c53a9c117cde735`
- Stage / substep: `Stage 1 / live rollout validation of grouped-delta CollectBuyMints reconcile`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup still reached runtime cleanly:
    - `sqlite_migrations_deferred` emitted `skipped` with pending `0039`
    - `sqlite_migrations_apply` completed without hang/timeout/abort
    - `app runtime loop started` appeared
  - process stability held:
    - `MainPID = 24755`
    - `NRestarts = 0`
    - no writer-death path
    - no restart loop
  - grouped-delta carry-forward reconcile clearly engaged after rollover:
    - `rebuild_collect_buy_mints_reconcile_expired_head_cursor_token` was emitted repeatedly
  - the previous pre-replay blocker is no longer the active blocker:
    - runtime reached `Replay` before the `2026-03-18 18:30 UTC` rollover
    - live telemetry showed `rebuild_replay_mode = wallet_stats_then_sol_leg`
    - pre-rollover replay telemetry reached `rebuild_replay_wallet_stats_rows_processed = 1329082`
- Blocked:
  - at `2026-03-18 19:00 UTC`, the boundary landed while runtime was still in `collect_buy_mints / reconcile_expired_head`
  - there was then no carry-forward log and no explicit discard log
  - the next slice resumed as a new `fresh_scan` with:
    - `rebuild_started_at = 2026-03-18 19:00:41 UTC`
    - `rebuild_chunks_completed = 1`
    - a fresh `collect_buy_mints_cursor_token`
  - by `2026-03-18 19:25 UTC`, runtime was still back in `CollectBuyMints`, so there was still no emitted `rebuild_replay_sol_leg_access_path`, no `raw_window_persisted_stream`, and no healthy publication
- Acceptance criteria closed:
  - grouped-delta reconcile fix materially improved pre-replay progress on live
  - runtime can now reach `Replay` before rollover on live
  - startup safety, carry-forward, and process stability did not regress
- Acceptance criteria remaining:
  - next code change must preserve exact progress when rollover lands during `reconcile_expired_head` or `reconcile_new_tail`
  - next rollout must still emit `rebuild_replay_sol_leg_access_path`
  - next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the new remaining blocker is now a narrower metrics-bucket-boundary case inside in-progress grouped-delta reconcile, not startup and not the earlier raw-swap reconcile cost
- Next action:
  - fix rollover handling for in-progress grouped-delta reconcile and then re-rollout to validate actual replay access-path telemetry and healthy publication

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / grouped-delta CollectBuyMints reconcile throughput fix`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib observed_buy_mint_count_page_query_respects_exclusive_time_bounds -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_collect_buy_mints_carry_forward_reconcile_reduces_work_on_large_noise_fixture_stage1 -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_collect_buy_mints_reconcile_legacy_raw_cursor_repairs_to_grouped_delta_stage1 -- --nocapture`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib persisted_stream_collect_buy_ -- --nocapture`
  - `cargo test -p copybot-discovery --lib persisted_stream_rebuild -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
- Done:
  - identified the remaining live wall-clock cost inside `CollectBuyMints`: carry-forward `ReconcileExpiredHead` and `ReconcileNewTail` were still streaming every raw swap in the delta windows even after fresh-scan had moved to direct distinct-mint pagination
  - replaced those two carry-forward subphases with grouped SOL-buy mint-count delta pagination on the existing `idx_observed_swaps_token_in_out_ts`
  - preserved exact canonical buy-mint membership semantics by subtracting/adding per-mint counts, then resyncing the canonical sorted mint set from those counts
  - added dedicated token cursors for the carry-forward reconcile subphases, plus progress telemetry for those cursors
  - added resume repair for already-persisted legacy raw reconcile cursors, rewinding only the current reconcile subphase onto the new grouped-delta contract instead of resetting the whole rebuild
  - avoided any new startup-critical migration or index requirement; this fix reuses the existing storage access path
- Acceptance criteria closed:
  - startup-safe migration rollout contract is not regressed by this fix
  - `CollectBuyMints` remains bounded and resumable across cycles and restarts
  - carry-forward still works, but now reconciles delta windows by grouped buy-mint work instead of raw-swap work
  - large noisy fixtures show materially faster exit-from-`CollectBuyMints` trajectory for carry-forwarded rebuilds
- Acceptance criteria remaining:
  - next live rollout must actually exit `CollectBuyMints`
  - next live rollout must emit `rebuild_replay_sol_leg_access_path`
  - next live rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - if live still spends too long before the first replay slice after this prepass optimization, the next blocker will move to another bounded pre-replay subpath rather than disappear silently
- Next action:
  - rollout this grouped-delta reconcile fix and confirm live telemetry reaches the first replay slice without regressing startup safety, carry-forward, or process stability

- Date: 2026-03-18
- Commit SHA: `1093a5556e82f8adb6ec73bb51e73d62b8d9ac02`
- Stage / substep: `Stage 1 / live rollout validation of startup-safe 0039 deferral + replay telemetry wiring`
- Status: `partial`
- Code changed:
  - none; live rollout validation only
- Tests run:
  - server rollout observation only
- Done:
  - startup-safe migration rollout contract is now validated on live:
    - `sqlite_migrations_deferred` emitted `skipped` with `deferred_versions=0039_observed_swaps_sol_leg_ts_index.sql`
    - `sqlite_migrations_apply` completed immediately instead of timing out or aborting
    - startup reached `app runtime loop started`
  - process stability held in the observed live windows:
    - `MainPID = 22242`
    - `NRestarts = 0`
    - no writer-death restart path
    - no startup abort loop
  - carry-forward across metrics bucket rollover remained validated on live at `16:00 UTC`, `16:30 UTC`, and `17:30 UTC`
  - runtime telemetry now proves the new replay contract is wired:
    - `rebuild_replay_mode = wallet_stats_then_sol_leg`
    - no sign of fallback to legacy full-window replay mode
- Blocked:
  - the rollout still did not reach the first actual replay slice
  - by `2026-03-18 17:50 UTC` runtime remained in `CollectBuyMints` with:
    - `rebuild_chunks_completed = 139`
    - `rebuild_prepass_rows_processed = 1660899`
    - `rebuild_unique_buy_mints = 38862`
    - `rebuild_replay_wallet_stats_rows_processed = 0`
    - `rebuild_replay_rows_processed = 0`
  - therefore there was still no emitted `rebuild_replay_sol_leg_access_path`, no `raw_window_persisted_stream`, and no healthy publication
- Acceptance criteria closed:
  - startup-safe deferral of the heavy `0039` replay index migration is validated on live
  - runtime now reaches discovery again with `wallet_stats_then_sol_leg` telemetry present
  - carry-forward and process stability did not regress
- Acceptance criteria remaining:
  - the next code change must make `CollectBuyMints` converge fast enough to reach the first replay slice on live
  - the next rollout must emit `rebuild_replay_sol_leg_access_path`
  - the next rollout must still land `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`
- Remaining risks:
  - the current remaining bottleneck is back in pre-replay convergence on live-size `CollectBuyMints`, not startup and not the replay access-path contract itself
- Next action:
  - fix `CollectBuyMints` completion time / convergence on live-size state, then re-rollout to validate actual replay slices and healthy publication

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / deferred 0039 telemetry truthfulness after offline apply`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/migrations.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo test -p copybot-storage --lib sqlite_startup_bootstrap_defers_optional_sol_leg_index_migration -- --nocapture`
  - `cargo test -p copybot-storage --lib sqlite_startup_bootstrap_does_not_report_deferred_sol_leg_index_after_offline_apply -- --nocapture`
- Done:
  - fixed the operationally misleading startup telemetry where `0039_observed_swaps_sol_leg_ts_index.sql` was reported as deferred purely by filename even after the index had already been applied offline
  - startup now reports `0039` as deferred only while it is actually still pending
  - if the SOL-leg partial index already exists offline, startup reports `sqlite_migrations_deferred = completed/deferred_count=0` and then records `0039` cheaply through normal migration apply without rebuilding the index
- Acceptance criteria closed:
  - deferred migration telemetry now reflects real pending work instead of just migration-file presence

- Date: 2026-03-18
- Commit SHA: `self-referential; exact final SHA is reported from git after commit`
- Stage / substep: `Stage 1 / startup-safe deferral of 0039 replay index migration`
- Status: `done in code; Stage 1 remains partial pending rollout validation`
- Code changed:
  - `crates/app/src/main.rs`
  - `crates/discovery/src/lib.rs`
  - `crates/storage/src/lib.rs`
  - `crates/storage/src/market_data.rs`
  - `crates/storage/src/migrations.rs`
  - `ROAD_TO_PRODUCTION_v2.md`
- Tests run:
  - `cargo fmt --all`
  - `cargo test -p copybot-storage --lib`
  - `cargo test -p copybot-discovery --lib persisted_stream_replay_ -- --nocapture`
  - `cargo test -p copybot-app --bin copybot-app app_tests::startup_ -- --nocapture`
- Done:
  - fixed the live regression from deploy `2072123e7ba90a9133494be0d70023d0c9b2cc4b`, where `0039_observed_swaps_sol_leg_ts_index.sql` ran inside fatal `sqlite_migrations_apply` and caused a 120s abort/restart loop before runtime
  - selected an operationally safe rollout design instead of increasing startup timeout:
    - `0039` is now an explicit startup-deferred optional performance migration
    - startup emits an explicit `sqlite_migrations_deferred` outcome with the deferred version list
    - app startup warns that runtime may use fallback replay access paths until the index is applied offline
  - preserved replay correctness before and after index availability:
    - replay sol-leg paging now auto-selects `sol_leg_partial_index` when `0039` exists
    - otherwise it uses `ts_cursor_fallback` against the older `idx_observed_swaps_ts_slot_signature` path
    - replay telemetry now logs the chosen access path so live rollout can prove which contract is active
  - added coverage for:
    - startup deferral of the heavy 0039 migration
    - explicit startup deferred-migration progress logging
    - replay sol-leg correctness before and after the deferred index becomes available
- Blocked:
  - Stage 1 is still blocked until the next live rollout proves startup reaches runtime again and then actually validates the replay throughput fix
- Acceptance criteria closed:
  - startup no longer requires the heavy 0039 index build inside fatal `sqlite_migrations_apply`
  - startup observability remains explicit for the deferred-migration path
  - replay remains correct both before and after index availability
- Acceptance criteria remaining:
  - next live rollout must reach `app runtime loop started`
  - next live rollout must emit replay telemetry under `wallet_stats_then_sol_leg`
  - next live rollout must still prove a completed healthy discovery cycle with `scoring_source = raw_window_persisted_stream` and `active_follow_wallets > 0`

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
