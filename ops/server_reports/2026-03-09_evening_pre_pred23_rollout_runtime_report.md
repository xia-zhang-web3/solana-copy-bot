# Evening Pre-PRED2-3 Rollout Runtime Report

Date (UTC snapshot window): `2026-03-09T16:52:32Z -> 2026-03-09T16:55:52Z`  
Server: `52.28.0.218`  
Deployed commit: `77458f3`  
Rollout start (service active): `2026-03-08 22:22:34 UTC`  
Observed runtime window: `~18h 33m 18s` (`2026-03-08T22:22:34Z -> 2026-03-09T16:55:52Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/server_time_utc.txt`
2. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/service_status.txt`
3. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/executor_status.txt`
4. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/adapter_status.txt`
5. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/df_root.txt`
6. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/db_files.txt`
7. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/sqlite_snapshot.txt`
8. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/live_server_config_excerpt.txt`
9. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/recent_errors.txt`
10. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/recent_metrics.log`
11. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/journal_tail_200.log`
12. `ops/server_reports/raw/2026-03-09_evening_pre_pred23_rollout_1652_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/restarts: **PASS**
   - `solana-copy-bot.service active`, `NRestarts=0`
   - `copybot-executor.service active`, `NRestarts=0`
   - `copybot-adapter.service active`, `NRestarts=0`
2. Fresh critical errors: **none observed in the current collection window**
   - `recent_errors.txt` is empty for:
     - `database is locked`
     - `failed to insert observed swap batch`
     - `failed to record heartbeat`
     - `disk I/O error`
     - `discovery cycle still running`
     - `shadow risk infra stop activated`
3. Discovery/head state: **near-head**
   - `observed_swaps_max_ts=2026-03-09T16:55:52.023488109+00:00`
   - `discovery_cursor_ts=2026-03-09T16:55:33.309359041+00:00`
   - direct `cursor/head gap ~= 19s`
4. Disk/WAL state: **stable, but main DB file remains large**
   - `/dev/root 145G`, `101G used`, `45G avail`, `70%`
   - `live_copybot.db ~96G`
   - `live_copybot.db-wal ~62M`
   - `live_copybot.db-shm ~128K`
   - `PRAGMA freelist_count = 0`
5. Runtime config checkpoint: **short-retention settings are still live**
   - `discovery.refresh_seconds = 600`
   - `discovery.metric_snapshot_interval_seconds = 1800`
   - `discovery.max_window_swaps_in_memory = 60000`
   - `discovery.max_fetch_swaps_per_cycle = 20000`
   - `discovery.max_fetch_pages_per_cycle = 5`
   - `discovery.fetch_time_budget_ms = 15000`
   - `discovery.observed_swaps_retention_days = 7`

## Ingestion and discovery metrics

1. SQLite contention counters remain bounded:
   - `sqlite_busy_error_total=31`
   - `sqlite_write_retry_total=31`
   - counters stayed flat across the captured tail
2. Ingestion remains healthy in the latest samples:
   - latest `ingestion_lag_ms_p95=1770`, `p99=1835`
   - latest `ws_to_fetch_queue_depth=1`
   - samples in the captured tail stayed near `1` after brief earlier transients
3. Websocket pressure remains bounded:
   - `ws_notifications_backpressured=4083`
   - `ws_notifications_replaced_oldest=4083`
   - counters stayed flat in the captured window
4. Discovery cycles are light and not cap-pinned:
   - latest `discovery_cycle_duration_ms=22`
   - typical tail window `~16-25 ms`
   - `snapshot_recomputed=false`
   - `metrics_persisted=false`
   - `swaps_fetch_limit_reached=false`
   - `swaps_fetch_pages=1`
   - `swaps_query_rows_last_page ~ 12.8k-13.6k`

## Business-state snapshot

1. `wallet_activity_days` continues to grow:
   - `wallet_activity_day_rows=821842`
   - `wallet_activity_day_min=2026-03-07`
   - `wallet_activity_day_max=2026-03-09`
2. Followlist/output tables remain empty:
   - `active_follow_wallets=0`
   - `total_follow_wallet_rows=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `shadow_lots_rows=0`
3. Latest discovery logs still show:
   - `eligible_wallets=0`
   - `follow_promoted=0`
   - `follow_demoted=0`
   - `followlist_deactivations_suppressed=true`

## Conclusion

1. This is a clean pre-patch baseline immediately before any new PRED2-3 rollout:
   - all three services are up,
   - there are no fresh critical runtime errors in the collection window,
   - discovery is still near-head at `~19s` lag to `observed_swaps`.
2. Runtime pressure remains low:
   - discovery cycles are `~16-25 ms`,
   - ingestion is still around `~1.8s` p95/p99,
   - contention counters are flat,
   - websocket backpressure counters are not actively growing.
3. The short-retention disk hotfix still looks operationally stable:
   - WAL remains small at `~62M`,
   - root has `45G` free,
   - no new disk-I/O or heartbeat incident is visible here.
4. The remaining storage concern is now structural rather than acute:
   - the main `live_copybot.db` file is already `~96G`,
   - `freelist_count=0` means this snapshot does not show reclaimed free pages inside the file,
   - so this baseline is good for runtime safety, but it does not by itself solve long-term storage growth.
