# Morning Post-Main Runtime Report

Date (UTC snapshot): `2026-03-09T06:24:08Z`  
Server: `52.28.0.218`  
Deployed commit: `77458f3`  
Rollout start (service active): `2026-03-08 22:22:34 UTC`  
Observed runtime window: `~8h 01m 34s` (`2026-03-08T22:22:34Z -> 2026-03-09T06:24:08Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/server_time_utc.txt`
2. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/service_status.txt`
3. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/executor_status.txt`
4. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/adapter_status.txt`
5. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/df_root.txt`
6. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/db_files.txt`
7. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/sqlite_snapshot.txt`
8. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/live_server_config_excerpt.txt`
9. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/recent_errors.txt`
10. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/recent_metrics.log`
11. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/journal_tail_120.log`
12. `ops/server_reports/raw/2026-03-09_post_main_followup_0623_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/restarts: **PASS**
   - `solana-copy-bot.service active`, `NRestarts=0`
   - `copybot-executor.service active`, `NRestarts=0`
   - `copybot-adapter.service active`, `NRestarts=0`
2. Post-rollout critical errors: **none observed in the fresh window**
   - filtered post-rollout journal window after `2026-03-08 22:22 UTC` shows no fresh:
     - `database is locked`
     - `failed to insert observed swap batch`
     - `failed to record heartbeat`
     - `disk I/O error`
   - note: bundled `recent_errors.txt` still contains pre-rollout `2026-03-08` disk-full lines because the collection window was intentionally broad
3. Discovery/head state: **near-head**
   - `observed_swaps_max_ts=2026-03-09T06:24:10.094923980+00:00`
   - `discovery_cursor_ts=2026-03-09T06:23:33.168740896+00:00`
   - direct `cursor/head gap ~= 37s`
4. Disk/WAL state: **healthy after last-night resize and truncate**
   - `/dev/root 145G`, `93G used`, `53G avail`, `64%`
   - `live_copybot.db ~88G`
   - `live_copybot.db-wal ~57M`
   - `live_copybot.db-shm ~128K`
5. Runtime config checkpoint: **expected short-retention hotfix settings are live**
   - `discovery.refresh_seconds = 600`
   - `discovery.metric_snapshot_interval_seconds = 1800`
   - `discovery.max_window_swaps_in_memory = 60000`
   - `discovery.max_fetch_swaps_per_cycle = 20000`
   - `discovery.max_fetch_pages_per_cycle = 5`
   - `discovery.fetch_time_budget_ms = 15000`
   - `discovery.observed_swaps_retention_days = 7`

## Ingestion and discovery metrics

1. SQLite contention counters remain bounded:
   - `sqlite_busy_error_total=10`
   - `sqlite_write_retry_total=10`
   - counters stayed flat across the captured tail
2. Ingestion remains healthy in the latest samples:
   - latest `ingestion_lag_ms_p95=1747`, `p99=1858`
   - latest `ws_to_fetch_queue_depth=1`
   - transient queue spikes were observed (`34`, `52`, `80`, `94`, `109`) but they drained immediately in the next samples
3. Websocket pressure remains bounded:
   - `ws_notifications_backpressured=133`
   - `ws_notifications_replaced_oldest=133`
   - no evidence of active escalation in the captured window
4. Discovery cycles are light and not cap-pinned:
   - latest `discovery_cycle_duration_ms=19`
   - typical tail window `~14-19 ms`
   - `snapshot_recomputed=false`
   - `metrics_persisted=false`
   - `swaps_fetch_limit_reached=false`
   - `swaps_fetch_pages=1`
   - `swaps_query_rows_last_page ~ 10.1k-12.0k`

## Business-state snapshot

1. `wallet_activity_days` continues to grow:
   - `wallet_activity_day_rows=687740`
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

1. The overnight `~8h` window after the full `77458f3` rollout is operationally clean: all three services are up, restart-free, and there are no fresh post-rollout critical errors in the filtered journal window.
2. Runtime remains near-head and low-pressure at the morning checkpoint:
   - direct `cursor/head gap ~= 37s`
   - discovery cycles `~14-19 ms`
   - WAL remains small at `~57M`
   - root filesystem still has `53G` free
3. The short-retention disk hotfix is therefore behaving as intended at runtime:
   - disk headroom is materially healthier than last night
   - discovery is not pinned to the old fetch cap behavior
   - no new lock, heartbeat, or disk-I/O incident is visible in the fresh post-rollout window
4. Business output is still unchanged at this checkpoint:
   - `active_follow_wallets=0`
   - downstream tables remain empty
   - this morning report records the runtime state only; it does not by itself resolve followlist semantics
