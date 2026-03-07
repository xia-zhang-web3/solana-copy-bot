# Evening Post-Deploy Runtime Report

Date (UTC snapshot): `2026-03-07T15:28:36Z`  
Server: `52.28.0.218`  
Deployed commit: `ac5c87b`  
Rollout start (service active): `2026-03-06 19:08:45 UTC`  
Observed runtime window: `~20h 19m 51s` (`2026-03-06T19:08:45Z -> 2026-03-07T15:28:36Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/io_pressure_snapshot.txt`
10. `ops/server_reports/raw/2026-03-07_post_deploy_followup_1527_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery duration: **still stable, still far below pre-deploy incident**
   - `p50=709 ms`, `p95=7663.95 ms`, `max=11850 ms`, `last=742 ms`
   - `snapshot_recomputed=true` in `41/122` cycles
   - `snapshot_recomputed=false` cycles remain sub-second
   - `discovery cycle still running` warnings: `0`
3. Ingestion lag/backpressure: **healthy in latest samples**
   - `ingestion_lag_ms_p95=1942`, `ingestion_lag_ms_p99=2047`
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_last=39331`
   - interpretation: latest samples remain healthy, and the cumulative backpressure counter did not grow versus the morning follow-up
4. Discovery cap/backlog state: **stable under cap, backlog still material and now clearly not burned down**
   - `swaps_fetch_limit_reached_ratio=1.0` (`122/122`)
   - `swaps_query_rows_last=20000`, `swaps_delta_fetched_last=20000`
   - `discovery_cursor_ts=2026-03-06T23:14:36.732929390+00:00`
   - `observed_swaps_max_ts=2026-03-07T15:29:59.106718151+00:00`
   - direct `cursor/head ts-gap ~= 58522 s` (`~16h 15m 22s`)
   - versus the morning snapshot, gap worsened from `~10h 02m 55s` to `~16h 15m 22s` (`+~6h 12m 27s`)
   - interpretation: the hotfix path stabilized runtime, but at the current `20k` cap discovery is not catching up to head
5. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter

## Ingestion summary

1. `samples=2427`
2. Throughput:
   - `rate_grpc_message_per_s ~356.39`
   - `rate_grpc_tx_updates_per_s ~356.19`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Transport notes:
   - cumulative `reconnect_count=0`
   - cumulative `stream_gap_detected=0`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1`
   - `ws_to_fetch_queue_depth_max=2049`
   - `ws_notifications_backpressured_last=39331`

## I/O, Memory, and DB

1. Direct I/O snapshot: **still materially softer than pre-deploy degradation, but not improving further**
   - `pressure io some avg10=10.99`, `avg60=10.26`
   - `vmstat wa last=11`
   - `iostat nvme0n1 aqu-sz last=2.00`, `%util last=24.1`
2. `copybot-app` process:
   - `VmRSS ~151.1 MB`
   - `VmHWM ~221.3 MB`
   - `State=S (sleeping)`
3. cgroup memory:
   - `MemoryCurrent ~6.65 -> 6.66 GB`
   - `MemoryPeak ~6.84 GB`
   - still dominated by `file` cache (`~6.31 GB`), not RSS growth (`anon ~143.5 MB`)
4. DB:
   - `live_copybot.db ~61G`
   - `live_copybot.db-shm ~128K`
   - `live_copybot.db-wal ~57M`
   - writes remain live at `observed_swaps_max_ts=2026-03-07T15:29:59.106718151+00:00`
5. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `fills_rows=0`
   - `shadow_lots_rows=0`
   - interpretation: still consistent with pre-filter-maturation state, not a runtime regression

## Conclusion

1. The `~20h 20m` post-deploy window confirms the same core point as the earlier snapshots: `ac5c87b` stabilized runtime. There are still no restart/OOM regressions, no discovery skip warnings, and discovery duration remains in milliseconds-to-single-digit-seconds instead of minutes.
2. Ingestion is operationally healthy in the latest samples. The queue is empty at the edge, and the cumulative backpressure counter did not increase beyond the overnight value already seen in the morning report.
3. The most important new fact is negative, not positive: the direct `discovery cursor ts` check worsened materially. The cursor/head gap grew from about `10h` in the morning to about `16h 15m` in the evening.
4. So the correct evening read is: **runtime stabilized, but discovery backlog is still material and is not yet burning down at the current cap**.
