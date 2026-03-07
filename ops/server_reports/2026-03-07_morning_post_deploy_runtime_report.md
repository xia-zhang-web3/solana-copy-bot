# Morning Post-Deploy Runtime Report

Date (UTC snapshot): `2026-03-07T07:27:19Z`  
Server: `52.28.0.218`  
Deployed commit: `ac5c87b`  
Rollout start (service active): `2026-03-06 19:08:45 UTC`  
Observed runtime window: `~12h 18m 34s` (`2026-03-06T19:08:45Z -> 2026-03-07T07:27:19Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/io_pressure_snapshot.txt`
10. `ops/server_reports/raw/2026-03-07_post_deploy_followup_0726_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery duration: **sustained improvement vs pre-deploy**
   - `p50=302 ms`, `p95=6412.15 ms`, `max=11850 ms`, `last=652 ms`
   - `snapshot_recomputed=true` in `25/74` cycles
   - `snapshot_recomputed=false` cycles remain sub-second
   - `discovery cycle still running` warnings: `0`
3. Ingestion lag/backpressure: **currently healthy, but not perfectly clean overnight**
   - `ingestion_lag_ms_p95=1845`, `ingestion_lag_ms_p99=1987`
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_delta=39331`
   - interpretation: the latest sample is healthy, but the cumulative counter confirms there were transient queue-overflow events somewhere in the overnight window
4. Discovery cap/backlog state: **still materially behind head**
   - `swaps_fetch_limit_reached_ratio=1.0` (`74/74`)
   - `swaps_query_rows_last=20000`, `swaps_delta_fetched_last=20000`
   - `discovery_cursor_ts=2026-03-06T21:24:42.987393790+00:00`
   - `observed_swaps_max_ts=2026-03-07T07:27:38.295258094+00:00`
   - direct `cursor/head ts-gap ~= 36175 s` (`~10h 02m 55s`)
   - interpretation: runtime stabilization is real, but backlog burn-down is not yet demonstrated; the cursor is still far behind the live head
5. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter

## Ingestion summary

1. `samples=1467`
2. Throughput:
   - `rate_grpc_message_per_s ~353.63`
   - `rate_grpc_tx_updates_per_s ~353.43`
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

1. Direct I/O snapshot: **still much softer than pre-deploy degradation**
   - `pressure io some avg10=8.92`, `avg60=8.19`
   - `vmstat wa last=10`
   - `iostat nvme0n1 aqu-sz last=1.87`, `%util last=17.8`
2. `copybot-app` process:
   - `VmRSS ~151.1 MB`
   - `VmHWM ~222.9 MB`
   - `State=S (sleeping)`
3. cgroup memory:
   - `MemoryCurrent ~6.66 -> 6.67 GB`
   - `MemoryPeak ~6.84 GB`
   - still dominated by `file` cache (`~6.81 GB`), not RSS growth (`anon ~145.2 MB`)
4. DB:
   - `live_copybot.db ~55G`
   - `live_copybot.db-shm ~128K`
   - `live_copybot.db-wal ~57M`
   - writes remain live at `observed_swaps_max_ts=2026-03-07T07:27:38.295258094+00:00`
5. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `fills_rows=0`
   - `shadow_lots_rows=0`
   - interpretation: still consistent with pre-filter-maturation state, not a runtime regression

## Conclusion

1. The `~12h 19m` post-deploy window confirms that the hotfix path stabilized runtime: no restart/OOM regressions, no discovery skip warnings, and discovery duration remains in milliseconds-to-single-digit-seconds instead of minutes.
2. Ingestion remains healthy in the latest samples, but the overnight cumulative backpressure counter shows that transient queue stress has not disappeared completely.
3. The direct `discovery cursor ts` check is the most important new fact in this morning window: despite stable runtime under the new cap, discovery still sits about `10h` behind the observed-swaps head.
4. So the correct morning read is: **stabilized under cap, backlog still material**. This is stronger and more defensible than either “incident persists” or “burn-down already proven.”
