# Evening Post-Patch Runtime Report

Date (UTC snapshot): `2026-03-06T16:41:24Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~52h 15m 59s` (`2026-03-04T12:25:25Z -> 2026-03-06T16:41:24Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-06_post_patch_followup_1623_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0` for `solana-copy-bot`, `copybot-executor`, `copybot-adapter`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery cap pressure: **improved further**
   - current: `swaps_fetch_limit_reached_ratio=0.1030` (`104/1010`)
   - previous morning (`2026-03-06 07:07 UTC`): `0.1218` (`104/854`)
3. Discovery duration: **degraded further**
   - current: `p50=11333.5 ms`, `p95=151088.0 ms`, `max=592013 ms`, `last=190520 ms`
   - previous morning: `p50=8506.0 ms`, `p95=74436.6 ms`, `max=109400 ms`, `last=107303 ms`
4. Ingestion lag/backpressure: **degraded**
   - current: `ingestion_lag_ms_p95=61630`, `ingestion_lag_ms_p99=62076`
   - `ws_to_fetch_queue_depth_last=2049` (`max=2049`)
   - `ws_notifications_backpressured_last=1124975`
5. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter

## Ingestion summary

1. `samples=6200`
2. Throughput:
   - `rate_grpc_message_per_s ~365.13`
   - `rate_grpc_tx_updates_per_s ~364.93`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Transport notes:
   - cumulative `reconnect_count=1`
   - cumulative `stream_gap_detected=1`
   - the last several samples remain degraded, so this is not just a one-sample spike
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=2049` (`max=2049`)
   - `ws_notifications_backpressured_last=1124975` (cumulative counter)

## Memory and DB

1. `copybot-app` process:
   - `VmRSS ~293 MB`
   - `VmHWM ~599 MB`
2. cgroup memory:
   - `MemoryCurrent ~7.38 -> 7.43 GB`
   - still dominated by `file` cache (`~6.95 -> 7.00 GB`), not RSS leak
3. DB:
   - `live_copybot.db ~45G`
   - `live_copybot.db-shm ~7.3M`
   - `live_copybot.db-wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-06T16:42:25.257029360+00:00` (writes are live)
4. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `shadow_lots_rows=0`
   - interpretation: still consistent with pre-filter-maturation state, not a runtime regression

## Conclusion

1. Runtime remains stable with no OOM/restart regressions for `~52h 16m`.
2. Cap saturation continues to improve, but both discovery duration and ingestion lag/backpressure are now materially degraded and should be treated as an active runtime attention item.
3. Zero followlist output remains consistent with the active `>=4 days` activity filter; live execution evidence stays pending until the filter window matures.
