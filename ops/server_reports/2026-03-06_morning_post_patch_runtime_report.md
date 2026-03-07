# Morning Post-Patch Runtime Report

Date (UTC snapshot): `2026-03-06T07:07:29Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~42h 41m 53s` (`2026-03-04T12:25:25Z -> 2026-03-06T07:07:18Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-06_post_patch_followup_0706_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0` for `solana-copy-bot`, `copybot-executor`, `copybot-adapter`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery cap pressure: **improved further**
   - current: `swaps_fetch_limit_reached_ratio=0.1218` (`104/854`)
   - previous evening (`2026-03-05 20:23 UTC`): `0.1625` (`104/640`)
3. Discovery duration: **regressed overnight**
   - current: `p50=8506.0 ms`, `p95=74436.6 ms`, `max=109400 ms`, `last=107303 ms`
   - previous evening: `p50=6827.5 ms`, `p95=25163.8 ms`, `max=96516 ms`, `last=16093 ms`
4. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter

## Ingestion summary

1. `samples=5104`
2. Throughput:
   - `rate_grpc_message_per_s ~362.82`
   - `rate_grpc_tx_updates_per_s ~362.62`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Current lag:
   - `ingestion_lag_ms_p95=1831`
   - `ingestion_lag_ms_p99=1952`
5. Transport notes:
   - cumulative `reconnect_count=1`
   - cumulative `stream_gap_detected=1`
   - near `2026-03-06T07:06:18Z` ingestion had a transient pressure spike (`ingestion_lag_ms_p95=17969`, `ws_to_fetch_queue_depth=1486`), but the latest sample recovered
6. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_last=158393` (cumulative counter)

## Memory and DB

1. `copybot-app` process:
   - `VmRSS ~251 MB`
   - `VmHWM ~599 MB`
2. cgroup memory:
   - `MemoryCurrent ~7.39 -> 7.43 GB`
   - still dominated by `file` cache (`~6.98 -> 7.02 GB`), not RSS leak
3. DB:
   - `live_copybot.db ~39G`
   - `live_copybot.db-wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-06T07:08:33.404664545+00:00` (writes are live)
4. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `shadow_lots_rows=0`
   - interpretation: still consistent with pre-filter-maturation state, not a runtime regression

## Conclusion

1. Runtime remains stable with no OOM/restart regressions for `~42h 42m`.
2. Discovery cap saturation continues to improve, but discovery cycle latency materially regressed overnight and now needs explicit observation/investigation.
3. Zero followlist output remains consistent with the active `>=4 days` activity filter; live execution evidence stays pending until the filter window matures.
