# Evening Post-Patch Runtime Report

Date (UTC snapshot): `2026-03-05T20:23:36Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~31h 57m 45s` (`2026-03-04T12:25:25Z -> 2026-03-05T20:23:10Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-05_post_patch_followup_2021_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0` for `solana-copy-bot`, `copybot-executor`, `copybot-adapter`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery cap pressure: **improved further**
   - current: `swaps_fetch_limit_reached_ratio=0.1625` (`104/640`)
   - afternoon (`2026-03-05 14:47 UTC`): `0.1970` (`104/528`)
3. Discovery duration: **slightly softer but controlled**
   - current: `p50=6827.5 ms`, `p95=25163.8 ms`, `max=96516 ms`, `last=16093 ms`
   - afternoon: `p50=6052.5 ms`, `p95=22817.65 ms`, `max=96516 ms`, `last=12693 ms`
4. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter
5. Transport counters: **stable**
   - cumulative `reconnect_count=1`
   - cumulative `stream_gap_detected=1`
   - both unchanged versus the afternoon snapshot

## Ingestion summary

1. `samples=3821`
2. Throughput:
   - `rate_grpc_message_per_s ~368.30`
   - `rate_grpc_tx_updates_per_s ~368.10`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Current lag:
   - `ingestion_lag_ms_p95=1790`
   - `ingestion_lag_ms_p99=1881`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_last=61427` (cumulative counter)

## Memory and DB

1. `copybot-app` process:
   - `VmRSS ~317 MB`
   - `VmHWM ~599 MB`
2. cgroup memory:
   - `MemoryCurrent ~7.44 -> 7.47 GB`
   - still dominated by `file` cache (`~6.97 -> 6.99 GB`), not RSS leak
3. DB:
   - `live_copybot.db ~32G`
   - `live_copybot.db-wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-05T20:28:05.148399863+00:00` (writes are live)
4. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `shadow_lots_rows=0`
   - interpretation: still consistent with pre-filter-maturation state, not a runtime regression

## Conclusion

1. Runtime remains stable with no OOM/restart regressions for `~31h 58m`.
2. Discovery saturation trend keeps improving (`cap ratio` down to `~0.16`), but latency softened slightly versus the afternoon window and the rare long tail remains.
3. Zero followlist output remains consistent with the active `>=4 days` activity filter; live execution evidence stays pending until the filter window matures.
