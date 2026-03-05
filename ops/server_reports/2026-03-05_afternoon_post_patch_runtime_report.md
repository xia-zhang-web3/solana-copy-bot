# Afternoon Post-Patch Runtime Report

Date (UTC snapshot): `2026-03-05T14:47:55Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~26h 21m 27s` (`2026-03-04T12:25:25Z -> 2026-03-05T14:46:52Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-05_post_patch_followup_1447_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0` for `solana-copy-bot`, `copybot-executor`, `copybot-adapter`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery cap pressure: **improved**
   - current: `swaps_fetch_limit_reached_ratio=0.1970` (`104/528`)
   - morning (`2026-03-05 07:44 UTC`): `0.2687` (`104/387`)
3. Discovery duration: **stable**
   - current: `p50=6052.5 ms`, `p95=22817.65 ms`, `max=96516 ms`, `last=12693 ms`
   - morning: `p50=5612 ms`, `p95=26121.8 ms`, `max=96516 ms`, `last=5042 ms`
4. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter

## Ingestion summary

1. `samples=3151`
2. Throughput:
   - `rate_grpc_message_per_s ~376.87`
   - `rate_grpc_tx_updates_per_s ~376.67`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Current lag:
   - `ingestion_lag_ms_p95=3218`
   - `ingestion_lag_ms_p99=3922`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_last=56053` (cumulative counter)

## Memory and DB

1. `copybot-app` process:
   - `VmRSS ~303 MB`
   - `VmHWM ~599 MB`
2. cgroup memory:
   - `MemoryCurrent ~7.29 -> 7.33 GB`
   - dominated by `file` cache (`~6.83 -> 6.87 GB`), not RSS leak
3. DB:
   - `live_copybot.db ~28G`
   - `live_copybot.db-wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-05T14:47:23.344617466+00:00` (writes are live)

## Conclusion

1. Runtime remains stable with no OOM/restart regressions.
2. Discovery saturation trend keeps improving (`cap ratio` down to `~0.20`).
3. Zero followlist output remains consistent with the active `>=4 days` activity filter and is not treated as an incident before the filter window matures.
