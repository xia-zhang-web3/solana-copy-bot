# Late-Evening Post-Deploy Runtime Report

Date (UTC snapshot): `2026-03-06T21:23:08Z`  
Server: `52.28.0.218`  
Deployed commit: `ac5c87b`  
Rollout start (service active): `2026-03-06 19:08:45 UTC`  
Observed runtime window: `~2h 14m 23s` (`2026-03-06T19:08:45Z -> 2026-03-06T21:23:08Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/io_pressure_snapshot.txt`
10. `ops/server_reports/raw/2026-03-06_post_deploy_followup_2121_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Deploy/migration gates: **PASS**
   - service active on `ac5c87b`
   - migrations `0018..0021` applied at `2026-03-06 19:08:45 UTC`
3. Discovery duration: **materially improved**
   - `p50=28 ms`, `p95=7290.25 ms`, `max=11850 ms`, `last=22 ms`
   - `snapshot_recomputed=true` only in `5/14` cycles
   - `snapshot_recomputed=false` cycles are now effectively swap-fetch-only and finish in `~22 ms`
   - `discovery cycle still running` warnings: `0`
4. Ingestion lag/backpressure: **recovered**
   - `ingestion_lag_ms_p95=1663`, `ingestion_lag_ms_p99=1720`
   - `ws_to_fetch_queue_depth_last=1` (`max=74`)
   - `ws_notifications_backpressured_last=0` (`max=0`)
5. Discovery cap pressure: **still saturated under new tighter cap**
   - `swaps_fetch_limit_reached_ratio=1.0` (`14/14`)
   - `swaps_query_rows_last=20000`, `swaps_delta_fetched_last=20000`
   - interpretation: the system is stable while continuously hitting the new `20k` per-cycle cap, but one snapshot is not enough to prove backlog burn-down yet; that needs a follow-up trend on cursor lag / ts-gap
6. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter

## Ingestion summary

1. `samples=270`
2. Throughput:
   - `rate_grpc_message_per_s ~358.50`
   - `rate_grpc_tx_updates_per_s ~358.30`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Transport notes:
   - cumulative `reconnect_count=0`
   - cumulative `stream_gap_detected=0`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1` (`max=74`)
   - `ws_notifications_backpressured_last=0`

## I/O, Memory, and DB

1. Direct I/O snapshot: **materially softer than pre-deploy degradation**
   - `pressure io some avg10=5.96`, `avg60=5.82`
   - `vmstat wa last=11`
   - `iostat nvme0n1 aqu-sz last=1.98`, `%util last=15.5`
2. `copybot-app` process:
   - `VmRSS ~120.9 MB`
   - `VmHWM ~184.0 MB`
   - `State=S (sleeping)`
3. cgroup memory:
   - `MemoryCurrent ~6.35 -> 6.36 GB`
   - `MemoryPeak ~6.36 GB`
   - still dominated by `file` cache (`~6.54 GB`), not RSS growth (`anon ~111.4 MB`)
4. DB:
   - `live_copybot.db ~48G`
   - `live_copybot.db-shm ~128K`
   - `live_copybot.db-wal ~52M`
   - `observed_swaps_max_ts=2026-03-06T21:25:07.115081402+00:00` (writes are live)
5. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `fills_rows=0`
   - `shadow_lots_rows=0`
   - interpretation: still consistent with pre-filter-maturation state, not a runtime regression

## Conclusion

1. The first `~2h 14m` post-deploy window on `ac5c87b` shows a real runtime improvement, not just a successful restart.
2. Discovery recompute no longer dominates every cycle: recompute cycles are single-digit seconds to `11.85s`, while non-recompute cycles complete in `~22 ms`.
3. Ingestion lag/backpressure has normalized and direct I/O pressure is dramatically below the pre-deploy degradation snapshot.
4. Fetch-limit saturation remains `14/14` under the new `20k` cap. That currently supports “stable under cap” much more strongly than “proven backlog burn-down”; confirming burn-down needs at least one more trend snapshot, but the current state is no longer an active degradation incident.
5. Zero followlist output remains consistent with the active `>=4 days` activity filter; business-output observation still depends on filter maturation, not runtime instability.
