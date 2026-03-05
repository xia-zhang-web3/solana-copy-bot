# Morning Post-Patch Runtime Report

Date (UTC snapshot): `2026-03-05T07:45:25Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~19h 19m 01s` (`12:25:25Z -> 07:44:26Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-05_post_patch_followup_0744_snapshot/computed_summary.json`

## Auditor checkpoint status

1. `swaps_fetch_limit_reached_ratio`: **improved further**
   - current morning: `0.2687` (`104/387`)
   - previous evening (`2026-03-04 21:08 UTC`): `0.5943` (`104/175`)
2. Discovery duration (`p50/p95/max`): **stable/improved**
   - current: `p50=5612 ms`, `p95=26121.8 ms`, `max=96516 ms`, `last=5042 ms`
   - previous: `p50=8197 ms`, `p95=25870.8 ms`, `max=96516 ms`, `last=4231 ms`
   - interpretation: median improved, worst absolute max unchanged, tail (`p95`) remains around ~26s
3. `eligible_wallets_last` / `active_follow_wallets_last`: **still zero**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
4. Stability guards: **PASS**
   - `oom_kernel_lines=0`
   - `NRestarts=0` for app/executor/adapter
   - `main_process_exited_count=0`

## Ingestion and health

1. All services are `active`; health endpoints `8080/8090/18080` return `ok`.
2. Ingestion summary:
   - `samples=2309`
   - `rate_grpc_message_per_s ~396.13`
   - `rate_grpc_tx_updates_per_s ~395.93`
   - `delta_rpc_429=0`, `delta_rpc_5xx=0`, `delta_parse_rejected_total=0`
   - last lag: `p95=1716 ms`, `p99=1788 ms`
3. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_last=53721` (counter cumulative, not drop events)

## Memory and DB

1. `copybot-app` memory is stable:
   - `VmRSS ~308 MB`
   - `VmHWM ~561 MB`
2. cgroup memory is still cache-dominated:
   - `MemoryCurrent ~7.29 -> 7.30 GB`
   - `file ~6.83 GB`, `anon ~307 MB`
3. DB growth continues (expected for this run):
   - `live_copybot.db ~24G`
   - `live_copybot.db-wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-05T07:44:53.230228183+00:00` (fresh writes confirmed)

## Conclusion

1. Overnight run remained stable with no OOM/restart regressions.
2. Discovery cap pressure improved materially (`ratio` reduced to `0.2687`).
3. Main remaining runtime gap is business output (`eligible_wallets`/`active_follow_wallets` still zero).
