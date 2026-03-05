# Post-Patch Follow-up Metrics (Pre-Sleep Snapshot)

Date (UTC snapshot): `2026-03-04T21:09:21Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~8h 43m 14s` (`12:25:25Z -> 21:08:39Z` by ingestion samples)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-04_post_patch_followup_2108_snapshot/computed_summary.json`

## Auditor checkpoints (4-6h follow-up)

1. `swaps_fetch_limit_reached_ratio`: **improved**
   - current: `0.5943` (`104/175`)
   - previous (`17:10` snapshot): `1.0` (`95/95`)
2. `duration_ms_p50/p95/max`: **improved in median/tail-last**
   - current: `p50=8197 ms`, `p95=25870.8 ms`, `max=96516 ms`, `last=4231 ms`
   - previous: `p50=12425 ms`, `max=96516 ms`, `last=51556 ms`
   - note: absolute `max` unchanged, but current cycle latency is significantly lower
3. `eligible_wallets_last` / `active_follow_wallets_last`: **unchanged zero**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
4. Stability guards (`oom_kernel_lines`, `NRestarts`, `main_process_exited_count`): **PASS**
   - `oom_kernel_lines=0`
   - `NRestarts=0` (`solana-copy-bot`, `copybot-executor`, `copybot-adapter`)
   - `main_process_exited_count=0`

## Ingestion / health summary

1. Services and health endpoints:
   - all services `active`
   - `8080/8090/18080` health = `ok`
2. Ingestion:
   - `samples=1042`
   - `rate_grpc_message_per_s ~419.82`
   - `rate_grpc_tx_updates_per_s ~419.62`
   - `delta_rpc_429=0`, `delta_rpc_5xx=0`, `delta_parse_rejected_total=0`
   - last lag: `p95=1724 ms`, `p99=1785 ms`

## Memory / DB snapshot

1. `copybot-app`:
   - `VmRSS ~292 MB`, `VmHWM ~561 MB`
2. cgroup memory:
   - `MemoryCurrent ~7.22 GB`
   - dominant component is `file` cache (`~6.78 GB`), not process RSS growth
3. DB:
   - `live_copybot.db ~17G`, `wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-04T21:08:49.662320657+00:00` (fresh writes present)
   - follow/execution tables remain zero in this non-live scope

## Conclusion

1. Pre-sleep follow-up confirms runtime stability remains intact (no OOM/restart regression).
2. Discovery pressure is no longer fully saturated (`cap ratio` fell from `1.0` to `0.594`), with better current cycle durations.
3. Next expected milestone is non-zero discovery output (`eligible_wallets`/`active_follow_wallets`), which is still pending.
