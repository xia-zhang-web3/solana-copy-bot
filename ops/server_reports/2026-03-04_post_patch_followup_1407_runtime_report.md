# Post-Patch Follow-up Metrics

Date (UTC snapshot): `2026-03-04T14:08:23Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~101m 55s` (`12:25:25Z -> 14:07:20Z` by ingestion samples)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/computed_summary.json`

## Checklist status (post-rollout monitoring)

1. `copybot-app` does not crash (`status=9/KILL`, restarts): **PASS**
   - `main_process_exited_count=0`, `failed_count=0`
   - `NRestarts=0` for `solana-copy-bot`, `copybot-executor`, `copybot-adapter`
2. Kernel OOM absence: **PASS**
   - `kernel_oom_since_rollout.log` is empty (`oom_kernel_lines=0`)
3. Memory not drifting toward 7+ GiB RSS: **PASS**
   - `copybot-app VmRSS`: `~323 MB` (sample), `VmHWM=487 MB`
   - cgroup `MemoryCurrent`: `~4.62 -> 4.64 GB`, dominated by `file` cache (`~4.21-4.23 GB`), `anon ~315 MB`
4. Discovery liveness: **PASS**
   - `discovery cycle completed`: `35`
   - `discovery cycle still running`: `0`
   - cycle duration: `min=7021 ms`, `p50=8431 ms`, `max=46085 ms`, `last=13307 ms`
5. New discovery telemetry fields: **ATTENTION**
   - `swaps_warm_loaded_gt0_count=0` (normal without cold rehydrate after cursor warm-start)
   - `swaps_fetch_limit_reached=true` in `35/35` cycles (`ratio=1.0`)
   - `swaps_evicted_due_cap_last=120000` (`max=120000`)
   - note: constant limit hit means window cap is always saturated; this is visible and should be tracked in 4h/24h checkpoints
6. Followlist is not unexpectedly reset on restart: **N/A (baseline zero)**
   - `followlist_active=0`, `followlist_total=0` for entire window
   - no false drop can be validated until non-zero followlist appears
7. Ingestion health: **PASS**
   - samples: `204`
   - throughput: `~353.83 msg/s`, `~353.64 tx-updates/s`
   - `delta_rpc_429=0`, `delta_rpc_5xx=0`, `delta_parse_rejected_total=0`
   - lag now: `p95=1718 ms`, `p99=1747 ms` (`max spikes at rollout warm-up: p95=18597, p99=19036`)
8. Adapter/executor health: **PASS**
   - `8080/healthz` adapter: `status=ok`
   - `8090/healthz` executor: `status=ok`
   - `18080/healthz` mock upstream: `status=ok`

## DB snapshot (fast probes)

1. `followlist_active=0`
2. `copy_signals_rows=0`
3. `orders_rows=0`
4. `shadow_lots_rows=0`
5. `observed_swaps_min_ts=2026-03-03T17:05:37+00:00`
6. `observed_swaps_max_ts=2026-03-04T14:11:27.445812280+00:00`
7. DB files:
   - `live_copybot.db ~12G`
   - `live_copybot.db-wal ~4.7G`
   - `live_copybot.db-shm ~3.6M`

## Conclusion

1. Hotfix stability objective is currently met on this window: no OOM, no restart loop, discovery cycles complete regularly.
2. Ingestion remains stable with zero `429/5xx` growth.
3. Main open runtime signal to track on 4h/24h checkpoints: persistent `swaps_fetch_limit_reached=true` (cap pressure on discovery window).
