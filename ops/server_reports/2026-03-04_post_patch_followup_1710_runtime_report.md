# Post-Patch Follow-up Metrics (Fresh Snapshot)

Date (UTC snapshot): `2026-03-04T17:10:52Z`  
Server: `52.28.0.218`  
Rollout start (service active): `2026-03-04 12:25:02 UTC`  
Observed runtime window: `~4h 44m 52s` (`12:25:25Z -> 17:10:17Z` by ingestion samples)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-04_post_patch_followup_1710_snapshot/computed_summary.json`

## Quick status

1. Services stable: `solana-copy-bot`, `copybot-executor`, `copybot-adapter`, `mock-upstream` all `active`, `NRestarts=0`.
2. No crash loop: `main_process_exited_count=0`, `failed_count=0`, no `status=9/KILL`.
3. No kernel OOM since rollout: `oom_kernel_lines=0`.
4. Health endpoints: `8080/8090/18080` all `status=ok`.

## Ingestion metrics

1. Samples: `568`
2. Throughput:
   - `delta_grpc_message_total=6,979,165` (`~408.33 msg/s`)
   - `delta_grpc_transaction_updates_total=6,975,747` (`~408.13 tx-updates/s`)
3. Errors/counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Lag now: `p95=1703 ms`, `p99=1817 ms`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1` (`max=2049`)
   - `ws_notifications_backpressured_last=44265`
   - `ws_notifications_dropped=0` (last sample)

## Discovery metrics

1. Cycles:
   - `completed_count=95`
   - `still_running_count=0`
2. Duration:
   - `min=7021 ms`
   - `p50=12425 ms`
   - `max=96516 ms`
   - `last=51556 ms`
3. Window/cap signals:
   - `swaps_delta_fetched=120000` each cycle
   - `swaps_fetch_limit_reached=true` in `95/95` cycles
   - `swaps_evicted_due_cap_last=120000`
4. Followlist outcome in this window:
   - `active_follow_wallets_last=0`
   - `eligible_wallets_last=0`
5. Quality-cache summary (from discovery logs):
   - `rpc_attempted_last=5`
   - `fetched_ok_last=5`
   - `rpc_spent_ms_last=1601`
   - `budget_exhausted_last=1203`
   - `cache_miss_last=894`

## Memory and DB snapshot

1. Process memory (`copybot-app`):
   - `VmRSS ~400 MB`
   - `VmHWM ~540 MB`
2. Cgroup memory:
   - `MemoryCurrent ~6.99 GB`
   - dominated by `file` cache (`~6.46-6.47 GB`), `anon ~394 MB`
3. DB:
   - `live_copybot.db ~14G`
   - `live_copybot.db-wal ~4.7G`
   - `observed_swaps_max_ts=2026-03-04T17:10:20.639921131+00:00` (fresh writes present)
   - `followlist_active=0`, `copy_signals_rows=0`, `orders_rows=0`

## Conclusion

1. Fresh 4h+ snapshot confirms runtime stability after hotfix: no OOM/restart regressions.
2. Ingestion is healthy (stable lag, no 429/5xx growth, no parse rejects).
3. Main attention point remains unchanged: discovery consistently hits fetch limit and cap eviction each cycle.
