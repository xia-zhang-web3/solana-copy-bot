# Late-Evening Post-PRED2-4 Runtime Report

Date (UTC snapshot): `2026-03-07T21:34:40Z`  
Server: `52.28.0.218`  
Deployed commit: `4f118da`  
Rollout start (service active): `2026-03-07 18:36:41 UTC`  
Observed runtime window: `~2h 57m 59s` (`2026-03-07T18:36:41Z -> 2026-03-07T21:34:40Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/io_pressure_snapshot.txt`
10. `ops/server_reports/raw/2026-03-07_post_pred24_followup_2134_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery runtime: **stable under expanded bounded fetch budget**
   - `p50=4188 ms`, `p95=10483.95 ms`, `max=11804 ms`, `last=3978 ms`
   - `snapshot_recomputed=true` in `6/18` cycles
   - `discovery cycle still running` warnings: `0`
3. Ingestion lag/backpressure: **healthy**
   - `ingestion_lag_ms_p95=1681`, `ingestion_lag_ms_p99=1740`
   - `ws_to_fetch_queue_depth_last=1` (`max=567`)
   - `ws_notifications_backpressured_last=0`
4. Discovery throughput/backlog state: **materially wider fetch, but still not burning down**
   - `swaps_fetch_limit_reached_ratio=1.0` (`18/18`)
   - `swaps_fetch_page_budget_exhausted_ratio=1.0` (`18/18`)
   - `swaps_fetch_pages_last=5/5`
   - `swaps_query_rows_last=100000`
   - `swaps_query_rows_last_page=20000`
   - `swaps_delta_fetched_last=100000`
   - `discovery_cursor_ts=2026-03-07T03:04:48.885805372+00:00`
   - `observed_swaps_max_ts=2026-03-07T21:35:39.600998349+00:00`
   - direct `cursor/head ts-gap ~= 66650 s` (`~18h 30m 50s`)
   - versus the pre-`PRED2-4` snapshot at `2026-03-07 15:28 UTC`, gap still worsened by `+8128 s` (`+~2h 15m 28s`)
   - inferred `cursor_advance_ratio` versus that prior trend snapshot improved to `~0.63`, but remains `< 1.0`
   - interpretation: effective discovery throughput improved materially, but it is still sub-real-time and does not yet produce backlog burn-down
5. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - `followlist_deactivations_suppressed=true`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter and current `HOLD` semantics

## Ingestion summary

1. `samples=357`
2. Throughput:
   - `rate_grpc_message_per_s ~379.38`
   - `rate_grpc_tx_updates_per_s ~379.18`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Transport notes:
   - cumulative `reconnect_count=0`
   - cumulative `stream_gap_detected=0`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1`
   - `ws_to_fetch_queue_depth_max=567`
   - `ws_notifications_backpressured_last=0`

## I/O, Memory, and DB

1. Direct I/O snapshot: **still healthy under the wider fetch budget**
   - `pressure io some avg10=9.30`, `avg60=7.77`
   - `vmstat wa last=7`
   - `iostat nvme0n1 aqu-sz last=1.80`, `%util last=22.5`
2. `copybot-app` process:
   - `VmRSS ~92.6 MB`
   - `VmHWM ~181.1 MB`
   - `State=S (sleeping)`
3. cgroup memory:
   - `MemoryCurrent ~6.27 -> 6.28 GB`
   - `MemoryPeak ~6.54 GB`
   - still dominated by `file` cache (`~6.05 GB`), not RSS growth (`anon ~82.3 MB`)
4. DB:
   - `live_copybot.db ~65G`
   - `live_copybot.db-shm ~128K`
   - `live_copybot.db-wal ~59M`
   - writes remain live at `observed_swaps_max_ts=2026-03-07T21:35:39.600998349+00:00`
5. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `fills_rows=0`
   - `shadow_lots_rows=0`

## Conclusion

1. The first `~3h` post-`PRED2-4` window is operationally clean: no restart/OOM regressions, no discovery skip warnings, healthy ingestion, and acceptable I/O.
2. The widened bounded-fetch path is clearly active: discovery now processes `100000` queried rows per cycle via `5` pages while keeping cycle duration in roughly `4-10s` instead of minutes.
3. But this is still not enough to close `PRED2-3`. The direct cursor/head gap grew further to about `18h 31m`, and the inferred trend ratio remains below real-time (`~0.63 < 1.0`).
4. So the correct late-evening read is: **`PRED2-4` rollout is runtime-safe and improves discovery throughput materially, but backlog burn-down is still not achieved; `PRED2-3` remains open.**
