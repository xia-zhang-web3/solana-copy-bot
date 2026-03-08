# Morning Post-PRED2-4 Runtime Report

Date (UTC snapshot): `2026-03-08T05:48:45Z`  
Server: `52.28.0.218`  
Deployed commit: `4f118da`  
Rollout start (service active): `2026-03-07 18:36:41 UTC`  
Observed runtime window: `~11h 12m 04s` (`2026-03-07T18:36:41Z -> 2026-03-08T05:48:45Z`)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/rollout_start.txt`
2. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/services_health_memory.txt`
3. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/ingestion_metrics_since_rollout.log`
4. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/discovery_since_rollout.log`
5. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/service_events_since_rollout.log`
6. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/kernel_oom_since_rollout.log`
7. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/memory_cgroup_samples.txt`
8. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/db_snapshot_since_rollout.txt`
9. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/io_pressure_snapshot.txt`
10. `ops/server_reports/raw/2026-03-08_post_pred24_followup_0548_snapshot/computed_summary.json`

## Checkpoint status

1. Stability/OOM: **PASS**
   - `NRestarts=0`
   - `main_process_exited_count=0`
   - `oom_kernel_lines=0`
2. Discovery runtime: **stable under 100k bounded fetch**
   - `p50=4144 ms`, `p95=11967.2 ms`, `max=12527 ms`, `last=4096 ms`
   - `snapshot_recomputed=true` in `23/68` cycles
   - `discovery cycle still running` warnings: `0`
3. Ingestion lag/backpressure: **healthy**
   - `ingestion_lag_ms_p95=1695`, `ingestion_lag_ms_p99=1779`
   - `ws_to_fetch_queue_depth_last=1` (`max=636`)
   - `ws_notifications_backpressured_last=0`, cumulative delta `0`
4. Discovery throughput/backlog state: **improved, but still sub-real-time**
   - `swaps_fetch_limit_reached_ratio=1.0` (`68/68`)
   - `swaps_fetch_page_budget_exhausted_ratio=1.0` (`68/68`)
   - `swaps_fetch_pages_last=5/5`
   - `swaps_query_rows_last=100000`
   - `swaps_query_rows_last_page=20000`
   - `swaps_delta_fetched_last=100000`
   - `discovery_cursor_ts=2026-03-07T10:00:27.658060832+00:00`
   - `observed_swaps_max_ts=2026-03-08T05:49:34.315203276+00:00`
   - direct `cursor/head ts-gap ~= 71347 s` (`~19h 49m 07s`)
   - versus the prior post-`PRED2-4` snapshot at `2026-03-07 21:34 UTC`, gap still worsened by `+4697 s` (`+~1h 18m 17s`)
   - inferred `cursor_advance_ratio` on that overnight `>=8h` window improved to `~0.84`, but remains `< 1.0`
   - interpretation: this is the first full overnight post-rollout trend window, and it shows better cursor advance than the first `~3h` read, but still no backlog burn-down
5. Followlist/discovery business output: **unchanged and still expected**
   - `eligible_wallets_last=0`
   - `active_follow_wallets_last=0`
   - `followlist_deactivations_suppressed=true`
   - interpretation: still consistent with the active `>=4 days` followlist activity filter and current discovery-semantics `HOLD`

## Ingestion summary

1. `samples=1342`
2. Throughput:
   - `rate_grpc_message_per_s ~394.22`
   - `rate_grpc_tx_updates_per_s ~394.02`
3. Error counters:
   - `delta_rpc_429=0`
   - `delta_rpc_5xx=0`
   - `delta_parse_rejected_total=0`
4. Transport notes:
   - cumulative `reconnect_count=0`
   - cumulative `stream_gap_detected=0`
5. Queue/backpressure:
   - `ws_to_fetch_queue_depth_last=1`
   - `ws_to_fetch_queue_depth_max=636`
   - `ws_notifications_backpressured_last=0`

## I/O, Memory, and DB

1. Direct I/O snapshot: **higher than the 23:34 read, still far below the pre-hotfix incident**
   - `pressure io some avg10=13.00`, `avg60=13.15`
   - `pressure io full avg10=12.99`, `avg60=13.10`
   - `vmstat wa last=18`
   - `iostat nvme0n1 aqu-sz last=2.07`, `%util last=31.2`
2. `copybot-app` process:
   - `VmRSS ~97.2 MB`
   - `VmHWM ~181.1 MB`
   - `State=S (sleeping)`
3. cgroup memory:
   - `MemoryCurrent ~6.45 -> 6.47 GB`
   - `MemoryPeak ~6.55 GB`
   - still dominated by `file` cache (`~6.19 GB`), not RSS growth (`anon ~87.0 MB`)
4. DB:
   - `live_copybot.db ~71G`
   - `live_copybot.db-shm ~128K`
   - `live_copybot.db-wal ~59M`
   - writes remain live at `observed_swaps_max_ts=2026-03-08T05:49:34.315203276+00:00`
5. Business-state tables remain empty:
   - `followlist_total=0`
   - `copy_signals_rows=0`
   - `orders_rows=0`
   - `fills_rows=0`
   - `shadow_lots_rows=0`

## Conclusion

1. The first overnight `>=8h` post-`PRED2-4` window is operationally clean: no restart/OOM regressions, no discovery skip warnings, healthy ingestion, and direct I/O still well below the pre-hotfix failure regime.
2. The widened bounded-fetch path remains active end-to-end: discovery continues processing `100000` queried rows per cycle via `5` pages while keeping cycle times in roughly `4-12s`.
3. But `PRED2-3` is still not closed. The direct cursor/head gap increased further to about `19h 49m`, and the overnight inferred `cursor_advance_ratio` stayed below real-time (`~0.84 < 1.0`).
4. So the correct morning read is: **`PRED2-4` rollout remains runtime-safe and improves discovery throughput again, but backlog burn-down is still not achieved; `PRED2-3` remains open.**
