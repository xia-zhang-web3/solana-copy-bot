# Post-Gym Runtime Report

Date (UTC snapshot): `2026-03-04T11:34:06Z`  
Server: `52.28.0.218`  
Scope: runtime status after ~3h run window.

## Raw artifacts

1. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/services_show_latest.txt`
2. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/health_and_ps_latest.txt`
3. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/db_kv.txt`
4. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/db_ts_bounds.txt`
5. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/discovery_misc_4h.log`
6. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/ingestion_metrics_4h.log`
7. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/ingestion_metrics_last15m.log`
8. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/service_events_4h.log`
9. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/kernel_oom_4h.log`
10. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/memory_snapshot.txt`
11. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/computed_summary.json`

## 1) Critical findings

1. `solana-copy-bot.service` is unstable due repeated OOM kill.
   1. In last ~4h: `service_events_killed_sig9_count=21`, `service_events_started_count=21`.
   2. Current `NRestarts=25` (at snapshot time).
   3. Kernel confirms OOM target is `copybot-app` in this cgroup (`task_memcg=/system.slice/solana-copy-bot.service`), with anon RSS around `~7.5 GB`.
2. Memory pressure is hard:
   1. `Mem: 7.6Gi total`, `7.1Gi used`, `127Mi free`, `Swap: 0B`.
   2. No swap configured.
3. Discovery scheduler is overloaded:
   1. `37` warnings in 4h: `discovery cycle still running, skipping scheduled trigger`.
   2. No `discovery cycle completed` lines in this 4h window.

## 2) What is stable

1. `copybot-executor`, `copybot-adapter`, `copybot-execution-mock-upstream` are continuously up from `2026-03-03 18:03 UTC` (`NRestarts=0`).
2. Health endpoints are up:
   1. `8080` adapter -> `status=ok`
   2. `8090` executor -> `status=ok`
   3. `18080` mock upstream -> `status=ok`

## 3) Ingestion behavior (4h journal window)

1. Parsed `278` ingestion telemetry samples.
2. Due frequent app restarts, data split into `23` monotonic segments (counter reset between segments).
3. Across valid segments:
   1. weighted avg `grpc_message_total` rate: `~320.96 msg/s`
   2. weighted avg `grpc_transaction_updates_total` rate: `~320.76 tx-updates/s`
   3. `rpc_429 delta=0`, `rpc_5xx delta=0`, `parse_rejected_total delta=0` in all parsed segments.
4. Backpressure remains high in many samples (`ws_to_fetch_queue_depth` often near cap, high `ws_notifications_backpressured` counters).

## 4) Discovery quality-cache behavior (4h)

1. `discovery token quality cache summary` samples: `21`.
2. `budget_exhausted` grew `11675 -> 12682` (`+1007`).
3. Last sample: `rpc_attempted=6`, `fetched_ok=6`, `rpc_spent_ms=1515`.

## 5) DB accumulation snapshot

From successful DB probes:

1. `observed_swaps_rows=8,569,143`
2. `wallets_rows=356,820`
3. `wallet_metrics_rows=1,057,737`
4. `followlist_active=0`
5. `copy_signals_rows=0`
6. `orders_rows=0`
7. `shadow_lots_rows=0`

Bounds:

1. `observed_swaps_min_ts=2026-03-03T17:05:37+00:00`
2. `observed_swaps_max_ts=2026-03-04T11:35:26.681637575+00:00`
3. `wallets_min_first_seen=2026-03-03T17:05:37+00:00`
4. `wallets_max_last_seen=2026-03-04T05:48:15.224322722+00:00`

Compared to prior morning sample (`7,889,132`), observed swaps increased by `+680,011` (`~45.9 rows/s` over `07:17:29 -> 11:24:14 UTC`).

## 6) Operational conclusion

1. Test run produced useful ingestion data and DB growth.
2. But current app runtime is not stable for long-window validation:
   1. frequent OOM kills reset counters and disrupt continuity,
   2. discovery cycle cannot complete on schedule.
3. Until memory pressure is fixed, statistics are valid only as short segmented windows, not as continuous runtime evidence.
