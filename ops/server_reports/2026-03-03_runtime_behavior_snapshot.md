# Runtime Behavior Snapshot

Date (UTC): 2026-03-03T20:10:15Z  
Server: `52.28.0.218`  
Scope: current runtime behavior (uptime, ingestion/discovery dynamics, DB counters)

## Raw artifacts

1. `ops/server_reports/raw/2026-03-03_runtime_snapshot/ingestion_metrics_current_pid.log`
2. `ops/server_reports/raw/2026-03-03_runtime_snapshot/discovery_cycles_current_pid.log`
3. `ops/server_reports/raw/2026-03-03_runtime_snapshot/discovery_quality_current_pid.log`

## 1) Service state and uptime

All services are `active/running`.

1. `solana-copy-bot.service`
   1. `MainPID=82611`
   2. `active_since=2026-03-03 18:03:16 UTC`
   3. `uptime=7512s` (`2:05:12`)
2. `copybot-executor.service`
   1. `MainPID=82609`
   2. `active_since=2026-03-03 18:03:16 UTC`
   3. `uptime=7512s` (`2:05:12`)
3. `copybot-adapter.service`
   1. `MainPID=82610`
   2. `active_since=2026-03-03 18:03:16 UTC`
   3. `uptime=7512s` (`2:05:12`)
4. `copybot-execution-mock-upstream.service`
   1. `MainPID=82478`
   2. `active_since=2026-03-03 18:02:56 UTC`
   3. `uptime=7532s` (`2:05:32`)

## 2) Health endpoints

1. `GET http://127.0.0.1:8080/healthz` -> `status=ok` (adapter)
2. `GET http://127.0.0.1:8090/healthz` -> `status=ok` (executor)
3. `GET http://127.0.0.1:18080/healthz` -> `status=ok` (mock upstream)

## 3) Process footprint (point-in-time)

1. `copybot-app` (`PID 82611`)
   1. `%CPU=54.8`
   2. `%MEM=25.0`
   3. `RSS=1,998,256 KB` (~1.9 GB)
2. `copybot-executor` (`PID 82609`)
   1. `%CPU=0.0`
   2. `RSS=9,372 KB`
3. `copybot-adapter` (`PID 82610`)
   1. `%CPU=0.0`
   2. `RSS=7,160 KB`
4. `mock upstream` (`PID 82478`)
   1. `%CPU=0.0`
   2. `RSS=20,264 KB`

## 4) Ingestion behavior (current PID window)

Window parsed from app logs:

1. `from=2026-03-03T18:37:54`
2. `to=2026-03-03T20:07:44`
3. `duration=5390s`
4. samples: `180`

Throughput deltas:

1. `grpc_message_total`: `+2,010,948` (`373.09 msg/s`)
2. `grpc_transaction_updates_total`: `+2,009,870` (`372.89 tx-updates/s`)
3. `ws_notifications_seen`: `+855,419` (`158.70/s`)

Error/quality counters:

1. `parse_rejected_total delta=0`
2. `rpc_429 delta=0`
3. `rpc_5xx delta=0`

Lag/queue:

1. `ingestion_lag_ms_p95` last=`1687`, max=`6971`
2. `ingestion_lag_ms_p99` last=`1724`, max=`7633`
3. `ws_to_fetch_queue_depth` last=`0`, max=`61`

Recent 30s snapshot:

1. `20:09:45`: `grpc_message_total=2,960,663`
2. `20:10:15`: `grpc_message_total=2,969,958`
3. recent rate: `~309.8 msg/s`

## 5) DB state (runtime data accumulation)

Current counters:

1. `observed_swaps_rows=1,431,397`
2. `wallets_rows=167,111`
3. `wallet_metrics_rows=489,129`
4. `followlist_active=0`
5. `copy_signals_rows=0`
6. `orders_rows=0`
7. `shadow_lots_rows=0`

Short-window activity (`observed_swaps`):

1. last `5m`: `48,551` swaps (`~161.8/s`)
2. last `30m`: `285,768` swaps (`~158.8/s`)
3. last `60m`: `559,082` swaps (`~155.3/s`)
4. distinct wallets in last `30m`: `64,190`

TS bounds:

1. `observed_swaps`: `2026-03-03 17:05:37` -> `2026-03-03 20:09:29`
2. `wallets`: `first_seen=2026-03-03T17:05:37+00:00`, `max_last_seen=2026-03-03T20:00:15.488466357+00:00`

DB files:

1. `live_copybot.db = 1.7G`
2. `live_copybot.db-wal = 91M`
3. `live_copybot.db-shm = 196K`

## 6) Discovery behavior

Config gates in runtime config:

1. `scoring_window_days=30`
2. `decay_window_days=7`
3. `follow_top_n=10`
4. `min_trades=10`
5. `min_active_days=4`
6. `min_score=0.6`
7. `min_buy_count=10`
8. `min_tradable_ratio=0.25`
9. `max_rug_ratio=0.60`
10. `refresh_seconds=180`

Cycle stats (parsed 23 latest completed cycles):

1. `duration_ms min=54,112`
2. `duration_ms p50=141,824`
3. `duration_ms max=248,784`
4. `duration_ms last=248,784`
5. growth in this sample: `+194,672 ms` (duration trend up)

Last completed cycle (`20:04:25Z`):

1. `eligible_wallets=0`
2. `follow_promoted=0`
3. `active_follow_wallets=0`
4. `wallets_seen=167,111`
5. `swaps_delta_fetched=62,196`
6. `swaps_window=1,331,188`

Latest wallet_metrics window summary:

1. `wallets_latest=167,111`
2. `score >= 0.6`: `0`
3. `score > 0`: `0`
4. `avg_score=0.0`, `max_score=0.0`
5. `wallets_trades_ge10=12,845`
6. `wallets_closed_gt0=28,894`

## 7) Token-quality RPC behavior (discovery)

From 24 quality summary samples:

1. `budget_exhausted`: `2047 -> 3498` (increasing)
2. latest: `cache_miss=3116`, `cache_stale=389`
3. latest: `rpc_attempted=7`, `fetched_ok=7`
4. latest: `rpc_spent_ms=1530` (near budget cap)

## 8) Runtime interpretation

1. Runtime is healthy in infra sense: services and healthz are stable, no rpc 429/5xx growth in ingestion counters.
2. Ingestion is high-throughput and steady (`~300-370 gRPC msg/s` observed in this snapshot).
3. Discovery pipeline is functional but overloaded by volume: cycle duration is rising far above configured `refresh_seconds=180`.
4. Followlist remains empty (`0`) in current history window; execution/shadow output remains zero (`copy_signals/orders/shadow_lots = 0`).
