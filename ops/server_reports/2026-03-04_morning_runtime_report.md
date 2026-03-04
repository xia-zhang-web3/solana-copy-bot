# Morning Runtime Report (Partial)

Date (UTC snapshot): `2026-03-04T07:24:28Z`  
Server: `52.28.0.218`  
Scope: утренний runtime-срез с привязкой к фактическому старту сервисов.

## Raw artifacts

1. `ops/server_reports/raw/2026-03-04_morning_snapshot/services_show_named.txt`
2. `ops/server_reports/raw/2026-03-04_morning_snapshot/health_and_ps.txt`
3. `ops/server_reports/raw/2026-03-04_morning_snapshot/summary_from_server.txt`
4. `ops/server_reports/raw/2026-03-04_morning_snapshot/observed_swaps_count_samples.txt`

## 1) Uptime from service start

На момент среза:

1. `solana-copy-bot.service`
   1. `MainPID=92713`
   2. `active_since=2026-03-04 07:16:20 UTC`
   3. `uptime_seconds=488` (~`00:08:08`)
2. `copybot-executor.service`
   1. `MainPID=82609`
   2. `active_since=2026-03-03 18:03:16 UTC`
   3. `uptime_seconds=48072` (~`13:21:12`)
3. `copybot-adapter.service`
   1. `MainPID=82610`
   2. `active_since=2026-03-03 18:03:16 UTC`
   3. `uptime_seconds=48072` (~`13:21:12`)
4. `copybot-execution-mock-upstream.service`
   1. `MainPID=82478`
   2. `active_since=2026-03-03 18:02:56 UTC`
   3. `uptime_seconds=48092` (~`13:21:32`)

Вывод: execution contour (executor+adapter+mock) идет непрерывно ~13.3 часа, но `copybot-app` был перезапущен в `07:16:20 UTC`.

## 2) Health endpoints

1. `GET http://127.0.0.1:8080/healthz` -> `status=ok` (adapter)
2. `GET http://127.0.0.1:8090/healthz` -> `status=ok` (executor)
3. `GET http://127.0.0.1:18080/healthz` -> `status=ok` (mock upstream)

## 3) Process footprint (point-in-time)

1. `copybot-app` (`PID 92713`)
   1. `%CPU=9.7`
   2. `%MEM=26.0`
   3. `RSS=2,077,976 KB` (~2.0 GB)
2. `copybot-executor` (`PID 82609`)
   1. `%CPU=0.0`
   2. `RSS=7,176 KB`
3. `copybot-adapter` (`PID 82610`)
   1. `%CPU=0.0`
   2. `RSS=5,436 KB`
4. `mock upstream` (`PID 82478`)
   1. `%CPU=0.0`
   2. `RSS=15,308 KB`

## 4) DB metric sample

Из успешных точечных проб `sqlite3`:

1. `observed_swaps_rows=7,880,934`
2. `observed_swaps_rows=7,883,702`
3. `observed_swaps_rows=7,889,132`

Вывод: ingestion продолжает накапливать данные; объем `observed_swaps` уже ~`7.88M`.

## 5) Limitations during collection

1. В период сбора наблюдались массовые `ssh banner exchange timeout` к `52.28.0.218`, из-за чего полный journal-срез (ingestion/discovery за ночь) снять не удалось в этом проходе.
2. Срез корректно подтверждает состояние сервисов/health и факт накопления БД, но не содержит полного агрегата по `discovery_cycle_duration_ms`, `eligible_wallets`, `rpc_429` за весь ночной интервал.

## 6) Next capture

При стабилизации SSH нужно добрать:

1. `journalctl -u solana-copy-bot --since "2026-03-03 18:03:00 UTC"` и пересчитать:
   1. `grpc_message_total` rate
   2. `rpc_429` delta
   3. `discovery_cycle_duration_ms` trend
   4. `eligible_wallets/active_follow_wallets`
