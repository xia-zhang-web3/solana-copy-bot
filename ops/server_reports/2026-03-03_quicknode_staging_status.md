# QuickNode Staging Status Report

Date (UTC): 2026-03-03  
Server: `52.28.0.218`  
Scope: staging run (shadow/ingestion + infra contract checks, no live trading)

## Server artifact bundle

1. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/summary.txt`
2. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/services_health.txt`
3. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/live_config_redacted.txt`
4. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/db_counters.txt`
5. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/execution_adapter_preflight.txt`
6. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/executor_preflight.txt`
7. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/runtime_snapshot_1h.txt`
8. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/execution_go_nogo_1h.txt`
9. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z/journal_last15m.txt`

## Result summary

1. `execution_adapter_preflight_verdict=PASS`
2. `executor_preflight_verdict=PASS`
3. `execution_enabled=false` (shadow-only run)
4. `overall_go_nogo_verdict=NO_GO`
5. `overall_go_nogo_reason=at least one readiness gate is WARN; rollout escalation required before live enable`
6. `journal_quiknode_mentions_count=11`
7. `journal_429_count=638` (last 15 min window)

## Operational meaning

1. Infrastructure is up (`PASS` preflights, services active), but ingestion is under RPC rate-limit pressure (`journal_429_count=638` in 15 minutes).
2. Ingestion is running on QuickNode (runtime logs mention `quiknode.pro`).
3. Readiness for execution stage remains `NO_GO` in this window due insufficient quality/sample gates.
