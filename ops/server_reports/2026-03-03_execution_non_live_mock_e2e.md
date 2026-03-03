# Execution Non-Live Mock E2E Report

Date (UTC): 2026-03-03  
Server: `52.28.0.218`  
Scope: validate `adapter -> executor` execution chain in safe non-live mode via contract-compatible local mock upstream.

## Changes applied on server

1. Deployed mock upstream service:
   1. binary/script: `/var/www/solana-copy-bot/tools/mock_execution_upstream.py`
   2. systemd unit: `/etc/systemd/system/copybot-execution-mock-upstream.service`
   3. bind: `127.0.0.1:18080`
2. Switched executor upstream endpoints:
   1. `COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=http://127.0.0.1:18080/submit`
   2. `COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=http://127.0.0.1:18080/simulate`
3. Restarted:
   1. `copybot-execution-mock-upstream.service`
   2. `copybot-executor.service`
   3. `copybot-adapter.service`

## Service status

1. `copybot-execution-mock-upstream` -> `active`
2. `copybot-executor` -> `active`
3. `copybot-adapter` -> `active`
4. `solana-copy-bot` -> `active`

## Health checks

1. `GET http://127.0.0.1:18080/healthz` -> `status=ok`
2. `GET http://127.0.0.1:8090/healthz` -> `status=ok`
3. `GET http://127.0.0.1:8080/healthz` -> `status=ok`

## Preflight checks

1. `tools/execution_adapter_preflight.sh` -> `PASS`
2. `tools/executor_preflight.sh` -> `PASS`

## E2E request run (through adapter)

Single synthetic request set posted with adapter bearer auth:

1. `POST http://127.0.0.1:8080/simulate` -> HTTP `200`, payload:
   1. `status=ok`
   2. `accepted=true`
   3. `route=rpc`
   4. `detail=mock_simulation_ok`
2. `POST http://127.0.0.1:8080/submit` -> HTTP `200`, payload:
   1. `status=ok`
   2. `accepted=true`
   3. `route=rpc`
   4. `submit_transport=upstream_signature`
   5. `tx_signature` present

## Chain verification

Mock upstream request log confirms both forwarded calls:

1. `/var/www/solana-copy-bot/state/mock_execution_upstream/requests.jsonl`
2. latest entries include:
   1. `POST /simulate` with matching `request_id`
   2. `POST /submit` with matching `request_id` and `client_order_id`

## Safety note

1. This is non-live validation only: no real upstream trading backend is used.
2. Runtime flag in app remains `SOLANA_COPY_BOT_EXECUTION_ENABLED=false` unless explicitly changed.
