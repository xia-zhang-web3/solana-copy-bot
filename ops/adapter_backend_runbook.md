# Adapter Backend Runbook

This service is the production adapter entrypoint for execution mode `adapter_submit_confirm`.
It exposes:

1. `POST /simulate`
2. `POST /submit`
3. `GET /healthz`

The service validates auth/HMAC + execution contract and forwards requests to upstream route executors.

## 1) Build

```bash
cd /var/www/solana-copy-bot
cargo build --release -p copybot-adapter
```

## 2) Required Environment

At minimum:

1. `COPYBOT_ADAPTER_BIND_ADDR=127.0.0.1:8080`
2. `COPYBOT_ADAPTER_CONTRACT_VERSION=v1`
3. `COPYBOT_ADAPTER_SIGNER_PUBKEY=<wallet pubkey>` (public address only)
4. `COPYBOT_ADAPTER_ROUTE_ALLOWLIST=rpc,paper` (example)
5. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=<executor submit URL>`
6. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=<executor simulate URL>`

Optional security:

1. `COPYBOT_ADAPTER_BEARER_TOKEN=<token>` (required from execution client)
2. `COPYBOT_ADAPTER_HMAC_KEY_ID=<key-id>`
3. `COPYBOT_ADAPTER_HMAC_SECRET=<secret>`
4. `COPYBOT_ADAPTER_HMAC_TTL_SEC=30`

Auth policy:

1. By default adapter startup is fail-closed without auth.
2. You must configure at least one inbound auth method:
   1. Bearer (`COPYBOT_ADAPTER_BEARER_TOKEN`) OR
   2. HMAC pair (`COPYBOT_ADAPTER_HMAC_KEY_ID` + `COPYBOT_ADAPTER_HMAC_SECRET`)
3. Unauthenticated mode is allowed only with explicit override:
   1. `COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=true`
   2. use only for controlled local/non-production tests.

Optional per-route upstream overrides:

1. `COPYBOT_ADAPTER_ROUTE_RPC_SUBMIT_URL=...`
2. `COPYBOT_ADAPTER_ROUTE_RPC_SIMULATE_URL=...`
3. `COPYBOT_ADAPTER_ROUTE_RPC_AUTH_TOKEN=...`
4. same pattern for `PAPER`, `JITO`, `FASTLANE`, etc.

## 3) Local Run

```bash
cd /var/www/solana-copy-bot
set -a
source /etc/solana-copy-bot/adapter.env
set +a
./target/release/copybot-adapter
```

Health check:

```bash
curl -sS http://127.0.0.1:8080/healthz | jq .
```

## 4) Systemd Unit Example

`/etc/systemd/system/copybot-adapter.service`

```ini
[Unit]
Description=CopyBot Adapter Backend
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=copybot
WorkingDirectory=/var/www/solana-copy-bot
EnvironmentFile=/etc/solana-copy-bot/adapter.env
ExecStart=/var/www/solana-copy-bot/target/release/copybot-adapter
Restart=always
RestartSec=2
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Apply:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now copybot-adapter.service
sudo systemctl status copybot-adapter.service --no-pager
```

## 5) Wire Runtime to Adapter

Set execution runtime config/env:

1. `execution.mode=adapter_submit_confirm`
2. `execution.submit_adapter_http_url=http://127.0.0.1:8080/submit`
3. `execution.submit_adapter_fallback_http_url=` (optional second adapter instance)
4. `execution.submit_adapter_contract_version=v1`
5. `execution.submit_adapter_require_policy_echo=true`
6. `execution.execution_signer_pubkey=<same signer pubkey>`

Simulation path uses the same adapter endpoint set and calls it with `action=simulate` contract.

## 6) Operational Notes

1. Adapter is fail-closed on malformed/invalid requests.
2. Unknown upstream status is fail-closed (`upstream_invalid_status`).
3. Upstream HTTP `429/5xx` is treated as retryable.
4. All endpoint diagnostics are redacted to `scheme://host[:port]` labels in logs.
