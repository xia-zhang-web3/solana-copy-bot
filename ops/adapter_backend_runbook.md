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
7. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_FALLBACK_URL=<optional fallback submit URL>`
8. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_FALLBACK_URL=<optional fallback simulate URL>`
9. `COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL=<optional RPC URL for post-submit signature visibility>`
10. `COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_FALLBACK_URL=<optional fallback verify RPC URL>`
11. `COPYBOT_ADAPTER_SUBMIT_VERIFY_ATTEMPTS=3` (optional, default `3`)
12. `COPYBOT_ADAPTER_SUBMIT_VERIFY_INTERVAL_MS=250` (optional, default `250`)
13. `COPYBOT_ADAPTER_SUBMIT_VERIFY_STRICT=false` (optional; if `true`, unseen signature causes retryable reject)

Optional security:

1. `COPYBOT_ADAPTER_BEARER_TOKEN=<token>` (required from execution client)
2. `COPYBOT_ADAPTER_BEARER_TOKEN_FILE=/etc/solana-copy-bot/secrets/adapter_bearer.token` (file-based alternative)
3. `COPYBOT_ADAPTER_HMAC_KEY_ID=<key-id>`
4. `COPYBOT_ADAPTER_HMAC_SECRET=<secret>`
5. `COPYBOT_ADAPTER_HMAC_TTL_SEC=30`
6. `COPYBOT_ADAPTER_HMAC_SECRET_FILE=/etc/solana-copy-bot/secrets/adapter_hmac.secret` (file-based alternative)
7. `COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE=/etc/solana-copy-bot/secrets/upstream_auth.token` (optional upstream auth default)
8. `COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE=/etc/solana-copy-bot/secrets/upstream_fallback_auth.token` (optional fallback upstream auth)

Auth policy:

1. By default adapter startup is fail-closed without auth.
2. You must configure at least one inbound auth method:
   1. Bearer (`COPYBOT_ADAPTER_BEARER_TOKEN`) OR
   2. HMAC pair (`COPYBOT_ADAPTER_HMAC_KEY_ID` + `COPYBOT_ADAPTER_HMAC_SECRET`)
3. Secret source rules are fail-closed:
   1. inline + file for same secret is startup error,
   2. secret file is trimmed; empty file is startup error.
4. Unauthenticated mode is allowed only with explicit override:
   1. `COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=true`
   2. use only for controlled local/non-production tests.

Optional per-route upstream overrides:

1. `COPYBOT_ADAPTER_ROUTE_RPC_SUBMIT_URL=...`
2. `COPYBOT_ADAPTER_ROUTE_RPC_SUBMIT_FALLBACK_URL=...`
3. `COPYBOT_ADAPTER_ROUTE_RPC_SIMULATE_URL=...`
4. `COPYBOT_ADAPTER_ROUTE_RPC_SIMULATE_FALLBACK_URL=...`
5. `COPYBOT_ADAPTER_ROUTE_RPC_AUTH_TOKEN=...`
6. `COPYBOT_ADAPTER_ROUTE_RPC_AUTH_TOKEN_FILE=/etc/solana-copy-bot/secrets/route_rpc_auth.token`
7. `COPYBOT_ADAPTER_ROUTE_RPC_FALLBACK_AUTH_TOKEN=...`
8. `COPYBOT_ADAPTER_ROUTE_RPC_FALLBACK_AUTH_TOKEN_FILE=/etc/solana-copy-bot/secrets/route_rpc_fallback_auth.token`
9. same pattern for `PAPER`, `JITO`, `FASTLANE`, etc.

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
4. Adapter failover policy: retryable upstream transport errors (`send`, `429`, `5xx`) try fallback endpoint when configured; terminal upstream rejects (`4xx`, invalid contract response) do not fail over.
   1. fallback can use dedicated auth token (`COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN[_FILE]` or route-specific `..._FALLBACK_AUTH_TOKEN[_FILE]`), otherwise inherits primary route auth token.
5. All endpoint diagnostics are redacted to `scheme://host[:port]` labels in logs.
6. Optional post-submit signature visibility check:
   1. if `COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL` is set, adapter polls `getSignatureStatuses` after upstream submit,
   2. `COPYBOT_ADAPTER_SUBMIT_VERIFY_STRICT=true` makes unseen signature fail-closed as retryable reject (`upstream_submit_signature_unseen`),
   3. strict mode should be enabled only after baseline RPC visibility is stable.
7. Secret rotation pattern (atomic):
   1. write new secret to a temp file in the same directory,
   2. set owner-only permissions (`chmod 600` or `chmod 400`),
   3. replace target file via atomic rename (`mv temp target`),
   4. restart adapter service and verify `/healthz`.
8. Rotation readiness/evidence helper:
   1. run `ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env OUTPUT_DIR=state/adapter-rotation ./tools/adapter_secret_rotation_report.sh`
   2. expected `rotation_readiness_verdict: PASS` (or `WARN` only for non-blocking permission hardening),
   3. attach emitted `artifact_report` file to ops evidence package.
