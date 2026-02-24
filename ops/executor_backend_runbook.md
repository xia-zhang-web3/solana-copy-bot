# Executor Backend Runbook

`copybot-executor` — upstream backend service for `copybot-adapter`.

Current phase scope (Phase 1 scaffold):

1. `GET /healthz`
2. `POST /simulate`
3. `POST /submit`
4. Bearer/HMAC ingress auth
5. Signer custody bootstrap validation (`file|kms`, fail-closed)

## 1) Build

```bash
cd /var/www/solana-copy-bot
cargo build --release -p copybot-executor
```

## 2) Required Environment (minimum)

```bash
COPYBOT_EXECUTOR_BIND_ADDR=127.0.0.1:8090
COPYBOT_EXECUTOR_CONTRACT_VERSION=v1
COPYBOT_EXECUTOR_SIGNER_PUBKEY=<base58 pubkey>
COPYBOT_EXECUTOR_SIGNER_SOURCE=file
COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE=/etc/solana-copy-bot/secrets/executor_signer.json
COPYBOT_EXECUTOR_ROUTE_ALLOWLIST=paper,rpc,jito,fastlane
COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=<backend submit url>
COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=<backend simulate url>
COPYBOT_EXECUTOR_BEARER_TOKEN_FILE=/etc/solana-copy-bot/secrets/executor_bearer.token
```

Signer source policy:

1. `COPYBOT_EXECUTOR_SIGNER_SOURCE=file`:
   1. requires `COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE`
   2. rejects `COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID`
   3. keypair file must exist, be readable, non-empty, and owner-only (`0600`/`0400`)
2. `COPYBOT_EXECUTOR_SIGNER_SOURCE=kms`:
   1. requires `COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID`
   2. rejects `COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE`

Auth policy:

1. Fail-closed by default.
2. Must provide Bearer (`COPYBOT_EXECUTOR_BEARER_TOKEN[_FILE]`) or HMAC pair (`COPYBOT_EXECUTOR_HMAC_KEY_ID` + `COPYBOT_EXECUTOR_HMAC_SECRET[_FILE]`).
3. Unauthenticated mode allowed only with explicit override:
   1. `COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true`

## 3) Local Run

```bash
cd /var/www/solana-copy-bot
set -a
source /etc/solana-copy-bot/executor.env
set +a
./target/release/copybot-executor
```

Quick health check:

```bash
curl -sS http://127.0.0.1:8090/healthz | jq .
```

Expected fields include:

1. `status`
2. `contract_version`
3. `enabled_routes`
4. `signer_source`

## 4) Systemd Unit Example

`/etc/systemd/system/copybot-executor.service`

```ini
[Unit]
Description=CopyBot Executor Backend
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=copybot
WorkingDirectory=/var/www/solana-copy-bot
EnvironmentFile=/etc/solana-copy-bot/executor.env
ExecStart=/var/www/solana-copy-bot/target/release/copybot-executor
Restart=always
RestartSec=2
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Apply:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now copybot-executor.service
sudo systemctl status copybot-executor.service --no-pager
```

## 5) Signer Rotation Readiness Helper

```bash
EXECUTOR_ENV_PATH=/etc/solana-copy-bot/executor.env \
OUTPUT_DIR=state/executor-signer-rotation \
./tools/executor_signer_rotation_report.sh
```

Outputs:

1. `rotation_readiness_verdict: PASS|WARN|FAIL`
2. `rotation_readiness_reason`
3. `artifacts_written`
4. optional `artifact_report`, `artifact_manifest`, `report_sha256`

## 6) Integration Note (Phase 4 target)

Adapter wiring target:

1. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=http://127.0.0.1:8090/submit`
2. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=http://127.0.0.1:8090/simulate`

Before rollout, run executor preflight/rehearsal/evidence helpers from Phase 4+ plan sections.
