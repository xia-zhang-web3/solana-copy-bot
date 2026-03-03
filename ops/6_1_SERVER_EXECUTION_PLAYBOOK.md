# 6.1 Server Execution Playbook

Goal: complete `ROAD_TO_PRODUCTION.md` section `6.1` on a real server and get factual `GO/NO_GO` result.

## 1) What is already prepared in repo

1. `ops/server_templates/copybot-executor.service`
2. `ops/server_templates/copybot-adapter.service`
3. `ops/server_templates/solana-copy-bot.service`
4. `ops/server_templates/executor.env.example`
5. `ops/server_templates/adapter.env.example`
6. `ops/server_templates/app.env.example`
7. `ops/test_server_rollout_6_1_tracker.md`

## 2) Server paths to create

1. `/etc/solana-copy-bot/live.server.toml`
2. `/etc/solana-copy-bot/executor.env`
3. `/etc/solana-copy-bot/adapter.env`
4. `/etc/solana-copy-bot/app.env`
5. `/etc/solana-copy-bot/secrets/*`
6. `/etc/systemd/system/copybot-executor.service`
7. `/etc/systemd/system/copybot-adapter.service`
8. `/etc/systemd/system/solana-copy-bot.service`

## 3) Copy commands (run on server from repo root)

```bash
cd /var/www/solana-copy-bot

sudo mkdir -p /etc/solana-copy-bot/secrets

sudo install -m 0644 configs/live.toml /etc/solana-copy-bot/live.server.toml
sudo install -m 0644 ops/server_templates/executor.env.example /etc/solana-copy-bot/executor.env
sudo install -m 0644 ops/server_templates/adapter.env.example /etc/solana-copy-bot/adapter.env
sudo install -m 0644 ops/server_templates/app.env.example /etc/solana-copy-bot/app.env

sudo install -m 0644 ops/server_templates/copybot-executor.service /etc/systemd/system/copybot-executor.service
sudo install -m 0644 ops/server_templates/copybot-adapter.service /etc/systemd/system/copybot-adapter.service
sudo install -m 0644 ops/server_templates/solana-copy-bot.service /etc/systemd/system/solana-copy-bot.service
```

## 4) Mandatory values to fill

### 4.1 `/etc/solana-copy-bot/live.server.toml`

Set these fields:

1. `ingestion.helius_ws_url` -> real WS endpoint (no `REPLACE_ME`)
2. `ingestion.helius_http_url` -> real HTTP endpoint (no `REPLACE_ME`)
3. `ingestion.yellowstone_grpc_url` -> real endpoint (or keep ingestion source consistent with your run mode)
4. `ingestion.yellowstone_x_token` -> real token
5. `execution.enabled` -> keep `false` for warm-up; preflight run uses `SOLANA_COPY_BOT_EXECUTION_ENABLED=true`
6. `execution.mode` -> `adapter_submit_confirm`
7. `execution.submit_adapter_http_url` -> `http://127.0.0.1:8080/submit`
8. `execution.submit_adapter_fallback_http_url` -> optional fallback adapter
9. `execution.execution_signer_pubkey` -> same pubkey as executor/adapter env
10. `execution.submit_adapter_require_policy_echo` -> `true`
11. `execution.submit_allowed_routes` -> must match executor/adapter allowlists
12. `execution.default_route` -> must be in `submit_allowed_routes`

### 4.2 `/etc/solana-copy-bot/executor.env`

Mandatory minimum:

1. `COPYBOT_EXECUTOR_SIGNER_PUBKEY`
2. `COPYBOT_EXECUTOR_SIGNER_SOURCE=file` or `kms`
3. `COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE` (if `file`) or `COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID` (if `kms`)
4. `COPYBOT_EXECUTOR_BEARER_TOKEN_FILE`
5. `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST` (must match adapter allowlist)
6. `COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL`
7. `COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL`
8. optional fallback/send-rpc URLs only if actually used

### 4.3 `/etc/solana-copy-bot/adapter.env`

Mandatory minimum:

1. `COPYBOT_ADAPTER_SIGNER_PUBKEY` (same as executor + live config)
2. `COPYBOT_ADAPTER_ROUTE_ALLOWLIST` (must match executor)
3. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=http://127.0.0.1:8090/submit`
4. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=http://127.0.0.1:8090/simulate`
5. `COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE` (must match executor bearer)
6. `COPYBOT_ADAPTER_BEARER_TOKEN_FILE` (for app->adapter ingress when required)

### 4.4 `/etc/solana-copy-bot/app.env`

1. `SOLANA_COPY_BOT_CONFIG=/etc/solana-copy-bot/live.server.toml`
2. `SOLANA_COPY_BOT_EXECUTION_ENABLED=false` for warm-up, `true` for execution tests

## 5) Secret files

Create these files with owner-only permissions:

```bash
sudo bash -lc 'openssl rand -hex 32 > /etc/solana-copy-bot/secrets/executor_bearer.token'
sudo bash -lc 'openssl rand -hex 32 > /etc/solana-copy-bot/secrets/adapter_bearer.token'
sudo chmod 600 /etc/solana-copy-bot/secrets/executor_bearer.token /etc/solana-copy-bot/secrets/adapter_bearer.token
```

If using file signer source:

```bash
sudo solana-keygen new -o /etc/solana-copy-bot/secrets/executor-signer.devnet.json --no-bip39-passphrase
sudo chmod 600 /etc/solana-copy-bot/secrets/executor-signer.devnet.json
sudo solana-keygen pubkey /etc/solana-copy-bot/secrets/executor-signer.devnet.json
```

Use returned pubkey in:

1. `/etc/solana-copy-bot/executor.env` (`COPYBOT_EXECUTOR_SIGNER_PUBKEY`)
2. `/etc/solana-copy-bot/adapter.env` (`COPYBOT_ADAPTER_SIGNER_PUBKEY`)
3. `/etc/solana-copy-bot/live.server.toml` (`execution.execution_signer_pubkey`)

## 6) Build and start services

```bash
cd /var/www/solana-copy-bot
cargo build --release -p copybot-executor -p copybot-adapter -p copybot-app

sudo systemctl daemon-reload
sudo systemctl enable --now copybot-executor.service
sudo systemctl enable --now copybot-adapter.service
sudo systemctl enable --now solana-copy-bot.service

sudo systemctl status copybot-executor.service --no-pager
sudo systemctl status copybot-adapter.service --no-pager
sudo systemctl status solana-copy-bot.service --no-pager
```

## 7) Mandatory 6.1 checks (strict order)

```bash
cd /var/www/solana-copy-bot

CONFIG_PATH=/etc/solana-copy-bot/live.server.toml \
SOLANA_COPY_BOT_EXECUTION_ENABLED=true \
bash tools/execution_adapter_preflight.sh

CONFIG_PATH=/etc/solana-copy-bot/live.server.toml \
EXECUTOR_ENV_PATH=/etc/solana-copy-bot/executor.env \
ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env \
SOLANA_COPY_BOT_EXECUTION_ENABLED=true \
bash tools/executor_preflight.sh

curl -sS http://127.0.0.1:8090/healthz | jq .
```

## 8) Hard fail conditions

Do not continue if any of these are true:

1. `preflight_verdict != PASS`
2. any `REPLACE_ME` / `REPLACE_WITH_*` left in server files
3. signer/secret files missing or wrong permissions
4. route allowlist mismatch between executor/adapter/runtime
5. healthz/auth mismatch in executor preflight output

