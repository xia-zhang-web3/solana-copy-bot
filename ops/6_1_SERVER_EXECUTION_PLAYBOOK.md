# 6.1 Server Execution Playbook

Goal: complete `ROAD_TO_PRODUCTION.md` section `6.1` on a real server and get factual `GO/NO_GO` result.

This playbook is synchronized with the current staging server snapshot (`52.28.0.218`, `2026-03-03`):
1. ingestion on QuickNode Yellowstone gRPC,
2. runtime `execution.enabled=false`,
3. non-live execution contour via local mock upstream (`127.0.0.1:18080`).

Important: these values are for staging/non-live contour only. Keep real RPC tokens/signer
material only in server-local files/env, never in repo-tracked config.

## 1) What is already prepared in repo

1. `ops/server_templates/live.server.toml.example`
2. `ops/server_templates/executor.env.example`
3. `ops/server_templates/adapter.env.example`
4. `ops/server_templates/app.env.example`
5. `ops/server_templates/copybot-executor.service`
6. `ops/server_templates/copybot-adapter.service`
7. `ops/server_templates/solana-copy-bot.service`
8. `ops/server_templates/copybot-execution-mock-upstream.service`
9. `tools/mock_execution_upstream.py`
10. `ops/test_server_rollout_6_1_tracker.md`

## 2) Server paths to create

1. `/etc/solana-copy-bot/live.server.toml`
2. `/etc/solana-copy-bot/executor.env`
3. `/etc/solana-copy-bot/adapter.env`
4. `/etc/solana-copy-bot/app.env`
5. `/etc/solana-copy-bot/secrets/*`
6. `/etc/systemd/system/copybot-execution-mock-upstream.service`
7. `/etc/systemd/system/copybot-executor.service`
8. `/etc/systemd/system/copybot-adapter.service`
9. `/etc/systemd/system/solana-copy-bot.service`

## 3) Copy commands (run on server from repo root)

```bash
cd /var/www/solana-copy-bot

sudo mkdir -p /etc/solana-copy-bot/secrets

sudo install -m 0644 ops/server_templates/live.server.toml.example /etc/solana-copy-bot/live.server.toml
sudo install -m 0644 ops/server_templates/executor.env.example /etc/solana-copy-bot/executor.env
sudo install -m 0644 ops/server_templates/adapter.env.example /etc/solana-copy-bot/adapter.env
sudo install -m 0644 ops/server_templates/app.env.example /etc/solana-copy-bot/app.env

sudo install -m 0644 ops/server_templates/copybot-execution-mock-upstream.service /etc/systemd/system/copybot-execution-mock-upstream.service
sudo install -m 0644 ops/server_templates/copybot-executor.service /etc/systemd/system/copybot-executor.service
sudo install -m 0644 ops/server_templates/copybot-adapter.service /etc/systemd/system/copybot-adapter.service
sudo install -m 0644 ops/server_templates/solana-copy-bot.service /etc/systemd/system/solana-copy-bot.service
```

## 4) Canonical staging values (current)

### 4.1 `/etc/solana-copy-bot/live.server.toml`

Expected key values:

1. `ingestion.source = "yellowstone_grpc"`
2. `ingestion.helius_ws_url = "wss://YOUR_QUICKNODE_HOST.solana-mainnet.quiknode.pro/REPLACE_ON_SERVER/"`
3. `ingestion.helius_http_url = "https://YOUR_QUICKNODE_HOST.solana-mainnet.quiknode.pro/REPLACE_ON_SERVER/"`
4. `ingestion.yellowstone_grpc_url = "https://YOUR_QUICKNODE_HOST.solana-mainnet.quiknode.pro:10000"`
5. `ingestion.yellowstone_x_token = "REPLACE_ON_SERVER"`
6. `execution.enabled = false` (shadow-only runtime)
7. `execution.mode = "adapter_submit_confirm"`
8. `execution.submit_adapter_http_url = "http://127.0.0.1:8080/submit"`
9. `execution.submit_adapter_auth_token_file = "/etc/solana-copy-bot/secrets/adapter_bearer.token"`
10. `execution.execution_signer_pubkey = "11111111111111111111111111111111"`
11. `execution.submit_allowed_routes = ["jito", "rpc"]`

The four ingestion credential values above must be edited only in
`/etc/solana-copy-bot/live.server.toml` on the server, or injected via env
overrides (`SOLANA_COPY_BOT_HELIUS_WS_URL`, `SOLANA_COPY_BOT_HELIUS_HTTP_URL`,
`SOLANA_COPY_BOT_YELLOWSTONE_GRPC_URL`, `SOLANA_COPY_BOT_YELLOWSTONE_X_TOKEN`).
Do not commit real values back into repo-tracked files.

### 4.2 `/etc/solana-copy-bot/executor.env`

Expected key values:

1. `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST=jito,rpc`
2. `COPYBOT_EXECUTOR_SIGNER_SOURCE=file`
3. `COPYBOT_EXECUTOR_SIGNER_PUBKEY=11111111111111111111111111111111`
4. `COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE=/etc/solana-copy-bot/secrets/executor-signer.devnet.json`
5. `COPYBOT_EXECUTOR_BEARER_TOKEN_FILE=/etc/solana-copy-bot/secrets/executor_bearer.token`
6. `COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=http://127.0.0.1:18080/submit`
7. `COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=http://127.0.0.1:18080/simulate`

### 4.3 `/etc/solana-copy-bot/adapter.env`

Expected key values:

1. `COPYBOT_ADAPTER_SIGNER_PUBKEY=11111111111111111111111111111111`
2. `COPYBOT_ADAPTER_ROUTE_ALLOWLIST=jito,rpc`
3. `COPYBOT_ADAPTER_BEARER_TOKEN_FILE=/etc/solana-copy-bot/secrets/adapter_bearer.token`
4. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=http://127.0.0.1:8090/submit`
5. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=http://127.0.0.1:8090/simulate`
6. `COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE=/etc/solana-copy-bot/secrets/executor_bearer.token`
7. Optional when executor HMAC ingress is enabled:
   1. `COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID` matches `COPYBOT_EXECUTOR_HMAC_KEY_ID`,
   2. `COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET_FILE` points to same shared secret as executor HMAC,
   3. `COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC` matches `COPYBOT_EXECUTOR_HMAC_TTL_SEC`.

### 4.4 `/etc/solana-copy-bot/app.env`

1. `SOLANA_COPY_BOT_CONFIG=/etc/solana-copy-bot/live.server.toml`
2. `SOLANA_COPY_BOT_EXECUTION_ENABLED=false`
3. `COPYBOT_APP_LOG_FILTER=info`
4. Optional alert delivery:
   1. `COPYBOT_APP_ALERT_WEBHOOK_URL=https://alerts.example.internal/copybot`
   2. `COPYBOT_APP_ALERT_TIMEOUT_MS=3000`
   3. `COPYBOT_APP_ALERT_TEST_ON_STARTUP=false` (set to `true` once to verify delivery, then revert)

## 5) Secret files

Create bearer files with owner-only permissions:

```bash
sudo bash -lc 'openssl rand -hex 32 > /etc/solana-copy-bot/secrets/executor_bearer.token'
sudo bash -lc 'openssl rand -hex 32 > /etc/solana-copy-bot/secrets/adapter_bearer.token'
sudo chmod 600 /etc/solana-copy-bot/secrets/executor_bearer.token /etc/solana-copy-bot/secrets/adapter_bearer.token
```

For exact staging footprint, create bootstrap keypair (all zeros):

```bash
sudo bash -lc 'cat > /etc/solana-copy-bot/secrets/executor-signer.devnet.json <<EOF
[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
EOF'
sudo chmod 600 /etc/solana-copy-bot/secrets/executor-signer.devnet.json
```

## 6) Build and start services

```bash
cd /var/www/solana-copy-bot
cargo build --release -p copybot-executor -p copybot-adapter -p copybot-app

sudo systemctl daemon-reload
sudo systemctl enable --now copybot-execution-mock-upstream.service
sudo systemctl enable --now copybot-executor.service
sudo systemctl enable --now copybot-adapter.service
sudo systemctl enable --now solana-copy-bot.service

sudo systemctl status copybot-execution-mock-upstream.service --no-pager
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

curl -sS http://127.0.0.1:18080/healthz | jq .
curl -sS http://127.0.0.1:8090/healthz | jq .
curl -sS http://127.0.0.1:8080/healthz | jq .
```

## 8) Hard fail conditions

Do not continue if any of these are true:

1. `preflight_verdict != PASS`
2. required service is not `active`
3. signer/secret files are missing or wrong permissions
4. route allowlist mismatch between executor/adapter/runtime
5. healthz/auth mismatch in executor preflight output
