# Ingestion Failover Watchdog Wiring

This runbook wires `tools/ingestion_failover_watchdog.sh` as an external supervisor helper.

## 1) Policy

Edit thresholds in:

- `ops/ingestion_failover_policy.toml`

Implemented trigger set:

1. `lag_p95 > threshold` for consecutive runs
2. `replaced_ratio > threshold` for consecutive runs
3. reconnect storm (`delta(reconnect_count)` over window)
4. decode/parse reject rate (`(delta(parse_rejected_total)+delta(grpc_decode_errors))/delta(grpc_message_total)`)
5. no processed swaps while inbound stream is active (`delta(ws_notifications_enqueued)==0 && delta(grpc_message_total)>=min`)

## 2) One-shot smoke run

```bash
cd /var/www/solana-copy-bot
POLICY_FILE=ops/ingestion_failover_policy.toml \
CONFIG_PATH=configs/paper.toml \
./tools/ingestion_failover_watchdog.sh
```

Expected artifacts:

- `state/ingestion_failover_state.json`
- `state/ingestion_source_override.env` (only on trigger)
- `state/ingestion_failover_cooldown.json` (only on trigger)

## 3) systemd service/timer (example)

`/etc/systemd/system/solana-copy-bot-watchdog.service`:

```ini
[Unit]
Description=Solana CopyBot ingestion failover watchdog
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/var/www/solana-copy-bot
Environment=POLICY_FILE=/var/www/solana-copy-bot/ops/ingestion_failover_policy.toml
Environment=CONFIG_PATH=/var/www/solana-copy-bot/configs/paper.toml
Environment=OVERRIDE_FILE=/var/www/solana-copy-bot/state/ingestion_source_override.env
ExecStart=/var/www/solana-copy-bot/tools/ingestion_failover_watchdog.sh
```

`/etc/systemd/system/solana-copy-bot-watchdog.timer`:

```ini
[Unit]
Description=Run Solana CopyBot ingestion failover watchdog every minute

[Timer]
OnBootSec=45s
OnUnitActiveSec=60s
Unit=solana-copy-bot-watchdog.service

[Install]
WantedBy=timers.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now solana-copy-bot-watchdog.timer
sudo systemctl status solana-copy-bot-watchdog.timer
```

### 3.1 Override path invariant (required)

Watchdog and bot must point to the exact same override file path.

Required mapping:

```ini
# watchdog service
Environment=OVERRIDE_FILE=/var/www/solana-copy-bot/state/ingestion_source_override.env

# bot service
Environment=SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE=/var/www/solana-copy-bot/state/ingestion_source_override.env
EnvironmentFile=-/var/www/solana-copy-bot/state/ingestion_source_override.env
```

Quick verification:

```bash
sudo systemctl show solana-copy-bot-watchdog.service -p Environment | rg OVERRIDE_FILE
sudo systemctl show solana-copy-bot.service -p Environment | rg SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE
```

## 4) Restart wiring for source override

Main bot unit should load optional override env file:

```ini
EnvironmentFile=-/var/www/solana-copy-bot/state/ingestion_source_override.env
Restart=always
RestartSec=2
```

After unit changes:

```bash
sudo systemctl daemon-reload
sudo systemctl restart solana-copy-bot
```

## 5) Recovery back to gRPC after fallback

If failover was false-positive and you want to restore `yellowstone_grpc`:

```bash
cd /var/www/solana-copy-bot
rm -f state/ingestion_source_override.env
rm -f state/ingestion_failover_cooldown.json
sudo systemctl restart solana-copy-bot
```

Optional: clear watchdog rolling state to reset streak/window counters:

```bash
rm -f state/ingestion_failover_state.json
```
