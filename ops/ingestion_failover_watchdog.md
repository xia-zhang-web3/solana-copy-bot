# Ingestion Failover Watchdog Wiring

This runbook wires `tools/ingestion_failover_watchdog.sh` as an external supervisor helper.

## 1) Policy

Edit thresholds in:

- `ops/ingestion_failover_policy.toml`

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
