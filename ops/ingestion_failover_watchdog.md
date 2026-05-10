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
CONFIG_PATH=/etc/solana-copy-bot/live.server.toml \
DRY_RUN=true \
./tools/ingestion_failover_watchdog.sh
```

The smoke run is read-only with `DRY_RUN=true`; it must not create or update
state, override, cooldown, or restart surfaces.
If the runtime snapshot or config cannot be read, the watchdog must exit
non-zero instead of reporting a healthy skip.

Expected runtime artifacts during a real non-dry scheduled run:

- `state/ingestion_failover_state.json`
- `state/ingestion_source_override.env` trigger metadata (only on trigger)
- `state/ingestion_failover_cooldown.json` (only on trigger)

## 3) systemd service/timer (example)

`/etc/systemd/system/solana-copy-bot-watchdog.service`:

```ini
[Unit]
Description=Solana CopyBot ingestion failover watchdog
After=network-online.target

[Service]
Type=oneshot
User=copybot
WorkingDirectory=/var/www/solana-copy-bot
Environment=POLICY_FILE=/var/www/solana-copy-bot/ops/ingestion_failover_policy.toml
Environment=CONFIG_PATH=/etc/solana-copy-bot/live.server.toml
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

Before enabling the timer on production, run the production preflight from
`ARTIFACT_DEPLOY.md` and state that only the watchdog service/timer is in scope:

1. record current server commit and exact artifact `git_sha`,
2. verify service/timer state,
3. verify disk and memory headroom,
4. verify no `cargo` or `rustc` build is running on production,
5. verify `execution.enabled = false`,
6. verify config deltas have been accepted, split, or reverted.

Enable after preflight:

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
```

Important:

1. Do not pin `SOLANA_COPY_BOT_INGESTION_SOURCE=...` as a static service
   `Environment=` value or load the watchdog state file as an `EnvironmentFile`.
2. The historical websocket fallback source has been removed. The watchdog
   records trigger metadata and cooldown state, but must not restart the daemon
   into an unsupported source.
3. Source routing comes from config unless a future supported alternate source
   is explicitly added and accepted.

Quick verification:

```bash
sudo systemctl show solana-copy-bot-watchdog.service -p Environment | rg OVERRIDE_FILE
sudo systemctl show solana-copy-bot.service -p Environment | rg SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE
```

## 4) Restart wiring

Main bot unit must pass only the override-file path. It must not load the
watchdog state file as an `EnvironmentFile`, because stale unsupported source
values are startup-fatal once the app validates the override file.

Before restarting the main bot, inspect or remove any stale override file that
still contains `SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws` or any other
unsupported source.

```ini
Environment=SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE=/var/www/solana-copy-bot/state/ingestion_source_override.env
Restart=always
RestartSec=2
```

After unit changes, run the production preflight from `ARTIFACT_DEPLOY.md`
before touching the daemon:

1. verify current `solana-copy-bot.service` and watchdog timer state,
2. record current server commit and exact artifact `git_sha`,
3. verify disk and memory headroom,
4. verify no `cargo` or `rustc` build is running on production,
5. state the exact unit files and daemon service that will be touched,
6. verify `execution.enabled = false`,
7. verify config deltas have been accepted, split, or reverted.

Only after that preflight:

```bash
sudo systemctl daemon-reload
sudo systemctl restart solana-copy-bot
```

## 5) Clear watchdog override metadata

The historical alternate source fallback has been removed. Clearing watchdog
metadata should leave source routing on the configured `yellowstone_grpc` path.

Before daemon restart, run the production preflight from `ARTIFACT_DEPLOY.md`,
state that only watchdog override metadata and `solana-copy-bot.service` are
being touched, then clear the files.

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
