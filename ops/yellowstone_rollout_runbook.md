# Yellowstone Rollout Runbook

This runbook executes Phase F/G of `YELLOWSTONE_GRPC_MIGRATION_PLAN.md` with explicit commands and gates.

## 1) Prepare Control and Canary

1. Keep control service on `helius_ws` (`configs/paper.toml`).
2. Create canary config (example `configs/paper-canary-yellowstone.toml`):
   - `ingestion.source = "yellowstone_grpc"`
   - separate `sqlite.path` (for example `state/paper_canary_copybot.db`)
   - keep discovery/shadow settings aligned with control
3. Set real credentials via env/secrets:
   - `SOLANA_COPY_BOT_YELLOWSTONE_GRPC_URL`
   - `SOLANA_COPY_BOT_YELLOWSTONE_X_TOKEN`
4. Keep `helius_ws` credentials present for fallback path.

## 2) Enable Watchdog Protection

1. Apply systemd timer/service wiring from `ops/ingestion_failover_watchdog.md`.
2. Validate one-shot watchdog run:

```bash
cd /var/www/solana-copy-bot
POLICY_FILE=ops/ingestion_failover_policy.toml \
CONFIG_PATH=configs/paper-canary-yellowstone.toml \
./tools/ingestion_failover_watchdog.sh
```

3. Confirm state file creation:
   - `state/ingestion_failover_state.json`

## 3) Replay Gate (same-input benchmark)

Run replay/control comparison and persist a JSON artifact:

```bash
cd /var/www/solana-copy-bot
./tools/ingestion_ab_report.sh \
  --control-config configs/paper.toml \
  --candidate-config configs/paper-canary-yellowstone.toml \
  --mode replay \
  --fixture-id <fixture_id> \
  --fixture-sha256 <fixture_sha256> \
  --output-json state/ab_report_replay.json
```

Replay gate target:

1. `observed_buy` delta (`candidate` vs `control`) >= `+15%`
2. `observed_sell` delta does not degrade by more than `3%`

## 4) Live Canary Gate (>= 6h)

After canary runs at least 6 hours, execute:

```bash
cd /var/www/solana-copy-bot
./tools/ingestion_ab_report.sh \
  --control-config configs/paper.toml \
  --candidate-config configs/paper-canary-yellowstone.toml \
  --control-service solana-copy-bot \
  --candidate-service solana-copy-bot-canary \
  --mode live \
  --window-minutes 360 \
  --output-json state/ab_report_live.json
```

Live gate targets:

1. `lag_p95_pass_ratio >= 0.95`
2. `lag_p99_pass_ratio >= 0.90`
3. zero breaches for:
   - reconnect storm
   - decode/parse reject rate
   - no-processed-with-inbound
4. if replaced-ratio windows are evaluable, `replaced_ratio_pass_ratio >= 0.95`
5. SQLite contention amplification vs control <= `+20%`

`ingestion_ab_report.sh` exits with code:

1. `0` when all active gates pass
2. `2` when one or more gates fail

## 5) Production Cutover

1. Switch prod config to `ingestion.source = "yellowstone_grpc"`.
2. Keep fallback-ready:
   - valid `helius_ws_url` + `helius_http_url`
   - watchdog timer enabled
3. Restart service and capture reports:

```bash
cd /var/www/solana-copy-bot
./tools/ingestion_ab_report.sh \
  --control-config configs/prod.toml \
  --candidate-config configs/prod.toml \
  --candidate-service solana-copy-bot \
  --mode live \
  --window-minutes 60 \
  --output-json state/ab_report_prod_1h.json
```

Repeat at 6h and 24h with updated `--window-minutes`.

## 6) Rollback Procedure

Immediate rollback (manual):

1. set `ingestion.source = "helius_ws"` in active config
2. restart service

Failover rollback (override-based):

1. write or keep `state/ingestion_source_override.env` with `SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws`
2. restart service

Recovery back to gRPC after false-positive fallback:

```bash
cd /var/www/solana-copy-bot
rm -f state/ingestion_source_override.env
rm -f state/ingestion_failover_cooldown.json
sudo systemctl restart solana-copy-bot
```
