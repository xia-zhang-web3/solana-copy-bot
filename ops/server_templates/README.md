# Server Templates (6.1 Bring-up)

These files are repository-side templates for the test-server bring-up tracked by `ops/test_server_rollout_6_1_tracker.md`.
They are synced with the current staging server snapshot (`52.28.0.218`, `2026-03-03`).

## Security note

1. `live.server.toml.example` currently contains staging QuickNode endpoint/token values.
2. bootstrap signer values are for non-live contour testing only.
3. rotate endpoint/token/signer before any tiny-live or production stage.

## Files

1. `live.server.toml.example`
2. `copybot-executor.service`
3. `copybot-adapter.service`
4. `solana-copy-bot.service`
5. `copybot-execution-mock-upstream.service`
6. `executor.env.example`
7. `adapter.env.example`
8. `app.env.example`

## Server target paths

1. `/etc/solana-copy-bot/live.server.toml`
2. `/etc/systemd/system/copybot-executor.service`
3. `/etc/systemd/system/copybot-adapter.service`
4. `/etc/systemd/system/solana-copy-bot.service`
5. `/etc/systemd/system/copybot-execution-mock-upstream.service`
6. `/etc/solana-copy-bot/executor.env`
7. `/etc/solana-copy-bot/adapter.env`
8. `/etc/solana-copy-bot/app.env`

## Apply sequence

1. Copy templates to server target paths and replace placeholders.
2. Ensure secret files exist and are owner-only (`0600`/`0400`).
3. Run `sudo systemctl daemon-reload`.
4. Enable/start in dependency order (non-live contour):
   1. `copybot-execution-mock-upstream.service`
   2. `copybot-executor.service`
   3. `copybot-adapter.service`
   4. `solana-copy-bot.service`
5. Run preflight sequence from `ROAD_TO_PRODUCTION.md` section `6.1`.
