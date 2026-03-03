# Server Templates (6.1 Bring-up)

These files are repository-side templates for the test-server bring-up tracked by `ops/test_server_rollout_6_1_tracker.md`.

## Files

1. `copybot-executor.service`
2. `copybot-adapter.service`
3. `solana-copy-bot.service`
4. `executor.env.example`
5. `adapter.env.example`
6. `app.env.example`

## Server target paths

1. `/etc/systemd/system/copybot-executor.service`
2. `/etc/systemd/system/copybot-adapter.service`
3. `/etc/systemd/system/solana-copy-bot.service`
4. `/etc/solana-copy-bot/executor.env`
5. `/etc/solana-copy-bot/adapter.env`
6. `/etc/solana-copy-bot/app.env`

## Apply sequence

1. Copy templates to server target paths and replace placeholders.
2. Ensure secret files exist and are owner-only (`0600`/`0400`).
3. Run `sudo systemctl daemon-reload`.
4. Enable/start in dependency order:
   1. `copybot-executor.service`
   2. `copybot-adapter.service`
   3. `solana-copy-bot.service`
5. Run preflight sequence from `ROAD_TO_PRODUCTION.md` section `6.1`.
