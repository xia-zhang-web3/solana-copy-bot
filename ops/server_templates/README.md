# Server Templates (6.1 Bring-up)

These files are repository-side templates for the test-server bring-up tracked by `ops/test_server_rollout_6_1_tracker.md`.
They are synced with the current staging server snapshot (`52.28.0.218`, `2026-03-03`).

## Security note

1. `live.server.toml.example` contains placeholder-only RPC values; populate real credentials only in the server-local copy or via env overrides.
2. `live.server.toml.example` also contains a placeholder-only `recent_raw_gap_fill.helius_http_url`; restore operators must replace it with the historical raw source URL used for bounded gap-fill.
3. `live.server.toml.example` also contains a placeholder-only `recent_raw_gap_fill_helius.helius_http_url`; this must point at a Helius endpoint that supports `getTransactionsForAddress`.
4. `live.server.toml.example` also contains a placeholder-only `program_history_validation.http_url`; this must point at the QuickNode RPC endpoint that will be used for bounded program-history validation through `getSlot`, `getBlockTime`, `getBlocks`, and `getBlock`.
5. bootstrap signer values are for non-live contour testing only.
6. rotate endpoint/token/signer before any tiny-live or production stage.

## Files

1. `live.server.toml.example`
2. `copybot-executor.service`
3. `copybot-adapter.service`
4. `solana-copy-bot.service`
5. `copybot-execution-mock-upstream.service`
6. `executor.env.example`
7. `adapter.env.example`
8. `app.env.example`
9. `copybot-discovery-runtime-export.service`
10. `copybot-discovery-runtime-export.timer`
11. `copybot-discovery-recent-raw-snapshot.service`
12. `copybot-discovery-recent-raw-snapshot.timer`

## Recent Raw Snapshot Timer Contract

1. `copybot-discovery-recent-raw-snapshot.service` now treats exit code `75` as
   an expected transient outcome for snapshot contention.
2. Operators must inspect the JSON `state` emitted by
   `discovery_recent_raw_snapshot`, not just the systemd success/failure bit.
3. Expected non-fatal states:
   - `written`
   - `self_healed_latest_surface`
   - `skipped_not_due`
   - `deferred`
   - `retryable_busy`
4. `deferred` means the service hit retryable SQLite contention but retained a
   healthy latest snapshot surface, so the timer can retry on the next run.
5. `retryable_busy` means retryable contention happened without a healthy latest
   surface to defer onto; rerun or investigate before calling the snapshot
   surface healthy again.
6. `hard_failure` remains a real failure and should leave the service in the
   normal non-zero failed state.

## Server target paths

1. `/etc/solana-copy-bot/live.server.toml`
2. `/etc/systemd/system/copybot-executor.service`
3. `/etc/systemd/system/copybot-adapter.service`
4. `/etc/systemd/system/solana-copy-bot.service`
5. `/etc/systemd/system/copybot-execution-mock-upstream.service`
6. `/etc/solana-copy-bot/executor.env`
7. `/etc/solana-copy-bot/adapter.env`
8. `/etc/solana-copy-bot/app.env`
9. `/etc/systemd/system/copybot-discovery-runtime-export.service`
10. `/etc/systemd/system/copybot-discovery-runtime-export.timer`
11. `/etc/systemd/system/copybot-discovery-recent-raw-snapshot.service`
12. `/etc/systemd/system/copybot-discovery-recent-raw-snapshot.timer`

## Apply sequence

1. Copy templates to server target paths and replace placeholders.
2. Ensure secret files exist and are owner-only (`0600`/`0400`).
3. Run `sudo systemctl daemon-reload`.
4. Enable/start in dependency order (non-live contour):
   1. `copybot-execution-mock-upstream.service`
   2. `copybot-executor.service`
   3. `copybot-adapter.service`
   4. `solana-copy-bot.service`
   5. `copybot-discovery-runtime-export.timer`
   6. `copybot-discovery-recent-raw-snapshot.timer`
5. Run preflight sequence from `ROAD_TO_PRODUCTION.md` section `6.1`.
6. Keep `ops/discovery_runtime_restore_runbook.md` on hand for the restore path and drill procedure.
