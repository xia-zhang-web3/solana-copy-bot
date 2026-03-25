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
13. `copybot-discovery-wallet-freshness-capture.service`
14. `copybot-discovery-wallet-freshness-capture.timer`

## Recent Raw Snapshot Timer Contract

1. `copybot-discovery-recent-raw-snapshot.service` now treats exit code `75` as
   an expected transient outcome for snapshot contention.
2. Operators must inspect the JSON `state` emitted by
   `discovery_recent_raw_snapshot`, not just the systemd success/failure bit.
3. The service template also sets `TimeoutStartSec=3min` so systemd has an
   outer bound if the process ever stops honoring its own attempt budget.
4. Operators should inspect `terminal_reason`, `attempt_duration_ms`,
   `backup_total_page_count`, `backup_copied_page_count`, `source_db_bytes`, and
   `source_wal_bytes` before deciding the timer is healthy on a large journal.
5. The snapshot path now pins the source journal inside a read transaction for
   the duration of the online backup, so live writes no longer force the
   snapshot to chase a moving target forever.
6. Latest surface publication now prefers a hard link from the fresh archive
   snapshot to `latest.sqlite`, falling back to an atomic copy only if linking
   is unavailable.
7. Expected non-fatal states:
   - `written`
   - `self_healed_latest_surface`
   - `skipped_not_due`
   - `deferred`
   - `retryable_busy`
8. `deferred` means the service exited cleanly with a transient non-success
   reason such as bounded attempt-duration exhaustion or, if a healthy latest
   surface existed, retained that surface after the failed attempt.
9. After the practical-completion rollout, repeated `deferred` with tiny
   `backup_copied_page_count` relative to `backup_total_page_count` should be
   treated as abnormal and investigated before leaving the timer disabled.
10. `retryable_busy` means retryable contention happened without a healthy latest
   surface to defer onto; rerun or investigate before calling the snapshot
   surface healthy again.
11. `hard_failure` remains a real failure and should leave the service in the
   normal non-zero failed state.

## Stage 3 Wallet Freshness Capture Timer Contract

1. `copybot-discovery-wallet-freshness-capture.timer` is the operational Stage 3
   evidence path. It exists only to accumulate persisted live-runtime captures
   for `discovery_wallet_freshness_report`; it does not change execution,
   restore, gap-fill, snapshot, or scoring semantics.
2. Discovery refresh currently runs every `600s`, so the accepted Stage 3
   capture cadence is a 15 minute capture cadence:
   - not every refresh cycle, because that would mostly resample the same
     bounded scoring window and add runtime cost without much new evidence
   - not hourly, because Stage 3 recent-history validation would accumulate too
     slowly to be operationally useful
3. `copybot-discovery-wallet-freshness-capture.service` runs:
   - `discovery_wallet_freshness_capture --config /etc/solana-copy-bot/live.server.toml --recent-cycles 3 --json`
4. The service is explicitly bounded by `TimeoutStartSec=5min`, so the scheduled
   path does not rely on an unbounded shell session.
5. Operators should inspect capture failures with:
   - `journalctl -u copybot-discovery-wallet-freshness-capture.service -n 20 --no-pager`
6. Operators should inspect the accumulated Stage 3 verdict with:
   - `discovery_wallet_freshness_report --config /etc/solana-copy-bot/live.server.toml --limit 5 --json`
7. For recent-cycle validation, the important report fields are:
   - `latest_capture_age_seconds`
   - `captures_within_recent_horizon`
   - `recent_horizon_seconds`
   - `stale_captures_excluded_from_verdict`
8. `execution.enabled = false` remains unchanged. This timer only accumulates
   evidence for Stage 3 and must not be interpreted as execution activation.

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
13. `/etc/systemd/system/copybot-discovery-wallet-freshness-capture.service`
14. `/etc/systemd/system/copybot-discovery-wallet-freshness-capture.timer`

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
   7. `copybot-discovery-wallet-freshness-capture.timer`
5. Run preflight sequence from `ROAD_TO_PRODUCTION.md` section `6.1`.
6. Keep `ops/discovery_runtime_restore_runbook.md` on hand for the restore path and drill procedure.
