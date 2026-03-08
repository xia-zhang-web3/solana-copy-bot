# Wallet Activity Hotfix Runtime Recovery Verdict

Date (UTC final checkpoint): `2026-03-08T12:27:08Z`  
Server: `52.28.0.218`  
Deployed commit: `8b2bc43`  
Rollout start (service active): `2026-03-08 09:18:57 UTC`  
Observed evidence window: `~3h 08m 11s` (`2026-03-08T09:18:57Z -> 2026-03-08T12:27:08Z`)

## Evidence sources

1. Live intermediate checkpoint captured at `2026-03-08T10:14:42Z` (`~55m 45s` after rollout start).
2. Live follow-up checkpoint captured at `2026-03-08T12:27:08Z` (`~3h 08m 11s` after rollout start).
3. Both checkpoints were read directly from:
   - `systemd` / `journalctl` for `solana-copy-bot.service`
   - live SQLite state in `/var/www/solana-copy-bot/state/live_copybot.db`

## Formal verdict

1. Hotfix rollout `8b2bc43` stability: **CONFIRMED** on the current `~3h 08m` window.
2. The prior rollback-triggering live regression is **not reproduced**:
   - `database is locked = 0`
   - `failed to insert observed swap batch = 0`
   - `failed to record heartbeat = 0`
   - `discovery cycle still running = 0`
   - `shadow risk infra stop activated = 0`
3. The intended runtime behavior is present:
   - SQLite contention is bounded and recovered via retry instead of cascading into live failure
   - ingestion remains healthy
   - discovery is near-head at the final checkpoint

## Key runtime evidence

1. Service/process health:
   - `solana-copy-bot.service active`
   - `NRestarts=0`
   - `ExecMainPID=161724`
   - `ActiveEnterTimestamp=2026-03-08 09:18:57 UTC`
2. SQLite contention:
   - intermediate checkpoint: `sqlite_busy_error_total=2`, `sqlite_write_retry_total=2`
   - final checkpoint: `sqlite_busy_error_total=2`, `sqlite_write_retry_total=2`
   - interpretation: counters stopped growing across the second `~2h 12m` interval; retry path is working and lock escalation is absent
3. Ingestion:
   - intermediate checkpoint: `ingestion_lag_ms_p95=1828`, `p99=1921`, `ws_to_fetch_queue_depth=1`
   - final checkpoint: `ingestion_lag_ms_p95=1762`, `p99=1823`, `ws_to_fetch_queue_depth=20`
   - interpretation: lag remains healthy and does not show degradation
4. Websocket/backpressure watch:
   - intermediate checkpoint: `ws_notifications_backpressured=111`, `ws_notifications_replaced_oldest=111`
   - final checkpoint: `ws_notifications_backpressured=111`, `ws_notifications_replaced_oldest=111`
   - interpretation: startup burst is not continuing; counters stayed flat through the second observation window
5. Discovery runtime:
   - intermediate checkpoint: non-recompute cycles around `~4.0-4.25s`
   - final checkpoint: non-recompute cycles around `15-18 ms`
   - final checkpoint:
     - `swaps_fetch_limit_reached=false`
     - `swaps_fetch_pages=1`
     - `swaps_query_rows_last_page ~ 11.8k-12.5k`
   - interpretation: discovery is no longer pinned to the bounded per-cycle cap at the final read

## Cursor/head recovery

1. Intermediate checkpoint:
   - `discovery_cursor_ts=2026-03-07T22:04:03.776382826+00:00`
   - `observed_swaps_max_ts=2026-03-08T10:14:54.968313871+00:00`
   - `cursor/head gap ~= 43851s` (`~12h 10m 51s`)
2. Final checkpoint:
   - `discovery_cursor_ts=2026-03-08T12:26:55.709206385+00:00`
   - `observed_swaps_max_ts=2026-03-08T12:27:06.973647345+00:00`
   - `cursor/head gap ~= 11s`
3. Trend across the two post-rollout checkpoints:
   - gap reduction: `43840s`
   - inferred `cursor_advance_ratio ~= 6.53`
4. Interpretation:
   - discovery backlog is no longer merely stable under cap
   - by the final checkpoint discovery has recovered to near-head operation

## Scope boundaries

1. This verdict is about **runtime recovery of the live hotfix rollout**.
2. This verdict does **not** by itself declare historical eligibility recovery complete:
   - `active_follow_wallets=0`
   - `total_follow_wallet_rows=0`
   - `wallet_activity_day_rows=243281`
   - `wallet_activity_day_min=2026-03-07`
   - `wallet_activity_day_max=2026-03-08`
3. Historical `wallet_activity_days` backfill remains a separate operational step via:
   - `tools/backfill_wallet_activity_days.py`

## Watch items

1. DB growth remains large:
   - `live_copybot.db ~80.9 GB`
   - `live_copybot.db-wal ~9.18 GB`
2. A later follow-up or overnight snapshot is still useful for audit trail hardening, even though the current rollout verdict is already positive.

## Conclusion

1. `8b2bc43` can be treated as a **successful live hotfix rollout** for the wallet-activity write-contention regression.
2. The exact failure mode that forced the previous rollback is absent on the current `~3h 08m` evidence window.
3. The system is not only stable; by the final checkpoint discovery has also returned to near-head operation (`~11s` gap).
4. The next step is not another emergency rollback/hotfix decision. The next step is routine operations:
   - capture one more later checkpoint for a stronger audit trail
   - run historical `wallet_activity_days` backfill separately when ready
