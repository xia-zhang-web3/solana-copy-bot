# Audit Package: Server Hotfix Backport (2026-02-20)

Branch: `feat/yellowstone-grpc-migration`

Target:
1. Server node that is behind current branch (example from ops chat: `HEAD=a82dad6`).
2. Minimal-risk backport verification without pulling the full feature stream.

Primary goal:
1. Prevent stale-close PnL distortion from single micro-swap outliers.
2. Ensure rug-rate hard-stop remains auto-clearing (no deadlock behavior).

## Suggested Backport Units

Required:
1. `f2c71f2` — stale-close reliable price hardening (`storage/app` + tests).

If missing on server history:
1. `92dab6b` — rug-rate window-based hard-stop logic + regression test.

Note:
1. If commit hashes drift on server history, verify behavior using code checks below and apply equivalent patch manually.

## Mandatory Verification Points

1. Stale-close must not use raw last swap price.
   1. `close_stale_shadow_lots()` reads reliable price path (`reliable_token_sol_price_for_stale_close`), not `latest_token_sol_price`.
2. Reliable stale-close price must enforce quality:
   1. windowed samples
   2. min SOL notional filter
   3. median-based aggregation
   4. minimum sample count gate
3. If reliable stale-close quote is unavailable:
   1. lot is skipped
   2. risk event `shadow_stale_close_price_unavailable` is emitted
4. Rug-rate deadlock guard:
   1. rug-rate query uses `since` window
   2. app passes `rug_window_start` into `shadow_rug_loss_rate_recent(...)`

## Quick Code Checks (server-side)

```bash
grep -n "reliable_token_sol_price_for_stale_close" crates/app/src/main.rs crates/storage/src/lib.rs
grep -n "shadow_stale_close_price_unavailable" crates/app/src/main.rs
grep -n "rug_window_start" crates/app/src/main.rs
grep -n "fn shadow_rug_loss_rate_recent" crates/storage/src/lib.rs
grep -n "closed_ts >= ?1" crates/storage/src/lib.rs
```

## Targeted Test Commands

```bash
cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price
cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing
cargo test -p copybot-app -q risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades

cargo test -p copybot-app -q
cargo test -p copybot-storage -q
```

## Optional SQL Spot Checks (Operator/Auditor)

```sql
-- stale-close reliable-price misses (should be visible if market is quiet/dirty)
SELECT COUNT(*) AS cnt
FROM risk_events
WHERE type = 'shadow_stale_close_price_unavailable';

-- rug-rate stop/clear events should both be observable over time
SELECT type, COUNT(*) AS cnt
FROM risk_events
WHERE type IN ('shadow_risk_rug_rate_stop', 'shadow_risk_rug_rate_cleared')
GROUP BY type;
```

## Minimal Backport Procedure (example)

```bash
git fetch origin feat/yellowstone-grpc-migration

# optional: only if not present in current server history
git cherry-pick -x 92dab6b

# required: stale-close price hardening
git cherry-pick -x f2c71f2

cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price
cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing
cargo test -p copybot-app -q risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades
```

## Expected Outcome

1. Stale-close logic is robust to micro-swap outliers.
2. No forced close happens when reliable price is unavailable; skip+event path is visible.
3. Rug-rate hard-stop clears automatically after window rolls forward.
4. Targeted tests pass on the server codebase before service restart.
