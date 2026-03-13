# 2026-03-13 Morning Stale-Close Rollout Report

## Scope

- Host: `52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Config: `/etc/solana-copy-bot/live.server.toml`
- Deployed commit: `d06da0a48c7c52ed469961d8dc5193dc1358f93e`
- Commit subject: `Bound terminal stale close zero pricing`

## Change Applied

The stale-close hotfix was deployed together with the matching live config update:

```text
risk.shadow_stale_close_terminal_zero_price_hours = 12
risk.max_hold_hours = 6
```

`solana-copy-bot.service` was rebuilt and restarted. `copybot-adapter.service` and `copybot-executor.service` were left untouched.

## Pre-Deploy Baseline

Immediately before rollout, the live shadow state was pinned:

```text
followlist.active            = 1502
copy_signals                = 496526
shadow_lots_open            = 63
shadow_closed_trades        = 667
shadow_realized_pnl_sol     = +2.190049326
shadow_open_cost_basis_sol  = 10.386682153
```

The stale-close deadlock signature was also present:

```text
risk_event shadow_stale_close_price_unavailable = 206007
risk_event shadow_risk_pause                    = 36483
```

Operationally, every open shadow lot was already older than both:

- `max_hold_hours = 6`
- `terminal_zero_price_hours = 12`

## Rollout Result

The new process came up cleanly:

```text
ActiveEnterTimestamp = Fri 2026-03-13 07:50:42 UTC
ExecMainPID          = 230109
NRestarts            = 0
```

Immediate startup logs confirmed the intended stale-close behavior:

```text
stale lot cleanup tick:
  closed_priced        = 0
  skipped_unpriced     = 0
  terminal_zero_closed = 63
  max_hold_hours       = 6
  terminal_zero_price_hours = 12
```

The stale backlog was therefore cleared in a single startup cleanup pass.

## Post-Rollout Checkpoint

Spot check taken after the restart:

```text
observed_swaps_max_ts        = 2026-03-13T07:53:42.767187177+00:00
discovery_runtime.cursor_ts  = 2026-03-13T07:53:41.286310356+00:00
direct cursor/head gap       ~= 1.5s
```

Disk / DB state:

```text
/dev/root                    = 145G total, 7.6G used, 137G avail, 6%
/dev/nvme1n1                 = 492G total, 113G used, 354G avail, 25%
live_copybot.db             ~= 113G
live_copybot.db-wal         ~= 844M
```

Business state after cleanup:

```text
followlist.active            = 1513
copy_signals                = 496828
shadow_lots_open            = 0
shadow_closed_trades        = 730
shadow_realized_pnl_sol     = -8.196632827
shadow_open_cost_basis_sol  = 0.000000000
orders                      = 0
positions                   = 0
fills                       = 0
```

Before / after delta:

```text
followlist.active       +11
copy_signals            +302
shadow_lots_open        -63
shadow_closed_trades    +63
realized shadow pnl     -10.386682153 SOL
```

## Risk Aftermath

The stale-close bug itself is closed:

- no stale shadow lots remain open,
- open shadow exposure dropped to zero,
- the old `shadow_risk_pause` re-arm condition from pinned exposure is removed.

But the rollout immediately produced a one-time shadow PnL shock:

```text
latest shadow_risk_hard_stop ts = 2026-03-13T07:50:43.022284751+00:00
shadow_risk_hard_stop count     = 2
shadow_stale_close_terminal_zero_price count = 63
```

Startup logs captured the new hard-stop trigger:

```text
shadow snapshot:
  open_lots            = 0
  closed_trades_24h    = 63
  realized_pnl_sol_24h = -10.386682143

shadow risk hard stop activated:
  reason = drawdown_24h: pnl_24h=-10.386682 <= stop=-5.000000
```

## Non-Blocking Notes

Two warnings appeared shortly after startup:

```text
failed persisting discovery runtime cursor
failed upserting token_quality_cache row
```

These were single-shot warnings in the checked log window and did not stop the service. Runtime stayed active and near-head.

## Verdict

The stale-close deadlock is fixed in live.

What changed:

1. The pinned stale shadow backlog was fully cleared.
2. Open shadow exposure dropped from `10.386682153 SOL` to `0`.
3. The previous exposure-soft-cap pause cause is no longer pinned by immortal lots.

What changed next:

1. The rollout realized the full stale backlog as terminal zero-price closes.
2. This pushed realized shadow PnL from `+2.190049326 SOL` to `-8.196632827 SOL`.
3. A shadow hard-stop was activated from the 24h drawdown guard.

So this rollout should be read as:

- stale-close bug: fixed,
- shadow accounting backlog: flushed,
- new immediate blocker: review / clear the shadow hard-stop before expecting normal shadow reopening behavior again.
