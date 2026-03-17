## 2026-03-16 noon — top-15 shadow snapshot

### Snapshot time

- Captured at `2026-03-16 12:11 Kyiv`
- UTC reference: `2026-03-16 10:11:33Z`

### Scope

This report excludes the old invalid `976`-wallet era.

All PnL and trade counts below are scoped to the restored valid top-15 window only:

- `shadow_closed_trades.opened_ts >= 2026-03-15T21:21:22+00:00`

### Service state

- Live commit: `f924312`
- `solana-copy-bot.service`: `active`
- `copybot-adapter.service`: `active`
- `copybot-executor.service`: `active`
- `solana-copy-bot.service MainPID = 304395`
- `NRestarts = 0`
- Current app runtime start: `2026-03-15 21:21:21 UTC`

### Selection / bootstrap state

- `trusted_selection_bootstrap_required = 0`
- reason = `trusted_selection_bootstrap_satisfied`
- `followlist.active = 15`
- trusted bootstrap bucket:
  - `wallet_metrics.window_start = 2026-03-10T21:00:00+00:00`

### Rotation check

Compared against the original trusted bootstrap set written at:

- `2026-03-15T21:21:21.753438104+00:00`

Current result:

- bootstrap set size: `15`
- current active set size: `15`
- still active from bootstrap: `15`
- rotated out: `0`
- rotated in: `0`

There has been no followlist rotation since the trusted top-15 bootstrap.

### Discovery state

Current stable cycle samples:

- `active_follow_wallets = 15`
- `eligible_wallets = 98`
- `follow_promoted = 0`
- `follow_demoted = 0`
- `top_wallets` non-empty

### Runtime health

- `observed_swap_writer_pending_requests = 0`
- `yellowstone_output_queue_fill_ratio = 0.0` in steady state
- one short transient spike reached `0.01953125`
- `sqlite_busy_error_total = 3`
- `sqlite_write_retry_total = 3`
- `sqlite_wal_size_bytes = 432542352`
- `ingestion_lag_ms_p95` mostly in the `1.6s–1.9s` range
- one recompute spike reached about `8.2s`, then returned to baseline

### Closed shadow exits PnL

For `shadow_closed_trades` opened after valid top-15 bootstrap:

- lot-close rows: `1284`
- distinct close signals: `860`
- distinct closed `(wallet, token)` pairs: `94`
- distinct wallets involved: `15`
- realized PnL: `-5.62625603233919 SOL`
- entry notional: `142.781395044063 SOL`

Important:

- `shadow_closed_trades` stores FIFO lot-close rows
- one sell or stale-close signal can close multiple lots
- so `1284` is **not** “1284 strategy trades”
- the correct event-level close count here is `860 distinct close signals`

### Open shadow positions

- open `shadow_lots`: `25`
- distinct open `(wallet, token)` positions: `14`
- distinct wallets with open positions: `11`
- open notional: `4.01860495593746 SOL`

### Note on unrealized PnL

There is no persisted shadow mark-to-market unrealized PnL metric for `shadow_lots` in the live DB.

So this report intentionally gives:

- realized PnL for closed shadow positions
- open position count and open notional for current open shadow positions

It does not invent a shadow unrealized PnL number.
