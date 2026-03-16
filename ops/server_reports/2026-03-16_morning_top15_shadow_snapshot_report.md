## 2026-03-16 morning — top-15 shadow snapshot after trusted bootstrap recovery

### Snapshot time

- Captured at `2026-03-16 07:39 Kyiv`
- UTC reference: `2026-03-16 05:39:13Z`

### Service state

- Live commit: `f924312`
- `solana-copy-bot.service`: `active`
- `copybot-adapter.service`: `active`
- `copybot-executor.service`: `active`
- `solana-copy-bot.service MainPID = 304395`
- `NRestarts = 0`
- Current app runtime start: `2026-03-15 21:21:21 UTC`

### Selection / bootstrap state

- `discovery_strategy_state.trusted_selection_bootstrap_required = 0`
- reason = `trusted_selection_bootstrap_satisfied`
- `followlist.active = 15`
- latest trusted bootstrap bucket:
  - `wallet_metrics.window_start = 2026-03-10T21:00:00+00:00`

### Discovery state

Latest stable cycle samples during the morning window showed:

- `active_follow_wallets = 15`
- `eligible_wallets = 59`
- `follow_promoted = 0`
- `follow_demoted = 0`
- `metrics_written = 0`
- `metrics_persisted = false` on steady-state cached cycles
- `scoring_source = raw_window`
- `top_wallets` non-empty

Top ranked wallets in repeated morning samples:

- `AtXrpE5D7gJhzxhzp5VTX62gnupLjpKRW8Kce2yLSouX`
- `3ojivG6PmdDA5Vcq1DGd7tVZabzetm1MsW779bSpHehi`
- `6ux5H1dZKVLAejyXtynspgPSk1khpANgHEnurvZjVQ2m`
- `5qovLy9TQ49555ZKXN2Yc5YoUaFZRxh8L33XvVLYnQ2s`
- `4EG1csPT8xAekLSjyo7TxafbXJNcA1BQkDAvihtzR3hE`

### Runtime health

Morning telemetry remained operationally healthy:

- `observed_swap_writer_pending_requests = 0`
- `yellowstone_output_queue_fill_ratio = 0.0`
- `sqlite_busy_error_total = 3`
- `sqlite_write_retry_total = 3`
- `sqlite_wal_size_bytes = 432542352` (`~432.5 MiB`)
- `ingestion_lag_ms_p95` mostly in the `1.7s–1.9s` range
- one short recompute spike reached `~5.8s`, then returned to baseline

`app_follow_rejected_ratio` during the morning window stayed around `0.999–1.0`, which is expected under global DEX-program ingestion while only `15` followed wallets are considered relevant.

### Shadow state

- real execution positions (`positions.state = 'open'`): `0`
- open `shadow_lots`: `21`
- distinct open `(wallet, token)` positions: `7`
- open shadow notional: `3.86971005986303 SOL`
- all current open `shadow_lots` were opened after the trusted bootstrap window began
- current open `shadow_lots` older than bootstrap: `0`

Current `shadow_lots` open range:

- earliest current open lot: `2026-03-16T04:26:53.842551050+00:00`
- latest current open lot: `2026-03-16T05:26:00.170115526+00:00`

### Correct overnight PnL boundary

The valid top-15 window starts at:

- `2026-03-15T21:21:22+00:00`

An earlier ad-hoc read based on `closed_ts >= bootstrap` was too broad and mixed in pre-bootstrap lots that merely closed later. That read must **not** be used as the overnight top-15 PnL.

The corrected overnight slice is based on:

- `shadow_closed_trades.opened_ts >= 2026-03-15T21:21:22+00:00`

### Corrected overnight shadow PnL

For lots opened after trusted bootstrap:

- closed `shadow_closed_trades`: `886`
- realized shadow PnL: `+2.08190646730867 SOL`
- entry notional on those closed trades: `95.330289940137 SOL`

This is the correct overnight realized PnL slice for the restored top-15 run.

### Why the earlier negative read was wrong

Closed after bootstrap, regardless of when the lot was opened:

- closed trades: `941`
- realized PnL: `-8.42532811148624 SOL`

The difference is entirely explained by pre-bootstrap lots that closed after the valid window started:

- pre-bootstrap-opened but closed-after-bootstrap trades: `55`
- PnL from those trades: `-10.5072345787949 SOL`
- entry notional from those trades: `10.5072345787949 SOL`

These `55` trades contaminated the first quick read and must be excluded from any overnight top-15 strategy assessment.

### Close-context breakdown for the corrected overnight slice

For `shadow_closed_trades` opened after bootstrap:

- `market`
  - trades: `814`
  - PnL: `+14.7547406110473 SOL`
  - entry notional: `82.6574557963983 SOL`
- `stale_terminal_zero_price`
  - trades: `72`
  - PnL: `-12.6728341437387 SOL`
  - entry notional: `12.6728341437387 SOL`

Net:

- `+2.08190646730867 SOL`

### Interpretation caveat

This corrected `+2.0819 SOL` overnight number still includes auto/stale-close effects.

Important limitation:

- `stale_terminal_zero_price` is explicitly identifiable as auto/stale close
- but some stale-lot cleanup exits that found a non-zero price are recorded with `close_context = 'market'`

So the corrected overnight figure is:

- valid as net realized PnL for lots opened in the restored top-15 regime
- **not** a pure discretionary market-exit PnL

### Morning verdict

- trusted top-15 bootstrap remains active
- fail-close is cleared
- runtime remains healthy
- shadow trading is active on the restored top-15 universe
- corrected overnight realized PnL for post-bootstrap lots is positive: `+2.08190646730867 SOL`
- the previous negative quick read was invalid because it included `55` pre-bootstrap lots
