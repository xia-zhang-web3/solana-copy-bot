# Live Simulation Dual-Threshold Tracking Plan (RPC-Safe Revision)

## Goal

For each accepted shadow BUY, compute additional strict "live" eligibility and track PnL on two tracks:

- Shadow track (existing behavior, unchanged)
- Live-sim track (subset of lots/trades marked `live_eligible=1`)

Primary safety objective: do this without increasing RPC pressure.

## Non-Negotiable Design Contracts

1. Shadow decisions must not change.
- No new `ShadowDropReason`.
- No gate order changes.
- No change to signal creation criteria.

2. No additional RPC calls caused by live-sim.
- Live-sim must reuse already available data.
- If data is missing, live-sim may classify as ineligible/unknown, but must not trigger extra refresh calls.

3. SELL semantics are lot-level.
- One SELL can close mixed FIFO inventory (`live_eligible=1` and `0`).
- Live metrics for SELL must come from closed-lot rows, not a signal-level boolean.

4. Historical rows remain non-live by default.
- Existing rows after migration keep `live_eligible=0`.
- Live analytics are meaningful only post-deploy.

## Scope

In scope:
- Config for live thresholds
- DB schema extension (`live_eligible`)
- Storage write/read changes
- Shadow service live classification
- App/runtime logs and snapshot reporting

Out of scope:
- Execution behavior changes
- New RPC workflows
- Backfilling historical live labels

## 0) Baseline Before Any Code Change

Capture a 30-60 minute baseline from current paper runtime:
- ingestion lag p95
- rpc_429/rpc_5xx deltas
- shadow signal counts (buy/sell)
- shadow drop reason distribution

This baseline is required for post-deploy comparison.

## 1) Config (`crates/config/src/lib.rs`)

Extend `ShadowConfig`:
- `live_sim_enabled: bool` (default `false`)
- `live_min_token_age_seconds: u64` (default `300`)
- `live_min_holders: u64` (default `50`)
- `live_min_liquidity_sol: f64` (default `10.0`)
- `live_min_volume_5m_sol: f64` (default `5.0`)
- `live_min_unique_traders_5m: u64` (default `3`)
- `live_max_signal_lag_seconds: u64` (default `30`)

Optional but recommended for ops safety:
- add env overrides for live fields (`SOLANA_COPY_BOT_SHADOW_LIVE_*`).

## 2) Migration (`migrations/0009_live_eligible.sql`)

```sql
ALTER TABLE shadow_lots ADD COLUMN live_eligible INTEGER NOT NULL DEFAULT 0;
ALTER TABLE shadow_closed_trades ADD COLUMN live_eligible INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_shadow_lots_live_opened_open
  ON shadow_lots(live_eligible, opened_ts)
  WHERE qty > 0;

CREATE INDEX IF NOT EXISTS idx_shadow_closed_trades_live_closed
  ON shadow_closed_trades(live_eligible, closed_ts);
```

Notes:
- Migration is forward-only.
- No data rewrite/backfill.

## 3) Storage (`crates/storage/src/lib.rs`)

### 3.1 Write-path updates

- `insert_shadow_lot(..., live_eligible: bool)`
- In FIFO close:
  - read `live_eligible` from each source lot,
  - write `live_eligible` into each inserted `shadow_closed_trades` row.

### 3.2 Outcome contract update (required)

Extend `ShadowCloseOutcome` with:
- `live_closed_qty: f64`
- `live_realized_pnl_sol: f64`

These are computed during the same FIFO transaction while closing per-lot slices.

### 3.3 Live metrics APIs

Add:
- `shadow_realized_pnl_since_live(since) -> Result<(u64, f64)>`
- `shadow_open_lots_count_live() -> Result<u64>` using `WHERE live_eligible=1 AND qty>0`
- `shadow_open_notional_sol_live() -> Result<f64>`

All must use indexed predicates on `live_eligible`.

## 4) Shadow Service (`crates/shadow/src/lib.rs`)

### 4.1 Snapshot model

Introduce `TokenQualitySnapshot`:
- `token_age_seconds`
- `holders`
- `liquidity_sol`
- `volume_5m_sol`
- `unique_traders_5m`
- `source` (`rpc_cache` / `rpc_cache_stale` / `db_proxy` / `cache_only`)

### 4.2 RPC-safe data acquisition rule

For BUY evaluation:
- if `quality_gates_enabled=true`:
  - use existing quality path (same behavior as today),
  - reuse resulting snapshot for live classification.
- if `quality_gates_enabled=false` and `live_sim_enabled=true`:
  - build snapshot from `token_market_stats + token_quality_cache` only,
  - do not call RPC refresh.

This preserves "live independent from quality_gates_enabled" while keeping RPC flat.

### 4.3 Live classification

Add:
- `LiveIneligibleReason` enum (analytics only)
- `classify_live(snapshot, latency_ms, config) -> (bool, Option<LiveIneligibleReason>)`

BUY flow:
- keep current shadow gates unchanged;
- after BUY is accepted, compute `live_eligible`;
- pass flag to `insert_shadow_lot`.

SELL flow:
- no signal-level `live_eligible` boolean;
- rely on FIFO lot-level split via `ShadowCloseOutcome`.

### 4.4 Public models

Extend:
- `ShadowSnapshot` with:
  - `closed_trades_24h_live`
  - `realized_pnl_sol_24h_live`
  - `open_lots_live`
- `ShadowSignalResult` with:
  - `live_eligible_buy: Option<bool>` (Some for BUY, None for SELL)
  - `live_closed_qty: f64` (SELL)
  - `live_realized_pnl_sol: f64` (SELL)

## 5) Main Loop (`crates/app/src/main.rs`)

Logging changes only, no gate changes:
- in `shadow signal recorded`:
  - BUY: include `live_eligible_buy`
  - SELL: include `live_closed_qty`, `live_realized_pnl_sol`
- in `shadow snapshot`:
  - include live and shadow fields side-by-side
- add separate analytics counter map for `live_ineligible_reasons`
  - must not be mixed into `shadow_drop_reason_counts`.

## 6) Paper Config (`configs/paper.toml`)

Enable live-sim with strict defaults:

```toml
live_sim_enabled = true
live_min_token_age_seconds = 300
live_min_holders = 50
live_min_liquidity_sol = 10.0
live_min_volume_5m_sol = 5.0
live_min_unique_traders_5m = 3
live_max_signal_lag_seconds = 30
```

## 7) Runtime Snapshot (`tools/runtime_snapshot.sh`)

Add "Live Simulation" section:
- `open_lots_live`
- `open_notional_sol_live`
- `realized_pnl_window_live`
- `realized_pnl_24h_live`
- `closed_trades_24h_live`
- `winrate_24h_live`
- deltas vs shadow totals

## 8) Tests (Minimum Gate)

1. Config defaults + deserialize for new live fields.
2. Migration applies on existing DB with prior schema.
3. BUY path writes `live_eligible` into `shadow_lots`.
4. FIFO SELL propagates `live_eligible` into `shadow_closed_trades`.
5. Mixed FIFO (live + non-live) returns correct split:
- `non_live_closed_qty = closed_qty - live_closed_qty`
- `non_live_realized_pnl_sol = realized_pnl_sol - live_realized_pnl_sol`
- and both derived values are consistent with inserted closed-trade rows.
6. Live SQL APIs return correct values with mixed datasets.
7. App logs include live fields; shadow drop counters unchanged.
8. Regression: signal count and drop reason distribution equal to baseline on same replay input.

## 9) Rollout Order (Safe)

1. Config + migration
2. Storage contract changes (+ tests)
3. Shadow snapshot/refactor + live classification (+ tests)
4. App log/snapshot extension
5. `runtime_snapshot.sh` updates
6. Enable `live_sim_enabled=true` in `configs/paper.toml`
7. Deploy and monitor 60+ minutes against baseline

## 10) Acceptance Criteria

Functional:
- Shadow behavior unchanged.
- Live metrics coherent:
  - `live_closed_trades_24h <= shadow_closed_trades_24h`
  - `live_open_lots <= shadow_open_lots`

Operational:
- No additional RPC pressure from live-sim:
  - `shadow_quality_refresh_attempts` remains flat/zero for the condition
    `quality_gates_enabled=false && live_sim_enabled=true`,
  - `rpc_429` and `rpc_5xx` trend unchanged vs baseline.
- No meaningful regression in ingestion lag p95.
- No increase in SQLite contention/lock retries beyond baseline noise.

## 11) Rollback Plan

If any regression is detected:
1. Set `live_sim_enabled=false` and restart.
2. Keep schema changes in place (non-breaking, additive).
3. Re-compare metrics to baseline.
4. Re-enable only after fix + targeted test coverage.
