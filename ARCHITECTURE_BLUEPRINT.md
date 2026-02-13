# Solana Copy Bot Architecture Blueprint (v1)

Date: 2026-02-12
Status: Draft for implementation
Language target: Rust (Tokio async)

## 1) Objective

Build an automated Solana copy-trading system that:
- discovers and ranks profitable wallets,
- follows top wallets with strict risk limits,
- executes copy trades with low operational risk,
- removes degraded leaders automatically.

Primary success criteria (first 60 days):
- Stable runtime (no critical downtime > 30 minutes/day).
- Positive net PnL after fees/slippage in paper mode over >= 30 days.
- Copy latency p95 <= 3 seconds from leader swap detection to submitted tx.

## 2) Scope and Non-Goals

In scope:
- Raydium + PumpSwap swap monitoring via Yellowstone gRPC.
- Wallet scoring, follow-list lifecycle, copy execution, risk controls.
- Telegram alerts + minimal web dashboard.
- SQLite as source of truth for v1.

Out of scope (v1):
- Pure latency races against top MEV bots.
- Sandwich/frontrun strategies.
- Multi-chain support.
- Fully on-chain smart-contract vault architecture.

## 3) System Overview

Proposed v1 deployment is one Rust workspace with modular services in one process (multi-task runtime), then split into microservices only if bottlenecks appear.

Data flow:
1. `ingestion` subscribes to Yellowstone gRPC streams.
2. `parser` normalizes raw instructions into `SwapEvent`.
3. `discovery` updates wallet stats and candidate scores.
4. `followlist` promotes/demotes wallets by policy.
5. `signal` emits copy intents when followed wallet buys/sells.
6. `risk` approves/rejects each intent.
7. `execution` submits swaps (Jito path first, RPC fallback).
8. `positions` reconciles fills and lifecycle exits.
9. `monitoring` sends alerts and persists metrics.

## 4) Module Breakdown

### 4.1 Ingestion
- Input: Yellowstone gRPC stream (transactions, account updates as needed).
- Output: normalized swap events:
  - wallet, token_in, token_out, amount_in, amount_out, slot, signature, dex, ts.
- Reliability:
  - reconnect with exponential backoff,
  - gap detection by slot,
  - heartbeat event every N seconds.

### 4.2 Wallet Discovery
- Maintains rolling metrics per wallet:
  - realized/unrealized PnL,
  - win rate,
  - median hold time,
  - trade count per day,
  - avg slippage proxy (if observable),
  - concentration risk by token.
- Filters:
  - suspicious frequency (e.g., > configured tx/min),
  - likely wash patterns,
  - rugged-token exposure penalty,
  - low sample size.

### 4.3 Scoring and Follow-list Manager
- Score recomputed every M minutes.
- Candidate eligibility:
  - min trades window,
  - min active days,
  - max drawdown threshold,
  - net positive over lookback.
- Follow-list:
  - top N by score, capped by per-wallet correlation checks,
  - demotion if score drops below threshold for K cycles.

### 4.4 Signal Engine
- For followed wallet events, generate:
  - `BUY_INTENT` when leader opens/increases.
  - `SELL_INTENT` when leader reduces/closes.
- Guard rails:
  - max copy delay (drop stale intent),
  - ignore tiny notional noise trades,
  - optional token denylist.

### 4.5 Risk Engine
- Pre-trade checks:
  - per-token max exposure (SOL terms),
  - total portfolio exposure,
  - daily realized loss cap,
  - max concurrent positions,
  - max slippage bps per token tier,
  - cooldown after stop events.
- Global kill-switch:
  - triggers on daily loss, drawdown, or execution anomaly rate.

### 4.6 Execution Engine
- Path:
  1) quote,
  2) simulate,
  3) build tx,
  4) submit via Jito,
  5) fallback to standard RPC if policy allows,
  6) confirm/reconcile.
- Requirements:
  - idempotent `client_order_id`,
  - retry policy per failure class,
  - strict timeout budget (latency-sensitive).

### 4.7 Position and Exit Manager
- Maintains local position state:
  - quantity, cost basis, entry ts, leader reference.
- Exit conditions:
  - leader-sell mirror,
  - max hold timeout (fallback exit),
  - hard stop-loss per position,
  - portfolio risk liquidation.

### 4.8 Monitoring and Ops
- Telegram alerts:
  - startup/reconnect/failure,
  - entry/exit with PnL,
  - kill-switch activation.
- Dashboard:
  - active positions,
  - top followed wallets,
  - daily/weekly PnL,
  - execution quality stats.

## 5) Data Model (SQLite v1)

Core tables:
- `wallets` (wallet_id, first_seen, last_seen, status).
- `wallet_metrics` (wallet_id, window_start, pnl, wr, trades, hold_median, score).
- `followlist` (wallet_id, added_at, removed_at, reason, active).
- `observed_swaps` (signature PK, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts).
- `copy_signals` (signal_id, wallet_id, side, token, notional_sol, ts, status).
- `orders` (order_id, signal_id, route, submit_ts, confirm_ts, status, err_code).
- `fills` (order_id, token, qty, avg_price, fee, slippage_bps).
- `positions` (position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state).
- `risk_events` (event_id, type, severity, ts, details_json).
- `system_heartbeat` (component, ts, status).

Indexes:
- `observed_swaps(wallet_id, ts)`,
- `wallet_metrics(score, window_start)`,
- `positions(state, token)`,
- `orders(status, submit_ts)`.

## 6) Event Contracts (Internal)

Recommended internal structs/events:
- `SwapEvent`
- `WalletMetricUpdated`
- `FollowlistChanged`
- `CopyIntentCreated`
- `RiskDecision`
- `OrderSubmitted`
- `OrderFilled`
- `PositionClosed`
- `KillSwitchTriggered`

All events should include:
- `event_id` (UUID),
- `source_component`,
- `ts_utc`,
- `correlation_id` (trace through pipeline).

## 7) Risk Policy Defaults (Initial)

Start conservative for v1:
- `max_position_sol`: 0.5-1.0 SOL per token.
- `max_total_exposure_sol`: 3-5 SOL.
- `daily_loss_limit_pct`: 2%.
- `max_drawdown_pct`: 8%.
- `max_hold_hours`: 6-12h.
- `max_concurrent_positions`: 5.
- `max_copy_delay_sec`: 3-8s.

These are bootstrap values; final values come from paper trading telemetry.

## 8) Ranking Policy (First Practical Version)

Weighted score (example):
- 35% net PnL over 30d,
- 20% risk-adjusted return proxy,
- 15% win rate with sample floor,
- 15% hold-time quality (avoid hyper-noise),
- 10% consistency (positive days ratio),
- 5% penalty bucket (suspicious behavior, rug adjacency, concentration).

Hard gates before score:
- min trades in window,
- min active days,
- no critical safety flags.

## 9) SLOs and KPIs

System SLO:
- Uptime >= 99% (excluding upstream provider outages).

Execution KPIs:
- signal-to-submit latency p50/p95,
- submit-to-confirm latency p50/p95,
- fail ratio by error class,
- realized slippage bps distribution.

Strategy KPIs:
- net PnL (SOL and USD),
- hit rate,
- avg win/loss,
- payoff ratio,
- max drawdown,
- leader contribution concentration.

## 10) Security and Key Management

- Keep signing key outside image (env/secret mount only).
- Separate keys for dev/test/prod.
- Read-only API keys where possible for data providers.
- Audit log for all order actions.
- If any key was shared in plain text, rotate immediately.

## 11) Repo Structure Proposal

```
solana-copy-bot/
  Cargo.toml
  README.md
  ARCHITECTURE_BLUEPRINT.md
  configs/
    dev.toml
    paper.toml
    prod.toml
  crates/
    core-types/
    ingestion/
    discovery/
    scoring/
    risk/
    execution/
    portfolio/
    storage/
    notifier/
    app/
  migrations/
  docker/
```

## 12) Delivery Plan (No Overbuild)

Phase 0 (3-5 days):
- Workspace skeleton, config loader, structured logging, SQLite migrations.

Phase 1 (5-7 days):
- Ingestion + parser + persistence of observed swaps.
- Wallet metrics job + scoring + follow-list updates.
- Shadow mode only (no trading).

Phase 2 (5-7 days):
- Signal + risk + execution adapters.
- Paper trading mode with full telemetry.

Phase 3 (5-7 days):
- Small capital canary (strict caps), kill-switch validated.
- Post-trade analytics and wallet demotion automation.

Phase 4:
- Improve ranking features, add additional DEX adapters if needed.

## 13) Critical Open Questions Before Coding

1. Which exact swap source for execution in v1 (Jupiter direct, Raydium direct, or hybrid)?
2. Exact definition of "profitable wallet" window (7d/30d/90d)?
3. Do we copy all buys or only buys above confidence threshold?
4. Preferred fail-closed behavior on provider outage (pause vs partial mode)?
5. Legal/compliance constraints for copied addresses/jurisdiction?

---

This blueprint is intentionally practical for a solo build. It optimizes for fast validation in shadow/paper mode before risking material capital.

## 14) Decision Log (2026-02-12)

These are the selected defaults for Phase 0/1 implementation:

1. Swap source for execution:
- Jupiter-first routing for v1 (best route aggregation by default), with direct-Dex adapters optional in later phases.

2. Profitable wallet window:
- 30d primary scoring window.
- 7d decay/health overlay for faster demotion.

3. Copy threshold:
- Do not copy all buys.
- Copy only when leader trade notional >= configured threshold (default 0.5-1.0 SOL equivalent) and risk checks pass.

4. Provider outage behavior:
- Fail-closed for new entries (`pause_new_trades=true`).
- Existing positions remain managed by timeout and hard-risk exits.

5. Legal/compliance baseline:
- Not a blocker for v1 prototype, but add minimum controls:
  - explicit audit logs,
  - denylist capability,
  - key separation by environment,
  - terms/jurisdiction review before scaling capital.

6. Ingestion transport for v1:
- Use Helius WebSocket (`logsSubscribe`) + HTTP `getTransaction` on Developer plan.
- Keep Yellowstone gRPC as optional future upgrade if latency/capacity requires it.
