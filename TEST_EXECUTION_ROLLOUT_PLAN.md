# Test Execution Rollout Plan

Updated: 2026-06-05

This document tracks the remaining engineering work before tiny real execution
tests. It is not an approval to enable live trading.

## Goal

Move from shadow-only to controlled test execution without risking uncontrolled live trades.

## Current Status

Done:

- Shadow trading is running with fixed `0.2 SOL` sizing.
- Dry-run execution canary records idempotent BUY candidates.
- No-submit execution canary state-machine is wired into the live daemon.
- Quote canary records BUY and SELL quotes.
- Quote canary records slippage decisions:
  - `would_execute`
  - `would_skip`
  - `would_force_exit`
  - `unknown`
- Priority fee canary is wired for measurement, but it still has occasional
  provider errors and is not yet used inside submitted transactions.
- Stale shadow closes now try quote price before terminal zero.
- PnL, win/loss, stale-close, skip, quote, and priority-fee data can be queried
  from SQLite.
- Metis swap-instructions dry-run HTTP proof is implemented behind an explicit
  config flag.
- Production artifact rollout for live no-submit canary is complete:
  - `copybot-app` artifact installed from an off-production Linux build
  - `execution.enabled = false`
  - `canary_enabled = true`
  - `canary_dry_run = true`
  - `canary_route = "metis-swap-instructions-dry-run"`
  - real submit remains disabled
- Live proof after rollout:
  - service active/running
  - ingestion `rpc_429`, `rpc_5xx`, gaps, drops, and replacements are `0`
  - `exec-canary` orders are being recorded
  - current readiness remains measurement-only with `production_green = false`

Latest live canary sample:

- `exec-canary` BUY orders: `4`
- order status: `execution_canary_submit_disabled`
- quote canary events after rollout: `8`
- quote status: `8/8 ok`
- priority fee status: `6 ok / 2 timeout`
- readiness window:
  - `would_enter`: `2`
  - `would_skip`: `1`
  - `unknown`: `1`

Still blocked:

- Real transaction build/simulate/submit/confirm path is not implemented.
- Real position state is not implemented.
- Real SELL handling for owned positions is not implemented.
- Safe retry/recovery after submit or confirm uncertainty is not implemented.
- Priority fee is measured, not applied to real transactions.
- Jito / Lil' Jit / sender routes are not production-wired.
- `execution.enabled` must remain fail-closed.

## Non-Negotiable Safety

- Do not enable `execution.enabled`.
- Do not submit real trades in this batch.
- Do not copy leader full size.
- Do not enable normal `0.2 SOL` real trading first.
- Do not deploy a submit path without idempotency tests.
- Do not rely on add-ons until their behavior is measured on tiny size.
- Do not use production-local release builds as the normal rollout path.

## QuickNode Add-On Decision

Current decision: paid add-ons are upgraded only where they are already measured
or needed for the next tiny execution gate.

Current account state:

- Scale plan is active and includes Solana gRPC.
- The separate paid Solana gRPC add-on was canceled after Scale upgrade.
- QuickNode issued a `499 USD` credit note for the old gRPC add-on.
- Keep the credit as reserve for execution add-ons instead of spending it now.

Current add-ons:

- Solana Priority Fee API: Base plan active.
- Metis Jupiter Swap API: Launch plan active.
- Lil' Jit / Jito Bundles & Transactions: not active.

When to buy:

- Priority Fee API Advanced/Premium:
  - buy only if Base still shows repeated 429/timeout or missing fee data after
    larger live samples
- Metis higher plans:
  - buy only if Launch still shows quote/instruction latency or reliability as
    the actual blocker
- Lil' Jit Freshman (`89 USD/month`):
  - buy only after Jito submit/bundle route is implemented in code
  - current runtime does not use Jito/Lil' Jit, so buying it now changes nothing

Do not buy based on theoretical benefit. Upgrade only when the canary report
shows a concrete blocker: rate limit, timeout rate, quote latency, submit
failure rate, or landing latency.

## Phase 1: Canary Execution State Machine

Add a dedicated tiny execution canary state machine. This must not be a plain
`execution.enabled=true` switch.

Required behavior:

- Keep production default fail-closed.
- Use a separate canary-only activation flag.
- Enforce tiny limits:
  - buy size: `0.01-0.02 SOL`
  - max open positions: `1`
  - max daily loss: very small fixed SOL cap
  - max failed submits before stop
  - manual kill switch path
- Record lifecycle states:
  - `candidate`
  - `quoted`
  - `built`
  - `simulated`
  - `submitted`
  - `confirmed`
  - `failed`
  - `expired`
- Add status output:
  - enabled/disabled
  - route
  - wallet
  - open exposure
  - last submit
  - last confirm
  - last error
  - kill switch state

## Phase 2: Submit Adapter

Implement the real transaction path behind the canary state machine.

Required behavior:

- Build swap transaction for a tiny BUY.
- Simulate before submit.
- Apply priority fee / compute budget when configured.
- Submit once with a deterministic client order id.
- Confirm transaction.
- Record tx signature, fee, confirmation latency, status, and errors.
- Retry only when the previous attempt is known not to have landed.
- Never double-buy the same signal.

## Phase 3: Real Position Accounting And SELL

SELL handling must be based on positions actually owned by the bot, not just
shadow close rows.

Required behavior:

- Store real position quantity with exact `qty_raw` and decimals.
- Close only real owned quantity.
- Support partial closes.
- Keep dust handling explicit.
- Treat SELL slippage differently from BUY:
  - BUY above limit can skip.
  - SELL above soft limit can force exit, but must record the reason.
- Never leave a position open forever only because SELL slippage is high.

## Phase 4: Routing And Add-Ons

Evaluate and wire only what is needed for tiny canary reliability.

Priority order:

1. Normal RPC with dynamic priority fee.
2. Jito / Lil' Jit route if landing latency or failed submit rate is bad.
3. Optional sender or broadcast add-ons only after real data shows a need.

Required behavior:

- Priority Fee API must feed actual transaction compute budget settings.
- Provider rate limits must degrade gracefully.
- Route choice must be stored per order.
- Fallback route must not double-submit the same order.

## Phase 5: Safety Gates

Before tiny real execution:

- Ingestion gaps/drops must be `0`.
- Writer backlog/WAL behavior must be understood and bounded.
- Shadow PnL should remain positive on 24h and 12h windows.
- No active shadow hard stop.
- Restart recovery must be verified.
- Open/close accounting must match expected signal flow.
- Canary quote data must show enough `would_execute` candidates.
- Priority fee errors must be low enough or safely ignored for the chosen route.

## Phase 6: Tiny Mainnet Canary

Run only after Phases 1-5 are implemented and reviewed.

Limits:

- `0.01-0.02 SOL` per BUY.
- `1` open position max.
- Short observation windows: 30m, 1h, 4h.
- Manual kill switch active and tested.

Compare:

- shadow entry/exit
- quote entry/exit
- real entry/exit
- slippage
- priority fee used
- confirmation latency
- failed submits
- missed sells
- realized PnL

## Required Tests Before Tiny Real

- No double-submit for same signal.
- Restart after `built`.
- Restart after `submitted` with unknown confirmation.
- Failed simulation.
- Submit error.
- Confirm timeout.
- Safe retry only when not landed.
- BUY slippage skip.
- SELL force-exit.
- Priority fee provider error fallback.
- Kill switch blocks new submits.
- Daily loss cap blocks new submits.
- Max open positions blocks new BUY.

## Completed Development Slice

Implemented locally:

- Canary order lifecycle state on top of the existing `orders` table.
- Deterministic canary order ids and client order ids.
- Canary idempotency separate from older dry-run order rows.
- No-submit submit adapter skeleton.
- No-submit transaction build/simulate skeleton.
- Canary status report counts lifecycle states and latest error order.
- State-machine entry handling for BUY candidates only.
- Failed build and failed simulation adapter cases record terminal failed orders.
- Timeout/expired lifecycle is covered for built orders.
- Restart after a pre-built canary order does not rebuild or submit.
- Tests for idempotent reservation, valid transitions, simulated submit-disabled,
  status reporting, terminal transition rejection, dry-run coexistence,
  no-submit build/simulate handling, failure handling, timeout handling, restart
  behavior, SELL skip behavior, canary-owned position state, and SELL force-exit
  decision shape for owned positions.
- Submitted canary confirm-timeout decisions are conservative:
  - wait before timeout
  - expire without retry when a tx signature exists
  - retry only when no tx signature exists
- Canary-owned position close accounting is covered for no-position, partial
  close, full close, exact quantity sidecars, realized PnL, and dust tail close.
- Canary state-machine SELL handling is wired to owned-position close accounting
  without submitting transactions:
  - no owned position stays no-op
  - normal SELL closes owned position
  - high-slippage SELL force-exits owned position
  - partial SELL preserves remaining exact quantity
- Canary state-machine submitted-timeout handling is wired without submitting
  transactions:
  - wait before timeout
  - retry only when no tx signature exists
  - expire without retry when a tx signature exists
- Dry end-to-end canary scenario records a synthetic canary-owned BUY position,
  processes SELL, and reports closed/open/PnL summary.
- Canary pre-submit safety gates are enforced before order reservation/build:
  - manual kill switch blocks BUY before reserve
  - max open canary positions blocks BUY before reserve
  - max daily canary realized loss blocks BUY before reserve
- Canary transaction build request/plan metadata is wired without submitting
  transactions:
  - deterministic client order id is carried into the build request and plan
  - latest BUY quote-canary route and expected raw amounts are attached
  - selected priority fee source/status/value is attached
- Canary build-plan metadata is persisted without submitting transactions:
  - quote route and expected raw amounts are stored per canary order
  - selected priority fee source/status/value is stored per canary order
  - latest build metadata is exposed through canary status/report coverage
- Metis `/swap-instructions` HTTP dry-run is implemented without submitting
  transactions:
  - enabled by `swap_instructions_dry_run_enabled`
  - posts a Jupiter-compatible `quoteResponse` from canary quote metadata
  - requires `swapInstruction` in the HTTP response before simulation passes
  - stores a short proof summary in `simulation_error` for passed orders
  - does not sign or submit anything
- Canary execution-readiness summary is available without submitting
  transactions:
  - latest canary order is joined with stored quote and priority-fee metadata
  - BUY readiness is classified as `would_enter`, `would_skip`, `unknown`, or
    missing metadata
  - operator JSON exposes route, raw in/out amounts, price impact, slippage,
    decision reason, and priority fee
  - production-green remains false; this is measurement only
- Canary execution-readiness window summary is available without submitting
  transactions:
  - bounded recent-order window counts `would_enter`, `would_skip`, and
    `unknown` decisions
  - skip reasons, provider errors, and routes are grouped for operator output
  - latest route, price impact, priority fee, and metadata age are exposed
  - operator remains read-only and `production_green` remains false

Wired into live runtime and deployed to production as no-submit/dry-run only.

- Live daemon now records `exec-canary:*` orders on BUY signals.
- SELL signals are quote-measured, but state-machine SELL close handling still
  needs full live owned-position integration before real submit.
- Production remains fail-closed:
  - `execution.enabled = false`
  - canary order status is `execution_canary_submit_disabled`
  - no real trades are submitted

## Next Bounded Batch

Continue toward tiny real execution without submitting transactions.

Scope:

- Roll out the HTTP swap-instructions artifact with
  `swap_instructions_dry_run_enabled = true`.
- Collect the first fresh eligible BUY and verify:
  - quote event exists
  - execution canary order reaches `execution_canary_submit_disabled`
  - simulation proof summary contains `metis_swap_instructions_ok`
  - no HTTP timeout/rate-limit pattern appears
- Then continue toward tiny execution gate.

Build target: `copybot-app` only if live daemon behavior changes; otherwise
operator-only batches should build the narrow operator artifact.

Production rollout: artifact-first only. Restart daemon only for runtime changes.
Do not enable real submit.
