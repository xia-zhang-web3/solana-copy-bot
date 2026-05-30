# Test Execution Rollout Plan

Current status:

- Shadow trading is useful and currently runs with fixed `0.2 SOL` sizing.
- Real execution is not ready to enable directly.
- `copybot-app` currently rejects `execution.enabled = true` via execution quarantine.
- Submit adapter / transaction sender integration is not active.
- Priority fee / Jito / sender add-ons are only config-shaped, not production-wired.

## Goal

Move from shadow-only to controlled test execution without risking uncontrolled live trades.

## Phase 1: Execution Readiness

- Add a separate canary flag for test execution, not a plain `execution.enabled=true` switch.
- Keep production default fail-closed.
- Define tiny limits:
  - buy size: `0.01-0.02 SOL`
  - max open positions: `1`
  - max daily loss: very small fixed SOL cap
  - manual kill switch required
- Add clear status output: enabled/disabled, route, wallet, exposure, last submit, last error.

## Phase 2: Submit Path

- Implement or restore submit adapter flow.
- Required behavior:
  - build transaction
  - simulate before submit
  - submit once with idempotency key
  - confirm transaction
  - record order/fill/status
  - retry only when safe
  - never double-buy the same signal
- Test buy and sell paths separately.

## Phase 3: Add-ons / Routing

Evaluate and wire only what we need:

- QuickNode Priority Fee API: dynamic compute unit price.
- Jito / Lil' Jit: bundle or priority route for faster landing.
- Iris / transaction sender: optional fast sender if Jito/RPC path is not enough.
- Multi-region broadcast: optional fallback, not first requirement.

Initial route order should likely be:

1. Jito / priority route
2. Normal RPC fallback
3. Optional sender/broadcast after we see real landing issues

## Phase 4: Safety Gates

Before real test execution:

- Ingestion gaps/drops must be `0`.
- Writer backlog/WAL warnings must be reduced or understood.
- Shadow PnL should stay positive on 24h and 12h windows.
- No active shadow hard stop.
- Restart recovery must be verified after maintenance restart.
- Open/close accounting must match expected signal flow.

## Phase 5: Tiny Mainnet Canary

Run tiny canary only after Phase 1-4:

- `0.01-0.02 SOL` per buy.
- `1` open position max.
- Short observation windows: 30m, 1h, 4h.
- Compare:
  - shadow entry/exit
  - real entry/exit
  - slippage
  - confirmation latency
  - failed submits
  - missed sells
  - realized PnL

## Do Not Do Yet

- Do not copy leader full size.
- Do not enable normal `0.2 SOL` real trading first.
- Do not enable execution without adapter/confirmation/idempotency tests.
- Do not rely on add-ons until their behavior is measured on tiny size.

## Next Work Item

Start with Phase 1 and Phase 2:

1. Unquarantine execution behind a dedicated canary-only flag.
2. Build dry-run submit path.
3. Add tiny-mainnet config profile.
4. Add tests for no double-submit and safe retry.

## Implementation Notes

- `execution.enabled` stays fail-closed.
- First implementation flag is `execution.canary_enabled`.
- Current canary mode is dry-run only: it records idempotent execution orders
  from existing shadow signals and does not submit transactions.
- Canary candidates are limited to fresh shadow signals only.
- Real tiny-mainnet execution remains blocked until submit/simulate/confirm
  code is implemented and reviewed.
