# Real Execution Decision Ledger

Updated: 2026-06-06

This file is the short source of truth for real execution decisions. It exists
to prevent rediscovering the same theories every day.

## Current Objective

Move to tiny real execution on the normal copy path.

Current target:

- route: selected paid Metis path:
  - paid generic Metis `/quote` for normal AMM/Raydium/Orca/Pump.fun AMM
  - paid `/pump-fun/quote` only for bonding-curve Pump.fun tokens
  - public generic only as fallback/comparison
- first size: `0.01 SOL`
- max open positions: `1`
- wallet: separate tiny-test Solana wallet only
- startup mode: canary-only, fail-closed

Do not start with sniper/launch-token execution.

## Current Production State

- latest deployed `copybot-app`: `d609ba86`
- deployed at: 2026-06-06 11:18 UTC
- `execution.enabled = false`
- `canary_enabled = true`
- `canary_dry_run = true`
- `canary_route = "metis-swap-instructions-dry-run"`
- paid Metis Launch URL is configured
- Priority Fee API Base is configured
- paid Pump.fun quote comparison is enabled
- selected-provider dry-run selector is implemented locally and pending rollout

## Proven Facts

- BUY mint parsing is not the current bug.
- Hot BUY quote path uses `WSOL -> swap.token_out`.
- The same mint can return `TOKEN_NOT_TRADABLE` and later return a valid
  `Pump.fun Amm` route.
- Paid `/pump-fun/quote` returning `Bonding curve for mint not found` usually
  means the token is not in the bonding-curve route stage anymore.
- For migrated `Pump.fun Amm` tokens, normal paid Metis `/quote` is currently
  the relevant route.
- For bonding-curve Pump.fun tokens, paid `/pump-fun/quote` with
  `meta.isCompleted=false` is the relevant route.
- `Bonding curve for mint not found` is a provider route mismatch, not a local
  parsing bug.
- `TOKEN_NOT_TRADABLE` is treated as a transient quote provider/indexer race,
  not a final local parsing failure.

## Latest Fixes

- `d609ba86`: retry transient paid Metis `/quote` `TOKEN_NOT_TRADABLE`.
  - retry delays: `100ms`, `300ms`, `700ms`
- `424a3651`: retry Metis dry-run responses with `Missing token program`.
- Local pending batch:
  - execution build metadata selects provider in this order:
    `pump_fun_paid` bonding curve -> paid generic Metis -> public fallback
  - Pump.fun bonding-curve selections use paid `/pump-fun/swap-instructions`
    dry-run proof
  - operator report exposes `provider_selection` so decisions are visible

Expected effect:

- fewer false `unknown`/skip outcomes when Metis sees the route shortly after
  the first failed quote
- no change for real rug/no-liquidity/high-slippage tokens

## Active Blockers Before Funding Wallet

- Fresh post-`d609ba86` BUY sample must prove the retry path does not introduce
  new failures.
- Pending selected-provider batch must be reviewed, tested, and rolled out
  before using its live data as readiness evidence.
- Submit path audit is still required:
  - wallet key handling
  - transaction build
  - signing
  - send
  - confirm
  - idempotency/no double-buy
  - failure recording
- Real owned-position accounting must be ready for SELL.
- Kill switch and tiny loss caps must be verified.
- Current repository rules still forbid enabling `execution.enabled` and
  submitting trades until the user explicitly changes that rule.

## Deferred But Not Forgotten

### Sniper / Launch-Token Path

Status: deferred.

Meaning:

- classify tokens bought seconds after mint/pool/liquidity creation
- measure token age, first successful quote delay, route type, slippage, and
  leader notional
- do not mix these with the normal tiny execution gate

Reason:

- launch/sniper wallets may buy before aggregator routes are indexed
- this is a separate execution strategy and should not block normal tiny tests

Next action when reopened:

- add a `launch_age_seconds`/`route_stage` report bucket and decide whether to
  filter these tokens or build a dedicated launch path.

### Jito / Lil' Jit

Status: deferred.

Reason:

- buying the add-on alone does nothing until submit routing uses it
- reopen only if tiny real execution shows landing latency or failed submit
  rate as the blocker

### Higher Slippage Thresholds

Status: cautious.

Current stance:

- use measured threshold buckets
- do not jump blindly to very high BUY slippage

## Next Engineering Step

Build the tiny execution preflight and submit audit report.

It must output:

- `READY` / `NOT_READY`
- exact blockers
- target wallet
- expected balance
- buy size
- max open positions
- daily loss cap
- kill-switch status
- latest quote/dry-run/priority-fee status
- whether any post-deploy `TOKEN_NOT_TRADABLE` was recovered by retry

Only after that report is clean should the wallet be funded.
