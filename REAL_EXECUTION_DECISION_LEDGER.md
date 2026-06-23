# Real Execution Decision Ledger

Updated: 2026-06-23

This file is the short source of truth for real execution decisions. It exists
to prevent rediscovering the same theories every day.

## Current Objective

Decide whether the current copy-follow strategy has any real executable edge.

Current trading decision:

- real entries remain OFF
- live SELL path must not be changed for strategy experiments
- no filter tightening should enable trading by itself
- current work is measurement, not a green-light path

Current measurement target:

- Track-B entry quote diagnostic
- question: can executable entry quote quality identify trades that should be
  skipped before committing real money?
- required evidence: clean matched pairs:
  `entry diagnostic quote -> eventual shadow close outcome`
- target sample: about 300 clean closed pairs or 48h of clean post-fix data,
  whichever comes first

If Track-B is also empty, the current hypothesis "copy these wallet signals
post-leader and recover edge with filters" should be treated as not
actionable in this regime.

## Current Production State

- latest deployed `copybot-app`: `414fb131`
- deployed at: 2026-06-23 07:12 UTC
- `execution.enabled = false`
- guarded tiny submit config may exist, but entries remain strategy-paused
- Track-B entry quote diagnostic is enabled
- rug wallet filter is enabled as followlist hygiene
- dashboard is live on `grindscout.com`
- latest rollout postflight:
  - daemon active
  - `NRestarts = 0`
  - site HTTP 200
  - first post-rollout Track-B event inserted with `quote_status = ok`
  - `quote_price_sol` was non-null and guard did not fire

## Closed Strategic Findings

### Shadow PnL Is Not A Trading Decision Metric

Shadow PnL consistently overstates copyable edge. The decision metric must be
executable or at least close-context aware.

Do not green-light entries from:

- shadow PnL alone
- discovery score alone
- leader historical PnL
- one favorable market window

### Exit Policy Investigation

Status: closed negative.

What was tested:

- observed 30m backstop simulation
- executable-at-30m diagnostic
- blind 30m backstop
- conditional price-decay 30m trigger
- maturity-gated context split
- 15m/activity-silent motivation

Result:

- observed +5.10 SOL was an upper-bound illusion
- clean executable-vs-executable 30m edge was near zero
- price-decay conditioning inverted the signal
- terminal-zero prize did not appear after maturity gating

Conclusion:

- practical exit timing is not the lever
- existing 2h stale-quote close already captures most salvageable exits
- do not build a live SELL exit layer from these tests

### Track-A Reconstructable Entry Filters

Status: closed negative.

What was tested:

- wallet rug-rate / tail-rate
- token age
- token-seen-before
- leader entry lag
- simple reconstructable entry features

Result:

- cheap reconstructable features did not separate rug-like losses from market
  winners well enough
- token-age gates cut too many winners
- wallet-history gates had thin coverage because most candidates lacked enough
  point-in-time history

Conclusion:

- do not tighten these filters for trading
- use them only as diagnostics unless new evidence appears

### Rug Wallet Filter

Status: enabled as hygiene, not as a trading green-light.

Current accepted shape:

- minimum closes: 7
- stale/terminal rate threshold: 20%
- PnL catastrophic floor: `-0.30 SOL`
- quarantine hold: 168h

Conclusion:

- useful for removing obvious feeder wallets
- protects followlist quality
- does not make the strategy profitable by itself
- entries remain OFF

### Executable Wallet Filter

Status: enabled as Phase-1 hygiene.

Important blind spot:

- it scores clean market-context round trips
- it cannot see the dominant stale/terminal rug tail

Conclusion:

- useful, but not rug protection
- do not treat it as a green-light signal

## Current Open Test: Track-B Entry Quote Diagnostic

Purpose:

- measure executable entry quality before trade commitment
- produce executable entry cost for scoring entry filters on the right
  objective

Latest fix:

- `414fb131`: fixed Track-B quote-price poisoning from bad decimals
- priority is now:
  1. decimals from quote response
  2. saved observed-leg token decimals
  3. RPC only if both are missing
- inferred raw/UI decimals are no longer used for Track-B executable price
- impossible low-impact price ratios are nulled instead of poisoning metrics

What to watch:

- total post-fix diagnostic events
- `quote_status = ok`
- `quote_price_sol IS NULL`
- `entry_diag_price_ratio_out_of_bounds`
- matched closed pairs
- executable entry-adjusted PnL by bucket

Decision rule:

- if Track-B finds a clean executable entry-quality signal, design a gate and
  backtest it across windows before any trading change
- if Track-B is also empty, close the current copy-follow hypothesis as
  not actionable in this regime

## Do Not Reopen Without New Evidence

- Do not enable real entries because infrastructure is healthy.
- Do not use shadow PnL as the green criterion.
- Do not build a live 30m/15m exit layer from the closed exit-policy tests.
- Do not tighten Track-A reconstructable filters from the negative backtest.
- Do not treat rug/executable wallet hygiene as permission to trade.
- Do not increase followed wallets to accelerate stats unless the analysis
  explicitly needs broader cohort data and RPC/write pressure is accepted.

## Historical Route Notes

These are older route/execution findings. They remain useful implementation
context, but they are not a reason to enable entries.

- BUY mint parsing was not the execution blocker.
- Hot BUY quote path uses `WSOL -> swap.token_out`.
- The same mint can return `TOKEN_NOT_TRADABLE` and later return a valid
  route; this is treated as a transient provider/indexer race.
- `Bonding curve for mint not found` usually means route-stage mismatch, not
  local parsing failure.
- For migrated Pump.fun AMM tokens, normal generic Metis quote is the relevant
  route.
- For bonding-curve Pump.fun tokens, paid Pump.fun quote/swap-instructions are
  the relevant route.
- Provider route selection and quote diagnostics are hygiene. They do not
  override the strategy-level executable-edge gate.

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

## Next Step

Collect the clean Track-B post-`414fb131` sample, then run a bounded analysis
of executable entry-adjusted outcomes.

Do not fund, resume, or increase real entries from this file alone.
