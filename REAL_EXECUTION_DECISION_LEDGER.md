# Real Execution Decision Ledger

Updated: 2026-06-24

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

- Track-B entry quote diagnostic plus market-exit shadow quote diagnostic
- question: does the dominant `market` bucket remain profitable after both
  follower entry and follower exit are measured with executable quotes?
- required evidence: clean matched pairs:
  `entry diagnostic quote -> shadow market close -> market-exit quote`
- current state: first 24h/12h/6h bounded report is available; it is
  directional, not a trading green light

If the fully executable market bucket goes flat or negative across windows, the
current hypothesis "copy these wallet signals post-leader and recover edge with
filters" should be treated as not actionable in this regime.

## Current Production State

- latest deployed `copybot-app`: `7c240bd7`
- latest deployed `copybot-operators`: `4e046a5b`
- latest production checks: 2026-06-24 10:12 UTC
- `execution.enabled = false`
- guarded tiny submit config may exist, but entries remain strategy-paused
- Track-B entry quote diagnostic is enabled
- market-exit shadow quote diagnostic is enabled
- rug wallet filter is enabled as followlist hygiene
- dashboard is live on `grindscout.com`
- latest rollout postflight:
  - daemon active
  - `NRestarts = 0`
  - site HTTP 200
  - operator artifact installed without daemon restart

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

## Current Open Test: Track-B Entry + Market Exit

Purpose:

- measure executable entry quality before trade commitment
- produce executable entry cost for scoring entry filters on the right
  objective
- measure executable market exits so the dominant `market` bucket is no longer
  paper-only

Latest fix:

- `414fb131`: fixed Track-B quote-price poisoning from bad decimals
- `7c240bd7`: added quote-only market-exit diagnostic
- `7580a640`: upgraded bounded report to join entry quotes to market-exit quotes
- `b22231c3`: booked failed market-exit quotes as zero-exit, kept missing rows
  as no-data, and added `--max-market-exit-delay-ms`
- `4e046a5b`: split market-exit errors into terminal dead/no-route errors
  versus transient provider/amount errors; only terminal errors book zero-exit
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
- if the fully executable market bucket is also flat/negative across windows,
  close the current copy-follow hypothesis as not actionable in this regime

Survivorship-corrected bounded report, 2026-06-24:

- 24h: 324 clean usable entry events; 238 fully executable events.
- 24h market bucket: 300 market events, 223 fully executable market events,
  fully executable market PnL `+2.500 SOL`; 134 missing rows make this window
  partly pre-enablement/no-data.
- 12h post-enablement: 196 clean usable events; 190 fully executable events.
- 12h market-exit errors: 11 total, 8 terminal dead/no-route zero-exit,
  3 transient no-data.
- 12h market bucket: 182 market events, 181 fully executable market events,
  market PnL `+2.025 SOL`.
- 6h: 60 clean usable events; 56 fully executable events.
- 6h market bucket: 59 market events, 56 fully executable market events,
  market PnL `-0.452 SOL`.
- fully executable total: `+0.227 SOL` over 12h, `-0.452 SOL` over 6h.
- stale_quote remains negative: 12h `-0.999 SOL`, 24h `-1.294 SOL`.
- market-exit quote delay: roughly p50 33-35s, p90 56-57s, p95 about 60s.
- delay-filtered 12h at 30s: only 46/182 market events remain fully
  executable; market PnL `+2.366 SOL`, but 167 rows become no-data, so this is
  a latency-quality view, not full bucket economics.
- sensitivity check: treating `CANNOT_COMPUTE_OTHER_AMOUNT_THRESHOLD` as a
  terminal illiquid-exit instead of transient moved the current 12h market
  bucket by about `-0.055 SOL`; no such events were present in the current 6h.

Interpretation:

- The old `+3.5 SOL` headline is retired as survivorship-biased.
- After correction, post-enablement market is still positive over 12h but much
  smaller and negative over 6h. This is directional, not a green light.
- The full strategy bucket can still go negative because stale_quote remains
  consistently negative.
- Entries remain OFF. Need more post-enablement/multi-regime windows before
  scoring/filter changes.

## Do Not Reopen Without New Evidence

- Do not enable real entries because infrastructure is healthy.
- Do not use shadow PnL as the green criterion.
- Do not build a live 30m/15m exit layer from the closed exit-policy tests.
- Do not tighten Track-A reconstructable filters from the negative backtest.
- Do not treat rug/executable wallet hygiene as permission to trade.
- Do not confuse observation-only followlist expansion with real-entry approval.
  Cohort split may widen shadow collection, but entries remain OFF.

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

Cohort split is approved as observation-only: rank 1-15 remains baseline,
rank 16-30 is the added measurement cohort. Keep rerunning the corrected
Track-B market report by cohort; entries remain OFF.

Slow-hold wallet pivot is approved as the next observation-only lateness test.
It must roll out default-off first, with baseline daemon health verified before
`slow_hold_wallets_enabled=true` is flipped. Success requires a smaller
executable follower gap and broad-based improvement versus baseline, not only
fat-tail winners. This does not authorize real entries.

Do not fund, resume, or increase real entries from this file alone.
