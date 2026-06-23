# Strategy Experiment Ledger

Status date: 2026-06-23

This file records strategic experiments and negative results so the project does
not keep re-litigating the same ideas from memory.

## Current State

- Entries remain OFF.
- Broader execution remains OFF.
- Rug wallet filter is ON as hygiene, not as proof that trading is profitable.
- Track-B entry quote diagnostic is live.
- Open question: can entry-side selection reject bad entries before capital
  commits?

## Core Findings

### Shadow PnL Is Not The Trading Objective

Shadow PnL repeatedly overstated the strategy because it assumes we get the
leader-like entry/exit economics. Real follower execution loses edge through
slippage, delay, no-route cases, and tail losses.

Decision rule:

- Do not use raw shadow PnL as a green-light metric.
- Prefer executable or limit-matched metrics.
- Treat shadow-only reports as directional context, not proof.

### Strategy Is Regime-Dependent

Observed windows showed large swings:

- 2026-06-10..2026-06-12: executable/canary negative.
- 2026-06-14..2026-06-16: executable positive in a favorable regime.
- Later fresh windows: near zero or negative after executable adjustment.

Conclusion:

- One good window is not enough; green criterion must be multi-window and
  executable-based.
- A profitable-looking window can be a regime artifact.

### Follower Tail Gap Is The Main Damage

The honest reconciliation showed executable capture far below shadow edge. A
large part of shadow edge was consumed by tail cases: stale closes, no-route
cases, route decay, and fast rugs.

Conclusion:

- Follower late-entry is structural. Any filter must reduce tail losses without
  deleting most winners.

### Publish Floor Is A Hard Constraint On Every Filter

Mechanism:

- `publish_min_candidate_wallets = 8` in prod and live.
- If surviving candidates fall below 8, publish fail-closes. The publication
  goes stale, the runtime can empty the follow universe, shadow fail-closed
  flips, and data collection can self-starve.

Implication:

- Over-rejection can black out the data pipeline.
- Any new/tightened filter must report combined rejection rate and surviving
  candidate count before enablement.
- Rug and executable filters compound; joint survivor count matters most.

### Close-Context Pricing Is Mixed

Not all close marks have the same quality:

- `stale_quote_price`: executable aggregator quote, trusted for
  executable-vs-executable comparisons.
- `market`, `stale_market_price`, `quarantined_legacy`: paper/observed marks.
- `stale_terminal_zero_price`, `recovery_terminal_zero_price`: forced zero.

Implication:

- Mixed-context aggregate PnL is mark-asymmetric.
- Prefer executable-vs-executable subsets for green/no-go calls.

## Closed Experiments

### Infrastructure Blackout / Self-Starvation

Problem:

- Followlist fell empty after artifact drift and publish fail-close.
- observed_swaps dried up because zero-universe persistence was too thin.

Fix:

- Best-effort zero-universe refill cooldown changed from 30s to 5s.
- SHA-sync discipline added for app/discovery deploys.

Result:

- Recovered from floor-edge to stable 15/15 candidates.
- This was an infrastructure fix, not a strategy edge.

Status: closed, monitor only.

### Rug Wallet Filter

Candidate:

- min closes: 7
- stale/rug rate: 20%
- PnL floor: -0.30 SOL
- hold: 168h

Result:

- Validated across bad, good, and fresh windows.
- Caught obvious feeder wallets.
- Did not reject active follow wallets in validation.
- Floor remained safe.

Conclusion:

- Good hygiene filter.
- Not enough to make entries profitable by itself.

Rejected naive variant:

- Thresholds `rate > 0.0` / `pnl < 0.0` are NO-GO.
- The PnL branch is near-total because stale/terminal closes are losses by
  nature.
- Survivors can collapse below the publish floor and recreate the
  self-starvation blackout.
- Current `rate 20% / pnl -0.30` thresholds deliberately avoid that.

Quarantine mechanics:

- Quarantine is 168h and forward-only.
- Enforcement is decoupled from extension after fix `9d813cee`: a wallet is
  re-quarantined only on a fresh data-driven reject, not because it is already
  quarantined.
- Expired quarantine rows are pruned.

Status: enabled as followlist hygiene.

### Executable Wallet Filter

Candidate:

- min samples: 10 per wallet
- window: 48h
- reject if executable PnL after priority fee < 0
- reject if flip rate > 40%
- combined with OR
- commits: `2f0c1767`, `b34b8722`

Result:

- Per-cycle reversible hygiene filter, not a sticky ratchet.
- Catches wallets whose shadow wins do not survive follower execution.

Important blind spot:

- Scores only clean market-context round trips.
- Excludes stale-close trades and cannot see positions with no successful sell
  quote.
- Therefore it is blind to the dominant stale/terminal rug tail.
- Rug protection must come from stale/terminal close-rate, not executable PnL.

Conclusion:

- Useful Phase-1 hygiene.
- Not the main loss lever.

Status: enabled in prod and live as Phase-1 hygiene.

### Blind 30m Exit Backstop

Initial observed-price simulator:

- Looked very strong: about +5.10 SOL.

Executable diagnostic:

- Reduced the benefit by about 10x.
- Later v2 context split showed trusted executable-vs-executable edge near zero.
- Maturity-gated rerun removed the terminal-zero censoring concern.

Conclusion:

- Blind 30m exit is not worth live SELL-path work.
- Existing stale quote close around 2h already captures most executable exits
  that are still capturable.

Status: NO-GO.

### Conditional Price-Decay Exit

Hypothesis:

- Exit at 30m only if price already decayed.

Result:

- Worse than blind.
- The condition excluded lots that were still healthy at 30m but died later.
- It fired on already-damaged lots where waiting sometimes recovered more.

Conclusion:

- Price decay inverted the signal.

Status: NO-GO.

### 15m / Activity-Silent Exit

Hypothesis:

- Earlier activity-based exit might catch rugs before 30m.

Result:

- Terminal-zero prize was not material after maturity-gated rerun.
- Most lots closed through existing stale quote / market paths before forced
  zero.
- Loss window is often in the first minutes, before practical follower exits can
  react.

Conclusion:

- Exit timing is structurally weak for this strategy.
- Do not build a live SELL exit layer from this data.

Status: NO-GO unless a new regime shows materially higher terminal-zero losses.

### Execution Latency

Hypothesis:

- Faster execution would reduce the loss tail.

Result:

- Tail losses persisted even with 1-3s execution.
- Losses concentrate in the first minutes; the follower is late by design, so
  shaving seconds does not recover the tail.

Conclusion:

- Latency must stay healthy for hygiene, but it is not the profit lever.

Status: NO-GO as a profit lever.

### Track-A Reconstructable Entry Filters

Tested cheap/reconstructable features:

- wallet rug rate
- recent wallet tail rate
- token seen before bad
- token age
- leader entry lag
- liquidity proxy

Important correction:

- The first Track-A version used shadow PnL, which was the wrong objective.
- It was corrected to a close-context tail objective:
  stale_quote_price plus terminal contexts.

Result after objective correction:

- Token age cut a small amount of tail but removed too many market winners.
- Wallet rug/tail features were weak and covered only a small subset because
  most entries had insufficient wallet history.
- token_seen and leader_lag showed no useful signal.

Scope caveat:

- The tail objective covers stale_quote plus terminal contexts and excludes
  `stale_market_price`, a separate larger decayed-loss population.
- Even a working rug-like filter would not address stale_market losses.

Conclusion:

- Cheap reconstructable features do not separate rug from winner well enough.

Status: closed negative.

### Followlist Expansion For Faster Data

Idea:

- Increase followed wallets from 15 to 30 to collect events faster.

Assessment:

- Faster collection, but changes the sample from top-15 to top-15 plus ranks
  16-30. Mixed data contaminates conclusions about the current strategy.

Safe use:

- Only as a separate observation experiment with cohorts split: rank 1-15 vs
  rank 16-30.

Status: optional, not a primary proof path.

## Active Experiment

### Leader Copyability Report

Purpose: test whether Discovery ranks wallets that are profitable for
themselves or wallets whose edge survives copy-following.

First live run was invalid (`copybot-operators` at `ec68c454`):
- 720h ending 2026-06-23 11:34 UTC.
- `wallet_scoring_close_facts` is empty on live because it is a dead v1/test
  path.
- Fallback to `wallet_metrics` made the result non-interpretable:
  2h leader window vs 720h follower window, near-total score tautology, and
  eligibility gated by 2h discovery activity.
- Do not use rank-vs-leader, copyability ratios, or candidate lists from that
  run. rank-vs-follower `+0.314` was only a weak hint with corrupted
  eligibility (`n=6`).

Observed-swaps replay run (`copybot-operators` at `866e9abf`):

- Strict 720h, min 5 leader / 5 follower closes: only 2 eligible.
- Exploratory min 5 / 1: 10 eligible, leader rho `-0.418`, follower rho
  `+0.103`, 3 high-leader/low-copyability candidates.
- Directional objective mismatch; unlock = Track-B or cohort split.

Status: active; no scoring change yet.

### Track-B Entry Quote Diagnostic

Purpose:

- Measure the real executable entry price at the moment a shadow buy signal
  appears.

What it records:

- signal_id-linked BUY quote, quote price, impact, route, and output amount.

Safety:

- Quote-only: no submit, sign, fill, order, or position accounting.
- Diagnostic events use prefix `quote:entry-shadow-diag:` and decision
  consumers exclude that prefix.

Why it matters:

- It gives executable entry cost, lets us rescore filters on the real objective
  instead of shadow PnL, and tests whether bad entry quotes predict stale/rug
  tails or bad fills.

First preliminary slice:

- 395 entry quote events; 368 OK; 0 NULL quote prices after decimals fix.
- Correct outcome join is wallet/token/opened_ts, not signal_id.
- 355 clean closed usable events after excluding 11 pre-fix ratio outliers.
- Fully executable `stale_quote_price` bucket is negative: entry-adjusted
  `-1.91`.
- Dominant market bucket is hybrid paper-exit; its `+10.53` is not bankable.
- Aggregate `+9.29` is misleading because it mixes executable/paper exits.
- price impact weak; quote/shadow ratio catches more stale_quote cases but is
  collateral-heavy and single-window.

Status: preliminary; no filter enable; needs repeatable split report; entries OFF.

## Do Not Reopen Without New Evidence

- Do not use raw shadow PnL as a trading green light.
- Do not use observed-price simulations as proof of executable edge.
- Do not build blind 30m exit.
- Do not build conditional price-decay exit.
- Do not build 15m exit unless terminal-zero/stale-tail mix materially changes.
- Do not widen filters or lower floor just to force green.
- Do not mix top-15 and rank 16-30 data without cohort separation.
- Do not set rug-filter thresholds to `0.0/0.0`.
- Do not enable or tighten any wallet/entry filter without reporting combined
  survivor count vs the publish floor of 8.
- Do not treat the executable wallet filter as rug protection.
- Do not re-test latency as a profit lever without a new mechanism.

## Next Decision

After enough Track-B samples:

1. Join each entry quote to the eventual shadow outcome and compute executable
   entry-adjusted PnL split by exit executability.
2. Sweep price impact, slippage, no-route/weak-route, route quality, and
   quote/shadow ratio.
3. Measure tail reduction, winner loss, real-vs-hybrid delta, and floor safety.
4. Only consider filters that improve real executable economics without cutting too
   many winners.

Until then, entries remain OFF.
