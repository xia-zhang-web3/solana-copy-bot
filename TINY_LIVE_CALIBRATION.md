# Tiny Live Calibration Notes

Last updated: 2026-06-13

This file records the current tiny-live strategy calibration state so the
project does not depend on chat memory.

It is not a deploy runbook. Use `ARTIFACT_DEPLOY.md` for production artifact
rollouts.

## Current State

- Tiny live execution engine is considered stabilized.
- Tiny entries are intentionally paused.
- Tiny sells remain enabled so existing positions can close through the daemon.
- The tiny book was cleaned after terminal write-off recovery guard validation.
- The H-1 resurrection check passed after 25h+.
- Discovery/followlist work is now the active phase.

Current production posture:

- `canary_entry_submit_enabled=false`
- `canary_tiny_submit_enabled=true`
- executable wallet filter: enabled
- rug wallet filter: disabled
- rug filter parameters in config are calibration placeholders, not accepted
  live thresholds.

## Why Entries Stay Paused

The execution layer proved it can buy, sell, account, reconcile, and recover.
The remaining losses are strategy/followlist/risk-policy losses, not missing
account builder failures.

Keep entries paused until a green criterion is defined from executable follower
data, not from Shadow market-only PnL.

Tiny size `0.01 SOL` is measurement mode. After fixed costs, it is not a valid
profitability target.

## Accepted Execution Milestones

These are done and should not be re-litigated unless new production evidence
contradicts them:

- PumpSwap direct BUY and SELL builder live-proofed.
- Token-2022 reconciliation fixed.
- Confirm/accounting is atomic enough for current canary accounting needs.
- Buy and sell fills are symmetric.
- Missing-account failures on target PumpSwap class were removed.
- Meteora sell route can fallback to entry PumpSwap AMM key.
- pAMM custom errors are decoded in diagnostics.
- Quote age and upstream shadow-risk blockers are observable.
- Zero-balance ATA sweep returned rent.
- Dead-book write-off no longer resurrects positions after 24h.

## Phase 1: Executable Wallet Filter

Status: enabled as follower-gap hygiene.

Purpose:

- Remove wallets whose trades look profitable in Shadow but lose in our
  executable follower prices.
- This is a defensive filter, not the rug-tail fix.

Current parameters:

```toml
publish_min_candidate_wallets = 8

executable_wallet_filter_enabled = true
executable_wallet_filter_window_hours = 48
executable_wallet_filter_min_samples = 10
executable_wallet_filter_max_pnl_sol = 0.0
executable_wallet_filter_max_flip_rate = 0.40
```

Interpretation:

- Require at least 10 executable samples before filtering a wallet.
- Reject if executable PnL is negative.
- Reject if more than 40% of sampled trades flip from Shadow-positive to
  executable-negative.

Known limitation:

- This does not catch rug/stale/terminal tail by itself, because dead tokens
  often have no executable sell quote.

## Phase 2: Rug Wallet Filter

Status: implemented behind config, disabled.

Do not enable yet.

Current placeholder parameters:

```toml
rug_wallet_filter_enabled = false
rug_wallet_filter_window_hours = 48
rug_wallet_filter_min_closed_trades = 10
rug_wallet_filter_max_stale_terminal_rate = 0.20
rug_wallet_filter_max_stale_terminal_pnl_sol = -0.05
```

These values are not accepted live thresholds.

### Captured Calibration Window

Use this fixed window when referring to the preserved pre-pause rug data:

```text
since = 2026-06-10T07:00:00Z
until = 2026-06-12T07:00:00Z
```

Production report path:

```text
/tmp/rug_feedback_distribution_20260610T0700_20260612T0700_33fe8ace.json
```

Key result:

- `eligible_wallet_count=15`
- `threshold_rejected_wallet_count=5`
- `rate_rejected_wallet_count=1`
- `pnl_rejected_wallet_count=5`
- one wallet, `J2A8tQeQTouDpsjeiEx55FAc7VnoXkWBHCe9kGcx96aD`, accounts for
  about 60% of eligible stale/terminal loss in that window.

Interpretation:

- The rug tail is concentrated enough for a rug filter to be useful.
- The placeholder `pnl < -0.05` gate over-rejects baseline meme-token noise.
- The real signal is stale/terminal rate.

Working hypothesis for later tuning:

- Primary gate: stale/terminal rate around `0.20` to `0.25`.
- Secondary gate: catastrophic stale/terminal PnL floor around `-0.30` to
  `-0.50 SOL`, or no PnL gate if data supports rate-only.
- Revalidate on fresh active trading data before enabling.

### Why Returned Top-N Reports Are Not Calibration

Do not use returned top-250 rejection counts as proof that the rug filter is
safe.

Rejected wallets have their selection score zeroed and fall below the retained
top-N set. A report showing `rug_rejected_returned=0` can be structurally true
even if many wallets were rejected before truncation.

Use the fixed-window rug feedback distribution report for calibration.

### Required Before Enabling Rug Filter

The rug filter must not be enabled until these are done:

1. Add durable/sticky quarantine for rug-rejected wallets.
2. Prevent reject starvation from making wallets eligible again after their bad
   trades age out of the rolling window.
3. Retune thresholds from fixed-window and fresh active-window data.
4. Revalidate that survivor count stays above `publish_min_candidate_wallets`
   with margin.
5. Keep rug filter off if validation data is stale or unavailable.

The durability rule is the same lesson as terminal write-off recovery: a
rejected wallet must not resurrect just because the evidence window expired.

## Exit Policy Direction

Current stale timers are too late for the observed rug tail.

Data summary:

- Winning market-context trades resolve quickly.
- Winner hold-time p90 was about 11 minutes in the analyzed window.
- Stale/rug tokens usually lose market activity between 30 and 60 minutes.
- 2h/4h stale timers still fire too late for the main loss mode.

Proposed direction, not yet implemented:

- Keep leader-sell exit path.
- Add event-driven exit triggers:
  - activity collapse
  - price collapse from entry
- Add 30-45 minute backstop force-exit.
- Keep long stale timers only as final cleanup.

This is a strategy/risk decision, not an execution fix.

## Risk Contour Direction

Do not use Shadow market-only PnL as the green signal for live entries.

Future risk contour should use:

- executable canary PnL across all relevant close contexts
- open mark-to-quote drawdown
- stale/terminal tail metrics
- existing rug-rate gates where still useful

Open unrealized losses must be visible to risk. The prior Shadow market-only
view hid bag-holding losses.

## Green Criterion Before Re-Enabling Entries

Entries should stay disabled until all are true:

- executable wallet filter remains healthy
- rug filter or equivalent rug-tail protection is validated
- exit policy addresses stale/rug bags before the market dies
- trailing executable edge is positive before fixed costs
- target live size is at least `0.05 SOL`
- expected edge exceeds break-even for the target size
- open mark-to-quote drawdown is included in risk reporting

Do not define green on `0.01 SOL` after-fee PnL.

## Do Not Do

- Do not enable `rug_wallet_filter_enabled=true` from placeholder thresholds.
- Do not read top-N returned rejection counts as rug calibration.
- Do not use rolling `now - 48h` while entries are paused to calibrate old
  active trading behavior.
- Do not restart the daemon for operator/report-only changes.
- Do not change `execution.enabled` as part of tiny canary work.
- Do not widen live trading filters to create faster tests.
- Do not manually submit trades outside the daemon path.

## Next Engineering Batch

Recommended next bounded batch:

1. Add sticky rug-wallet quarantine in `storage-core` and `discovery-v2`.
2. Keep `rug_wallet_filter_enabled=false`.
3. Add tests proving a rug-rejected wallet stays rejected after rolling-window
   evidence ages out.
4. Keep the rollout target to `copybot-discovery-v2` only.
5. Run fixed-window distribution report again after deploy.

After that:

- tune rug thresholds
- revalidate on fresh active-window data
- then decide whether to enable the rug filter

