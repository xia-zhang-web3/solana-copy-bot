## 2026-03-15 late evening — controlled clone-latest bridge restored trusted top-15 bootstrap

### Scope

- Live code remained on narrow streamed-bootstrap commit: `f924312`
- No new code was deployed for this recovery step
- This was a controlled server-side DB bridge to recover a fresh trusted `wallet_metrics` bootstrap bucket from the latest persisted snapshot

### Why this was attempted

Before this bridge:

- runtime was healthy
- strategy was still intentionally fail-closed
- `discovery_strategy_state.trusted_selection_bootstrap_required = 1`
- `followlist.active = 0`
- latest persisted `wallet_metrics` snapshot existed but was stale for the freshness gate:
  - `2026-03-10T14:00:00+00:00`
  - `29496` rows

The one-shot streamed bootstrap tool had already proven:

- memory/OOM behavior fixed
- wall-clock behavior still not operationally viable on the live DB

So the immediate recovery experiment was:

- clone the latest trusted snapshot into the current expected `metrics_window_start`
- let discovery consume it on the next cycle

### Controlled bridge sequence

1. Stopped the live services for a clean SQLite window.
2. Confirmed source snapshot:
   - latest `wallet_metrics.window_start = 2026-03-10T14:00:00+00:00`
   - source row count = `29496`
3. Computed target bootstrap window:
   - `2026-03-10T21:00:00`
4. Confirmed target bucket did not yet exist.
5. Inserted a cloned target bucket from the latest source snapshot:
   - inserted rows = `29496`

### First bridge attempt and root cause

The first insert used:

- `2026-03-10T21:00:00Z`

That still did **not** clear fail-close.

Root cause:

- discovery loads `MAX(window_start)` from `wallet_metrics`
- parses it into `DateTime<Utc>`
- then re-queries the latest snapshot using `window_start.to_rfc3339()`
- `to_rfc3339()` canonicalizes UTC as `+00:00`

So:

- stored bridge bucket: `2026-03-10T21:00:00Z`
- discovery lookup string: `2026-03-10T21:00:00+00:00`

The exact-string lookup missed the cloned bucket even though it was logically the same timestamp.

### Corrective bridge

The target bucket was then rewritten in-place to canonical UTC RFC3339:

- from `2026-03-10T21:00:00Z`
- to `2026-03-10T21:00:00+00:00`

The app was restarted again immediately after that correction.

### Result

The corrected bridge succeeded.

Observed live state after restart:

- `solana-copy-bot.service` active
- `copybot-adapter.service` active
- `copybot-executor.service` active
- `solana-copy-bot.service MainPID = 304395`
- `NRestarts = 0`
- `ActiveEnterTimestamp = 2026-03-15 21:21:21 UTC`
- `discovery_strategy_state.trusted_selection_bootstrap_required = 0`
- reason = `trusted_selection_bootstrap_satisfied`
- `followlist.active = 15`

Discovery log proof:

- `discovery restored trusted top-N selection from persisted wallet_metrics bootstrap`
- `active_follow_wallets = 15`
- `follow_top_n = 15`
- `trusted_eligible_wallets = 358`
- `trusted_wallets_seen = 29496`
- `metrics_window_start = 2026-03-10 21:00:00 UTC`

Runtime health remained good:

- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `yellowstone_output_queue_fill_ratio = 0.0` in steady-state samples

### Shadow/copy validation

Immediately after recovery:

- `app_follow_rejected_ratio` dropped below `1.0`
- recent telemetry samples showed ratios around `0.9992` to `0.9997`

This confirms the bot is no longer hard fail-closing every swap.

In the short validation window after recovery:

- the latest rows in `copy_signals` were still the older pre-fail-close entries from `2026-03-15 16:19:41+00:00`

So the proof of recovery is:

- trusted bootstrap cleared
- active followlist converged to `15`
- relevance gate reopened for the selected universe

But there was not yet a fresh `copy_signals` row in the short post-restart observation window.

### Verdict

This recovery was successful.

What is now true in live:

1. the bot is no longer strategy-fail-closed
2. the bot is no longer running on the invalid historical `976`-wallet set
3. trusted persisted bootstrap selection restored a real top-`15` active followlist
4. runtime remained healthy throughout the recovery

### Operational note

If this bridge ever needs to be repeated:

- use canonical UTC RFC3339 with `+00:00`
- do **not** use `Z`

The exact-string lookup in current discovery/storage code makes that distinction operationally significant.
