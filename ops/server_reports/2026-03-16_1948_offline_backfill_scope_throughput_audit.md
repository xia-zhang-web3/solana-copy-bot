# 2026-03-16 19:48 Kyiv — offline backfill scope / throughput audit

This was a decision-support audit, not a backfill run and not aggregate activation.

## Scope

- No new backfill run was started for this audit
- No `--reset` was executed
- No DB state was intentionally mutated
- Aggregate writes remained disabled
- Aggregate reads remained disabled
- Runtime was not restarted for the audit

Current live stayed on:
- server commit: `630689c4417e0ba2162d19edd648e75553538ae4`
- `solana-copy-bot.service`: `active`
- `MainPID=333267`
- `NRestarts=0`

## Inputs used

- last offline run report:
  - [2026-03-16_1740_offline_aggregate_backfill_completion_report.md](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1740_offline_aggregate_backfill_completion_report.md#L1)
- last offline raw log:
  - [2026-03-16_1740_offline_aggregate_backfill_completion.raw.log](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1740_offline_aggregate_backfill_completion.raw.log)
- current live aggregate readiness status
- live config values:
  - [live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L68)
  - [live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L83)
- code/contract references:
  - [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L741)
  - [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L752)
  - [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L903)
  - [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L926)
  - [discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/discovery_scoring.rs#L496)
  - [STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md](/Users/tigranambarcumyan/Documents/solana-copy-bot/STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md#L245)

## 1. Exact throughput facts from the last offline run

These numbers use the logged offline run only, not the earlier stray wrapper attempt.

- wall-clock duration: `1804.90s` = `30m04.90s`
- rows processed: `300000`
- batches processed: `30`
- cursor before:
  - `2026-03-08T14:03:25.621028942Z`
  - slot `405048130`
  - sig `38Gt6sMAF3sGmmdWV9mdiBqvwy7YN34DJjM9WZboueeest1CCm6GKP6etMzwbA1DCD7yHM4Q3h9TKsXoJQk1tBaJ`
- cursor after:
  - `2026-03-08T14:31:20.243478746Z`
  - slot `405052391`
  - sig `2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW`
- cursor advance:
  - `1674.62245s`
  - `27m54.62245s`
  - `0.46517 cursor-hours`

Derived throughput:
- rows/sec: `166.2142`
- rows/hour: `598371.10`
- cursor-seconds/hour: `3340.1523`
- cursor-hours/hour: `0.927820`

Interpretation:
- offline path is materially faster than the online bounded path
- but it is still advancing cursor-time slightly slower than real time: about `0.93 cursor-hours` per `1 wall-clock hour`

## 2. Exact persisted backfill scope

Current persisted backfill state from live:
- `backfill_progress.start_ts = 2026-03-03T17:05:37Z`
- current `resume_ts = 2026-03-08T14:31:20.243478746Z`
- current `resume_slot = 405052391`
- current `resume_signature = 2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW`

Current readiness state:
- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

## 3. Current scoring target context

Live config:
- `scoring_window_days = 2` in [live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L68)
- `metric_snapshot_interval_seconds = 1800` in [live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L83)

Current live readiness snapshot reported:
- `now = 2026-03-16T17:47:52.910156785Z`
- current effective scoring horizon start:
  - `window_start = 2026-03-11T17:47:52.910156785Z`

How far current scope extends left of the needed horizon:
- current `start_ts` is earlier than the current effective horizon by:
  - `693735.910156s`
  - `192.7044h`
  - about `8d 0h 42m 15.910s`

This is the main “scope drag” fact:
- the current backfill start is almost `8 days` earlier than the minimum horizon currently needed for readiness

## 4. Remaining gap estimates

### Gap from current cursor to scoring horizon

Current cursor to current effective horizon:
- `270992.666678s`
- `75.2757h`
- about `3d 3h 16m 32.667s`

At the last offline run throughput, rough estimates are:
- remaining rows to horizon: `~48.55M`
- remaining wall-clock to horizon: `~81.13h`

### Gap from current cursor to current head

This matters because coverage markers are only written on `full_forward_completion`, not when the cursor merely reaches the scoring horizon:
- full forward completion is required before `covered_since` / `covered_through_cursor` are written in [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L903) and [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L926)

Current cursor to current head (`now`) is:
- `195.2757h`

At the last offline throughput, rough estimate to full forward completion on the current scope is:
- remaining rows to head: `~125.94M`
- remaining wall-clock to completion: `~210.47h`

Interpretation:
- current offline tranche path is not just “a bit slow”
- on current scope it projects to roughly `8.8 days` of wall-clock work to reach full forward completion at the observed throughput

## 5. Reset-candidate assessment

### Candidate start_ts

If reset were considered for code-level review, the conservative candidate would be:
- `2026-03-11T17:30:00Z`

Why this candidate:
- it is bucket-aligned to `metric_snapshot_interval_seconds = 1800`
- it is `<= current effective scoring horizon start (2026-03-11T17:47:52.910156785Z)`
- it avoids using a moving, subsecond `now - 5d` boundary as a rebuild anchor

This candidate is earlier than the dynamic horizon by:
- `17m52.910156785s`

### What this reset candidate would change

Relative to the current `start_ts = 2026-03-03T17:05:37Z`, this candidate would drop:
- `192.4064h`
- about `7d 23h 24m 23s`
of earlier backfill scope

If the system rebuilt from that candidate to current head at the same offline throughput, rough wall-clock would still be:
- candidate reset to head gap: `120.2980h`
- estimated wall-clock from candidate to head: `~129.66h`

Interpretation:
- horizon-scoped reset is a real scope reduction
- but even that reduced path is still very expensive at the currently observed offline throughput

## 6. Contract / risk note on reset

Reset is not just an operational shortcut. It changes the lineage and readiness contract.

Relevant facts:
- resumed backfill requires persisted continuous lineage from `start_ts`, otherwise the tool explicitly demands `--reset` in [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L747)
- `--reset` wipes discovery-scoring tables and starts a new rebuild in [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L741)
- when coverage is finally marked, `covered_since` is written from `config.start_ts` itself in [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs#L928)
- Phase 2 of the steady-state plan explicitly treats this as an exact-cursor continuity-repair track and requires `covered_since_ts` to reach at least the scoring horizon start in [STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md](/Users/tigranambarcumyan/Documents/solana-copy-bot/STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md#L253)

Risk implications:
- a horizon-scoped reset may be mathematically sufficient for `covered_since <= now - scoring_window_days`
- but it changes the provenance/lineage boundary that readiness will later certify
- it may also change cross-boundary continuity assumptions for aggregate facts derived around the reset edge
- rug facts use a short configured lookahead (`rug_lookahead_seconds = 900`) in [live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L82) and [discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/discovery_scoring.rs#L496), but that alone does **not** prove a horizon reset is semantically safe for all continuity-sensitive aggregate facts

Conclusion on risk:
- `reset to horizon` is only a **candidate for code-level review**
- it is **not** safe to execute from this audit alone

## 7. Recommendation

Primary recommendation:
- **reset-candidate is worth code-level review**

Why:
- continuing blind offline tranches on the current scope projects to `~210.47h` wall-clock for full completion
- the current scope still includes about `192.70h` of history earlier than the minimum current scoring horizon
- current cursor is still `~75.28h` short of even reaching the useful horizon boundary

But the recommendation is conditional, not unconditional:
- if a horizon-scoped reset is rejected on continuity/readiness grounds, then the current throughput is too poor for practical completion on the existing scope and the next answer is architectural change, not more blind tranches
- if a horizon-scoped reset is accepted, it still only reduces the projected completion path to about `~129.66h` wall-clock at the currently observed throughput

So the exact decision-support takeaway is:
- **do not just continue offline tranches by inertia**
- **review a bucket-aligned horizon reset candidate (`2026-03-11T17:30:00Z`) at code/contract level**
- **if that review says no, treat current full-scope throughput as operationally inadequate**

## Final verdict

- continue offline tranches on current full scope: **no**
- reset-candidate worth code-level review: **yes**
- throughput on current full scope is already poor enough that, if reset is rejected, architectural change becomes the more honest next path: **yes**
