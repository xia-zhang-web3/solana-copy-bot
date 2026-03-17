# Aggregate Recovery Audit Synthesis

Status: Active decision document for current recovery hotfix/fix ordering

Role: Audit synthesis and next-fix prioritization for aggregate recovery/completion.

This is not the full active roadmap.
Primary roadmap remains:
- `STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md`

This document is the source-of-truth for:
- current audit verdict
- P0/P1 recovery fix ordering
- what should and should not be retried on the server right now

Date: 2026-03-17

## Purpose

This document consolidates two independent audits of the current aggregate recovery/completion path after `Batch 12.1` and `Batch 13`.

The goal is not to restate the full project history. The goal is to answer:

1. What is already sound.
2. What is still broken.
3. What the next fix/hotfix should be.

## Inputs

This synthesis is based on:

- Auditor 1 findings:
  - split-commit crash-safety hole in exact recovery
  - write/apply path as the dominant operational bottleneck
  - plain horizon reset correctly rejected
  - seeded exact boundary contract mostly sound in its declared no-carryover scope
- Auditor 2 findings:
  - seeded boundary contract sound in normal uninterrupted replay
  - builder replay semantically aligned enough with current row-by-row replay on representative coverage
  - current builder validation run is real but incomplete
  - missed simpler path candidate: lot-only boundary fast-forward

## Shared Conclusions

Both audits independently converge on the following:

1. `plain horizon reset` is not the right production answer under the current exact-lineage contract.
2. The `seeded exact boundary` direction is the correct semantic narrowing path in the current declared `no-carryover` scope.
3. The `builder replay` direction is broadly correct and materially faster than the old SQL replay path for normal uninterrupted replay, but that does not imply crash-safe resume semantics.
4. The first server-side seeded-horizon builder run was useful, but it did not prove end-to-end completion:
   - it stayed in `boundary_build`
   - it never reached `seed_boundary_exported`
   - it never reached `seed_boundary_installed`
5. The real blocker is no longer bootstrap/control-plane logic. It is aggregate recovery/completion.

## Where The Audits Add Different Value

### Auditor 1: P0 Semantic Durability Hole

Auditor 1 identified the highest-severity issue, and this is confirmed by the current code path:

- batch materialization commits before `backfill_progress`
- these are two separate durable operations
- if the process crashes between them, tables move forward but resume checkpoint stays behind
- replay can then re-apply the same rows after restart
- `close_facts` may dedupe via `INSERT OR IGNORE`, but lot state and day/minute aggregates continue mutating, so this is not true idempotency

This is not a throughput issue. This is a semantic recovery issue.

The current exact recovery path is therefore not crash-safe enough to be a production recovery answer.

### Auditor 2: Strong Candidate To Cut Boundary Waste

Auditor 2 identified a simpler optimization target:

- current `boundary_build` does full builder replay work
- but everything except final `open_lots` state is discarded at `seed install`
- therefore `boundary_build` is doing expensive writes and aggregate work that are thrown away

Suggested narrower path:

- add a lot-only boundary fast-forward mode
- replay only buy/sell lot accounting from current cursor to boundary
- do not write days / tx_minutes / buy_facts / close_facts during boundary build
- export exact `open_lots` boundary snapshot

This does not appear to change the contract. It changes only how the boundary snapshot is reached.

However, this is still a candidate path, not a proven full answer:

- it should make `seed install` reachable faster
- it should cut wasted pre-seed writes
- it does not by itself make readiness `true`
- it does not by itself prove that post-seed replay cost is acceptable

## Consolidated Verdict

The current architecture is not "dead", but the current recovery path is still not acceptable as-is.

### What is already sound

- `plain reset` rejection
- `seeded exact boundary` as the semantic narrowing direction in the declared `no-carryover` scope
- `builder replay` as a real and faster replay engine for uninterrupted replay

### What is still not sound enough

- crash safety of `apply batch` vs `backfill_progress` durability
- treating readiness metadata as if it proved semantic coherence of materialized tables

### What is still too expensive

- `boundary_build` doing full write-heavy builder work just to throw it away at `seed install`
- possible next bottleneck to measure after `P1`: post-seed `apply_ms` if the same write amplification remains dominant after boundary narrowing

## Priority Order

### P0 Hotfix

Make exact recovery crash-safe.

Accepted shapes:

1. make batch apply + resume checkpoint one atomic durable unit
2. introduce a batch journal / idempotent replay ledger that makes post-crash resume exact

Minimum acceptable outcome:

- after any crash between "rows materialized" and "progress advanced", restart must not double-apply the same logical batch

Reason for priority:

- without this, throughput work improves speed on top of a semantically unsafe recovery path

### P1 Next Fix

Add `lot-only boundary build` for seeded mode as the strongest current candidate to cut wasted pre-seed work.

Goal:

- reach exact boundary `open_lots` much faster
- avoid writing intermediate aggregate tables during `boundary_build`
- preserve the exact seeded boundary contract

Non-goal:

- this does not unlock readiness by itself
- this does not replace post-seed replay

Expected effect:

- substantial reduction in `boundary_build` wall-clock time
- `seed install` becomes reachable without spending most of the run budget on discarded work
- after `seed install`, the system still needs exact replay forward to clear `covered_since` / `covered_through` blockers

### P2 Follow-Up

After P0 + P1:

1. re-measure full seeded run
2. if still too slow, optimize:
   - post-seed `apply_ms`
   - bulk flush path
   - SQLite write amplification
   - `rug finalize`
3. add first-class checkpoints after meaningful milestones, especially after `seed install`
4. add an explicit operator mode such as `stop-after-seed-install`, so exact scope reduction becomes a first-class bounded milestone instead of an implicit side effect

## Operational Disposition Of Existing State

Pre-`P0` materialized aggregate state should be treated as potentially untrusted.

Reason:

- the current split-commit recovery hole means an interrupted run may already have advanced tables beyond the persisted resume checkpoint
- readiness metadata does not prove that this did not happen

Default safe plan after shipping `P0`:

1. reset aggregate materialization
2. rerun exact recovery on the crash-safe path

Optional follow-up, but not a prerequisite for `P0`:

- add a validator / integrity audit for already-materialized pre-fix state if preserving old state is operationally important

## What Should Not Be Done Next

These paths now look like dead-ends:

- more online bounded replay tuning
- going back to `plain horizon reset`
- assuming readiness alone proves semantic coherence
- repeating long server runs before fixing crash safety
- repeating long server runs before cutting `boundary_build` waste
- treating `seed install` or `boundary reached` as equivalent to readiness unlock
- assuming `lot-only boundary build` alone solves the whole completion path

## Recommended Immediate Next Track

If only one thing can be done next, it should be:

`P0: crash-safe exact recovery`

If a second tightly scoped track can run immediately after that, it should be:

`P1: lot-only boundary build`

## Practical Recommendation

Server-side experimentation should be on hold until the next code change is made.

Recommended next implementation order:

1. hotfix crash-safe recovery durability
2. implement lot-only boundary fast-forward
3. rerun seeded-horizon validation
4. only then decide whether additional `apply_ms` / `rug finalize` optimization is needed

## Coder-Ready Work Order

### Task 1: Make exact replay crash-safe

Target outcome:

- one logical batch cannot be applied twice after restart

Implementation shape:

1. move `batch apply` and `backfill_progress` advance into one atomic durable unit
2. if that is not practical, introduce a batch journal / replay ledger that makes restarts idempotent
3. keep the current fail-closed behavior if the durable checkpoint cannot be advanced safely

Acceptance:

- crash or forced stop between materialization and checkpoint must not change final lots, close facts, day aggregates, or tx minute counts after resume
- in seeded mode, crash before committed `seed install` must leave the pre-seed lineage intact, and crash after committed `seed install` must resume from the narrowed exact boundary cursor without mixed pre-seed / post-seed state

### Task 2: Add lot-only boundary build

Target outcome:

- seeded mode can reach `seed_boundary_installed` without paying full aggregate write cost during `boundary_build`

Implementation shape:

1. scan swaps from current cursor to seeded boundary
2. mutate only in-memory lot state needed to produce exact boundary `open_lots`
3. skip intermediate writes for `buy_facts`, `close_facts`, `wallet_scoring_days`, and `wallet_scoring_tx_minutes`
4. export and install the exact boundary snapshot as today

Acceptance:

- for the same input fixture, lot-only boundary build must produce the same boundary `open_lots` snapshot as full builder boundary replay
- for the same seeded fixture, `lot-only boundary build -> seed install -> replay_after_seed` must produce the same final `open_lots`, `buy_facts`, `close_facts`, `wallet_scoring_days`, and `wallet_scoring_tx_minutes` as the current full builder seeded path

### Task 3: Make seed install a first-class bounded milestone

Target outcome:

- operators can intentionally stop after exact scope reduction and resume later without ambiguous semantics

Implementation shape:

1. add explicit `stop-after-seed-install` or equivalent mode
2. emit clear logs for `seed_boundary_exported` and `seed_boundary_installed`
3. document that this narrows replay scope but does not mark readiness complete

Acceptance:

- after `seed install`, restart should resume from the narrowed exact boundary cursor and preserve the same final result as a single uninterrupted seeded run
- `seed_boundary_exported` is not treated as a durable recovery milestone; only committed `seed_boundary_installed` is allowed to change restart semantics
- there must be no durable mixed state where aggregate tables have been reset/seeded but persisted replay lineage still points to the old pre-seed progress

## Short Final Answer

The aggregate direction is salvageable, but not by operator tuning.

The next real answer is not "wait longer" and not "change a flag".

The next real answer is:

1. make recovery crash-safe
2. stop wasting `boundary_build` work on data that is deleted at seed install

That is the cleanest synthesis of the two independent audits.
