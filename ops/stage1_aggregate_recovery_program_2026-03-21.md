# Stage 1 Aggregate Recovery Program

## Goal

Close the current Stage 1 blocker by moving the primary engineering track off raw replay throughput and onto deterministic aggregate readiness.

This program keeps production fail-closed on raw scoring until aggregate truth is materially ready.

## Preconditions

- Keep live runtime on the current safe state:
  - `discovery.scoring_aggregates_write_enabled = false`
  - `discovery.scoring_aggregates_enabled = false`
- Do not spend more engineering time on raw replay throughput tuning as the main closure path.
- Do not run historical aggregate backfill on the production database first. Start from a clone.

## Operator Tooling

- Bounded backfill runner:
  - [tools/discovery_aggregate_backfill_loop.sh](/Users/tigranambarcumyan/Documents/solana-copy-bot/tools/discovery_aggregate_backfill_loop.sh)
- Runtime-equivalent readiness status:
  - [aggregate_readiness_status.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/discovery/src/bin/aggregate_readiness_status.rs)
- Exact backfill/resume engine:
  - [backfill_discovery_scoring.rs](/Users/tigranambarcumyan/Documents/solana-copy-bot/crates/storage/src/bin/backfill_discovery_scoring.rs)

## Clone Sequence

1. Build the release binaries used by the operator loop.
2. Take a clone of the production SQLite database.
3. Run the bounded loop on the clone with the exact start cursor contract already known from the current partial lineage.
4. Inspect the emitted readiness snapshots after each run.
5. Continue until one of the terminal outcomes:
   - `coverage_marked`
   - `gap_latched`
   - `progress_stalled`
   - `run_failed`

Example shape:

```bash
tools/discovery_aggregate_backfill_loop.sh \
  --config /etc/solana-copy-bot/live.server.toml \
  --db-path /path/to/live_copybot_clone.db \
  --start-ts <historical-start-ts> \
  --mode resume \
  --resume-ts <resume-ts> \
  --resume-slot <resume-slot> \
  --resume-signature <resume-signature> \
  --max-runs 24 \
  --max-runtime-seconds 1800 \
  --batch-size 10000 \
  --sleep-ms 0 \
  --report-dir /tmp/aggregate-backfill-loop
```

For a seeded-boundary recovery, use `--mode seeded-reset` only on the first run. The wrapper chains later iterations from the persisted exact resume cursor.

## Clone Acceptance

The clone phase is only considered successful when all of the following are true:

- `covered_since != null`
- `covered_through_cursor != null`
- `materialization_gap_cursor = null`
- `backfill_resume_required = false`

`effective_reads_ready = false` is acceptable at this stage if the clone is not near head.

## Production Sequence

1. Keep both aggregate flags `false`.
2. Run the same bounded loop against production only after the clone has proven deterministic completion semantics.
3. Finish the historical phase and mark coverage.
4. Run a short production tail catch-up until `covered_through_cursor` approaches head.
5. Enable aggregate writes and watch writer pressure.
6. Enable aggregate reads only after the runtime-equivalent readiness report shows `effective_reads_ready = true`.

## Hard Stop Conditions

Stop and investigate immediately if any of the following appear:

- `materialization_gap_cursor != null`
- `backfill_resume_required = true` with no cursor advancement between runs
- SQLite pressure escalates into writer backlog or restart instability
- any signal of false healthy / early publication

## Invariants

- exact carry-forward
- no false healthy
- fail-closed until truth is ready
- no writer death / no restart loop
