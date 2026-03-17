# 2026-03-16 17:00 Kyiv — guarded aggregate backfill resume attempt report

This was a controlled backfill resume, not aggregate activation.

## Scope

- Server repo updated to Batch 9 commit `7a0c9d8`
- Main runtime was not restarted
- Aggregate writes remained disabled
- Aggregate reads remained disabled
- Bootstrap track was not touched

## Server / commit

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact server commit: `7a0c9d82c7314dff293cf48f3686160c97519d83`
- Runtime:
  - `MainPID=317246`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`

## Pre-run state

Pre-run readiness artifact was captured on the same commit before the resume attempt.

Key facts:

- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_active = true`
- `backfill_resume_required = false`
- `coverage_markers_pending_backfill_completion = true`
- persisted resume cursor:
  - `start_ts = 2026-03-03T17:05:37Z`
  - `resume_ts = 2026-03-08T13:57:48.590691231Z`
  - `resume_slot = 405047271`
  - `resume_signature = 5wqgY1r1yz54xT3LHcMuPCY3xhsf7rV4uGiwtZEqjLNkA8CcCNPsPvS7inNKMfwSizgxmDVjqXgtKymtLr4bgeTQ`

## Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T13:57:48.590691231Z \
  --resume-slot 405047271 \
  --resume-signature 5wqgY1r1yz54xT3LHcMuPCY3xhsf7rV4uGiwtZEqjLNkA8CcCNPsPvS7inNKMfwSizgxmDVjqXgtKymtLr4bgeTQ \
  --mark-covered \
  --abort-on-runtime-infra-stop \
  --batch-size 500 \
  --sleep-ms 100
```

Server-side log file:

- `/tmp/backfill_discovery_scoring_guarded_resume_20260316T1457Z.log`

## Duration

- log birth time: `2026-03-16 14:55:10.784000598 +0000`
- log final modify time: `2026-03-16 14:58:30.594087538 +0000`
- observed run duration: `~3m19.81s`

## Result

Status: **aborted manually for runtime safety**

Why:

- the new `--abort-on-runtime-infra-stop` guard was present
- but no fresh `shadow_risk_infra_stop` event was latched during the run
- runtime pressure still crossed the safety line
- the run was therefore interrupted manually with `SIGINT`

Backfill log end state:

- final log line: `event=controlled_abort source=termination_signal`

## Progress achieved before abort

Persisted backfill cursor moved forward to:

- `backfill_progress_start_ts = 2026-03-03T17:05:37+00:00`
- `backfill_progress_cursor_ts = 2026-03-08T14:01:55.315746972+00:00`
- `backfill_progress_cursor_slot = 405047901`
- `backfill_progress_cursor_signature = 5ZyWHCr3dbnFUVY2qgSwZLA3PoWTEb5UcZvWPPWX8iCCHGcMYECYo4EVqPAdf2GJBqEh18ZW8XX3oC959dad5RKK`

Last committed batch observed in the log:

- `rows = 500`
- `total_rows = 49500`
- `batches = 99`
- `cursor_ts = 2026-03-08T14:01:55.315746972+00:00`
- `cursor_slot = 405047901`
- `cursor_signature = 5ZyWHCr3dbnFUVY2qgSwZLA3PoWTEb5UcZvWPPWX8iCCHGcMYECYo4EVqPAdf2GJBqEh18ZW8XX3oC959dad5RKK`

## Runtime impact

### Before

- `NRestarts = 0`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `yellowstone_output_queue_fill_ratio = 0.0`
- `ingestion_lag_ms_p95 ≈ 1724`

### During peak pressure

At the worst observed point:

- `yellowstone_output_queue_fill_ratio = 1.0`
- `yellowstone_output_queue_depth = 2048 / 2048`
- `yellowstone_output_oldest_age_ms = 10138`
- `ingestion_lag_ms_p95 = 13645`
- later sustained at:
  - `yellowstone_output_queue_fill_ratio = 1.0`
  - `ingestion_lag_ms_p95 = 21659`
  - `ingestion_lag_ms_p99 = 22907`
- still:
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### After abort / recovery

Runtime recovered without restart:

- `NRestarts = 0`
- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`
- `ingestion_lag_ms_p95 = 1904`
- `ingestion_lag_ms_p99 = 2140`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`

## Risk-event / guard context

Latest relevant risk event during the checked run window:

- `shadow_risk_infra_cleared` at `2026-03-16T14:54:08.293474329+00:00`

No newer `shadow_risk_infra_stop` event appeared during the guarded run.

Interpretation:

- the new runtime-infra-stop guard did not trigger
- this run did not exercise the new stop-path
- graceful signal cleanup did work

## Post-run aggregate readiness status

Human output:

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T14:59:07.146766375+00:00
window_start=2026-03-11T14:59:07.146766375+00:00
writes_enabled=false
reads_enabled=false
runtime_gate_max_lag_seconds=600
audit_max_lag_buckets=2
audit_max_lag_seconds=3600
covered_since=null
covered_through_ts=null
covered_through_cursor=null
covered_through_lag_seconds=null
materialization_gap_cursor=null
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T14:01:55.315746972+00:00,slot=405047901,signature=5ZyWHCr3dbnFUVY2qgSwZLA3PoWTEb5UcZvWPPWX8iCCHGcMYECYo4EVqPAdf2GJBqEh18ZW8XX3oC959dad5RKK)
backfill_protected_since=null
backfill_active=false
backfill_resume_required=true
coverage_markers_pending_backfill_completion=true
scoring_horizon_covered=false
covered_through_within_runtime_lag=false
covered_through_within_audit_lag=false
storage_ready_for_runtime_gate=false
effective_writes_ready=false
effective_reads_ready=false
write_blockers=[writes_disabled_by_config,covered_through_cursor_pending_backfill_completion,backfill_resume_required]
read_blockers=[reads_disabled_by_config,covered_since_pending_backfill_completion,covered_through_cursor_pending_backfill_completion,backfill_resume_required]
```

JSON artifact:

- [2026-03-16_1700_backfill_resume_guarded_attempt_report.json](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1700_backfill_resume_guarded_attempt_report.json)

## State cleanup verdict

Cleanup state after manual abort is clean:

- `backfill_progress` persisted
- `backfill_protected_since = null`
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

This is materially cleaner than the earlier interrupted run that left protection latched.

## Updated blocker inventory

Ignoring config guardrails, real blockers remain:

1. `covered_since_pending_backfill_completion`
2. `covered_through_cursor_pending_backfill_completion`
3. `backfill_resume_required`

Good signal that remains unchanged:

- `materialization_gap_cursor = null`

## Verdict

- completion status: **not completed**
- safety outcome: **aborted safely with clean cleanup**
- progress persistence: **yes**
- runtime restart: **no**
- aggregate writes ready: **no**
- aggregate reads ready: **no**

## Next required step

The next step is not activation.

It is one of:

1. another controlled resume with an even softer runtime-pressure profile, or
2. a code fix in the backfill/runtime interaction contract, because Batch 9 cleanup worked but the runtime-infra-stop guard still did not abort before queue saturation

