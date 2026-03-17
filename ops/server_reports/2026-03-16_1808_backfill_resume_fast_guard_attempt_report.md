# 2026-03-16 18:08 Kyiv — backfill resume fast-guard attempt report

This was a controlled backfill resume, not aggregate activation.

## Scope

- Server repo updated to Batch 10.1 commit `44a6b4d`
- Main runtime was not restarted
- Main service was not stopped
- Aggregate writes remained disabled
- Aggregate reads remained disabled
- Bootstrap track was not touched

## Server / commit

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact server commit: `44a6b4d89bc2638b133dc70d3da28f6ad7e6e15e`
- Runtime before and after:
  - `MainPID=317246`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`

## Pre-run snapshot

### Runtime

- `MainPID=317246`
- `NRestarts=0`
- queue baseline before run:
  - `yellowstone_output_queue_fill_ratio = 0.0`
  - `yellowstone_output_queue_depth = 0`
  - `ingestion_lag_ms_p95 ≈ 1670`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`

### Aggregate readiness human output

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T16:05:36.633553678+00:00
window_start=2026-03-11T16:05:36.633553678+00:00
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

### Exact persisted resume cursor

- `start_ts = 2026-03-03T17:05:37Z`
- `resume_ts = 2026-03-08T14:01:55.315746972Z`
- `resume_slot = 405047901`
- `resume_signature = 5ZyWHCr3dbnFUVY2qgSwZLA3PoWTEb5UcZvWPPWX8iCCHGcMYECYo4EVqPAdf2GJBqEh18ZW8XX3oC959dad5RKK`

## Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T14:01:55.315746972Z \
  --resume-slot 405047901 \
  --resume-signature 5ZyWHCr3dbnFUVY2qgSwZLA3PoWTEb5UcZvWPPWX8iCCHGcMYECYo4EVqPAdf2GJBqEh18ZW8XX3oC959dad5RKK \
  --mark-covered \
  --abort-on-runtime-pressure \
  --max-yellowstone-fill-ratio 0.20 \
  --max-ingestion-lag-ms-p95 8000 \
  --max-runtime-pressure-sample-age-seconds 35 \
  --abort-on-runtime-infra-stop \
  --batch-size 250 \
  --sleep-ms 150
```

Server-side log file:

- `/tmp/backfill_discovery_scoring_fast_guard_resume_20260316T1606Z.log`

## Duration

- log birth time: `2026-03-16 16:05:56.043567032 +0000`
- log final modify time: `2026-03-16 16:06:08.529571819 +0000`
- observed duration: `~12.49s`

## Result

Status: **aborted safely by fast guard**

Tool output:

```text
event=batch_committed rows=250 total_rows=250 batches=1 cursor_ts=2026-03-08T14:01:56.713958592+00:00 cursor_slot=405047904 cursor_signature=3GRqfEVWFE7V9cvdrrkE9ZkdkFWhnznajMN4L88AGMMGQYPywBpgZQSTFDGTNKDcMeTZPZRPTfJrvPAM32DLvna2
event=runtime_pressure_fast_abort source=journalctl:solana-copy-bot sample_ts=2026-03-16T16:06:08.344848+00:00 reason=yellowstone_output_queue_fill_ratio=0.2891 threshold=0.2000 yellowstone_output_queue_depth=592 yellowstone_output_queue_capacity=2048 yellowstone_output_queue_fill_ratio=0.2891 yellowstone_output_oldest_age_ms=1983 ingestion_lag_ms_p95=2484 max_yellowstone_fill_ratio=0.2000 max_ingestion_lag_ms_p95=8000 max_runtime_pressure_sample_age_seconds=35
Error: runtime pressure fast guard aborted backfill: yellowstone_output_queue_fill_ratio=0.2891 threshold=0.2000
```

## Guard outcome

- fast guard fired: **yes**
- infra-stop guard fired: **no**
- manual `SIGINT` needed: **no**
- manual hard kill needed: **no**

Why fast guard fired:

- fresh runtime sample timestamp: `2026-03-16T16:06:08.344848+00:00`
- sample age was within the configured `35s` budget
- sample values:
  - `yellowstone_output_queue_fill_ratio = 0.2890625`
  - `yellowstone_output_queue_depth = 592 / 2048`
  - `yellowstone_output_oldest_age_ms = 1983`
  - `ingestion_lag_ms_p95 = 2484`
- trigger reason:
  - fill ratio exceeded configured threshold `0.20`
- lag threshold `8000ms` was **not** the trigger

## Runtime metrics

### Before

- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`
- `ingestion_lag_ms_p95 ≈ 1670`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `NRestarts = 0`

### During

At the abort-trigger sample:

- `yellowstone_output_queue_fill_ratio = 0.2890625`
- `yellowstone_output_queue_depth = 592 / 2048`
- `yellowstone_output_oldest_age_ms = 1983`
- `ingestion_lag_ms_p95 = 2484`
- `ingestion_lag_ms_p99 = 3257`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `NRestarts = 0`

### After

Immediate after-state recovered cleanly:

- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`
- `ingestion_lag_ms_p95 = 1763`
- `ingestion_lag_ms_p99 = 1873`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `NRestarts = 0`

## Backfill progress after run

Progress advanced slightly and persisted cleanly:

- previous cursor:
  - `2026-03-08T14:01:55.315746972Z`
  - slot `405047901`
  - signature `5ZyWHCr3dbnFUVY2qgSwZLA3PoWTEb5UcZvWPPWX8iCCHGcMYECYo4EVqPAdf2GJBqEh18ZW8XX3oC959dad5RKK`
- new cursor:
  - `2026-03-08T14:01:56.713958592Z`
  - slot `405047904`
  - signature `3GRqfEVWFE7V9cvdrrkE9ZkdkFWhnznajMN4L88AGMMGQYPywBpgZQSTFDGTNKDcMeTZPZRPTfJrvPAM32DLvna2`

Committed work:

- `rows = 250`
- `batches = 1`

Cleanup state:

- `backfill_protected_since = null`
- `backfill_active = false`
- `backfill_resume_required = true`

## Post-run aggregate readiness status

Human output:

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T16:06:37.878919693+00:00
window_start=2026-03-11T16:06:37.878919693+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T14:01:56.713958592+00:00,slot=405047904,signature=3GRqfEVWFE7V9cvdrrkE9ZkdkFWhnznajMN4L88AGMMGQYPywBpgZQSTFDGTNKDcMeTZPZRPTfJrvPAM32DLvna2)
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

- [2026-03-16_1808_backfill_resume_fast_guard_attempt_report.json](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1808_backfill_resume_fast_guard_attempt_report.json)

## Updated blocker inventory

Ignoring config guardrails, blockers are unchanged:

1. `covered_since_pending_backfill_completion`
2. `covered_through_cursor_pending_backfill_completion`
3. `backfill_resume_required`

Good signal that remains unchanged:

- `materialization_gap_cursor = null`

## Verdict

- completion status: **not completed**
- safety outcome: **aborted safely by fast guard**
- progress advanced: **yes**
- runtime restart: **no**
- aggregate writes ready: **no**
- aggregate reads ready: **no**

## Interpretation

Batch 10.1 did what it was supposed to do:

- the new fast pressure guard fired on live
- it fired before queue saturation
- it avoided the previous manual-`SIGINT` path
- cleanup remained clean

What remains open:

- the backfill can now be resumed safely, but this profile is too conservative to finish meaningfully in one pass
- the next step is a deliberate decision about whether to:
  - tune thresholds/profile upward carefully, or
  - change the backfill/runtime contract again to gain more safe throughput

