# 2026-03-16 18:35 Kyiv — bounded backfill progress attempt report

This was a controlled backfill resume, not aggregate activation.

## Scope

- Server repo updated to Batch 11.1 commit `630689c`
- Main runtime was not restarted
- Main service was not stopped
- Aggregate writes remained disabled
- Aggregate reads remained disabled
- This run tested the bounded-progress contract only

## Server / commit

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact server commit: `630689c4417e0ba2162d19edd648e75553538ae4`
- Runtime before and after:
  - `MainPID=317246`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`

## Pre-run snapshot

### Runtime

- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`
- `ingestion_lag_ms_p95 ≈ 1744`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`

### Aggregate readiness human output

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T16:34:43.761918720+00:00
window_start=2026-03-11T16:34:43.761918720+00:00
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

### Exact persisted resume cursor

- `start_ts = 2026-03-03T17:05:37Z`
- `resume_ts = 2026-03-08T14:01:56.713958592Z`
- `resume_slot = 405047904`
- `resume_signature = 3GRqfEVWFE7V9cvdrrkE9ZkdkFWhnznajMN4L88AGMMGQYPywBpgZQSTFDGTNKDcMeTZPZRPTfJrvPAM32DLvna2`

## Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T14:01:56.713958592Z \
  --resume-slot 405047904 \
  --resume-signature 3GRqfEVWFE7V9cvdrrkE9ZkdkFWhnznajMN4L88AGMMGQYPywBpgZQSTFDGTNKDcMeTZPZRPTfJrvPAM32DLvna2 \
  --mark-covered \
  --abort-on-runtime-pressure \
  --max-yellowstone-fill-ratio 0.20 \
  --max-ingestion-lag-ms-p95 8000 \
  --max-runtime-pressure-sample-age-seconds 35 \
  --abort-on-runtime-infra-stop \
  --batch-size 250 \
  --sleep-ms 150 \
  --max-batches-per-run 1
```

Server-side log file:

- `/tmp/backfill_discovery_scoring_bounded_resume_20260316T1635Z.log`

## Duration

- log birth time: `2026-03-16 16:35:01.455201044 +0000`
- log final modify time: `2026-03-16 16:35:13.181205220 +0000`
- observed duration: `~11.73s`

## Result

Status: **completed one bounded slice and stopped due to batch budget**

Tool output:

```text
event=batch_committed rows=250 total_rows=250 batches=1 cursor_ts=2026-03-08T14:01:58.209012788+00:00 cursor_slot=405047908 cursor_signature=2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx
event=coverage_not_marked reason=completion_required
event=wal_checkpoint busy=1 log_frames=-1 checkpointed_frames=-1
summary outcome=stopped_due_to_batch_budget stop_reason=stopped_due_to_batch_budget coverage_marked=false total_rows=250 batches=1 final_cursor_ts=2026-03-08T14:01:58.209012788+00:00 final_cursor_slot=405047908 final_cursor_signature=2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx
```

## Contract checks

What this run proves:

- tool executed exactly one bounded slice
- cursor advanced and persisted
- stop reason was `stopped_due_to_batch_budget`
- stop was not caused by fast guard
- stop was not caused by infra-stop guard
- cleanup completed normally
- runtime stayed healthy

Guard outcomes:

- fast guard fired: **no**
- infra-stop guard fired: **no**
- manual `SIGINT` needed: **no**
- manual hard kill needed: **no**

## Runtime metrics

### Before

- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`
- `ingestion_lag_ms_p95 ≈ 1744`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `NRestarts = 0`

### During

No unsafe pressure developed.

Closest observed runtime sample after the bounded slice:

- `yellowstone_output_queue_fill_ratio = 0.16748046875`
- `yellowstone_output_queue_depth = 343 / 2048`
- `yellowstone_output_oldest_age_ms = 1926`
- `ingestion_lag_ms_p95 = 5386`
- `ingestion_lag_ms_p99 = 5749`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `NRestarts = 0`

This stayed below the configured fast-guard thresholds:

- max fill ratio `0.20`
- max lag p95 `8000`

### After

- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`
- `ingestion_lag_ms_p95 ≈ 1744`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `NRestarts = 0`

## Progress after run

Progress advanced and persisted:

- previous cursor:
  - `2026-03-08T14:01:56.713958592Z`
  - slot `405047904`
  - signature `3GRqfEVWFE7V9cvdrrkE9ZkdkFWhnznajMN4L88AGMMGQYPywBpgZQSTFDGTNKDcMeTZPZRPTfJrvPAM32DLvna2`
- new cursor:
  - `2026-03-08T14:01:58.209012788Z`
  - slot `405047908`
  - signature `2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx`

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
now=2026-03-16T16:35:33.225959536+00:00
window_start=2026-03-11T16:35:33.225959536+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T14:01:58.209012788+00:00,slot=405047908,signature=2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx)
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

- [2026-03-16_1835_backfill_bounded_progress_attempt_report.json](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1835_backfill_bounded_progress_attempt_report.json)

## Updated blocker inventory

Ignoring config guardrails, blockers remain:

1. `covered_since_pending_backfill_completion`
2. `covered_through_cursor_pending_backfill_completion`
3. `backfill_resume_required`

Good signal unchanged:

- `materialization_gap_cursor = null`

## Verdict

- completion status: **not completed**
- bounded-progress contract: **confirmed on live**
- stop reason: **batch budget**
- progress advanced: **yes**
- cleanup clean: **yes**
- runtime restart: **no**
- aggregate writes ready: **no**
- aggregate reads ready: **no**

## Interpretation

Batch 11.1 proved the intended bounded-run semantics on live:

- one run can advance progress by a small exact slice
- stop reason is explicit and non-incident
- no manual intervention is required
- runtime remains healthy

The next question is no longer whether bounded progress works. It is how aggressively the slice size and cadence can be increased while preserving this clean contract.

