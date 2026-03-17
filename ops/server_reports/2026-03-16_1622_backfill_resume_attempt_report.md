# 2026-03-16 16:22 Kyiv — aggregate backfill resume attempt report

Это backfill/coverage completion step, не activation rollout.

## Scope

- Server repo stayed on `f97e879`
- Runtime/service was not restarted
- Aggregate writes/reads remained disabled
- Controlled resume attempt was executed from persisted backfill cursor with `--mark-covered`

## Server / runtime

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact commit: `f97e879128fb20491139d8cd1d9a7f6459d9dd21`
- Runtime during attempt:
  - `MainPID=317246`
  - `NRestarts=0`

## Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-03T17:50:26.766980955Z \
  --resume-slot 403986679 \
  --resume-signature 3QaUt6PeXEd8Pktn47GiYybQgCwnxD5gp6DfUZR7pBegmWw3zR5RYH1xEeXVxYUDsLtq7hxvEBdkDdzFSFHVgtmB \
  --mark-covered \
  --batch-size 2000 \
  --sleep-ms 10
```

Server-side log file:

- `/tmp/backfill_discovery_scoring_resume_20260316T1418Z.log`

## Result

Status: **aborted intentionally due runtime pressure**

The run was stopped with `Ctrl-C` after runtime pressure crossed the acceptable line.

## Backfill progress achieved before abort

Observed last committed batch:

- `total_rows = 60000`
- `batches = 30`
- `cursor_ts = 2026-03-08T13:57:48.590691231+00:00`
- `cursor_slot = 405047271`
- `cursor_signature = 5wqgY1r1yz54xT3LHcMuPCY3xhsf7rV4uGiwtZEqjLNkA8CcCNPsPvS7inNKMfwSizgxmDVjqXgtKymtLr4bgeTQ`

Persisted state after abort confirms the same progress:

- `backfill_progress_start_ts = 2026-03-03T17:05:37+00:00`
- `backfill_progress_cursor_ts = 2026-03-08T13:57:48.590691231+00:00`
- `backfill_progress_cursor_slot = 405047271`
- `backfill_progress_cursor_signature = 5wqgY1r1yz54xT3LHcMuPCY3xhsf7rV4uGiwtZEqjLNkA8CcCNPsPvS7inNKMfwSizgxmDVjqXgtKymtLr4bgeTQ`

## Runtime impact during run

Healthy signals that stayed intact:

- `NRestarts = 0`
- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`

But runtime pressure became too high:

- `yellowstone_output_queue_fill_ratio` rose from `0.46` to `0.69`
- then hit `1.0` at maintenance gate check
- maintenance log:
  - `observed swap retention blocked by runtime health gate`
- later pipeline sample still showed:
  - `yellowstone_output_queue_fill_ratio = 0.95556640625`
  - `yellowstone_output_queue_depth = 1957`
  - `ingestion_lag_ms_p95 = 14187`

This is why the backfill was stopped.

## Post-abort recovery

Runtime recovered after abort:

- queue drained back to `yellowstone_output_queue_fill_ratio = 0.0`
- `NRestarts` remained `0`
- no busy/retry spikes appeared

## Post-run aggregate readiness status

Human output:

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T14:22:16.118690180+00:00
window_start=2026-03-11T14:22:16.118690180+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T13:57:48.590691231+00:00,slot=405047271,signature=5wqgY1r1yz54xT3LHcMuPCY3xhsf7rV4uGiwtZEqjLNkA8CcCNPsPvS7inNKMfwSizgxmDVjqXgtKymtLr4bgeTQ)
backfill_protected_since=2026-03-03T17:05:37+00:00
backfill_active=true
backfill_resume_required=false
coverage_markers_pending_backfill_completion=true
scoring_horizon_covered=false
covered_through_within_runtime_lag=false
covered_through_within_audit_lag=false
storage_ready_for_runtime_gate=false
effective_writes_ready=false
effective_reads_ready=false
write_blockers=[writes_disabled_by_config,covered_through_cursor_pending_backfill_completion,backfill_in_progress]
read_blockers=[reads_disabled_by_config,covered_since_pending_backfill_completion,covered_through_cursor_pending_backfill_completion,backfill_in_progress]
```

JSON artifact:

- [2026-03-16_1622_backfill_resume_attempt_report.json](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1622_backfill_resume_attempt_report.json)

## Interpretation

What changed:

- persisted backfill cursor advanced substantially
- `backfill_resume_required` is now `false`
- `backfill_active` is now `true`
- `backfill_protected_since` is set

Why this happened:

- the interrupted run left source-protection latched in SQLite
- the process was no longer running, but the protection state remained persisted because the normal cleanup path did not execute after `Ctrl-C`

What did **not** change:

- `covered_since` is still `null`
- `covered_through_cursor` is still `null`
- readiness is still **not** sufficient for aggregate writes or reads

## Updated blocker inventory

Ignoring config guardrails, the remaining real blockers are still:

1. `covered_since_pending_backfill_completion`
2. `covered_through_cursor_pending_backfill_completion`
3. effective backfill completion has still not happened

Operational nuance after abort:

- tool now reports `backfill_in_progress` rather than `backfill_resume_required`
- on live this is currently driven by persisted source-protection state after the interrupted run

## Verdict

- Completion status: **not completed**
- Runtime impact: **too high for this parameter set**
- Aggregate writes ready: **no**
- Aggregate reads ready: **no**

## Next step

The next step should be chosen explicitly before another server run:

1. either resume again with a much more conservative throttling profile
2. or adjust the backfill/runtime interaction contract so partial aggregate completion can proceed without driving Yellowstone queue pressure into the danger zone

What should not happen next:

- do not enable aggregate writes
- do not enable aggregate reads
- do not treat current shadow PnL as strategy-valid
