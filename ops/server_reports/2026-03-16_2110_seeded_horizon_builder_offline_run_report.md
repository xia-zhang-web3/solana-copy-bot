# 2026-03-16 21:10 UTC Seeded Horizon Builder Offline Run

This was an offline seeded horizon validation run, not aggregate activation.

## Scope

- Host: `ubuntu@52.28.0.218`
- Server repo: `/var/www/solana-copy-bot`
- DB: `/var/www/solana-copy-bot/state/live_copybot.db`
- Config: `/etc/solana-copy-bot/live.server.toml`
- Server commit: `ab5debd835ea631408fce9b6d1112c33f940266f`
- Runtime before run:
  - `solana-copy-bot.service`: `active`
  - `MainPID=333267`
  - `NRestarts=0`
- Chosen seeded boundary `start_ts`: `2026-03-11T21:09:02.243949701+00:00`
- Current persisted cursor before run:
  - `start_ts=2026-03-03T17:05:37Z`
  - `resume_ts=2026-03-08T14:31:20.243478746+00:00`
  - `resume_slot=405052391`
  - `resume_signature=2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW`

## Exact Command

```bash
/var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-11T21:09:02.243949701+00:00 \
  --resume-ts 2026-03-08T14:31:20.243478746+00:00 \
  --resume-slot 405052391 \
  --resume-signature 2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW \
  --seeded-reset \
  --mark-covered \
  --batch-size 10000 \
  --sleep-ms 0 \
  --max-runtime-seconds 7200
```

## Pre-Run Aggregate Readiness

### Human

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T21:09:02.243949701+00:00
window_start=2026-03-11T21:09:02.243949701+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T14:31:20.243478746+00:00,slot=405052391,signature=2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW)
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

### JSON

```json
{
  "config_path": "/etc/solana-copy-bot/live.server.toml",
  "db_path": "/var/www/solana-copy-bot/state/live_copybot.db",
  "now": "2026-03-16T21:08:54.215069202Z",
  "window_start": "2026-03-11T21:08:54.215069202Z",
  "writes_enabled": false,
  "reads_enabled": false,
  "runtime_gate_max_lag_seconds": 600,
  "audit_max_lag_buckets": 2,
  "audit_max_lag_seconds": 3600,
  "covered_since": null,
  "covered_through_ts": null,
  "covered_through_cursor": null,
  "covered_through_lag_seconds": null,
  "materialization_gap_cursor": null,
  "backfill_progress": {
    "start_ts": "2026-03-03T17:05:37Z",
    "cursor": {
      "ts_utc": "2026-03-08T14:31:20.243478746Z",
      "slot": 405052391,
      "signature": "2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW"
    }
  },
  "backfill_protected_since": null,
  "backfill_active": false,
  "backfill_resume_required": true,
  "coverage_markers_pending_backfill_completion": true,
  "scoring_horizon_covered": false,
  "covered_through_within_runtime_lag": false,
  "covered_through_within_audit_lag": false,
  "storage_ready_for_runtime_gate": false,
  "effective_writes_ready": false,
  "effective_reads_ready": false,
  "write_blockers": [
    "writes_disabled_by_config",
    "covered_through_cursor_pending_backfill_completion",
    "backfill_resume_required"
  ],
  "read_blockers": [
    "reads_disabled_by_config",
    "covered_since_pending_backfill_completion",
    "covered_through_cursor_pending_backfill_completion",
    "backfill_resume_required"
  ]
}
```

## Run Outcome

- Runtime was stopped cleanly before run.
- Builder path was confirmed:
  - `phase=boundary_build`
  - `replay_engine=builder`
- Final summary:
  - `outcome=stopped_due_to_runtime_budget`
  - `stop_reason=stopped_due_to_runtime_budget`
  - `coverage_marked=false`
- Wall-clock duration: `7203.24s`
- Rows processed: `3,430,000`
- Batches processed: `343`
- Cursor before:
  - `2026-03-08T14:31:20.243478746+00:00`
  - slot `405052391`
  - sig `2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW`
- Cursor after:
  - `2026-03-08T21:06:45.726139749+00:00`
  - slot `405112624`
  - sig `2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d`
- Cursor advance:
  - `23725.482661s`
  - `6.590411850h`

## Throughput

- `rows/sec = 476.174610`
- `rows/hour = 1714228.597131`
- `cursor-hours/hour = 3.293723749`

## Stage Timing Summary

From final tool summary:

- `scan_ms=166488`
- `prepare_ms=195232`
- `apply_ms=6801821`
- `rug_finalize_ms=0`
- `progress_update_ms=958`

Observations:

- `apply_ms` dominated total cost.
- The run never exited `boundary_build`.
- No `seed_boundary_exported`, `seed_boundary_installed`, or `phase=replay_after_seed` events were emitted.
- `seed install` was **not** reached.

## WAL / Finalization

- No tool-emitted `final_rug_finalize` or tool-emitted `wal_checkpoint` event was present in the raw log.
- This is consistent with a stop during `boundary_build` before seed install / post-seed finalization.
- Manual post-run checkpoint before restarting the service:

```text
PRAGMA wal_checkpoint(TRUNCATE);
0|0|0
```

- Result: clean checkpoint, `busy=0`.

## Post-Run Aggregate Readiness

### Human

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T23:10:48.059078034+00:00
window_start=2026-03-11T23:10:48.059078034+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T21:06:45.726139749+00:00,slot=405112624,signature=2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d)
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

### JSON

```json
{
  "config_path": "/etc/solana-copy-bot/live.server.toml",
  "db_path": "/var/www/solana-copy-bot/state/live_copybot.db",
  "now": "2026-03-16T23:10:48.065192641Z",
  "window_start": "2026-03-11T23:10:48.065192641Z",
  "writes_enabled": false,
  "reads_enabled": false,
  "runtime_gate_max_lag_seconds": 600,
  "audit_max_lag_buckets": 2,
  "audit_max_lag_seconds": 3600,
  "covered_since": null,
  "covered_through_ts": null,
  "covered_through_cursor": null,
  "covered_through_lag_seconds": null,
  "materialization_gap_cursor": null,
  "backfill_progress": {
    "start_ts": "2026-03-03T17:05:37Z",
    "cursor": {
      "ts_utc": "2026-03-08T21:06:45.726139749Z",
      "slot": 405112624,
      "signature": "2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d"
    }
  },
  "backfill_protected_since": null,
  "backfill_active": false,
  "backfill_resume_required": true,
  "coverage_markers_pending_backfill_completion": true,
  "scoring_horizon_covered": false,
  "covered_through_within_runtime_lag": false,
  "covered_through_within_audit_lag": false,
  "storage_ready_for_runtime_gate": false,
  "effective_writes_ready": false,
  "effective_reads_ready": false,
  "write_blockers": [
    "writes_disabled_by_config",
    "covered_through_cursor_pending_backfill_completion",
    "backfill_resume_required"
  ],
  "read_blockers": [
    "reads_disabled_by_config",
    "covered_since_pending_backfill_completion",
    "covered_through_cursor_pending_backfill_completion",
    "backfill_resume_required"
  ]
}
```

## Runtime Restart

- Service restart after clean checkpoint:
  - `MainPID=337549`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`
- Trusted selection startup state:
  - `trusted_selection_bootstrap_required=true`
  - `trusted_selection_reason=trusted_selection_bootstrap_unavailable`
  - `trusted_selection_state=Some(Invalid)`
  - `trusted_selection_legacy_bool_fallback_used=false`
- Early post-restart health:
  - `yellowstone_output_queue_fill_ratio=0.0`
  - `yellowstone_output_queue_depth=0`
  - `observed_swap_writer_pending_requests=0`
  - `sqlite_busy_error_total=0`
  - `sqlite_write_retry_total=0`
  - `ingestion_lag_ms_p95=2605`

## Verdict

- Final verdict: **partial but promising**
- Why:
  - seeded run used the new builder path end-to-end for `boundary_build`
  - throughput was materially better than previous old-path offline measurement
  - resumable state remained clean
  - service restarted cleanly
- What did **not** happen:
  - no seed install
  - no post-seed replay
  - no coverage markers
  - no aggregate readiness unlock

## Artifacts

- Raw tool log: `ops/server_reports/2026-03-16_2110_seeded_horizon_builder_offline_run.raw.log`
