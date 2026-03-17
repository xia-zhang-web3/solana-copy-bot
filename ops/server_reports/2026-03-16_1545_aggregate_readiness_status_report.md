# 2026-03-16 15:45 Kyiv — aggregate readiness status report

Это readiness audit, не activation rollout.

## Scope

- Read-only live audit for `STEADY_STATE_DISCOVERY_SOURCE_RECOVERY_PLAN.md`
- No service stop
- No restart exercise
- No config change
- No backfill run
- No SQLite mutation

## Server / build

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact server commit: `909c1fc0936aed66eb7841882f8c28591394eaf6`
- Short commit: `909c1fc`
- Runtime remained up during audit:
  - `MainPID=317246`
  - `NRestarts=0`
  - `ActiveEnterTimestamp=2026-03-16 12:13:07 UTC`

## Tool invocation

Live runtime config path is:

- `/etc/solana-copy-bot/live.server.toml`

Live DB path is:

- `/var/www/solana-copy-bot/state/live_copybot.db`

Server layout note:

- `live.server.toml` stores relative sqlite path `state/live_copybot.db`
- `/etc/solana-copy-bot/state/live_copybot.db` does not exist
- so this audit used explicit `--db-path /var/www/solana-copy-bot/state/live_copybot.db`
- config file was read via `sudo` because direct read by `ubuntu` is denied

Commands used:

```bash
sudo /var/www/solana-copy-bot/target/release/aggregate_readiness_status \
  --config /etc/solana-copy-bot/live.server.toml \
  --db-path /var/www/solana-copy-bot/state/live_copybot.db
```

```bash
sudo /var/www/solana-copy-bot/target/release/aggregate_readiness_status \
  --config /etc/solana-copy-bot/live.server.toml \
  --db-path /var/www/solana-copy-bot/state/live_copybot.db \
  --json
```

## Human output

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T13:45:41.465332962+00:00
window_start=2026-03-11T13:45:41.465332962+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-03T17:50:26.766980955+00:00,slot=403986679,signature=3QaUt6PeXEd8Pktn47GiYybQgCwnxD5gp6DfUZR7pBegmWw3zR5RYH1xEeXVxYUDsLtq7hxvEBdkDdzFSFHVgtmB)
backfill_protected_since=null
scoring_horizon_covered=false
covered_through_within_runtime_lag=false
covered_through_within_audit_lag=false
storage_ready_for_runtime_gate=false
effective_writes_ready=false
effective_reads_ready=false
write_blockers=[writes_disabled_by_config,missing_covered_through_cursor,backfill_in_progress]
read_blockers=[reads_disabled_by_config,missing_covered_since,missing_covered_through_cursor,backfill_in_progress]
```

## JSON artifact

Saved separately in:

- [2026-03-16_1545_aggregate_readiness_status_report.json](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1545_aggregate_readiness_status_report.json)

Inline copy:

```json
{
  "config_path": "/etc/solana-copy-bot/live.server.toml",
  "db_path": "/var/www/solana-copy-bot/state/live_copybot.db",
  "now": "2026-03-16T13:45:41.463055612Z",
  "window_start": "2026-03-11T13:45:41.463055612Z",
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
      "ts_utc": "2026-03-03T17:50:26.766980955Z",
      "slot": 403986679,
      "signature": "3QaUt6PeXEd8Pktn47GiYybQgCwnxD5gp6DfUZR7pBegmWw3zR5RYH1xEeXVxYUDsLtq7hxvEBdkDdzFSFHVgtmB"
    }
  },
  "backfill_protected_since": null,
  "scoring_horizon_covered": false,
  "covered_through_within_runtime_lag": false,
  "covered_through_within_audit_lag": false,
  "storage_ready_for_runtime_gate": false,
  "effective_writes_ready": false,
  "effective_reads_ready": false,
  "write_blockers": [
    "writes_disabled_by_config",
    "missing_covered_through_cursor",
    "backfill_in_progress"
  ],
  "read_blockers": [
    "reads_disabled_by_config",
    "missing_covered_since",
    "missing_covered_through_cursor",
    "backfill_in_progress"
  ]
}
```

## Interpretation against steady-state plan

### Flags

- `writes_enabled = false`
- `reads_enabled = false`

### Coverage

- `covered_since = null`
- `covered_through_ts = null`
- `covered_through_cursor = null`
- `covered_through_lag_seconds = null`

Verdict:

- aggregate history does **not** cover the scoring horizon
- there is **no** exact `covered_through_cursor`

### Gap / continuity

- `materialization_gap_cursor = null`

Verdict:

- no latched materialization gap is currently recorded
- but this does **not** imply readiness, because coverage markers are still absent

### Backfill state

- `backfill_progress` is present
- `backfill_protected_since = null`

Verdict:

- backfill appears to exist and is not complete
- source protection is not active

### Derived readiness

- `scoring_horizon_covered = false`
- `covered_through_within_runtime_lag = false`
- `covered_through_within_audit_lag = false`
- `storage_ready_for_runtime_gate = false`
- `effective_writes_ready = false`
- `effective_reads_ready = false`

## Exact blocker inventory

### Write blockers

- `writes_disabled_by_config`
- `missing_covered_through_cursor`
- `backfill_in_progress`

### Read blockers

- `reads_disabled_by_config`
- `missing_covered_since`
- `missing_covered_through_cursor`
- `backfill_in_progress`

## Verdict

- aggregate writes ready: **no**
- aggregate reads ready: **no**

Current blocker summary:

1. aggregate writes are disabled in config
2. aggregate reads are disabled in config
3. aggregate coverage markers are not materialized:
   - no `covered_since`
   - no `covered_through_ts`
   - no `covered_through_cursor`
4. backfill is still in progress
5. storage is not ready for the runtime gate

## Next mandatory step

The next mandatory step is **not activation**.

It is:

1. inventory the exact aggregate backfill/continuity state behind the existing `backfill_in_progress` marker
2. determine why `covered_since` and exact `covered_through_cursor` are still absent
3. define the narrowest code/runbook step that can produce trustworthy coverage markers and complete the aggregate storage readiness path

Until that is done:

- do not enable aggregate writes
- do not enable aggregate reads
- do not treat current shadow PnL as strategy-valid

## Runtime note

The audit itself did not destabilize runtime:

- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `yellowstone_output_queue_fill_ratio` stayed at `0.0` in the checked post-audit window
- live remains explicit `invalid + fail-close`
