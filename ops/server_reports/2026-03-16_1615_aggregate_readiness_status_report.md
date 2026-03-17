# 2026-03-16 16:15 Kyiv — aggregate readiness status report

Это readiness audit, не activation rollout.

## Scope

- Server repo updated to Batch 8 commit `f97e879`
- Runtime/service not restarted
- Read-only tool run only
- No config change
- No aggregate write enable
- No aggregate read enable
- No backfill run
- No SQLite mutation

## Server / commit

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact server commit: `f97e879128fb20491139d8cd1d9a7f6459d9dd21`
- Short commit: `f97e879`

Runtime remained untouched:

- `MainPID=317246`
- `NRestarts=0`
- `ActiveEnterTimestamp=2026-03-16 12:13:07 UTC`

## Tool invocation

Config:

- `/etc/solana-copy-bot/live.server.toml`

DB:

- `/var/www/solana-copy-bot/state/live_copybot.db`

Server layout note:

- config uses relative sqlite path
- actual live DB is under `/var/www/solana-copy-bot/state`
- tool was run with explicit `--db-path` to ensure it reads the real live DB
- config was read via `sudo` because `/etc/solana-copy-bot/live.server.toml` is not readable by plain `ubuntu`

Commands:

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
now=2026-03-16T14:15:39.888075756+00:00
window_start=2026-03-11T14:15:39.888075756+00:00
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

## JSON artifact

Saved separately in:

- [2026-03-16_1615_aggregate_readiness_status_report.json](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1615_aggregate_readiness_status_report.json)

## What changed vs previous readiness audit

New status semantics now make the transitional aggregate state explicit:

- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

This is cleaner than the previous generic `backfill_in_progress` blocker.

## Interpretation

### Guardrail flags

- `writes_enabled = false`
- `reads_enabled = false`

These are expected guardrails, not defects.

### Real working blockers

- `covered_since = null`
- `covered_through_cursor = null`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

### Good signal

- `materialization_gap_cursor = null`

This is not a blocker. It means no latched materialization gap is currently recorded.

### Coverage / continuity verdict

- scoring horizon is **not** covered
- there is **no** exact `covered_through_cursor`
- aggregate storage is **not** ready for the runtime gate
- backfill exists as persisted state, but it is currently **not active**
- instead, it requires explicit resume / completion before coverage markers can materialize

### Backfill state

- `backfill_progress` is present
- `backfill_protected_since = null`
- `backfill_active = false`
- `backfill_resume_required = true`

Interpretation:

- the aggregate backfill contract has persisted progress
- but live is in a paused/incomplete transitional state rather than an actively advancing one
- source protection is not active

## Verdict

- aggregate writes ready: **no**
- aggregate reads ready: **no**

Real blocker inventory, excluding config guardrails:

1. `covered_since_pending_backfill_completion`
2. `covered_through_cursor_pending_backfill_completion`
3. `backfill_resume_required`

## Next mandatory step

Next track is **aggregate backfill / coverage state contract**.

The next narrow code task should answer:

1. why live has persisted `backfill_progress` but still has no `covered_since`
2. why live has persisted `backfill_progress` but still has no exact `covered_through_cursor`
3. whether this state is:
   - a defect in marker materialization / readiness accounting, or
   - an allowed transitional state that must be formalized explicitly

Until that is resolved:

- do not enable aggregate writes
- do not enable aggregate reads
- do not touch bootstrap track
- do not treat any current shadow PnL as strategy-valid

## Runtime note

This audit did not disturb runtime:

- `MainPID` unchanged
- `NRestarts = 0`
- runtime stayed active
