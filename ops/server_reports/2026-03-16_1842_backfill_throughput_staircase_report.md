# 2026-03-16 18:42 Kyiv — backfill throughput staircase report

This was a bounded throughput staircase test, not aggregate activation.

## Scope

- Server repo stayed on `630689c`
- Aggregate writes remained disabled
- Aggregate reads remained disabled
- Runtime was not restarted
- Main service was not stopped
- Goal: find a safe bounded slice, not complete backfill

## Server / commit

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Exact server commit: `630689c4417e0ba2162d19edd648e75553538ae4`
- Runtime before / through / after staircase:
  - `MainPID=317246`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`

## Baseline before staircase

- current persisted cursor:
  - `start_ts = 2026-03-03T17:05:37Z`
  - `resume_ts = 2026-03-08T14:01:58.209012788Z`
  - `resume_slot = 405047908`
  - `resume_signature = 2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx`
- pre-run runtime baseline:
  - `yellowstone_output_queue_fill_ratio = 0.0`
  - `yellowstone_output_queue_depth = 0`
  - `ingestion_lag_ms_p95 ≈ 1744`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
- pre-run readiness:
  - `covered_since = null`
  - `covered_through_cursor = null`
  - `materialization_gap_cursor = null`
  - `backfill_active = false`
  - `backfill_resume_required = true`
  - `coverage_markers_pending_backfill_completion = true`

## Level 2

### Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T14:01:58.209012788Z \
  --resume-slot 405047908 \
  --resume-signature 2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx \
  --mark-covered \
  --abort-on-runtime-pressure \
  --max-yellowstone-fill-ratio 0.20 \
  --max-ingestion-lag-ms-p95 8000 \
  --max-runtime-pressure-sample-age-seconds 35 \
  --abort-on-runtime-infra-stop \
  --batch-size 250 \
  --sleep-ms 150 \
  --max-batches-per-run 2
```

### Duration

- log birth: `2026-03-16 16:40:11.547325583 +0000`
- log mtime: `2026-03-16 16:40:25.805331961 +0000`
- observed duration: `~14.26s`

### Outcome

- `summary outcome = stopped_due_to_batch_budget`
- `stop_reason = stopped_due_to_batch_budget`
- fast guard: **no**
- infra-stop guard: **no**
- manual `SIGINT`: **no**

### Cursor progress

- before:
  - `2026-03-08T14:01:58.209012788Z`
  - slot `405047908`
  - sig `2kKwzCmFLHHYcw4d7y336jf8WMKGBGMXDysTzL4ahyayi96SPSTN1KQSP7a6RkhHgHkq2z2LHzQeNrqzaFHPoDtx`
- after:
  - `2026-03-08T14:02:00.701810275Z`
  - slot `405047915`
  - sig `4SB9q6oKziqKMoGQJtfqPLSW8Qr2xK9dK4hLwN22VU5tuMT3jqHc7mrESgVvpidnnmLUUWC5sCuwnpW6ApkjgWEr`

### Runtime profile

- no unsafe pressure observed
- after-state next runtime sample:
  - `yellowstone_output_queue_fill_ratio = 0.0`
  - `yellowstone_output_queue_depth = 0`
  - `ingestion_lag_ms_p95 = 1770`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### Post-run readiness

Readiness remained logically unchanged except for cursor advancement:

- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

## Level 4

### Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T14:02:00.701810275Z \
  --resume-slot 405047915 \
  --resume-signature 4SB9q6oKziqKMoGQJtfqPLSW8Qr2xK9dK4hLwN22VU5tuMT3jqHc7mrESgVvpidnnmLUUWC5sCuwnpW6ApkjgWEr \
  --mark-covered \
  --abort-on-runtime-pressure \
  --max-yellowstone-fill-ratio 0.20 \
  --max-ingestion-lag-ms-p95 8000 \
  --max-runtime-pressure-sample-age-seconds 35 \
  --abort-on-runtime-infra-stop \
  --batch-size 250 \
  --sleep-ms 150 \
  --max-batches-per-run 4
```

### Duration

- log birth: `2026-03-16 16:40:37.261336886 +0000`
- log mtime: `2026-03-16 16:40:46.736340649 +0000`
- observed duration: `~9.48s`

### Outcome

- `summary outcome = stopped_due_to_batch_budget`
- `stop_reason = stopped_due_to_batch_budget`
- fast guard: **no**
- infra-stop guard: **no**
- manual `SIGINT`: **no**

### Cursor progress

- before:
  - `2026-03-08T14:02:00.701810275Z`
  - slot `405047915`
  - sig `4SB9q6oKziqKMoGQJtfqPLSW8Qr2xK9dK4hLwN22VU5tuMT3jqHc7mrESgVvpidnnmLUUWC5sCuwnpW6ApkjgWEr`
- after:
  - `2026-03-08T14:02:05.702307539Z`
  - slot `405047927`
  - sig `DePhtaC8GwZAEcuWRvkA6LgsuTxW2wdbWDbR547gJAv96hmFLiWdHnMEXEJE7HMyXpk4m5kacy74h4gHujQJGCR`

### Runtime profile

- bounded stop still succeeded cleanly
- but the closest post-run pipeline sample showed a noticeable spike:
  - `yellowstone_output_queue_fill_ratio = 0.416015625`
  - `yellowstone_output_queue_depth = 852 / 2048`
  - `ingestion_lag_ms_p95 = 5541`
  - `ingestion_lag_ms_p99 = 5971`
- still:
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`
- recovery:
  - within about `30s`, runtime returned to:
    - `yellowstone_output_queue_fill_ratio = 0.0`
    - `ingestion_lag_ms_p95 = 2057`

### Post-run readiness

Readiness remained logically unchanged except for cursor advancement:

- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

## Level 8

### Exact command

```bash
sudo /var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T14:02:05.702307539Z \
  --resume-slot 405047927 \
  --resume-signature DePhtaC8GwZAEcuWRvkA6LgsuTxW2wdbWDbR547gJAv96hmFLiWdHnMEXEJE7HMyXpk4m5kacy74h4gHujQJGCR \
  --mark-covered \
  --abort-on-runtime-pressure \
  --max-yellowstone-fill-ratio 0.20 \
  --max-ingestion-lag-ms-p95 8000 \
  --max-runtime-pressure-sample-age-seconds 35 \
  --abort-on-runtime-infra-stop \
  --batch-size 250 \
  --sleep-ms 150 \
  --max-batches-per-run 8
```

### Duration

- log birth: `2026-03-16 16:41:32.013358388 +0000`
- log mtime: `2026-03-16 16:41:48.809364746 +0000`
- observed duration: `~16.80s`

### Outcome

- `summary outcome = stopped_due_to_fast_guard`
- `stop_reason = stopped_due_to_fast_guard`
- fast guard: **yes**
- infra-stop guard: **no**
- manual `SIGINT`: **no**

### Cursor progress

- before:
  - `2026-03-08T14:02:05.702307539Z`
  - slot `405047927`
  - sig `DePhtaC8GwZAEcuWRvkA6LgsuTxW2wdbWDbR547gJAv96hmFLiWdHnMEXEJE7HMyXpk4m5kacy74h4gHujQJGCR`
- after:
  - `2026-03-08T14:02:15.641388084Z`
  - slot `405047954`
  - sig `2Wvc6YaoHwcum3QZE5HE6ePk523rUki6HQspeDKKPMv84N2g4BAPPm3tz7b6HfmJsEh9jp6fxFqG6uotUg7Yrppn`

### Runtime profile

Fast guard triggered on a fresh runtime sample:

- `yellowstone_output_queue_fill_ratio = 0.65625`
- `yellowstone_output_queue_depth = 1344 / 2048`
- `yellowstone_output_oldest_age_ms = 5309`
- `ingestion_lag_ms_p95 = 5700`
- `ingestion_lag_ms_p99 = 6152`
- trigger reason:
  - fill ratio exceeded configured threshold `0.20`
- lag threshold `8000ms` did not trigger first

After abort:

- runtime recovered without restart
- later recovery sample:
  - `yellowstone_output_queue_fill_ratio = 0.0888671875`
  - `yellowstone_output_queue_depth = 182`
  - `ingestion_lag_ms_p95 = 4564`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### Post-run readiness

Human output after level 8:

```text
event=aggregate_readiness_status
config_path=/etc/solana-copy-bot/live.server.toml
db_path=/var/www/solana-copy-bot/state/live_copybot.db
now=2026-03-16T16:42:05.218619626+00:00
window_start=2026-03-11T16:42:05.218619626+00:00
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
backfill_progress=start_ts=2026-03-03T17:05:37+00:00,cursor=(ts_utc=2026-03-08T14:02:15.641388084+00:00,slot=405047954,signature=2Wvc6YaoHwcum3QZE5HE6ePk523rUki6HQspeDKKPMv84N2g4BAPPm3tz7b6HfmJsEh9jp6fxFqG6uotUg7Yrppn)
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

## Readiness summary after staircase

Across the staircase, readiness remained unchanged except for cursor advancement:

- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`
- aggregate writes ready: **no**
- aggregate reads ready: **no**

## Verdict

### Results by level

- `2` batches:
  - clean
  - stopped due to batch budget
  - no fast guard
  - no infra-stop
- `4` batches:
  - clean
  - stopped due to batch budget
  - no fast guard
  - no infra-stop
  - noticeable but recoverable runtime spike
- `8` batches:
  - not budget-clean
  - aborted by fast guard
  - this is the first level that crosses the configured safe envelope

### Safe level

- **Safe bounded level: `4` batches per run**

### Practical ceiling

- **Practical ceiling: between `4` and `8`**
- Under current parameters, `8` is above the safe ceiling because it trips the fast guard.

### Recommended per-run slice

- Recommended incremental completion slice under the current contract:
  - **`--max-batches-per-run 4`**

Reason:

- `2` is clean but leaves throughput on the table
- `4` still finishes by batch budget and recovers to healthy runtime
- `8` no longer stays within the configured safety envelope

