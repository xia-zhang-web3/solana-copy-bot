# 2026-03-16 18:56 Kyiv — backfill incremental completion runs report

These were repeatable bounded completion runs, not aggregate activation.

## Scope

- Server repo stayed on `630689c4417e0ba2162d19edd648e75553538ae4`
- Aggregate writes remained disabled
- Aggregate reads remained disabled
- Runtime was not restarted
- Main service was not stopped
- Goal: safely advance aggregate backfill with the discovered safe slice `--max-batches-per-run 4`

## Runtime baseline

- Host: `ubuntu@52.28.0.218`
- `MainPID=317246`
- `NRestarts=0`
- `ActiveState=active`
- `SubState=running`

Initial persisted cursor before the tranche:
- `start_ts = 2026-03-03T17:05:37Z`
- `resume_ts = 2026-03-08T14:02:15.641388084Z`
- `resume_slot = 405047954`
- `resume_signature = 2Wvc6YaoHwcum3QZE5HE6ePk523rUki6HQspeDKKPMv84N2g4BAPPm3tz7b6HfmJsEh9jp6fxFqG6uotUg7Yrppn`

Baseline readiness:
- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

## Safe profile used

All runs used the same profile:

```bash
cargo run -p copybot-storage --bin backfill_discovery_scoring -- \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts <persisted_resume_ts> \
  --resume-slot <persisted_resume_slot> \
  --resume-signature <persisted_resume_signature> \
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

## Run series

### Run 1

- duration: `13.675s`
- outcome: `stopped_due_to_batch_budget`
- fast guard: `false`
- infra-stop guard: `false`
- before:
  - `2026-03-08T14:02:15.641388084Z`
  - slot `405047954`
- after:
  - `2026-03-08T14:02:20.883838707Z`
  - slot `405047967`
- runtime:
  - nearest post-run sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 5847`
  - later recovery sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 1748`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### Run 2

- duration: `11.251s`
- outcome: `stopped_due_to_batch_budget`
- fast guard: `false`
- infra-stop guard: `false`
- before:
  - `2026-03-08T14:02:20.883838707Z`
  - slot `405047967`
- after:
  - `2026-03-08T14:02:25.499272267Z`
  - slot `405047979`
- runtime:
  - max seen sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 4777`
  - latest post-run sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 1818`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### Run 3

- duration: `9.933s`
- outcome: `stopped_due_to_batch_budget`
- fast guard: `false`
- infra-stop guard: `false`
- before:
  - `2026-03-08T14:02:25.499272267Z`
  - slot `405047979`
- after:
  - `2026-03-08T14:02:30.410052746Z`
  - slot `405047991`
- runtime:
  - nearest post-run sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 4507`
  - later recovery sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 1701`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### Run 4

- duration: `9.660s`
- outcome: `stopped_due_to_batch_budget`
- fast guard: `false`
- infra-stop guard: `false`
- before:
  - `2026-03-08T14:02:30.410052746Z`
  - slot `405047991`
- after:
  - `2026-03-08T14:02:35.526207170Z`
  - slot `405048003`
- runtime:
  - nearest post-run sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 4191`
  - later recovery sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 1697`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

### Run 5

- duration: `10.500s`
- outcome: `stopped_due_to_batch_budget`
- fast guard: `false`
- infra-stop guard: `false`
- before:
  - `2026-03-08T14:02:35.526207170Z`
  - slot `405048003`
- after:
  - `2026-03-08T14:02:39.906498746Z`
  - slot `405048014`
- runtime:
  - peak seen sample: `yellowstone_output_queue_fill_ratio = 0.09375`, `yellowstone_output_queue_depth = 192/2048`, `ingestion_lag_ms_p95 = 3369`
  - recovery sample: `yellowstone_output_queue_fill_ratio = 0.0`, `ingestion_lag_ms_p95 = 1944`
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `NRestarts = 0`

## Aggregate progress

Across this tranche:
- runs completed: `5`
- rows processed: `5000`
- stop reason on every run: `stopped_due_to_batch_budget`
- fast guard fired: `0` times
- infra-stop guard fired: `0` times
- manual `SIGINT`: `0` times

Cursor advanced from:
- `2026-03-08T14:02:15.641388084Z` / slot `405047954`

To:
- `2026-03-08T14:02:39.906498746Z` / slot `405048014`

Net cursor advance in this tranche:
- about `24.265s`

## Runtime impact verdict

What held:
- no restarts
- no service stop
- no writer backlog
- no sqlite busy/retry growth
- no fast-guard abort
- no infra-stop abort

What did not stay flat:
- nearest post-run `ingestion_lag_ms_p95` samples were repeatedly elevated on runs 1–4
- however queue fill returned to `0.0`, and later recovery samples returned to roughly `1.7–1.9s`

Interpretation:
- safe slice `4` is repeatable
- but it is not “zero-impact”
- it creates short-lived lag spikes that recover within about one ingestion telemetry interval

## Post-tranche readiness

State after run 5:
- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_progress` still present
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`
- aggregate writes ready: `no`
- aggregate reads ready: `no`

Blockers remain:
- `covered_since_pending_backfill_completion`
- `covered_through_cursor_pending_backfill_completion`
- `backfill_resume_required`

## Verdict

- repeatable bounded completion runs on safe slice `4` are confirmed
- this tranche progressed safely
- completion was **not** reached
- readiness blockers remain unchanged in kind; only the persisted cursor advanced

Recommended next step:
- continue incremental completion in additional bounded tranches at the same safe profile
- do not increase throughput above `4` without a separate decision
- keep aggregate writes/reads disabled until completion markers appear
