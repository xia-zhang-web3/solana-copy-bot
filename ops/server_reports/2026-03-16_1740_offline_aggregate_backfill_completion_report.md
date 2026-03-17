# 2026-03-16 17:40 UTC / 19:40 Kyiv — offline aggregate backfill completion report

This was an offline aggregate backfill completion run, not aggregate activation.

## Scope

- Aggregate writes remained disabled
- Aggregate reads remained disabled
- Bootstrap track was not touched
- PnL was not analyzed
- `solana-copy-bot` was intentionally stopped for offline backfill work

## Pre-run state

- Server commit: `630689c4417e0ba2162d19edd648e75553538ae4`
- Pre-run service state:
  - `MainPID=317246`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`
- Pre-run persisted cursor:
  - `start_ts = 2026-03-03T17:05:37Z`
  - `resume_ts = 2026-03-08T14:02:39.906498746Z`
  - `resume_slot = 405048014`
  - `resume_signature = 2mPgnfKxM7tBMZQueWXYXLw5EC2XDyzEbCBVFeGNi8wF8nqh3WzzLuXMDcFYNr3RPKtyhD6VGcJysFuJgygceNFG`
- Pre-run aggregate readiness:
  - `covered_since = null`
  - `covered_through_cursor = null`
  - `materialization_gap_cursor = null`
  - `backfill_active = false`
  - `backfill_resume_required = true`
  - `coverage_markers_pending_backfill_completion = true`
  - write blockers:
    - `writes_disabled_by_config`
    - `covered_through_cursor_pending_backfill_completion`
    - `backfill_resume_required`
  - read blockers:
    - `reads_disabled_by_config`
    - `covered_since_pending_backfill_completion`
    - `covered_through_cursor_pending_backfill_completion`
    - `backfill_resume_required`

## Runtime stop

- `sudo systemctl stop solana-copy-bot`
- confirmed stopped:
  - `MainPID=0`
  - `ActiveState=inactive`
  - `SubState=dead`

## Exact offline run command

I used the existing release binary directly instead of `cargo run` to avoid rebuild overhead during downtime. The runtime semantics were the same as the requested command.

```bash
/var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-03T17:05:37Z \
  --resume-ts 2026-03-08T14:03:25.621028942Z \
  --resume-slot 405048130 \
  --resume-signature 38Gt6sMAF3sGmmdWV9mdiBqvwy7YN34DJjM9WZboueeest1CCm6GKP6etMzwbA1DCD7yHM4Q3h9TKsXoJQk1tBaJ \
  --mark-covered \
  --batch-size 10000 \
  --sleep-ms 0 \
  --max-runtime-seconds 7200
```

## Important execution note

- The first wrapper attempt had a remote shell quoting bug and started the same backfill command without a usable log path.
- That stray run still executed real work and advanced the durable cursor from:
  - `2026-03-08T14:02:39.906498746Z`
- to:
  - `2026-03-08T14:03:25.621028942Z`
- I did **not** restart runtime between these attempts.
- After confirming the stray process exited and left a clean partial state, I started the logged offline run above from the new persisted cursor.

## Outcome

- The logged offline run did **not** complete coverage.
- It was intentionally stopped with `SIGINT` after enough offline throughput evidence was gathered and after it became clear that one downtime window would not finish the remaining backlog.
- Tool summary:
  - `outcome = stopped_due_to_termination_signal`
  - `stop_reason = stopped_due_to_termination_signal`
  - `coverage_marked = false`
  - `total_rows = 300000`
  - `batches = 30`
- Tool final cursor:
  - `final_cursor_ts = 2026-03-08T14:31:20.243478746Z`
  - `final_cursor_slot = 405052391`
  - `final_cursor_signature = 2PJeW9P2zGHp2BsxUyFJkSUQJf3Jhcaybm84mvjeavqkuCpeLDEjuBZEFpKdBL4JH9ppL4HzSsvJjtj9peCtu8YW`
- Tool emitted:
  - `event=controlled_abort source=termination_signal`
  - `Error: termination signal received; aborting backfill after durable checkpoint`

## Wall-clock duration

- Logged run start: `2026-03-16T17:10:07Z`
- Logged run end: `2026-03-16T17:40:12Z`
- Duration: about `30m05s`

## Progress

- Pre-run cursor for this step:
  - `2026-03-08T14:02:39.906498746Z`
  - slot `405048014`
- Post-run cursor after the combined offline work:
  - `2026-03-08T14:31:20.243478746Z`
  - slot `405052391`
- Net cursor advance:
  - about `28m40s`

## Aggregate readiness after run

- `covered_since = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_progress` still present
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`
- `effective_writes_ready = false`
- `effective_reads_ready = false`

Current blockers:
- `covered_since_pending_backfill_completion`
- `covered_through_cursor_pending_backfill_completion`
- `backfill_resume_required`
- plus config guardrails:
  - `writes_disabled_by_config`
  - `reads_disabled_by_config`

## WAL checkpoint result

The tool output did not contain a final `event=wal_checkpoint ...` line on this controlled termination path.

Because of that, I explicitly verified checkpoint cleanliness before restarting runtime:

```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

Result:

```text
0|0|0
```

Interpretation:
- `busy = 0`
- no active SQLite lock-holder remained
- restart was safe

## Runtime restart outcome

- `sudo systemctl start solana-copy-bot`
- Post-start service state:
  - `MainPID=333267`
  - `NRestarts=0`
  - `ActiveState=active`
  - `SubState=running`

Startup trusted-selection state:
- `trusted_selection_bootstrap_required = true`
- `trusted_selection_reason = trusted_selection_bootstrap_unavailable`
- `trusted_selection_state = invalid`
- `trusted_selection_legacy_bool_fallback_used = false`

Persisted `discovery_strategy_state` row after restart:
- `trusted_selection_bootstrap_required = 1`
- `trusted_selection_reason = trusted_selection_bootstrap_unavailable`
- `trusted_selection_state = invalid`
- `active_trusted_snapshot_id = ''`
- `active_trusted_snapshot_window_start = ''`
- `last_trusted_bootstrap_source_kind = ''`

Post-start runtime health:
- startup WAL checkpoint:
  - `wal_checkpoint_busy = 0`
  - `wal_checkpoint_mode = truncate`
- ingestion recovered:
  - `yellowstone_output_queue_fill_ratio = 0.0`
  - `ingestion_lag_ms_p95 = 1790`
- writer healthy:
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`

## Verdict

- This was a **partial offline backfill run**, not completion.
- Completion markers did **not** appear.
- The run materially advanced durable progress and left the DB in a clean resumable state.
- Runtime was brought back successfully and remains in the expected fail-closed selection state.

## Artifacts

- raw offline run log:
  - [2026-03-16_1740_offline_aggregate_backfill_completion.raw.log](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_reports/2026-03-16_1740_offline_aggregate_backfill_completion.raw.log)
