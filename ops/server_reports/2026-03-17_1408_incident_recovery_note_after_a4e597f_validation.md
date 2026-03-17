## Incident Recovery Note After `a4e597f` Aggregate Validation

This note records the post-incident recovery facts after the offline aggregate validation run on server commit `a4e597fa111b1af109212ffda4d482c8fb897615`.

This was incident recovery, not aggregate activation.

### Exact Commit

- server repo: `/var/www/solana-copy-bot`
- server commit after recovery: `a4e597fa111b1af109212ffda4d482c8fb897615`
- no server-side local modifications observed in `git status --short`

### Exact Validation Command That Preceded The Incident

```bash
/var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-12T09:23:53.684376514Z \
  --resume-ts 2026-03-08T21:06:45.726139749Z \
  --resume-slot 405112624 \
  --resume-signature 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d \
  --seeded-reset \
  --mark-covered \
  --batch-size 10000 \
  --sleep-ms 0 \
  --max-runtime-seconds 7200
```

### What Was Confirmed Before Host Manageability Was Lost

- `lot_only_boundary` was confirmed in live logs:
  - `phase=boundary_build`
  - `replay_engine=lot_only_boundary`
- boundary batches showed the intended lot-only shape:
  - `apply_ms=0`
  - `rug_finalize_ms=0`
  - `progress_update_ms=0`
- boundary throughput materially improved relative to the previous builder-boundary validation

### What Was Not Confirmed

- `seed_boundary_installed` was not confirmed
- post-seed replay was not validated
- `seed_boundary_resume_from_persisted_progress` was not validated
- post-run readiness immediately after the validation run was not captured before host manageability was lost

### Incident Fact

- during the offline validation run the host became unmanageable under offline load
- SSH stopped accepting new sessions and timed out during connect/banner exchange
- the instance later required AWS stop/start for recovery

### Recovery Facts After AWS Stop/Start

Collected at approximately `2026-03-17T12:05Z`.

System:

- `uptime`: `12:05:18 up 1 min, 2 users, load average: 0.75, 0.29, 0.11`
- memory:
  - total `7.6Gi`
  - used `688Mi`
  - free `6.0Gi`
  - available `6.9Gi`
- root filesystem:
  - `/dev/root` `145G`, used `9.6G`, avail `135G`
- state volume:
  - `/dev/nvme1n1` mounted at `/var/www/solana-copy-bot/state`
  - size `492G`, used `367G`, avail `100G`

Top memory consumers:

- `copybot-app` PID `601`, RSS `171956 KiB`, `%MEM 2.1`
- no surviving `backfill_discovery_scoring` process was found

Runtime:

- `solana-copy-bot.service` auto-started after reboot
- `Active: active (running)`
- `MainPID: 601`
- `NRestarts: 0`

Current runtime posture:

- trusted selection still `invalid + fail-close`
- service logs show:
  - `discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`
- telemetry after reboot looked healthy:
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
  - `yellowstone_output_queue_fill_ratio = 0.0`

Boot/journal warnings:

- journald reported unclean shutdown / journal replacement:
  - `system.journal corrupted or uncleanly shut down, renaming and replacing`
- this is consistent with the forced AWS stop/start recovery path

### Persisted Aggregate State After Reboot

Backfill progress remained persisted:

- `backfill_progress_start_ts = 2026-03-03T17:05:37+00:00`
- `backfill_progress_cursor_ts = 2026-03-08T21:06:45.726139749+00:00`
- `backfill_progress_cursor_slot = 405112624`
- `backfill_progress_cursor_signature = 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d`

Backfill protection latch remained present:

- `backfill_protect_since_ts = 2026-03-03T17:05:37+00:00`
- `backfill_protect_expires_at = 2026-03-17T13:56:19.260328242+00:00`

Durable seed marker after reboot:

- no `seed_boundary_install_*` rows were present in `discovery_scoring_state`

### Aggregate Readiness After Reboot

`aggregate_readiness_status` after reboot:

- `writes_enabled = false`
- `reads_enabled = false`
- `covered_since = null`
- `covered_through_ts = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_active = true`
- `backfill_resume_required = false`
- `coverage_markers_pending_backfill_completion = true`
- `scoring_horizon_covered = false`
- `effective_writes_ready = false`
- `effective_reads_ready = false`

Current blockers:

- write blockers:
  - `writes_disabled_by_config`
  - `covered_through_cursor_pending_backfill_completion`
  - `backfill_in_progress`
- read blockers:
  - `reads_disabled_by_config`
  - `covered_since_pending_backfill_completion`
  - `covered_through_cursor_pending_backfill_completion`
  - `backfill_in_progress`

### Short Verdict

- `lot_only_boundary` was confirmed
- boundary throughput materially improved
- `seed_boundary_installed` was not confirmed
- post-seed replay was not validated
- host manageability was lost during the offline validation run
- after AWS stop/start, runtime recovered and auto-started, but aggregate readiness remained locked and the durable seed marker was still absent
