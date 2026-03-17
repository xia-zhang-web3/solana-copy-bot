## Seed Install Stop Mode Validation Incident After `0c2a114`

This was a two-step aggregate recovery validation attempt after updating the server to `0c2a114d5b2c20566d84ed6b1638cbcf4283fc19`.

This was not aggregate activation.

### Exact Server Commit

- `0c2a114d5b2c20566d84ed6b1638cbcf4283fc19`

### Pre-run Facts

- runtime service before validation:
  - `solana-copy-bot.service active`
  - `MainPID = 601`
  - `NRestarts = 0`
- pre-run aggregate readiness:
  - `covered_since = null`
  - `covered_through_cursor = null`
  - `backfill_progress.start_ts = 2026-03-03T17:05:37+00:00`
  - `backfill_progress.cursor_ts = 2026-03-08T21:06:45.726139749+00:00`
  - `backfill_resume_required = false`
  - `backfill_active = true`
- pre-run seed marker:
  - no `seed_boundary_install_*` rows

### Runtime Stop

Runtime was stopped cleanly before Phase 1:

- `sudo systemctl stop solana-copy-bot`
- confirmed:
  - `MainPID = 0`
  - `ActiveState = inactive`
  - `SubState = dead`

### Exact Phase 1 Command

```bash
/var/www/solana-copy-bot/target/release/backfill_discovery_scoring \
  /var/www/solana-copy-bot/state/live_copybot.db \
  --config /etc/solana-copy-bot/live.server.toml \
  --start-ts 2026-03-12T13:24:56.214884589Z \
  --resume-ts 2026-03-08T21:06:45.726139749Z \
  --resume-slot 405112624 \
  --resume-signature 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d \
  --seeded-reset \
  --stop-after-seed-install \
  --mark-covered \
  --batch-size 10000 \
  --sleep-ms 0 \
  --max-runtime-seconds 7200
```

### What Was Confirmed

- Phase 1 used:
  - `phase=boundary_build`
  - `replay_engine=lot_only_boundary`
- no evidence of `replay_after_seed`
- boundary batches stayed lot-only:
  - `apply_ms=0`
  - `rug_finalize_ms=0`
  - `progress_update_ms=0`

### Best Confirmed Progress Before Incident

From the live streamed output before the host became unmanageable again:

- `total_rows = 27,060,000`
- `batches = 2706`
- boundary cursor advanced from:
  - `2026-03-08T21:06:45.726139749+00:00`
- to at least:
  - `2026-03-10T16:11:21.749243982+00:00`

### What Was Not Confirmed

- `event=seed_boundary_exported`
- `event=seed_boundary_installed`
- `event=seed_boundary_stop_requested`
- `summary outcome=stopped_after_seed_install`
- committed durable `seed_boundary_install_*` marker
- Phase 2 post-seed resume

### Incident Trigger

During Phase 1, SSH/manageability degraded again:

- new SSH sessions started timing out during banner exchange
- this matched the earlier offline-load failure mode

Per incident rule, the experiment was stopped immediately via the existing control session.

### Post-stop State

Post-stop state was **not fully confirmed** because host manageability was lost again immediately after the stop request:

- unable to confirm:
  - whether `backfill_discovery_scoring` exited cleanly
  - whether any durable seed marker was written
  - post-run aggregate readiness
  - runtime restart

### Short Verdict

- exact commit on server: `0c2a114d5b2c20566d84ed6b1638cbcf4283fc19`
- Phase 1 did **not** confirm committed `seed_boundary_installed`
- Phase 1 did confirm the new stop-mode run still uses `lot_only_boundary`
- host remained manageable only up to a point; manageability was lost again under offline validation load before seed install was proven
- Phase 2 was **not attempted**
- next blocker is still operational: reaching and validating committed seed install without losing host manageability
