## Aggregate Recovery Validation After `a4e597f`

This was a server-side validation run for aggregate recovery after updating live server code to `a4e597fa111b1af109212ffda4d482c8fb897615`.

This was not aggregate activation.

### Scope

- Update server repo to `origin/main` at `a4e597f`
- Confirm seeded offline validation uses the new lot-only boundary path
- Check whether `seed_boundary_installed` becomes reachable under bounded runtime
- If seed install is reached, validate resume semantics on a second bounded run

### Server Update

- Server repo path: `/var/www/solana-copy-bot`
- Final server branch before run: `main`
- Final server commit before run: `a4e597fa111b1af109212ffda4d482c8fb897615`
- No local deviations from `main` were added on the server

### Pre-run State

- `solana-copy-bot.service`: `active`
- `MainPID`: `337549`
- `NRestarts`: `0`

Aggregate readiness before run:

- `writes_enabled = false`
- `reads_enabled = false`
- `covered_since = null`
- `covered_through_ts = null`
- `covered_through_cursor = null`
- `materialization_gap_cursor = null`
- `backfill_progress.start_ts = 2026-03-03T17:05:37Z`
- `resume_ts = 2026-03-08T21:06:45.726139749Z`
- `resume_slot = 405112624`
- `resume_signature = 2wsWtL4P7TzBS52S74LMkGqyhFu7Jfq83PomHiPzXLu27vr2QvBNuS9kKjVJrRDhsCn6BU17vYLSUvPEHiFNB2d`
- `backfill_active = false`
- `backfill_resume_required = true`
- `coverage_markers_pending_backfill_completion = true`

Durable seed marker before run:

- no rows in `discovery_scoring_state` matching `seed_boundary_install_%`

### Chosen Start Boundary

Seeded reset start boundary at launch:

- `2026-03-12T09:23:53.684376514Z`

This matched the effective scoring horizon start reported by readiness status at launch time.

### Runtime Stop

Runtime was stopped cleanly before the offline validation run:

- `sudo systemctl stop solana-copy-bot`
- post-stop state observed:
  - `MainPID = 0`
  - `ActiveState = inactive`
  - `SubState = dead`

### Exact Command

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

Raw log target on server:

- `/tmp/seeded_horizon_lot_only_offline_20260317T0924Z.log`

### Observed Validation Facts

The new seeded boundary path was confirmed on live:

- boundary phase emitted:
  - `event=boundary_batch_buffered`
  - `phase=boundary_build`
  - `replay_engine=lot_only_boundary`
  - `durable=false`

The boundary batches also showed the expected lot-only shape:

- `apply_ms = 0`
- `rug_finalize_ms = 0`
- `progress_update_ms = 0`

This is factual confirmation that pre-seed boundary work no longer applies pre-seed facts/aggregates/quality windows during `boundary_build`.

### Observed Progress Before Host Access Degraded

Last observed progress sample before host access degraded:

- `total_rows = 26930000`
- boundary cursor before run:
  - `2026-03-08T21:06:45.726139749Z`
- boundary cursor at last observed sample:
  - `2026-03-10T15:57:33.775171840Z`
- observed boundary cursor advance:
  - `42h 50m 48.049032091s`

Additional observed intermediate samples confirmed fast forward progress through boundary replay:

- `240000` rows -> cursor `2026-03-08T21:31:19.588533230Z`
- `12170000` rows -> cursor `2026-03-09T15:22:52.217398464Z`
- `18040000` rows -> cursor `2026-03-10T01:21:28.186812867Z`
- `24030000` rows -> cursor `2026-03-10T10:17:17.386726207Z`

### Comparison With Previous Seeded Builder Validation

Previous server artifact:

- `2026-03-16_2110_seeded_horizon_builder_offline_run_report.md`

Previous measured builder throughput:

- `rows/sec = 476.1746103142475`
- `rows/hour = 1714228.597131291`
- `cursor-hours/hour = 3.2937237494516354`

The new `lot_only_boundary` path was materially better before host access degraded:

- boundary cursor advanced `42h 50m 48.049032091s` before the last observable sample
- old builder validation advanced only about `6h 35m 25.482661003s` over its full bounded run

This is enough to say the boundary phase became materially more practical than the old builder boundary run.

### Seed Install Status

Before host access degraded, no log evidence was observed for:

- `seed_boundary_exported`
- `seed_boundary_installed`
- `seed_boundary_resume_from_persisted_progress`

So at the last confirmed observation point:

- `seed install` had **not yet been proven reached**
- validation had **not yet reached post-seed replay**

### Host Manageability Failure During Validation

After the long offline run, the host stopped accepting new SSH sessions:

- repeated attempts timed out during SSH connect/banner exchange
- even after the original controlling session sent `SIGINT`, the host remained unreachable

Because of that, the following post-run checks could not be completed:

- post-run `aggregate_readiness_status` human/json
- persisted `backfill_progress` after interruption
- durable `seed_boundary_install_*` state after interruption
- clean `WAL checkpoint` verification
- bounded second run to validate `seed_boundary_resume_from_persisted_progress`
- runtime restart verification

### Current Validation Status

Validated:

- server was updated to `a4e597f`
- seeded offline path used `replay_engine=lot_only_boundary`
- boundary phase is materially faster than the old builder boundary run

Not yet validated:

- `seed_boundary_installed`
- explicit durable marker after seed install
- post-seed resume behavior
- post-run readiness state
- runtime restart outcome after this validation attempt

### Short Verdict

- factual server change: new offline seeded boundary replay is using `lot_only_boundary`
- `seed install` was **not confirmed reached**
- recovery became **materially more practical at the boundary phase**
- the next real blocker is no longer boundary semantics alone; it is whether the host can stay manageable through long offline replay and whether the run can reach `seed_boundary_installed` before operational limits are hit

This was a server-side validation run after `a4e597f`, not aggregate activation.
