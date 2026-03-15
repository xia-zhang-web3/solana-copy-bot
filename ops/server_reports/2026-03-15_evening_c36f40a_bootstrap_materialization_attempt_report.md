## 2026-03-15 evening — `c36f40a` trusted bootstrap materialization attempt

### Scope

- Rollout target: `c36f40a Materialize trusted bootstrap wallet metrics`
- Goal:
  - keep live on the fail-closed selection contract,
  - materialize one fresh trusted `wallet_metrics` bucket from persisted `observed_swaps`,
  - let discovery recover to a real trusted top-`15` followlist.

### Pre-attempt state

- Server host: `ubuntu@52.28.0.218`
- Repo before rollout: `ea57e44`
- Live strategy state before attempt:
  - runtime healthy,
  - selection fail-closed,
  - historical `976`-wallet recovered set no longer used,
  - durable `trusted_selection_bootstrap_required = 1`,
  - latest known `wallet_metrics.window_start = 2026-03-10T14:00:00+00:00` and therefore stale for the current bootstrap bucket.

### Local validation before rollout

Validated on a detached worktree at exact commit `c36f40a`:

- `cargo test -p copybot-discovery -p copybot-app -p copybot-storage`
- `cargo check -p copybot-discovery -p copybot-app -p copybot-storage --quiet`

Results:

- `copybot-app`: `234 passed`
- `copybot-discovery`: `54 passed`
- `copybot-storage`: `102 passed`
- `backfill_discovery_scoring`: `3 passed`

Only pre-existing `ShadowRiskGuard` dead-code warnings remained.

### What was deployed

1. Server repo was moved from `ea57e44` to detached `c36f40a`.
2. Release binaries were built successfully, including:
   - `copybot-app`
   - `copybot-adapter`
   - `copybot-executor`
   - `materialize_wallet_metrics_bootstrap`

### What was attempted

Controlled bootstrap sequence:

1. Inspect pre-state.
2. Stop `solana-copy-bot.service`.
3. Run:

   `target/release/materialize_wallet_metrics_bootstrap state/live_copybot.db --config configs/live.toml`

4. Expect one fresh trusted `wallet_metrics` bucket for the current bootstrap window.
5. Restart `solana-copy-bot.service`.

### What actually happened

The admin bootstrap tool was **code-correct but operationally unsafe on the live-size DB**.

Observed on the live host:

- The tool stayed active for about `19m`.
- Process telemetry showed continuous forward progress:
  - `read_bytes` grew past `5.7 GB`,
  - CPU stayed around `25%`,
  - the process remained in active disk-I/O state.
- Memory usage grew steadily to about:
  - `6.6 GiB RSS`,
  - `~83%` of system RAM,
  - with available memory falling under `1 GiB`.

At that point the attempt was aborted deliberately to avoid an OOM / host instability incident.

Important fact:

- `/proc/<pid>/io` showed `write_bytes = 0` for the materialization process before abort.
- So the tool did **not** complete the materialization write and did **not** create a fresh trusted bootstrap bucket.

### Recovery actions

Because the aborted attempt left a very large WAL / cold-start burden:

1. `solana-copy-bot.service` was restarted once on `c36f40a`, but startup stuck in disk-I/O / WAL recovery.
2. The app was stopped again.
3. Offline `PRAGMA wal_checkpoint(TRUNCATE)` was run to completion.
   - Start: `2026-03-15T17:34:57Z`
   - End: `2026-03-15T17:46:03Z`
   - Result: `0|0|0`
4. `solana-copy-bot.service` was started again after the offline checkpoint.

### Final live state after recovery

- Server repo / deployed binary: `c36f40a`
- `solana-copy-bot.service`:
  - `MainPID=298829`
  - `NRestarts=0`
  - `ActiveEnterTimestamp=2026-03-15 17:46:03 UTC`
- `copybot-adapter.service` and `copybot-executor.service` remained `active`
- Startup checkpoint completed cleanly:
  - `wal_checkpoint_mode = truncate`
  - `wal_checkpoint_busy = 0`
  - `wal_log_frames = 0`
  - `wal_checkpointed_frames = 0`
- Runtime returned to the same safe fail-closed contract:
  - `trusted_selection_bootstrap_required = true`
  - startup held recovered active wallets out of runtime truth,
  - discovery again logged `trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`.

### Verdict

`c36f40a` was deployed successfully, but the **live bootstrap materialization attempt failed operationally**.

What is true now:

- The server is healthy again.
- The strategy remains fail-closed, which is the correct safety state.
- The attempted one-shot bootstrap tool is **not operationally viable** on the current live DB as implemented.

What is not true:

- Top-`15` strategy operation was **not** restored.
- No fresh trusted bootstrap bucket was materialized.

### Required next step

Do **not** retry the same admin tool on the live host.

Next work item must be:

- rewrite trusted bootstrap materialization to a **streaming / bounded-memory** path over persisted `observed_swaps`,
- then retry materialization,
- then verify:
  - `trusted_selection_bootstrap_required = 0`
  - `followlist.active <= 15`
  - trusted top-wallet labels are non-empty
  - `copy_signals` resume only after trusted bootstrap succeeds.
