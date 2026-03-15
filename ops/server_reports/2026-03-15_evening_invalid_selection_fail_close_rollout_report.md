## 2026-03-15 evening invalid-selection fail-close rollout

### Scope

- Intended rollout request: `9f85f0c Fail close invalid selection bootstrap`
- Actual live rollout target left on server: `ea57e44 Add trusted bootstrap pending window state`

`ea57e44` was required because `9f85f0c` as pushed was incomplete for compile: `crates/discovery/src/lib.rs` referenced `DiscoveryWindowState.trusted_selection_bootstrap_pending`, but `crates/discovery/src/windows.rs` was not part of that commit. The server was fast-forwarded to `9f85f0c`, build failed before restart, and no live binary was replaced. A narrow follow-up commit (`ea57e44`) added the missing field, was pushed to `main`, and the server was then rolled out successfully.

### Local validation

Full relevant local suite completed successfully before the successful rollout:

- `cargo test -p copybot-discovery -p copybot-app -p copybot-storage`
- Results:
  - `copybot-app`: `234 passed`
  - `copybot-discovery`: `52 passed`
  - `copybot-storage`: `102 passed`
  - `backfill_discovery_scoring` bin tests: `3 passed`
  - doc-tests: `0 failed`

Follow-up compile after `ea57e44`:

- `cargo check -p copybot-discovery -p copybot-app -p copybot-storage --quiet`
- Result: success, only pre-existing `ShadowRiskGuard dead_code` warnings

### Server rollout steps

1. Connected to the actual copybot host at `ubuntu@52.28.0.218`.
2. Confirmed pre-rollout live runtime:
   - deployed commit: `a2db7ee`
   - `solana-copy-bot.service`, `copybot-adapter.service`, `copybot-executor.service`: `active`
   - `followlist.active = 976`
3. Fast-forwarded `/var/www/solana-copy-bot` to `9f85f0c`.
4. Attempted release build on server.
5. Build failed before restart because `windows.rs` change was missing from `9f85f0c`.
6. Added follow-up commit `ea57e44`, pushed `main`, and fast-forwarded the server again.
7. Built release binaries successfully:
   - `copybot-app`
   - `copybot-adapter`
   - `copybot-executor`
8. Restarted:
   - `solana-copy-bot.service`
   - `copybot-adapter.service`
   - `copybot-executor.service`

### Post-rollout runtime state

New live start:

- `solana-copy-bot.service ActiveEnterTimestamp = 2026-03-15 16:19:46 UTC`
- `copybot-adapter.service ActiveEnterTimestamp = 2026-03-15 16:19:46 UTC`
- `copybot-executor.service ActiveEnterTimestamp = 2026-03-15 16:19:46 UTC`
- `solana-copy-bot MainPID = 293912`
- `NRestarts = 0`

Immediate startup health:

- startup WAL checkpoint completed with:
  - `wal_checkpoint_mode = truncate`
  - `wal_checkpoint_busy = 0`
  - `wal_log_frames = 0`
  - `wal_checkpointed_frames = 0`

Observed runtime health after rollout:

- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `yellowstone_output_queue_fill_ratio = 0.0` in steady-state snapshots, with only a small transient sample of `0.0380859375`
- `ws_notifications_replaced_oldest = 0`
- `observed_swaps` continued advancing:
  - `MAX(ts) = 2026-03-15T16:23:00.083295267+00:00`

### Strategy-selection outcome

The new contract is active and behaving as intended for the invalid-selection case.

At startup:

- runtime explicitly held the recovered historical set out of the runtime snapshot:
  - `recovered_active_follow_wallets = 976`
  - `trusted_selection_bootstrap_required = false`

First discovery bootstrap result:

- `2026-03-15T16:19:56Z`
- log:
  - `discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`
  - `recovered_follow_wallets_cleared = 976`

Durable state after rollout:

- `followlist.active = 0`
- `discovery_strategy_state.trusted_selection_bootstrap_required = 1`
- `discovery_strategy_state.trusted_selection_reason = trusted_selection_bootstrap_unavailable`
- latest durable update observed at `2026-03-15 16:22:46`

Repeated discovery cycles continued to preserve the fail-closed state instead of falling back to raw-window bootstrap:

- subsequent logs repeated:
  - `discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`
  - `recovered_follow_wallets_cleared = 0`

### Shadow/copy fail-close evidence

The app loop is no longer progressing strategy state from the recovered historical set:

- `app_follow_rejected_ratio = 1.0`
- sample telemetry:
  - `app_consumer_swaps_seen = 4534`, `app_follow_rejected = 4534`
  - later `app_consumer_swaps_seen = 5662`, `app_follow_rejected = 5662`
- `copy_signals` written since new runtime start (`2026-03-15T16:19:46Z`):
  - `0`

This confirms the requested fail-close semantics:

- runtime still persists observed swaps,
- but shadow/copy progression does not continue while trusted selection is unavailable.

### Verdict

Server rollout is successful as a safety/contract fix.

What is now true in live:

- runtime/data-plane is healthy,
- the bot no longer runs on the recovered historical `976`-wallet followlist,
- invalid selection state is durable across restarts,
- shadow/copy progression is fail-closed while trusted persisted selection is unavailable.

What is not yet true:

- the bot has **not** resumed normal trusted top-`N` strategy operation,
- because no acceptable trusted persisted `wallet_metrics` bootstrap source was available at rollout time.

So the live state after this rollout is:

- **runtime healthy**
- **strategy safely paused/fail-closed**
- **no invalid top-976 shadow run continuing**
