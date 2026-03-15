## 2026-03-13 evening PR2/PR3/PR4 risk rollout report

### Scope

- Deployed `origin/main = 06ade443fe94f9149cc17aef5905f1f86adf46d9` to live.
- Functional runtime delta for this rollout:
  - `dc107bb Harden sqlite restart recovery paths`
  - `f50eef2 Split shadow risk exposure accounting`
  - `a400e43 Add shadow quarantine risk controls`
  - `9743a59 Add stale close recovery dead-zone mode`
- `06ade44` itself is docs-only.

### Controlled rollout procedure

1. Backed up live config to `/etc/solana-copy-bot/live.server.toml.bak.20260313T172019Z`.
2. Stopped `solana-copy-bot.service`, `copybot-adapter.service`, `copybot-executor.service`.
3. Ran static `PRAGMA wal_checkpoint(TRUNCATE);` on `/var/www/solana-copy-bot/state/live_copybot.db`.
4. Fast-forwarded server repo from `06ade44` to `9743a594f333e3f3527b6f965912932cdec74c72`.
5. Rebuilt `copybot-app`, `copybot-adapter`, `copybot-executor`.
6. Restarted all three services.

### Server-local config after rollout

- `risk.execution_buy_cooldown_seconds = 60`
- `risk.max_hold_hours = 6`
- `risk.shadow_stale_close_terminal_zero_price_hours = 12`
- `risk.shadow_stale_close_recovery_zero_price_enabled = false`
- `risk.shadow_soft_exposure_cap_sol = 10.0`
- `risk.shadow_soft_exposure_resume_below_sol = 9.5`

### Migrations applied on live

- `0036_shadow_close_context.sql`
- `0037_risk_events_type_index.sql`
- `0038_shadow_lot_risk_context.sql`

### Runtime evidence

Services:

- `solana-copy-bot.service`: `active`
- `copybot-adapter.service`: `active`
- `copybot-executor.service`: `active`
- `solana-copy-bot.service ActiveEnterTimestamp = 2026-03-13 17:21:18 UTC`
- `NRestarts = 0`

Startup WAL hardening:

- startup checkpoint logged `wal_checkpoint_mode=truncate`
- `wal_checkpoint_busy=0`
- first heartbeat logged `sqlite_wal_size_bytes`
- writer-side runtime checkpointing no longer depends on truncate-only success

Discovery / restart behavior:

1. First post-restart cycle used protected bridge:
   - `scoring_source = persisted_wallet_metrics_truncated_warm_restore`
   - `follow_demoted = 0`
   - `follow_promoted = 0`
   - `metrics_written = 0`
   - `metrics_persisted = false`
   - `followlist_activations_suppressed = true`
   - `followlist_deactivations_suppressed = true`
   - `active_follow_wallets = 206`
2. Subsequent cycles returned to `raw_window`:
   - first raw cycle promoted `14`
   - deactivations remained suppressed on capped raw window
   - `active_follow_wallets` stabilized at `220`

Current live snapshot after rollout soak:

- `observed_swaps_max_ts = 2026-03-13T17:26:27.921713129+00:00`
- `discovery cursor_ts = 2026-03-13T17:24:27.925460237+00:00`
- `followlist.active = 220`
- `copy_signals = 509105`
- `shadow_lots_open = 51`
- `shadow_closed_trades = 732`
- `live_copybot.db-wal ~= 1.1G` during early soak

### Risk-pause semantics after rollout

- `shadow_risk_pause` was restored from durable state on startup instead of being re-armed every refresh.
- Latest `shadow_risk_pause` event in DB remained pre-rollout:
  - `2026-03-13T17:18:58.485528114+00:00`
- No new `shadow risk timed pause activated` spam appeared after restart.

This is the key PR1/PR2 live effect:

- soft-cap pause now behaves as durable latch with hysteresis,
- not as rolling self-extending timed pause.

### Residual watch item

- One transient `failed persisting discovery runtime cursor` occurred during the first protected cycle.
- It did not repeat in the immediate soak window.
- Treat this as residual cursor-persist contention to monitor, not as a fresh rollback blocker.

### Rollout verdict

- Rollout successful.
- PR2/PR3/PR4 schema and runtime surfaces are present on live.
- Restart hardening and warm-restore protection behaved correctly in practice.
- Residual status:
  - successful with cursor-persist contention still worth monitoring.
