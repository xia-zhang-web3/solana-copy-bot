## Stage 1 Discovery Runtime Contract Rollout

Source of truth:

- deploy target commit: `2eb5c30aedb7dd72bdb99e189cdc893f4cb18f07`
- commit message: `Implement Stage 1 discovery runtime contract`

This rollout did not include aggregate activation, aggregate backfill, or execution enablement.

### Pre-rollout State

Server before deploy:

- repo: `/var/www/solana-copy-bot`
- `HEAD = 0c2a114d5b2c20566d84ed6b1638cbcf4283fc19`
- clean worktree
- `solana-copy-bot.service active`

Observed old behavior before rollout:

- repeated log line:
  - `discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`

Live config guardrails stayed off:

- `execution.enabled = false`
- `discovery.scoring_aggregates_enabled = false`
- `discovery.scoring_aggregates_write_enabled = false`
- `discovery.follow_top_n = 15`

### Exact Deploy

Server fetch confirmed:

- `origin/main = 6c5a58790aec32250e1405818fcfd7dddb421edb`
- `2eb5c30` is an ancestor of `origin/main`

Deployed exact code artifact:

- checked out detached `2eb5c30aedb7dd72bdb99e189cdc893f4cb18f07`
- rebuilt:

```bash
cargo build --release -p copybot-app
```

- restarted only:

```bash
sudo systemctl restart solana-copy-bot.service
```

### Post-rollout Facts

Service:

- `solana-copy-bot.service active`
- post-restart `MainPID = 2624`
- no crash loop observed

Startup logs:

- `execution runtime disabled`
- `startup has no recent published follow universe; recovered historical wallets stay out of runtime until discovery publishes fresh or degraded runtime truth`
- `trusted_selection_legacy_bool_fallback_used=false`

Discovery runtime logs over several cycles:

- cycle 1:
  - `discovery_runtime_mode = fail_closed`
  - `active_follow_wallets = 0`
  - `eligible_wallets = 0`
  - `scoring_source = raw_window_incomplete_no_recent_published_universe`
  - `swaps_warm_loaded = 100000`
  - `wallets_seen = 0`
- later cycles:
  - still `discovery_runtime_mode = fail_closed`
  - still `active_follow_wallets = 0`
  - still `eligible_wallets = 0`
  - still `scoring_source = raw_window_incomplete_no_recent_published_universe`
  - `swaps_evicted_due_cap > 0`

Key behavior change confirmed:

- old bootstrap-only fail-close message did not recur after Stage 1 restart
- fail-closed state is now explicit raw-runtime fail-close:

`discovery fail-closed because raw runtime truth is unavailable and no recent published universe exists`

### Risk / Runtime Checks

- no new `shadow_risk_universe_stop` after restart:
  - count since `2026-03-17T15:53:17+00:00` = `0`
- execution remained disabled
- no aggregate/backfill actions were used

### Verdict

Short verdict: **degraded but not acceptable for production recovery**

What passed:

- exact Stage 1 artifact deployed successfully
- service is stable
- runtime no longer depends on the old trusted-bootstrap fail-close path by inertia
- no aggregate/backfill actions were required

What failed:

- runtime did not reach `healthy`
- runtime did not reach `degraded`
- runtime remained persistently `fail_closed`
- active follow universe stayed at `0`

Current blocker after rollout:

- not bootstrap logic
- not aggregate dependency
- the live raw-runtime path still reports `raw_window_incomplete_no_recent_published_universe`
- practical blocker is now raw runtime truth availability on this host/state:
  - cap-truncated warm load
  - `wallets_seen = 0`
  - no recent published universe to degrade from
