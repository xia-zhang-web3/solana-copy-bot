## Stage 1 Persisted-Stream Follow-Up Rollout Report

- Timestamp: `2026-03-17 18:39 Europe/Kiev`
- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Config: `/etc/solana-copy-bot/live.server.toml`
- DB: `/var/www/solana-copy-bot/state/live_copybot.db`
- Service: `solana-copy-bot.service`
- Rollout type: Stage 1 discovery runtime contract follow-up
- Exact deployed commit: `0c58abadd2f0d3e3807cc0013ac37e6047d9c71c`
- Commit message: `Harden Stage 1 persisted-stream discovery fallback`

### Guardrails

- `execution.enabled = false`
- `discovery.scoring_aggregates_enabled = false`
- `discovery.scoring_aggregates_write_enabled = false`
- No aggregate/backfill/seeded-reset/offline recovery commands were run.
- No ad hoc code changes were made on the server.

### Pre-Rollout State

- Previous deployed commit: `2eb5c30aedb7dd72bdb99e189cdc893f4cb18f07`
- Worktree before deploy: clean relative to `origin/main`
- Service before deploy: `active`
- Previous live behavior under Stage 1:
  - repeated `discovery fail-closed because raw runtime truth is unavailable and no recent published universe exists`
  - `scoring_source = raw_window_incomplete_no_recent_published_universe`
  - `active_follow_wallets = 0`
  - `eligible_wallets = 0`

### Commands Run

```bash
ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  'cd /var/www/solana-copy-bot && git fetch origin && git checkout --detach 0c58aba && git rev-parse HEAD && git status --short && source ~/.cargo/env && cargo build --release -p copybot-app'

ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  'cd /var/www/solana-copy-bot; git rev-parse HEAD; sudo systemctl restart solana-copy-bot.service; systemctl show solana-copy-bot.service -p MainPID -p NRestarts -p ActiveState -p SubState -p ExecMainStartTimestamp --value; systemctl status solana-copy-bot.service --no-pager --lines=25'

ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  'journalctl -u solana-copy-bot.service --since "2026-03-17 16:32:30 UTC" --no-pager'

ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  "sudo sqlite3 /var/www/solana-copy-bot/state/live_copybot.db \"SELECT COUNT(*) FROM followlist WHERE active = 1; SELECT COUNT(*) FROM risk_events WHERE type = 'shadow_risk_universe_stop' AND ts >= '2026-03-17T16:32:30+00:00';\""
```

### Post-Restart Service State

- Restart timestamp: `2026-03-17T16:32:30Z`
- `solana-copy-bot.service`: `active (running)`
- `MainPID = 3384`
- `NRestarts = 0`
- `git status --short`: clean
- No crash loop observed

### Key Log Evidence

Startup:

```text
2026-03-17T16:32:30.489798Z  INFO {"message":"execution runtime disabled"}
2026-03-17T16:32:30.489801Z  INFO {"message":"startup has no recent published follow universe; recovered historical wallets stay out of runtime until discovery publishes fresh or degraded runtime truth", ... "trusted_selection_reason":"Some(\"raw_window_incomplete_no_recent_published_universe\")", ...}
2026-03-17T16:32:30.649904Z  INFO {"message":"recomputing discovery snapshots from persisted observed_swaps stream","metrics_window_start":"2026-03-12 16:30:00 UTC","raw_window_cap_truncated":true,"swaps_window":100000,"window_start":"2026-03-12 16:32:30.490611266 UTC"}
```

Observed runtime during validation:

```text
2026-03-17T16:33:30.449391Z  WARN {"message":"discovery cycle still running, skipping scheduled trigger"}
2026-03-17T16:34:30.433951Z  WARN {"message":"discovery cycle still running, skipping scheduled trigger"}
2026-03-17T16:35:30.434678Z  WARN {"message":"discovery cycle still running, skipping scheduled trigger"}
2026-03-17T16:36:30.433658Z  WARN {"message":"discovery cycle still running, skipping scheduled trigger"}
2026-03-17T16:37:30.434470Z  WARN {"message":"discovery cycle still running, skipping scheduled trigger"}
2026-03-17T16:38:30.434545Z  WARN {"message":"discovery cycle still running, skipping scheduled trigger"}
```

Shadow/runtime snapshots during the same window:

```text
active_follow_wallets=0
app_follow_rejected_ratio=1.0
observed_swap_writer_pending_requests=0
sqlite_busy_error_total=0
sqlite_write_retry_total=0
yellowstone_output_queue_fill_ratio mostly 0.0, brief samples up to 0.0410
ingestion_lag_ms_p95 mostly ~1.7s-3.7s
```

### What Did Not Appear

- No `scoring_source = raw_window_persisted_stream`
- No `discovery_runtime_mode = healthy`
- No `discovery_runtime_mode = degraded`
- No `discovery cycle completed`
- No finish log with `observed_swaps_loaded`
- No emitted `eligible_wallets`
- No emitted `wallets_seen`
- No emitted `swaps_warm_loaded` / `swaps_evicted_due_cap` in a completed-cycle summary
- No `shadow_risk_universe_stop` after restart

### DB / Runtime Facts After Observation Window

- `followlist.active = 0`
- `shadow_risk_universe_stop` events since restart: `0`
- Process state around 6 minutes after restart:
  - `%CPU ≈ 17.5`
  - `RSS ≈ 121148 KiB`
- DB files:
  - `live_copybot.db = 116G`
  - `live_copybot.db-wal = 4.4G`

### Interpretation

- The follow-up code was deployed exactly and the service remained stable.
- The new persisted-stream path definitely engaged:
  - startup moved into `recomputing discovery snapshots from persisted observed_swaps stream`
- But within the observed window, discovery did not finish the first persisted-stream cycle.
- Because the first cycle did not complete:
  - `raw_window_persisted_stream` was not published,
  - no non-zero active follow universe appeared,
  - runtime did not transition to an observable `healthy` or `degraded` cycle state.
- There was no false empty `healthy`.
- There was also no return to aggregate/backfill/bootstrap activation paths.

### Verdict

- Verdict: `degraded but acceptable`

Why:

- good:
  - exact follow-up artifact deployed
  - service stable
  - execution remained disabled
  - aggregate/backfill remained out of runtime path
  - no `shadow_risk_universe_stop`
  - no false empty `healthy`
- not good enough:
  - persisted-stream discovery did not complete a cycle in the observed window
  - `raw_window_persisted_stream` never surfaced
  - `active_follow_wallets` stayed `0`

### Next Real Blocker

- The blocker is no longer the old bootstrap-only fail-close path.
- The blocker is the operational latency / boundedness of the first persisted-stream discovery rebuild on live-size state:
  - startup enters the right path,
  - but it does not publish usable runtime truth fast enough to leave `active_follow_wallets = 0` during validation.
