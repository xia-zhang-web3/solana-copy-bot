# Yellowstone Rollout Runbook

Status: historical/deprecated.

Yellowstone is already the active production ingestion source. This file is not
an active production cutover, failover, rollback, or daemon deployment
procedure.

Current operations must use:

1. `ARTIFACT_DEPLOY.md` for artifact install, rollback, and daemon restart
   procedure.
2. `ops/ingestion_failover_watchdog.md` for watchdog wiring.
3. `ops/server_templates/live.server.toml.example` for the active production
   config template.

Do not use this historical runbook to:

1. change `ingestion.source`,
2. install or roll back binaries,
3. restart `solana-copy-bot.service`,
4. load watchdog state files as systemd `EnvironmentFile`,
5. reintroduce removed websocket fallback behavior.

Historical migration notes may be recovered from git history if needed for
forensics. They are intentionally not kept here as runnable operator commands.
