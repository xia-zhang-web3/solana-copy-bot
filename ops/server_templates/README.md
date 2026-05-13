# Server Templates

Date: 2026-05-03
Status: active templates after operator-lane cleanup

This directory contains only production runtime templates that still belong to
the active Discovery V2 / artifact-first workflow.

## Active Files

1. `solana-copy-bot.service`
2. `app.env.example`
3. `live.server.toml.example`
4. `copybot-discovery-runtime-export.service`
5. `copybot-discovery-runtime-export.timer`
6. `copybot-discovery-recent-raw-snapshot.service`
7. `copybot-discovery-recent-raw-snapshot.timer`
8. `copybot-discovery-v2-prepare-quality.service`
9. `copybot-discovery-v2-prepare-quality.timer`
10. `copybot-discovery-v2-publish.service`
11. `copybot-discovery-v2-publish.timer`
12. `copybot-discovery-v2-watchdog.service`
13. `copybot-discovery-v2-watchdog.timer`

## Removed Files

The executor, adapter, execution mock upstream, devnet rehearsal, and
activation-package templates were removed from the active server surface. They
are not Discovery V2 gates and must not be restored as `copybot-app` operators.

## Deployment Rule

Do not compile release binaries on the production host as the normal rollout
path. Build artifacts locally or in CI, verify them, then install only the
required binaries and units.

Server-side builds are emergency fallback only and must be recorded as an
exception.

## Runtime Service

`solana-copy-bot.service` owns the live app process.

Required invariants:

1. `execution.enabled = false`; this template never authorizes enabling it
2. `SOLANA_COPY_BOT_CONFIG` points at `/etc/solana-copy-bot/live.server.toml`
3. the service has an explicit restart policy (`Restart=always`,
   `RestartSec=2`) and restart counters must be checked after rollout
4. runtime DB, recent_raw journal, and artifact paths live under the production
   state directory

## Discovery Timers

`copybot-discovery-runtime-export.timer` maintains the runtime artifact export.

`copybot-discovery-recent-raw-snapshot.timer` maintains the recent_raw snapshot
surface.

`copybot-discovery-v2-prepare-quality.timer` refreshes bounded observed-window
token quality evidence used by Discovery V2 gates and materializes the current
V2 status snapshot.

`copybot-discovery-v2-publish.timer` commits only a fresh materialized V2 status
snapshot on cadence. `copybot-app` live-reloads fresh publication truth from
SQLite, so this timer does not restart the daemon.

`copybot-discovery-v2-watchdog.timer` runs a read-only V2
publication/followlist guard. It exits non-zero on warning or critical states
so systemd surfaces missed publish cycles, stale publication truth, identity
mismatch, stale runtime cursor, or an empty/below-floor active followlist before
the daemon loses its current V2 wallet universe.

These timers are maintenance surfaces, not production-green proof by
themselves. Discovery V2 status / publish checks decide the current publication
contract.

## Config Contract

`live.server.toml.example` documents active production config only.

Removed config sections must stay removed:

1. raw-history gap-fill
2. program-history validation
3. program-history gap-fill
4. execution rehearsal / activation package lanes

## Rollout Checks

Before restart:

1. record current server commit and target artifact `git_sha`
2. verify service/timer state for every unit in scope
3. verify disk and memory headroom
4. verify no `cargo` or `rustc` build is running on production
5. state the exact artifact, package, binary, service, and timer touch list
6. confirm `execution.enabled = false`
7. confirm config deltas were accepted, split, or reverted
8. confirm only affected services/timers are changed

After restart:

1. `systemctl is-active solana-copy-bot.service`
2. `systemctl status --no-pager solana-copy-bot.service`
3. check recent logs for terminal writer failures
4. check recent_raw freshness if the batch touched discovery/runtime data
5. run the relevant Discovery V2 status or publish dry-run command when the
   batch affects publication truth

## Operator Boundaries

New operators must live in dedicated operator crates or the Discovery V2 crate.
Do not add them to `crates/app/src/bin`.

The active server template set is intentionally small. If a rollout requires a
new service or timer, add it with a narrow contract and update this file in the
same batch.
