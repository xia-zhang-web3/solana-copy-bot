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
3. the service runs with a bounded restart policy
4. runtime DB, recent_raw journal, and artifact paths live under the production
   state directory

## Discovery Timers

`copybot-discovery-runtime-export.timer` maintains the runtime artifact export.

`copybot-discovery-recent-raw-snapshot.timer` maintains the recent_raw snapshot
surface.

Both timers are maintenance surfaces, not production-green proof by themselves.
Discovery V2 status / publish checks decide the current publication contract.

## Config Contract

`live.server.toml.example` documents active production config only.

Removed config sections must stay removed:

1. raw-history gap-fill
2. program-history validation
3. program-history gap-fill
4. execution rehearsal / activation package lanes

## Rollout Checks

Before restart:

1. confirm the artifact version to install
2. confirm `execution.enabled = false`
3. confirm only affected services/timers are changed

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
