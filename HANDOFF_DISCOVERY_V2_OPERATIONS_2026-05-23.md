# Discovery V2 Operations Handoff - 2026-05-23

This is the operational handoff for the next agent/operator while the owner is
away. Keep this file as a working checklist; do not treat it as acceptance by
itself.

## Current Production State

Last verified: `2026-05-23T14:14:35Z` (`17:14` Kyiv).

Production host:

- Host: `ubuntu@52.28.0.218`
- Runtime DB: `/var/www/solana-copy-bot/state/live_runtime.db`
- Server config: `/etc/solana-copy-bot/live.server.toml`
- App service: `solana-copy-bot`
- App artifact in use:
  `/var/www/solana-copy-bot/bin/releases/copybot-app-978fe13a4666a39db5d17084bb5df7f787349304/copybot-app`
- Repo commit deployed: `978fe13a Fix shadow infra progress baseline`

Current health:

- `solana-copy-bot`: active
- `NRestarts=0`
- `execution.enabled = false`
- V2 watchdog: `state=ok`
- V2 publication mode: `healthy`
- Active follow wallets: `15`
- V2 candidates: `15`
- V2 blockers: `[]`
- Recent app `WARN`/`ERROR` after `2026-05-23T06:57Z`: none

Latest V2 scan at the last check:

- Rows scanned: `11169532`
- Unique wallets: `404494`
- Row budget: `15000000`
- Row budget exhausted: `false`
- Time budget exhausted: `false`

Latest trading state at the last check:

- Since `2026-05-23T11:40:00Z`:
  - Buy signals: `2`, notional `0.4 SOL`
  - Sell signals: `10`, notional `2.0 SOL`
  - Opened shadow positions: `3`, cost `0.4 SOL`
  - Closed rows: `3`
  - Closed lot keys: `3`
  - PnL: `-0.054171 SOL`
- Day from `2026-05-23T00:00:00Z`:
  - Closed rows: `18`
  - Closed lot keys: `17`
  - Entry: `3.369096 SOL`
  - Exit: `3.089728 SOL`
  - PnL: `-0.279368 SOL`
- Currently open:
  - `1` lot
  - Cost: `0.030904 SOL`
  - Opened at: `2026-05-23T13:50:55.454075620Z`

Interpretation:

- Infrastructure is stable.
- Discovery V2 is working and publishing.
- The shadow strategy is still not proven profitable. Keep observing and report
  wallet/token level loss drivers before changing filters.

## Non-Negotiable Rules

- Do not enable `execution.enabled`.
- Do not submit real trades.
- Do not build release binaries on the production host as the normal path.
- Do not restart the daemon for read-only reports.
- Do not weaken V2 fail-closed gates.
- Do not widen discovery windows or loosen filters without an explicit owner
  decision.
- Do not delete or rewrite production DB data during observation.

## SSH Pattern

Use a key outside the repository. On the current Mac the key lived under
`/Users/blacktower/Documents/keys`, but a new machine may have a different path.

```bash
ssh -i <KEY_PATH> -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  ubuntu@52.28.0.218 '<command>'
```

When reading protected files or the runtime DB, use `sudo` on the server.

## Fast Health Check

Run this first every time.

```bash
ssh -i <KEY_PATH> -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  ubuntu@52.28.0.218 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db

echo "TIME"
date -u +"%Y-%m-%dT%H:%M:%SZ"

echo "SERVICE"
systemctl is-active solana-copy-bot
systemctl show solana-copy-bot -p MainPID -p NRestarts -p ActiveState -p SubState --no-pager

echo "ARTIFACT"
readlink -f /var/www/solana-copy-bot/bin/packages/copybot-app/current/copybot-app

echo "EXECUTION"
sudo awk '$0 ~ /^\[/ {section=$0} section=="[execution]" && $1=="enabled" {print}' \
  /etc/solana-copy-bot/live.server.toml

echo "FAILED_UNITS"
systemctl --failed --no-pager || true

echo "BUILDS_ON_PROD"
ps -eo pid,comm,args | awk '/[c]argo|[r]ustc/ {print}' || true

echo "DISK"
df -h /var/www/solana-copy-bot /tmp

echo "MEMORY"
free -h

echo "APP_WARN_ERROR"
journalctl -u solana-copy-bot --since "2026-05-23 06:57:00 UTC" --no-pager \
  | grep -E "WARN|ERROR|shadow risk infra stop" || true

echo "WATCHDOG_LAST"
journalctl -u copybot-discovery-v2-watchdog.service --since "20 min ago" --no-pager \
  | tail -n 35 || true
REMOTE
```

Expected normal result:

- service active
- `NRestarts=0` unless an intentional rollout just happened
- `execution.enabled = false`
- no failed units
- no `cargo` or `rustc`
- no app `WARN`/`ERROR`
- watchdog `state=ok`, `publication_runtime_mode=healthy`, active wallets `15`

## V2 Status Report

```bash
ssh -i <KEY_PATH> -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  ubuntu@52.28.0.218 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db

sudo sqlite3 "$DB" \
  "select status_json from discovery_v2_status_snapshot order by updated_at desc limit 1;" \
  | jq '{
      green:.production_green,
      candidates:(.candidate_wallets|length),
      scan:.scan,
      blockers:.blockers,
      published_now:.now,
      window_minutes:.window_minutes
    }'

sudo sqlite3 -header -column "$DB" \
  "select sum(active=1) active, sum(active=0) inactive, count(*) total from followlist;"
REMOTE
```

Alert if:

- `green` is not `true`
- candidate count is below `15`
- active follow wallets is below `15`
- blockers is not empty
- `max_rows_exhausted`, `time_budget_exhausted`, or `budget_exhausted` is true

## Shadow Trading Report

Use UTC timestamps. Kyiv time is UTC+3 on this date.

For a report since the last check, set `SINCE` to the last report time. For a
day report, use `DAY=YYYY-MM-DDT00:00:00+00:00`.

```bash
ssh -i <KEY_PATH> -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  ubuntu@52.28.0.218 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="2026-05-23T11:40:00+00:00"
DAY="2026-05-23T00:00:00+00:00"

echo "SIGNALS_SINCE"
sudo sqlite3 -header -column "$DB" "
select side,status,count(*) as n,round(coalesce(sum(notional_sol),0),6) as notional_sol
from copy_signals
where ts >= '$SINCE'
group by side,status
order by side,status;"

echo "OPENED_SINCE"
sudo sqlite3 -header -column "$DB" "
select
  (select count(*) from shadow_closed_trades where opened_ts >= '$SINCE')
  + (select count(*) from shadow_lots where opened_ts >= '$SINCE') as opened,
  round(
    (select coalesce(sum(entry_cost_sol),0) from shadow_closed_trades where opened_ts >= '$SINCE')
    + (select coalesce(sum(cost_sol),0) from shadow_lots where opened_ts >= '$SINCE'),
    6
  ) as opened_cost_sol;"

echo "CLOSED_SINCE"
sudo sqlite3 -header -column "$DB" "
select
  count(*) as closed_rows,
  count(distinct wallet_id||':'||token||':'||opened_ts) as closed_lot_keys,
  round(coalesce(sum(entry_cost_sol),0),6) as entry_sol,
  round(coalesce(sum(exit_value_sol),0),6) as exit_sol,
  round(coalesce(sum(pnl_sol),0),6) as pnl_sol
from shadow_closed_trades
where closed_ts >= '$SINCE';"

echo "DAY_CLOSED"
sudo sqlite3 -header -column "$DB" "
select
  count(*) as closed_rows,
  count(distinct wallet_id||':'||token||':'||opened_ts) as closed_lot_keys,
  round(coalesce(sum(entry_cost_sol),0),6) as entry_sol,
  round(coalesce(sum(exit_value_sol),0),6) as exit_sol,
  round(coalesce(sum(pnl_sol),0),6) as pnl_sol
from shadow_closed_trades
where closed_ts >= '$DAY';"

echo "OPEN_NOW"
sudo sqlite3 -header -column "$DB" "
select
  count(*) as open_lots,
  round(coalesce(sum(cost_sol),0),6) as open_cost_sol,
  min(opened_ts) as oldest_open,
  max(opened_ts) as newest_open
from shadow_lots;"

echo "OPEN_ROWS"
sudo sqlite3 -header -column "$DB" "
select
  id,
  substr(wallet_id,1,8)||'...' wallet,
  substr(token,1,8)||'...' token,
  round(cost_sol,6) cost,
  opened_ts,
  accounting_bucket,
  risk_context
from shadow_lots
order by opened_ts;"

echo "CLOSED_ROWS_SINCE"
sudo sqlite3 -header -column "$DB" "
select
  id,
  substr(wallet_id,1,8)||'...' wallet,
  substr(token,1,8)||'...' token,
  round(entry_cost_sol,6) entry,
  round(exit_value_sol,6) exit,
  round(pnl_sol,6) pnl,
  opened_ts,
  closed_ts,
  close_context
from shadow_closed_trades
where closed_ts >= '$SINCE'
order by closed_ts;"
REMOTE
```

Report back with:

- buy signal count and notional
- sell signal count and notional
- opened count and opened cost
- closed count, entry, exit, PnL
- current open lots and exposure
- worst closed rows by PnL
- whether losses cluster by wallet or token

## Wallet And Token Loss Driver Report

Run this when the PnL is negative.

```bash
ssh -i <KEY_PATH> -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  ubuntu@52.28.0.218 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="2026-05-23T00:00:00+00:00"

echo "BY_WALLET"
sudo sqlite3 -header -column "$DB" "
select
  substr(wallet_id,1,12)||'...' wallet,
  count(*) closed_rows,
  count(distinct token) tokens,
  round(sum(entry_cost_sol),6) entry,
  round(sum(pnl_sol),6) pnl
from shadow_closed_trades
where closed_ts >= '$SINCE'
group by wallet_id
order by pnl asc
limit 20;"

echo "BY_TOKEN"
sudo sqlite3 -header -column "$DB" "
select
  substr(token,1,12)||'...' token,
  count(*) closed_rows,
  count(distinct wallet_id) wallets,
  round(sum(entry_cost_sol),6) entry,
  round(sum(pnl_sol),6) pnl
from shadow_closed_trades
where closed_ts >= '$SINCE'
group by token
order by pnl asc
limit 20;"
REMOTE
```

Use this report to decide whether a wallet-level cooldown, token-level cooldown,
or copy-delay/slippage rule needs adjustment. Do not change filters blindly.

## Useful Logs

```bash
journalctl -u solana-copy-bot --since "30 min ago" --no-pager
journalctl -u solana-copy-bot --since "10 min ago" --no-pager | grep "ingestion pipeline metrics" | tail -n 5
journalctl -u solana-copy-bot --since "30 min ago" --no-pager | grep "shadow snapshot" | tail -n 10
systemctl list-timers --all --no-pager | grep copybot-discovery
journalctl -u copybot-discovery-v2-watchdog.service --since "30 min ago" --no-pager
journalctl -u copybot-discovery-v2-publish.service --since "2 hours ago" --no-pager
journalctl -u copybot-discovery-v2-prepare-quality.service --since "2 hours ago" --no-pager
```

## Known Recent Fixes

- `978fe13a Fix shadow infra progress baseline`: deployed active app artifact;
  no-progress now uses a pre-window progress baseline while rolling ratio gates
  still use an in-window baseline.
- `fb93e742 Raise V2 scan row budget`: live V2 row budget is `15000000`.

## Rollout Rules For The Next Agent

For `copybot-app` changes: local fix, narrow tests, `cargo fmt --all -- --check`,
`git diff --check`, `tools/architecture_guard.sh --changed`, commit, push, wait
for GitHub `Operator Artifacts`, install `live-daemon-<sha>-linux-x86_64` by
artifact installer dry-run plus install, restart only `solana-copy-bot`, then
verify service, restart count, logs, V2 watchdog, and shadow state.

Do not use production-local release builds except as an explicitly recorded
emergency fallback.

## What To Do If Something Breaks

If `solana-copy-bot` is inactive, read app logs, disk, memory, failed units,
and production build processes before changing config. If V2 watchdog is not
ok, read watchdog JSON, status blockers, and prepare/publish logs; do not
publish from red status. If shadow PnL is negative, run the wallet/token loss
driver report and propose a bounded rule. If `shadow risk infra stop` appears
again, collect surrounding logs and compare ingestion counters; do not disable
the infra gate.

## Evening Report Template

```text
Time window:
Service:
Artifact:
Execution:
V2:
Active wallets:
Warnings/errors:
Signals:
- buy:
- sell:
Positions:
- opened:
- closed:
- currently open:
PnL:
- closed PnL:
- open exposure:
- worst wallets:
- worst tokens:
Interpretation:
Next proposed action:
```
