# Operations Handoff And Status Report Runbook

Updated: `2026-06-03T16:50:00Z`

This file is the current operator checklist for health checks, shadow PnL
reports, execution-canary reports, and stale-close triage. It is not an
approval to change production state.

## Current Host

- Host: `ubuntu@52.28.0.218`
- Runtime DB: `/var/www/solana-copy-bot/state/live_runtime.db`
- Server config: `/etc/solana-copy-bot/live.server.toml`
- App service: `solana-copy-bot.service`
- Current app artifact:
  `/var/www/solana-copy-bot/bin/releases/copybot-app-abfab839f9be935aca912efd0883ea075ee86206/copybot-app`
- Current deployed commit: `abfab839 Quote hot buys before fee sample`
- Last canary baseline starts at: `2026-06-03T13:02:37Z`

## Non-Negotiable Rules

- Do not enable `execution.enabled`.
- Do not submit trades.
- Do not restart the daemon for read-only reports.
- Do not run `cargo build --release` on production as the normal path.
- Do not weaken fail-closed behavior.
- Do not mix stale-close PnL into normal market PnL.
- Do not call BUY quote events "trades"; closed BUY-to-SELL pairs are trades.
- Do not change thresholds or add-ons from theory. Change only from current
  canary data.

## SSH Pattern

Use the local key if present:

```bash
KEY=/Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem
HOST=ubuntu@52.28.0.218
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" '<command>'
```

For protected files and the runtime DB, run server-side commands with `sudo` or
`sudo -u copybot`.

## Fast Health Check

Run this first. It is read-only.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db

echo "TIME"
date -u +"%Y-%m-%dT%H:%M:%SZ"

echo "SERVICE"
systemctl is-active solana-copy-bot.service
systemctl show solana-copy-bot.service \
  -p ActiveEnterTimestamp -p NRestarts -p ActiveState -p SubState --no-pager

echo "ARTIFACT"
readlink -f /var/www/solana-copy-bot/bin/copybot-app || true
readlink -f /var/www/solana-copy-bot/bin/releases/*/copybot-app 2>/dev/null | tail -5 || true

echo "CONFIG_EXECUTION"
sudo awk '$0 ~ /^\[/ {section=$0}
  section=="[execution]" && ($1=="enabled" || $1=="canary_enabled" || $1=="quote_canary_enabled" ||
  $1=="quote_canary_buy_slippage_bps" || $1=="quote_canary_sell_slippage_bps") {print}' \
  /etc/solana-copy-bot/live.server.toml

echo "FAILED_UNITS"
systemctl --failed --no-pager || true

echo "BUILDS_ON_PROD"
ps -eo pid,comm,args | awk '/[c]argo|[r]ustc/ {print}' || true

echo "DISK_MEMORY"
df -h / /var/www/solana-copy-bot
free -h

echo "WARN_ERROR_LAST_30M"
journalctl -u solana-copy-bot.service --since "30 minutes ago" --no-pager \
  | grep -E " WARN | ERROR | panic | panicked" || true

echo "INGESTION_LAST"
journalctl -u solana-copy-bot.service --since "20 minutes ago" --no-pager \
  | grep "ingestion pipeline metrics" | tail -1 || true

echo "SHADOW_SNAPSHOT_LAST"
journalctl -u solana-copy-bot.service --since "20 minutes ago" --no-pager \
  | grep "shadow snapshot" | tail -1 || true

echo "ACTIVE_FOLLOWLIST"
sudo -u copybot sqlite3 -readonly "$DB" \
  "select count(*) from followlist where active=1;"
REMOTE
```

Normal result:

- service is `active`
- restart count is unchanged unless a rollout just happened
- no failed units
- no cargo/rustc process on production
- app WARN/ERROR is empty for the recent window
- ingestion has `stream_gap_detected=0`, `ws_notifications_dropped=0`,
  `rpc_429=0`, `rpc_5xx=0`
- active follow wallets is `15`

`ws_notifications_replaced_oldest` or `ws_notifications_backpressured` can be
non-zero. Treat it as a warning only if drops/gaps rise or queue fill stays high.

## Discovery V2 Check

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db

sudo -u copybot sqlite3 "$DB" \
  "select status_json from discovery_v2_status_snapshot order by updated_at desc limit 1;" \
  | jq '{
      green:.production_green,
      candidates:(.candidate_wallets|length),
      scan:.scan,
      blockers:.blockers,
      published_now:.now,
      window_minutes:.window_minutes
    }'

sudo -u copybot sqlite3 -header -column "$DB" \
  "select sum(active=1) active, sum(active=0) inactive, count(*) total from followlist;"
REMOTE
```

Alert if production green is false, blockers are non-empty, candidates are below
15, or active follow wallets are below 15.

## Shadow Market PnL Report

Use this for the normal short status. It separates market closes from stale
closes. Stale closes are not normal trading results.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="${SINCE:-2026-06-03T13:02:37Z}"

echo "MARKET_24H"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select count(*) closed,
       sum(case when pnl_sol > 0 then 1 else 0 end) wins,
       sum(case when pnl_sol < 0 then 1 else 0 end) losses,
       round(coalesce(sum(pnl_sol),0),6) pnl_sol
from shadow_closed_trades
where closed_ts >= datetime('now','-24 hours')
  and coalesce(close_context,'market')='market';"

echo "MARKET_SINCE"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select count(*) closed,
       sum(case when pnl_sol > 0 then 1 else 0 end) wins,
       sum(case when pnl_sol < 0 then 1 else 0 end) losses,
       round(coalesce(sum(pnl_sol),0),6) pnl_sol,
       min(closed_ts) first_close,
       max(closed_ts) last_close
from shadow_closed_trades
where closed_ts >= '$SINCE'
  and coalesce(close_context,'market')='market';"

echo "CLOSE_CONTEXT_24H"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select coalesce(close_context,'market') close_context,
       count(*) closed,
       sum(case when pnl_sol > 0 then 1 else 0 end) wins,
       sum(case when pnl_sol < 0 then 1 else 0 end) losses,
       round(coalesce(sum(pnl_sol),0),6) pnl_sol
from shadow_closed_trades
where closed_ts >= datetime('now','-24 hours')
group by coalesce(close_context,'market')
order by close_context;"

echo "OPEN_LOTS"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select count(*) open_lots,
       round(coalesce(sum(cost_sol),0),6) open_cost_sol,
       min(opened_ts) oldest_open,
       max(opened_ts) newest_open
from shadow_lots;"

sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select id,
       substr(wallet_id,1,10)||'...' wallet,
       substr(token,1,10)||'...' token,
       round(cost_sol,6) cost_sol,
       opened_ts,
       accounting_bucket,
       risk_context
from shadow_lots
order by opened_ts;"
REMOTE
```

User-facing summary should be short:

```text
Service: active, restarts 0
Warnings/errors: none
24h market: <closed> closed, win/loss <wins>/<losses>, PnL <pnl> SOL
Post-fix market: <closed> closed, win/loss <wins>/<losses>, PnL <pnl> SOL
Open: <open_lots>, exposure <open_cost> SOL
Stale close: <count> in window, PnL <pnl> SOL
```

## Execution Canary Quote PnL Report

Canary BUY quote events are not trades. Use closed market pairs to evaluate
thresholds.

If the operator artifact is installed, prefer:

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="${SINCE:-2026-06-03T13:02:37Z}"
BIN=/var/www/solana-copy-bot/bin/copybot_execution_canary_quote_pnl

if test -x "$BIN"; then
  "$BIN" --db-path "$DB" --since "$SINCE" --limit 200 --json | jq '.summary | {
    total_closed_trades,
    pnl_counted_trades,
    skipped_trades,
    unknown_trades,
    shadow_win_count,
    shadow_loss_count,
    shadow_pnl_sol,
    quote_win_count,
    quote_loss_count,
    quote_adjusted_pnl_after_priority_fee_sol,
    skipped_shadow_pnl_sol,
    stale:.shadow_close_breakdown.contexts
  }'
else
  echo "operator_not_installed"
fi
REMOTE
```

Fallback SQLite report for `150/300/500/1000 bps`:

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="${SINCE:-2026-06-03T13:02:37Z}"

sudo -u copybot sqlite3 -header -column -readonly "$DB" "
with buy as (
  select * from execution_quote_canary_events where lower(side)='buy'
),
joined as (
  select closed.id,
         closed.pnl_sol shadow_pnl,
         buy.slippage_bps buy_slip,
         buy.quote_status buy_qs,
         sell.quote_status sell_qs,
         buy.decision_status buy_decision,
         sell.decision_status sell_decision,
         cast(buy.quote_in_amount_raw as real) entry_in,
         cast(buy.quote_out_amount_raw as real) entry_out,
         cast(sell.quote_in_amount_raw as real) exit_in,
         cast(sell.quote_out_amount_raw as real) exit_out,
         coalesce(buy.priority_fee_lamports,0)+coalesce(sell.priority_fee_lamports,0) fee_lamports
  from shadow_closed_trades closed
  left join execution_quote_canary_events sell
    on sell.shadow_closed_trade_id=closed.id and lower(sell.side)='sell'
  left join buy
    on buy.event_id=(
      select candidate.event_id
      from execution_quote_canary_events candidate
      where lower(candidate.side)='buy'
        and candidate.wallet_id=closed.wallet_id
        and candidate.token=closed.token
        and substr(candidate.signal_ts,1,19)=substr(closed.opened_ts,1,19)
      order by candidate.request_ts desc, candidate.event_id desc
      limit 1
    )
  where closed.closed_ts >= '$SINCE'
    and coalesce(closed.close_context,'market')='market'
),
calc as (
  select *,
    case when entry_in>0 and entry_out>0 and exit_in>0 and exit_out is not null
      then (exit_out/1000000000.0)*(min(exit_in,entry_out)/exit_in)
         - (entry_in/1000000000.0)*(min(exit_in,entry_out)/entry_out)
         - fee_lamports/1000000000.0
    end quote_pnl_fee
  from joined
),
thresholds(label, max_bps) as (
  values ('all', null), ('le150',150), ('le300',300), ('le500',500), ('le1000',1000)
)
select label,
       count(*) closed_market,
       sum(case when buy_qs='ok' and sell_qs='ok'
                 and (max_bps is null or buy_slip<=max_bps)
                 and quote_pnl_fee is not null then 1 else 0 end) counted,
       round(sum(case when max_bps is null or buy_slip<=max_bps then shadow_pnl else 0 end),6)
         shadow_pnl_sol,
       round(sum(case when buy_qs='ok' and sell_qs='ok'
                       and (max_bps is null or buy_slip<=max_bps)
                      then quote_pnl_fee else 0 end),6) quote_pnl_after_fee_sol,
       sum(case when (max_bps is null or buy_slip<=max_bps) and quote_pnl_fee>0 then 1 else 0 end)
         quote_wins,
       sum(case when (max_bps is null or buy_slip<=max_bps) and quote_pnl_fee<0 then 1 else 0 end)
         quote_losses,
       sum(case when buy_decision='would_skip'
                 and (max_bps is null or buy_slip<=max_bps) then 1 else 0 end) skipped_entries
from thresholds
cross join calc
group by label, max_bps
order by case label when 'all' then 0 when 'le150' then 1 when 'le300' then 2
                    when 'le500' then 3 else 4 end;"
REMOTE
```

Interpretation:

- `le150` means entry slippage vs leader is at most `1.5%`.
- `le300` means at most `3%`.
- `le500` means at most `5%`.
- At `0.2 SOL`, those are roughly `0.003`, `0.006`, and `0.010 SOL`.
- A higher threshold is only better if `quote_pnl_after_fee_sol` improves on a
  meaningful sample of closed pairs.

## Canary Event Counters

Use this to explain "why are there thousands of events?"

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="${SINCE:-2026-06-03T13:02:37Z}"

sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select side,
       decision_status,
       quote_status,
       priority_fee_status,
       count(*) events,
       round(avg(decision_delay_ms),1) avg_delay_ms,
       round(avg(quote_latency_ms),1) avg_quote_ms,
       round(avg(case when abs(slippage_bps)<10000 then slippage_bps end),1) avg_slippage_bps,
       round(avg(price_impact_pct),4) avg_price_impact
from execution_quote_canary_events
where request_ts >= '$SINCE'
group by side, decision_status, quote_status, priority_fee_status
order by side, decision_status, priority_fee_status;"
REMOTE
```

Report BUY quote event count separately from closed trades. Example wording:
`BUY quote events: 2891; closed market pairs: 18`.

## Stale Close Triage

Stale close is emergency accounting, not a normal market close. If stale closes
appear after the current post-fix baseline, investigate immediately.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="${SINCE:-2026-06-03T13:02:37Z}"

echo "STALE_SINCE"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select coalesce(close_context,'market') close_context,
       count(*) closed,
       round(coalesce(sum(pnl_sol),0),6) pnl_sol,
       min(closed_ts) first_close,
       max(closed_ts) last_close
from shadow_closed_trades
where closed_ts >= '$SINCE'
group by coalesce(close_context,'market')
order by close_context;"

echo "STALE_ROWS_RECENT"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select id,
       substr(wallet_id,1,10)||'...' wallet,
       substr(token,1,10)||'...' token,
       round(entry_cost_sol,6) entry,
       round(exit_value_sol,6) exit,
       round(pnl_sol,6) pnl,
       opened_ts,
       closed_ts,
       close_context
from shadow_closed_trades
where coalesce(close_context,'market')!='market'
order by closed_ts desc
limit 20;"
REMOTE
```

If `stale_quote_price` or `stale_terminal_zero_price` appears after the
baseline, say that stale is back and do not hide it inside total PnL. It usually
means a position missed normal SELL handling or became unsafe to price.

`SELL would_force_exit` is different from stale. It means a SELL quote exceeded
the normal slippage threshold, but the canary would still close an already-owned
position instead of leaving it hanging.

## Loss Driver Report

Run this when market PnL is bad or when a threshold gets worse.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
SINCE="${SINCE:-2026-06-03T13:02:37Z}"

echo "BY_WALLET"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select substr(wallet_id,1,12)||'...' wallet,
       count(*) closed,
       count(distinct token) tokens,
       round(sum(entry_cost_sol),6) entry,
       round(sum(pnl_sol),6) pnl
from shadow_closed_trades
where closed_ts >= '$SINCE'
  and coalesce(close_context,'market')='market'
group by wallet_id
order by pnl asc
limit 20;"

echo "BY_TOKEN"
sudo -u copybot sqlite3 -header -column -readonly "$DB" "
select substr(token,1,12)||'...' token,
       count(*) closed,
       count(distinct wallet_id) wallets,
       round(sum(entry_cost_sol),6) entry,
       round(sum(pnl_sol),6) pnl
from shadow_closed_trades
where closed_ts >= '$SINCE'
  and coalesce(close_context,'market')='market'
group by token
order by pnl asc
limit 20;"
REMOTE
```

## Useful Logs

```bash
journalctl -u solana-copy-bot.service --since "30 min ago" --no-pager
journalctl -u solana-copy-bot.service --since "10 min ago" --no-pager | grep "ingestion pipeline metrics" | tail -5
journalctl -u solana-copy-bot.service --since "30 min ago" --no-pager | grep "shadow snapshot" | tail -10
journalctl -u solana-copy-bot.service --since "30 min ago" --no-pager | grep "execution canary" | tail -40
systemctl list-timers --all --no-pager | grep copybot
journalctl -u copybot-discovery-v2-watchdog.service --since "30 min ago" --no-pager
journalctl -u copybot-discovery-v2-publish.service --since "2 hours ago" --no-pager
journalctl -u copybot-discovery-v2-prepare-quality.service --since "2 hours ago" --no-pager
```

## Short User Report Template

Keep reports short unless debugging is requested.

```text
Service: active, restarts <n>, warnings/errors <none/count>
Ingestion: gaps/drops/429/5xx <values>
24h market: <closed> closed, win/loss <w>/<l>, PnL <x> SOL
Post-fix market: <closed> closed, win/loss <w>/<l>, PnL <x> SOL
Canary:
- closed pairs <n>
- 150 bps: <n> counted, PnL after fee <x> SOL
- 300/500 bps: <n> counted, PnL after fee <x> SOL
Stale close: <none/count and pnl>
Open: <n> lots, exposure <x> SOL
Decision: observe / change threshold / investigate stale / investigate delay
```

## Current Interpretation Rules

- Wait for at least `20-30` post-fix closed market pairs before changing BUY
  slippage threshold.
- `150 bps` remains the default unless `300` or `500` improves quote PnL after
  fee on closed pairs.
- Quote latency around `90ms` means the HTTP quote path is not the primary
  bottleneck. If overall decision delay is still around `2s`, investigate event
  timing and ingestion path first.
- Do not buy paid add-ons unless current data shows rate limits, quote errors,
  route failures, submit failures, or landing latency that the add-on directly
  addresses.
- Any new stale close after the post-fix baseline is a separate incident from
  normal market PnL.
