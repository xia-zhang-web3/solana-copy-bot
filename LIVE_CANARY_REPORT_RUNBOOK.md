# Live Canary Report Runbook

Updated: `2026-06-10`

This is the current production report runbook for shadow trading, quote canary,
Metis swap dry-run proof, tiny execution readiness, and stale-close triage. It
is not an approval to change production state.

## Current Stage

- Shadow trading uses fixed `0.2 SOL` accounting.
- Quote canary is the main measurement path for entry/exit quality.
- BUY slippage threshold is currently evaluated around `1000 bps`.
- Metis swap-instructions dry-run is measured through the canary route.
- Metis swap transaction dry-run may also be measured when
  `swap_transaction_dry_run_enabled=true`.
- Public-vs-paid generic quote comparison is measured when
  `quote_canary_public_parallel_enabled=true`; it compares public Jupiter/Metis
  `/quote` against paid/private Metis `/quote` on the same event. Guarded tiny
  execution prefers a buildable paid Metis generic quote; public generic is a
  fallback when Metis is unavailable, not buildable, or errors.
- Paid Metis Pump.fun quote comparison is measured when
  `quote_canary_pump_fun_parallel_enabled=true`; it compares the old generic
  Metis/Jupiter `/quote` result against paid `/pump-fun/quote` on the same
  event. `Bonding curve for mint not found` usually means the token is already
  migrated to Pump.fun AMM, so the public-vs-paid generic comparison is the
  relevant paid-route signal for that event.
- Selected provider is now explicit:
  - `pump_fun_paid` only when paid `/pump-fun/quote` is OK and
    `quote.meta.isCompleted=false`
  - `generic_metis` for paid generic Metis quotes when it is the best generic
    route, including migrated Pump.fun AMM/Raydium/Orca routes
  - `generic_public` when public generic slippage is better or paid generic is
    unavailable
  - `Bonding curve for mint not found` is a route mismatch, not a system error
- Builder dry-run uses paid Metis first. If paid `/swap-instructions` or
  `/swap` returns `Missing token program`, it retries the same dry-run through
  public Jupiter and records `metis_swap_*_public_fallback_ok` in simulation
  proof. This fallback is dry-run only.
- Priority Fee API is measured and included in quote PnL after fee.
- Guarded tiny execution may submit real tiny trades when
  `execution.canary_tiny_submit_enabled=true` and
  `execution.canary_entry_submit_enabled=true`. Broad `execution.enabled` is a
  separate production cutover and must remain off unless explicitly approved.
- Tiny execution gate now loads the live execution config when the report is
  run with `--config`; use that mode for readiness/preflight checks.
- Treat `tiny_execution_gate.runtime_status`, `can_open_new_tiny_entries`,
  `can_process_tiny_sells`, `runtime_blockers`, and `why_not_trading_now` as
  the live on/off proof. `startup_readiness_status` is a startup/readiness
  aggregate and can be blocked while runtime tiny live remains enabled.
- Main quote PnL now includes `buy_shadow_gate`: quote-approved BUYs are split
  into `shadow_recorded`, `shadow_dropped`, and `shadow_pending`. This is the
  source of truth for whether a good quote would actually become a shadow entry.
  Strategy filters such as `below_notional` and `low_holders` are warnings, not
  execution blockers; unexpected drops such as `recent_sell_cooldown` remain
  blockers.

Use the newest relevant rollout/config timestamp as `SINCE`. Do not use a
rolling 24h window as the main answer unless the user asks for it.

## Rules

- Do not enable `execution.enabled`.
- Do not manually submit trades outside the daemon path.
- Do not restart the daemon for read-only reports.
- Do not run release builds on production for reports.
- Do not mix stale/rug-like closes into normal market PnL.
- Do not call BUY quote events "trades"; closed BUY-to-SELL pairs are trades.
- Do not hand-build a new SQLite report unless the operator is missing or
  failed, and say that it is fallback.
- `copybot_execution_canary_quote_pnl --limit` max is `200`.
- `copybot_execution_canary_readiness` does not support `--since`; use `--limit`.

## Environment

```bash
KEY=/Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem
HOST=ubuntu@52.28.0.218
DB=/var/www/solana-copy-bot/state/live_runtime.db
CONFIG=/etc/solana-copy-bot/live.server.toml
QUOTE_BIN=/var/www/solana-copy-bot/bin/copybot_execution_canary_quote_pnl
READY_BIN=/var/www/solana-copy-bot/bin/copybot_execution_canary_readiness

# Set this explicitly for every report.
SINCE=2026-06-05T10:00:00Z
LIMIT=200
```

## Health First

Run this before any trading report.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db

echo "TIME"
date -u +"%Y-%m-%dT%H:%M:%SZ"

echo "SERVICE"
systemctl show solana-copy-bot.service \
  -p ActiveState -p SubState -p MainPID -p NRestarts --no-pager

echo "ARTIFACT"
readlink -f /var/www/solana-copy-bot/bin/copybot-app || true

echo "BUILDS_ON_PROD"
ps -eo pid,comm,args | awk '/[c]argo|[r]ustc/ {print}' || true

echo "DISK_MEMORY"
df -h / /var/www/solana-copy-bot/state
free -h

echo "CONFIG_EXECUTION"
sudo awk '$0 ~ /^\[/ {section=$0}
  section=="[execution]" && (
    $1=="enabled" ||
    $1=="canary_enabled" ||
    $1=="canary_dry_run" ||
    $1=="canary_route" ||
    $1=="quote_canary_enabled" ||
    $1=="quote_canary_pump_fun_parallel_enabled" ||
    $1=="quote_canary_buy_slippage_bps" ||
    $1=="quote_canary_sell_slippage_bps" ||
    $1=="swap_instructions_dry_run_enabled" ||
    $1=="swap_transaction_dry_run_enabled"
  ) {print}' /etc/solana-copy-bot/live.server.toml

echo "WARN_ERROR_LAST_30M"
journalctl -u solana-copy-bot.service --since "30 minutes ago" --no-pager \
  | grep -E " WARN | ERROR | panic | panicked" || true

echo "INGESTION_LAST"
journalctl -u solana-copy-bot.service --since "20 minutes ago" --no-pager \
  | grep "ingestion pipeline metrics" | tail -1 || true

echo "SQLITE_LAST"
journalctl -u solana-copy-bot.service --since "20 minutes ago" --no-pager \
  | grep "sqlite contention counters" | tail -1 || true

echo "OPEN_LOTS"
sudo -u copybot sqlite3 -readonly -header -column "$DB" "
select count(*) open_lots,
       round(coalesce(sum(cost_sol),0),6) open_cost_sol,
       min(opened_ts) oldest_open,
       max(opened_ts) newest_open
from shadow_lots;"
REMOTE
```

Normal health:

- service `ActiveState=active`, `SubState=running`
- `NRestarts=0` unless a rollout just happened
- no `cargo` or `rustc` on production
- ingestion has `stream_gap_detected=0`, `ws_notifications_dropped=0`,
  `rpc_429=0`, `rpc_5xx=0`
- SQLite `busy` and `write_retry` are not increasing

## Main Canary PnL Report

This is the primary report for the current development stage.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" \
  "SINCE='$SINCE' LIMIT='$LIMIT' bash -s" <<'REMOTE'
set -euo pipefail
CONFIG=/etc/solana-copy-bot/live.server.toml
BIN=/var/www/solana-copy-bot/bin/copybot_execution_canary_quote_pnl

REPORT="$(sudo -u copybot "$BIN" --config "$CONFIG" --since "$SINCE" --limit "$LIMIT" --json)"
printf '%s\n' "$REPORT" | jq -r '
if .reason_class != "execution_canary_quote_pnl_loaded" then
  "QUOTE_PNL_ERROR reason=\(.reason_class) error=\(.error)"
else
  .summary as $s |
  .tiny_execution_gate as $g |
  (.provider_comparison // {}) as $pc |
  (.public_paid_comparison // {}) as $ppc |
  (.provider_selection // {}) as $sel |
  "WINDOW since=\(.since) as_of=\(.as_of) limit=\($s.limit)",
  "SHADOW_TOTAL closed=\($s.shadow_close_breakdown.total_closed_trades) win_loss=\($s.shadow_close_breakdown.total_win_count)/\($s.shadow_close_breakdown.total_loss_count) pnl=\($s.shadow_close_breakdown.total_pnl_sol)",
  "SHADOW_MARKET closed=\($s.shadow_close_breakdown.market_closed_trades) pnl=\($s.shadow_close_breakdown.market_pnl_sol)",
  "SHADOW_STALE closed=\($s.shadow_close_breakdown.stale_closed_trades) rug_like=\($s.shadow_close_breakdown.stale_rug_like_closed_trades) pnl=\($s.shadow_close_breakdown.stale_pnl_sol)",
  "CANARY_MARKET closed=\($s.total_closed_trades) counted=\($s.pnl_counted_trades) skipped=\($s.skipped_trades) unknown=\($s.unknown_trades)",
  "CANARY_PNL quote_win_loss=\($s.quote_win_count)/\($s.quote_loss_count) shadow_pnl=\($s.shadow_pnl_sol) quote_after_fee=\($s.quote_adjusted_pnl_after_priority_fee_sol) delta_after_fee=\($s.quote_after_fee_vs_shadow_delta_sol)",
  "SKIPS skipped_shadow_pnl=\($s.skipped_shadow_pnl_sol) skipped_counterfactual_after_fee=\($s.skipped_counterfactual_pnl_after_priority_fee_sol)",
  "BUY_SHADOW_GATE quote_buy=\($s.buy_shadow_gate.total_buy_quote_events) quote_would_execute=\($s.buy_shadow_gate.quote_would_execute_events) recorded=\($s.buy_shadow_gate.shadow_recorded_events) dropped=\($s.buy_shadow_gate.shadow_dropped_events) pending=\($s.buy_shadow_gate.shadow_pending_events) execute_recorded=\($s.buy_shadow_gate.quote_would_execute_shadow_recorded_events) execute_dropped=\($s.buy_shadow_gate.quote_would_execute_shadow_dropped_events) execute_pending=\($s.buy_shadow_gate.quote_would_execute_shadow_pending_events)",
  "BUY_SHADOW_GATE_REASONS " + (
    $s.buy_shadow_gate.drop_reason_counts
    | map("\(.reason)=\(.events)")
    | join(" | ")
  ),
  "BUY_SHADOW_GATE_EXECUTE_REASONS " + (
    $s.buy_shadow_gate.quote_would_execute_drop_reason_counts
    | map("\(.reason)=\(.events)")
    | join(" | ")
  ),
  "FORCE_EXIT counted=\($s.force_exit_counted_trades) skipped_entry=\($s.force_exit_skipped_entry_trades)",
  "ENTRY_DIAG counted_events=\($s.quote_diagnostics.entry_counted.events) avg_delay_ms=\($s.quote_diagnostics.entry_counted.decision_delay_ms_avg) avg_quote_ms=\($s.quote_diagnostics.entry_counted.quote_latency_ms_avg) avg_slippage_bps=\($s.quote_diagnostics.entry_counted.slippage_bps_avg)",
  "THRESHOLDS " + (
    $s.threshold_summaries
    | map("\(.threshold_bps)bps counted=\(.counted_trades) skipped=\(.skipped_trades) unknown=\(.unknown_trades) pnl_fee=\(.quote_adjusted_pnl_after_priority_fee_sol)")
    | join(" | ")
  ),
  "BUY_SLIPPAGE_BUCKETS " + (
    $s.buy_slippage_buckets
    | map("\(.bucket):n=\(.trades),shadow=\(.shadow_pnl_sol),quote_fee=\(.quote_adjusted_pnl_after_priority_fee_sol)")
    | join(" | ")
  ),
  "DELAY_BUCKETS " + (
    $s.entry_decision_delay_buckets
    | map("\(.bucket):n=\(.trades),quote_fee=\(.quote_adjusted_pnl_after_priority_fee_sol)")
    | join(" | ")
  ),
  "LEADER_NOTIONAL_BUCKETS " + (
    $s.buy_leader_notional_buckets
    | map("\(.bucket):n=\(.trades),quote_fee=\(.quote_adjusted_pnl_after_priority_fee_sol)")
    | join(" | ")
  ),
  "ROUTES " + (
    $s.route_counts
    | map("\(.side):\(.label)=\(.events)")
    | join(" | ")
  ),
  "PRIORITY_FEE " + (
    $s.priority_fee_status_counts
    | map("\(.side):\(.status)=\(.events)")
    | join(" | ")
  ),
  "PUBLIC_PAID paired=\($ppc.paired_events // 0) both_ok=\($ppc.both_ok_events // 0) public_only_ok=\($ppc.public_only_ok_events // 0) paid_only_ok=\($ppc.paid_only_ok_events // 0) paid_better=\($ppc.paid_better_slippage_events // 0) public_better=\($ppc.public_better_slippage_events // 0) avg_public_ms=\($ppc.avg_public_latency_ms // 0) avg_paid_ms=\($ppc.avg_paid_latency_ms // 0) avg_paid_minus_public_bps=\($ppc.avg_paid_minus_public_slippage_bps // 0)",
  "PUBLIC_PAID_LATEST " + (
    ($ppc.latest // [])[:5]
    | map("\(.side):\(.better_provider // \"n/a\") delta_bps=\(.slippage_delta_bps // \"n/a\") public=\(.public_status // \"n/a\")/\(.public_slippage_bps // \"n/a\") paid=\(.paid_status // \"n/a\")/\(.paid_slippage_bps // \"n/a\")")
    | join(" | ")
  ),
  "PROVIDERS paired=\($pc.paired_events // 0) both_ok=\($pc.both_ok_events // 0) generic_only_ok=\($pc.generic_only_ok_events // 0) pump_fun_only_ok=\($pc.pump_fun_only_ok_events // 0) pump_fun_better=\($pc.pump_fun_better_slippage_events // 0) generic_better=\($pc.generic_better_slippage_events // 0) avg_generic_ms=\($pc.avg_generic_latency_ms // 0) avg_pump_fun_ms=\($pc.avg_pump_fun_latency_ms // 0) avg_pump_fun_minus_generic_bps=\($pc.avg_pump_fun_minus_generic_slippage_bps // 0)",
  "PROVIDER_LATEST " + (
    ($pc.latest // [])[:5]
    | map("\(.side):\(.better_provider // \"n/a\") delta_bps=\(.slippage_delta_bps // \"n/a\") generic=\(.generic_status // \"n/a\")/\(.generic_slippage_bps // \"n/a\") pump=\(.pump_fun_status // \"n/a\")/\(.pump_fun_slippage_bps // \"n/a\")")
    | join(" | ")
  ),
  "SELECTED_PROVIDERS total=\($sel.total_events // 0) generic_metis=\($sel.selected_generic_metis_events // 0) pump_fun_paid=\($sel.selected_pump_fun_paid_events // 0) generic_public=\($sel.selected_generic_public_events // 0) unresolved=\($sel.unresolved_events // 0)",
  "SELECTED_LATEST " + (
    ($sel.latest // [])[:5]
    | map("\(.side):\(.selected_provider) reason=\(.selected_reason) generic=\(.generic_metis_status // \"n/a\") public=\(.generic_public_status // \"n/a\") pump=\(.pump_fun_paid_status // \"n/a\") pump_completed=\(.pump_fun_paid_is_completed // \"n/a\") pump_error=\(.pump_fun_paid_error // \"\")")
    | join(" | ")
  ),
  "QUOTE_GATE status=\($s.readiness_gate.status) can_start=\($s.readiness_gate.can_start_tiny_execution) blockers=\($s.readiness_gate.blocker_count) warnings=\($s.readiness_gate.warning_count) min_market=\($s.readiness_gate.min_market_closed_trades) market=\($s.readiness_gate.market_closed_trades) skip_rate=\($s.readiness_gate.skip_rate_pct) unknown_rate=\($s.readiness_gate.unknown_rate_pct) shadow_gate_drop_rate=\($s.readiness_gate.entry_shadow_gate_drop_rate_pct)",
  "TINY_GATE status=\($g.status) can_start=\($g.can_start_tiny_execution) blockers=\($g.blocker_count) warnings=\($g.warning_count) latest_status=\($g.latest_order_status) latest_simulation=\($g.latest_simulation_status) latest_age_s=\($g.latest_metadata_age_seconds)",
  "TINY_CHECKS " + (
    $g.checks
    | map("\(.name)=\(.status)(value=\(.value),threshold=\(.threshold))")
    | join(" | ")
  )
end'
REMOTE
```

This report covers current sample size, Shadow vs canary PnL after priority
fee, skipped/unknown trades, threshold candidates, slippage/latency buckets,
provider comparison, selected provider, Metis dry-run, and tiny gate checks.
`execute_dropped` reasons come from `BUY_SHADOW_GATE_EXECUTE_REASONS`.
`generic_public` is fallback only. `Bonding curve for mint not found` with
selected `generic_metis` is expected for migrated Pump.fun AMM tokens.
If `reason` is not `execution_canary_quote_pnl_loaded`, report the operator
error first.

## Latest Entry Readiness Report

Use this to inspect the latest canary BUY path and recent canary-order window.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" \
  "LIMIT='$LIMIT' bash -s" <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db
BIN=/var/www/solana-copy-bot/bin/copybot_execution_canary_readiness

REPORT="$(sudo -u copybot "$BIN" --db-path "$DB" --limit "$LIMIT" --json)"
printf '%s\n' "$REPORT" | jq -r '
if .reason_class != "execution_canary_readiness_loaded" then
  "READINESS_ERROR reason=\(.reason_class) error=\(.error)"
else
  .summary as $s |
  .window as $w |
  "LATEST status=\($s.readiness_status) reason=\($s.readiness_reason) orders=\($s.total_orders) submit_disabled=\($s.submit_disabled_orders) failed=\($s.failed_orders)",
  "LATEST_META order_status=\($s.latest.order_status) route=\($s.latest.route) quote_status=\($s.latest.quote_status) decision=\($s.latest.decision_status) slippage_bps=\($s.latest.slippage_bps) price_impact=\($s.latest.price_impact_pct) priority_fee=\($s.latest.priority_fee_status):\($s.latest.priority_fee_lamports)",
  "WINDOW limit=\($w.limit) total=\($w.total_orders) metadata=\($w.metadata_orders) missing=\($w.missing_metadata_orders) would_enter=\($w.would_enter_orders) would_skip=\($w.would_skip_orders) unknown=\($w.unknown_orders) latest_age_s=\($w.latest_metadata_age_seconds)",
  "WINDOW_REASONS " + ($w.decision_reasons | map("\(.key)=\(.count)") | join(" | ")),
  "WINDOW_PROVIDER_ERRORS " + ($w.provider_errors | map("\(.key)=\(.count)") | join(" | ")),
  "WINDOW_ROUTES " + ($w.routes | map("\(.key)=\(.count)") | join(" | "))
end'
REMOTE
```

Use this report to answer whether canary BUY candidates are reaching
`would_enter`, `would_skip`, or `unknown`. It is not a PnL report.

## Stale And Rug-Like Triage

Use only when stale appears in the main report.

```bash
ssh -i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$HOST" \
  "SINCE='$SINCE' bash -s" <<'REMOTE'
set -euo pipefail
DB=/var/www/solana-copy-bot/state/live_runtime.db

sudo -u copybot sqlite3 -readonly -header -column "$DB" "
select close_context,
       count(*) closed,
       round(coalesce(sum(pnl_sol),0),6) pnl_sol,
       min(closed_ts) first_close,
       max(closed_ts) last_close
from shadow_closed_trades
where closed_ts >= '$SINCE'
group by close_context
order by close_context;"

sudo -u copybot sqlite3 -readonly -header -column "$DB" "
select id,
       substr(wallet_id,1,10)||'...' wallet,
       token,
       round(entry_cost_sol,6) entry_sol,
       round(exit_value_sol,6) exit_sol,
       round(pnl_sol,6) pnl_sol,
       opened_ts,
       closed_ts,
       close_context
from shadow_closed_trades
where closed_ts >= '$SINCE'
  and coalesce(close_context,'market')!='market'
order by closed_ts desc
limit 20;"
REMOTE
```

Interpretation:

- `market` is normal Shadow close accounting.
- `stale_quote_price` or terminal stale is defensive accounting.
- Rug-like stale usually means the token became unsafe to price or liquidity was
  removed; do not hide it inside market PnL.
- `SELL would_force_exit` is not stale; it means canary would close an owned
  position even when SELL slippage is above the soft threshold.

## User Report Template

Keep the user report short and current-window based.

```text
Window: <SINCE> -> <as_of>, sample <market_closed>/<min_required> market closes
Service: active, restarts <n>, warnings/errors <none/count>
Ingestion/SQLite: gaps/drops/429/5xx <values>, sqlite busy/retry <values>
Shadow market: <closed>, win/loss <w>/<l>, PnL <x> SOL
Shadow stale/rug-like: <count>, PnL <x> SOL
Canary quote PnL: counted <n>, skipped <n>, unknown <n>,
  win/loss <w>/<l>, PnL after fee <x> SOL, delta vs shadow <x> SOL
Thresholds: 150=<pnl/count>, 300=<pnl/count>, 500=<pnl/count>, 1000=<pnl/count>
Public vs paid: paired <n>, paid_better <n>, public_better <n>,
  avg_paid_minus_public_bps <x>, latest <provider/delta>
Providers: paired <n>, pump_fun_better <n>, generic_better <n>,
  avg_paid_minus_generic_bps <x>, latest <provider/delta>
Selected provider: generic_metis <n>, pump_fun_paid <n>, public_fallback <n>,
  unresolved <n>, latest <provider/reason>
Metis dry-run: latest_simulation <passed/failed/missing>, proof <instructions/transaction/missing>, gate <ready/blocked>
Latency/routes/fees: entry delay <ms>, quote <ms>, route <top>, priority <ok/errors>
Open: <n> lots, exposure <x> SOL
Decision: observe / investigate stale / investigate skips / move to next dev batch
```

Do not include old 24h numbers as the main answer after a fresh fix. If a 24h
context is useful, label it explicitly as context.

## When To Move Forward

Do not call the canary sample ready from BUY event count alone.

Minimum report checks:

- at least `30` current-window market closes
- quote-PnL operator status is loaded
- stale/rug-like is separated and understood
- skipped and unknown rates are acceptable for the current threshold
- Metis dry-run latest simulation is `passed` or failure reason is understood
- If `swap_transaction_dry_run_enabled=true`, latest simulation proof contains
  `metis_swap_transaction_ok`
- public-vs-paid generic comparison has enough paired events to judge if
  paid/private Metis improves slippage/latency versus public routes
- paid Pump.fun comparison has enough paired events where `/pump-fun/quote`
  supports the token
- priority fee errors are low or safely ignored
- ingestion gaps/drops/429/5xx are zero
- SQLite busy/retry is not rising
- no hidden production build or unexpected restart

Next engineering step must be chosen from report data, not from a one-off manual
query.
