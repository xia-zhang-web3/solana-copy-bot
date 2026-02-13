#!/usr/bin/env bash
set -euo pipefail

HOURS="${1:-3}"            # report window in hours
NOTIONAL="${2:-0.5}"       # eligible notional threshold in SOL
DB="${DB:-state/paper_copybot.db}"
SERVICE="${SERVICE:-solana-copy-bot}"
SOL="So11111111111111111111111111111111111111112"

echo "=== NOW (UTC) ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"

echo
echo "=== SERVICE ==="
sudo systemctl status "$SERVICE" --no-pager -l | sed -n '1,20p'

echo
echo "=== CONFIG ==="
grep -E "follow_top_n|min_trades|min_active_days|min_score|max_tx_per_minute|^\\[shadow\\]|copy_notional_sol|min_leader_notional_sol|max_signal_lag_seconds" configs/paper.toml

echo
echo "=== LOGS (${HOURS}h, tail 300) ==="
sudo journalctl -u "$SERVICE" --since "${HOURS} hours ago" --no-pager \
| grep -E "configuration loaded|helius ws connected|idle timeout|stream ended|ingestion error|tx fetch attempt failed|discovery cycle completed|shadow signal recorded|shadow drop reasons|shadow snapshot" \
| tail -n 300

echo
echo "=== DB REPORT (${HOURS}h, notional>=${NOTIONAL}) ==="
python3 - "$DB" "$HOURS" "$NOTIONAL" "$SOL" <<'PY'
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

db = sys.argv[1]
hours = float(sys.argv[2])
notional = float(sys.argv[3])
SOL = sys.argv[4]

con = sqlite3.connect(db)
cur = con.cursor()

cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

def one(sql, params=()):
    return cur.execute(sql, params).fetchone()[0]

base_exists = """
exists (
  select 1
  from followlist f
  where f.wallet_id = os.wallet_id
    and os.ts >= f.added_at
    and (f.removed_at is null or os.ts < f.removed_at)
)
"""

eligible_all = one(f"""
select count(*)
from observed_swaps os
where os.ts >= ?
  and (
    (os.token_in = ? and os.token_out <> ? and os.qty_in >= ?)
    or
    (os.token_out = ? and os.token_in <> ? and os.qty_out >= ?)
  )
  and {base_exists}
""", (cutoff, SOL, SOL, notional, SOL, SOL, notional))

eligible_buy = one(f"""
select count(*)
from observed_swaps os
where os.ts >= ?
  and (os.token_in = ? and os.token_out <> ? and os.qty_in >= ?)
  and {base_exists}
""", (cutoff, SOL, SOL, notional))

eligible_sell = one(f"""
select count(*)
from observed_swaps os
where os.ts >= ?
  and (os.token_out = ? and os.token_in <> ? and os.qty_out >= ?)
  and {base_exists}
""", (cutoff, SOL, SOL, notional))

signals_all = one("select count(*) from copy_signals where ts >= ?", (cutoff,))
signals_buy = one("select count(*) from copy_signals where ts >= ? and side='buy'", (cutoff,))
signals_sell = one("select count(*) from copy_signals where ts >= ? and side='sell'", (cutoff,))

obs_all = one("select count(*) from observed_swaps where ts >= ?", (cutoff,))
active_follow = one("select count(*) from followlist where active=1")
open_lots = one("select count(*) from shadow_lots")
closed_all, pnl_all = cur.execute("select count(*), coalesce(sum(pnl_sol),0) from shadow_closed_trades").fetchone()
closed_win, pnl_win = cur.execute("select count(*), coalesce(sum(pnl_sol),0) from shadow_closed_trades where closed_ts >= ?", (cutoff,)).fetchone()

print(f"window_start: {cutoff}")
print(f"observed_swaps_window: {obs_all}")
print(f"active_follow_now: {active_follow}")
print(f"eligible_window_total: {eligible_all}")
print(f"eligible_window_buy:   {eligible_buy}")
print(f"eligible_window_sell:  {eligible_sell}")
print(f"signals_window_total:  {signals_all}")
print(f"signals_window_buy:    {signals_buy}")
print(f"signals_window_sell:   {signals_sell}")
ratio = (signals_all / eligible_all) if eligible_all else None
print(f"signal_to_eligible_ratio: {ratio if ratio is not None else 'n/a'}")
print(f"shadow_open_lots_now: {open_lots}")
print(f"shadow_closed_trades_total: {closed_all}, pnl_total_sol: {round(float(pnl_all), 6)}")
print(f"shadow_closed_trades_window: {closed_win}, pnl_window_sol: {round(float(pnl_win), 6)}")

print("\nTop eligible wallets in window:")
for r in cur.execute(f"""
select os.wallet_id, count(*) as n
from observed_swaps os
where os.ts >= ?
  and (
    (os.token_in = ? and os.token_out <> ? and os.qty_in >= ?)
    or
    (os.token_out = ? and os.token_in <> ? and os.qty_out >= ?)
  )
  and {base_exists}
group by os.wallet_id
order by n desc
limit 20
""", (cutoff, SOL, SOL, notional, SOL, SOL, notional)):
    print(r)

print("\nTop signal wallets in window:")
for r in cur.execute("""
select wallet_id, count(*) as n
from copy_signals
where ts >= ?
group by wallet_id
order by n desc
limit 20
""", (cutoff,)):
    print(r)

print("\nLast 20 copy_signals:")
for r in cur.execute("""
select ts, wallet_id, side, token, round(notional_sol,4), status
from copy_signals
order by ts desc
limit 20
"""):
    print(r)
PY
