#!/usr/bin/env bash
set -euo pipefail

HOURS="${1:-3}"            # report window in hours
NOTIONAL="${2:-0.5}"       # eligible notional threshold in SOL
INGEST_MINUTES="${3:-20}"  # ingestion window in minutes
DB="${DB:-state/paper_copybot.db}"
SERVICE="${SERVICE:-solana-copy-bot}"
SOL="So11111111111111111111111111111111111111112"
INGEST_LOG="$(mktemp -t phase2_ingestion.XXXXXX.log)"
trap 'rm -f "$INGEST_LOG"' EXIT

echo "=== NOW (UTC) ==="
date -u +"%Y-%m-%dT%H:%M:%SZ"

echo
echo "=== SERVICE ==="
sudo systemctl status "$SERVICE" --no-pager -l | sed -n '1,20p'

echo
echo "=== CONFIG ==="
grep -E "follow_top_n|min_trades|min_active_days|min_score|max_tx_per_minute|min_buy_count|min_tradable_ratio|max_rug_ratio|rug_lookahead_seconds|thin_market_min_volume_sol|thin_market_min_unique_traders|^\\[shadow\\]|copy_notional_sol|min_leader_notional_sol|max_signal_lag_seconds|min_token_age_seconds|min_holders|min_liquidity_sol|min_volume_5m_sol|min_unique_traders_5m" configs/paper.toml

echo
echo "=== INGESTION METRICS (${INGEST_MINUTES}m, tail 30) ==="
sudo journalctl -u "$SERVICE" --since "${INGEST_MINUTES} min ago" --no-pager \
| grep "ingestion pipeline metrics" > "$INGEST_LOG" || true
if [ -s "$INGEST_LOG" ]; then
    tail -n 30 "$INGEST_LOG"
else
    echo "no ingestion pipeline metrics for the last ${INGEST_MINUTES} minutes"
fi

echo
echo "=== INGESTION SUMMARY (${INGEST_MINUTES}m) ==="
python3 - "$INGEST_LOG" <<'PY'
import json
import re
import sys
from datetime import datetime

path = sys.argv[1]
lines = [line.strip() for line in open(path) if line.strip()]
rows = []
for line in lines:
    ts_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s+INFO', line)
    payload_match = re.search(r'INFO\s+(\{.*\})\s*$', line)
    if not payload_match:
        continue
    try:
        payload = json.loads(payload_match.group(1))
    except json.JSONDecodeError:
        continue
    ts = None
    if ts_match:
        try:
            ts = datetime.fromisoformat(ts_match.group(1).replace("Z", "+00:00"))
        except ValueError:
            pass
    rows.append((ts, payload))

if not rows:
    print("no parseable ingestion samples")
    raise SystemExit(0)

def series(key):
    return [r[1].get(key) for r in rows if isinstance(r[1].get(key), (int, float))]

def delta(key):
    first = rows[0][1].get(key)
    last = rows[-1][1].get(key)
    if isinstance(first, (int, float)) and isinstance(last, (int, float)):
        return last - first
    return None

lag95 = series("ingestion_lag_ms_p95")
queue = series("ws_to_fetch_queue_depth")
inflight = series("fetch_concurrency_inflight")
rpc429 = series("rpc_429")
rpc5xx = series("rpc_5xx")

last = rows[-1][1]
duration_s = None
if rows[0][0] and rows[-1][0]:
    duration_s = (rows[-1][0] - rows[0][0]).total_seconds()

print(f"samples={len(rows)}")
if lag95:
    print(f"lag_p95_ms: last={int(lag95[-1])} min={int(min(lag95))} max={int(max(lag95))}")
if queue:
    print(f"ws_queue_depth: last={int(queue[-1])} min={int(min(queue))} max={int(max(queue))}")
if inflight:
    print(f"fetch_inflight: last={int(inflight[-1])} min={int(min(inflight))} max={int(max(inflight))}")
if rpc429:
    print(f"rpc_429: last={int(rpc429[-1])} delta={int(rpc429[-1]-rpc429[0])}")
if rpc5xx:
    print(f"rpc_5xx: last={int(rpc5xx[-1])} delta={int(rpc5xx[-1]-rpc5xx[0])}")

if duration_s and duration_s > 0:
    fetched = (delta("fetch_success") or 0) + (delta("fetch_failed") or 0)
    enqueued = delta("ws_notifications_enqueued") or 0
    replaced = delta("ws_notifications_replaced_oldest") or 0
    print(f"window_seconds={int(duration_s)}")
    print(f"fetch_rps≈{fetched/duration_s:.2f} enqueue_rps≈{enqueued/duration_s:.2f} replaced_oldest_rps≈{replaced/duration_s:.2f}")

ok_lag = isinstance(last.get("ingestion_lag_ms_p95"), (int, float)) and last["ingestion_lag_ms_p95"] < 10_000
ok_429 = isinstance(last.get("rpc_429"), (int, float)) and (last["rpc_429"] - rows[0][1].get("rpc_429", last["rpc_429"])) <= 0
print(f"health_check: lag_p95_lt_10s={'yes' if ok_lag else 'no'} rpc429_not_growing={'yes' if ok_429 else 'no'}")
PY

echo
echo "=== TRADING KPI SNAPSHOT ==="
python3 - "$DB" "$HOURS" <<'PY'
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

db = sys.argv[1]
hours = float(sys.argv[2])
windows = sorted(set([hours, 24.0]))

con = sqlite3.connect(db)
cur = con.cursor()
for h in windows:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat()
    signals = cur.execute("select count(*) from copy_signals where ts >= ?", (cutoff,)).fetchone()[0]
    closed, pnl, wins = cur.execute(
        "select count(*), coalesce(sum(pnl_sol), 0), "
        "coalesce(sum(case when pnl_sol > 0 then 1 else 0 end), 0) "
        "from shadow_closed_trades where closed_ts >= ?",
        (cutoff,),
    ).fetchone()
    winrate = (wins / closed * 100.0) if closed else 0.0
    print(f"window={int(h)}h signals={signals} closed_trades={closed} pnl_sol={float(pnl):.6f} winrate={winrate:.2f}%")
    statuses = cur.execute(
        "select status, count(*) from copy_signals where ts >= ? group by status order by 2 desc",
        (cutoff,),
    ).fetchall()
    if statuses:
        print("signal_statuses:", ", ".join(f"{s}:{n}" for s, n in statuses))
PY

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
queue_sat = one(
    "select count(*) from risk_events where ts >= ? and type='shadow_queue_saturated'",
    (cutoff,),
)

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
print(f"shadow_queue_saturated_window: {queue_sat}")
ratio = (signals_all / eligible_all) if eligible_all else None
print(f"signal_to_eligible_ratio: {ratio if ratio is not None else 'n/a'}")
print(f"shadow_open_lots_now: {open_lots}")
print(f"shadow_closed_trades_total: {closed_all}, pnl_total_sol: {round(float(pnl_all), 6)}")
print(f"shadow_closed_trades_window: {closed_win}, pnl_window_sol: {round(float(pnl_win), 6)}")
try:
    cache_rows = one("select count(*) from token_quality_cache")
    cache_fresh = one("select count(*) from token_quality_cache where julianday(fetched_at) >= julianday('now','-10 minutes')")
    print(f"token_quality_cache_rows: {cache_rows}, fresh_10m: {cache_fresh}")
except sqlite3.OperationalError:
    pass

latest_window = cur.execute("select max(window_start) from wallet_metrics").fetchone()[0]
if latest_window:
    print(f"\nLatest wallet_metrics window_start: {latest_window}")
    try:
        rows = cur.execute("""
        select wallet_id, round(score, 3), buy_total, round(tradable_ratio, 3), round(rug_ratio, 3), trades, closed_trades
        from wallet_metrics
        where window_start = ?
        order by score desc, trades desc
        limit 10
        """, (latest_window,)).fetchall()
        print("Top scored wallets (score, buy_total, tradable_ratio, rug_ratio, trades, closed_trades):")
        for r in rows:
            print(r)
    except sqlite3.OperationalError as exc:
        print(f"wallet_metrics quality columns unavailable: {exc}")

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
