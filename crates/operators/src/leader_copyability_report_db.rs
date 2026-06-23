use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::time::Duration as StdDuration;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const EPSILON_QTY: f64 = 1e-12;

#[derive(Debug, Clone)]
pub(crate) struct ActiveWallet {
    pub(crate) wallet_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct WalletMetric {
    pub(crate) wallet_id: String,
    pub(crate) rank: u64,
    pub(crate) window_start: DateTime<Utc>,
    pub(crate) score: f64,
    pub(crate) pnl_sol: f64,
    pub(crate) win_rate: f64,
    pub(crate) closed_trades: u64,
    pub(crate) hold_median_seconds: i64,
    pub(crate) rug_ratio: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderCloseFact {
    pub(crate) pnl_sol: f64,
    pub(crate) win: bool,
}

#[derive(Debug, Clone)]
struct ObservedSwapRow {
    token_in: String,
    token_out: String,
    qty_in: f64,
    qty_out: f64,
}

#[derive(Debug, Clone)]
struct LeaderLot {
    qty: f64,
    cost_sol: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct FollowerCloseFact {
    pub(crate) pnl_sol: f64,
    pub(crate) close_context: String,
}

pub(crate) fn open_read_only_db(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags)
        .with_context(|| format!("failed opening read-only sqlite db {}", path.display()))?;
    conn.busy_timeout(StdDuration::from_secs(5))?;
    conn.pragma_update(None, "query_only", true)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "cache_size", -131_072)?;
    Ok(conn)
}

pub(crate) fn load_active_wallets(conn: &Connection, limit: u32) -> Result<Vec<ActiveWallet>> {
    let mut stmt = conn
        .prepare(
            "SELECT wallet_id
             FROM followlist INDEXED BY idx_followlist_one_active_wallet
             WHERE active = 1
             ORDER BY wallet_id ASC
             LIMIT ?1",
        )
        .context("failed preparing active followlist query")?;
    let rows = stmt.query_map([i64::from(limit)], |row| {
        Ok(ActiveWallet {
            wallet_id: row.get(0)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading active followlist wallets")
}

pub(crate) fn latest_wallet_metric_window_at_or_before(
    conn: &Connection,
    until: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT window_start
             FROM wallet_metrics INDEXED BY idx_wallet_metrics_window_start
             ORDER BY window_start DESC
             LIMIT 32",
        )
        .context("failed preparing latest wallet_metrics window query")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("failed querying latest wallet_metrics windows")?;
    let mut best = None;
    for raw in rows {
        let ts = parse_ts(&raw?, "wallet_metrics.window_start")?;
        if ts <= until {
            best = Some(best.map_or(ts, |current: DateTime<Utc>| current.max(ts)));
        }
    }
    Ok(best)
}

pub(crate) fn load_wallet_metrics_for_window(
    conn: &Connection,
    window_start: DateTime<Utc>,
) -> Result<Vec<WalletMetric>> {
    let canonical = window_start.to_rfc3339();
    let legacy_z = window_start.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut stmt = conn
        .prepare(
            "SELECT wallet_id, window_start, score, pnl, win_rate, closed_trades,
                    hold_median_seconds, rug_ratio
             FROM wallet_metrics INDEXED BY idx_wallet_metrics_window_start
             WHERE window_start IN (?1, ?2)
             ORDER BY score DESC, wallet_id ASC",
        )
        .context("failed preparing wallet_metrics rank query")?;
    let mut rows = stmt
        .query(params![canonical, legacy_z])
        .context("failed querying wallet_metrics ranks")?;
    let mut out = Vec::new();
    let mut rank = 1_u64;
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_metrics ranks")?
    {
        let window_raw: String = row
            .get(1)
            .context("failed reading wallet_metrics.window_start")?;
        let closed_trades: i64 = row
            .get(5)
            .context("failed reading wallet_metrics.closed_trades")?;
        out.push(WalletMetric {
            wallet_id: row
                .get(0)
                .context("failed reading wallet_metrics.wallet_id")?,
            rank,
            window_start: parse_ts(&window_raw, "wallet_metrics.window_start")?,
            score: row.get(2).context("failed reading wallet_metrics.score")?,
            pnl_sol: row.get(3).context("failed reading wallet_metrics.pnl")?,
            win_rate: row
                .get(4)
                .context("failed reading wallet_metrics.win_rate")?,
            closed_trades: closed_trades.max(0) as u64,
            hold_median_seconds: row
                .get(6)
                .context("failed reading wallet_metrics.hold_median_seconds")?,
            rug_ratio: row
                .get(7)
                .context("failed reading wallet_metrics.rug_ratio")?,
        });
        rank += 1;
    }
    Ok(out)
}

pub(crate) fn load_leader_close_facts(
    conn: &Connection,
    wallet_id: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<LeaderCloseFact>> {
    let mut stmt = conn
        .prepare(
            "SELECT token_in, token_out, qty_in, qty_out
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE wallet_id = ?1
               AND ts >= ?2
               AND ts < ?3
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?4",
        )
        .context("failed preparing leader observed swap replay query")?;
    let rows = stmt.query_map(
        params![
            wallet_id,
            since.to_rfc3339(),
            until.to_rfc3339(),
            i64::from(limit)
        ],
        |row| {
            Ok(ObservedSwapRow {
                token_in: row.get(0)?,
                token_out: row.get(1)?,
                qty_in: row.get(2)?,
                qty_out: row.get(3)?,
            })
        },
    )?;
    let swaps = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading leader observed swaps")?;
    Ok(replay_leader_fifo(swaps))
}

pub(crate) fn load_follower_close_facts(
    conn: &Connection,
    wallet_id: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<FollowerCloseFact>> {
    let mut stmt = conn
        .prepare(
            "SELECT pnl_sol, COALESCE(close_context, 'market') AS close_context
             FROM shadow_closed_trades INDEXED BY idx_shadow_closed_trades_wallet_closed_ts
             WHERE wallet_id = ?1
               AND closed_ts >= ?2
               AND closed_ts < ?3
             ORDER BY closed_ts ASC, id ASC
             LIMIT ?4",
        )
        .context("failed preparing follower close facts query")?;
    let rows = stmt.query_map(
        params![
            wallet_id,
            since.to_rfc3339(),
            until.to_rfc3339(),
            i64::from(limit)
        ],
        |row| {
            Ok(FollowerCloseFact {
                pnl_sol: row.get(0)?,
                close_context: row.get(1)?,
            })
        },
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading follower close facts")
}

fn parse_ts(raw: &str, label: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {label}: {raw}"))
}

fn replay_leader_fifo(swaps: Vec<ObservedSwapRow>) -> Vec<LeaderCloseFact> {
    let mut positions: HashMap<String, VecDeque<LeaderLot>> = HashMap::new();
    let mut closes = Vec::new();
    for swap in swaps {
        if is_sol_buy(&swap) {
            record_buy(&mut positions, &swap);
        } else if is_sol_sell(&swap) {
            record_sell(&mut positions, &swap, &mut closes);
        }
    }
    closes
}

fn is_sol_buy(swap: &ObservedSwapRow) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &ObservedSwapRow) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn record_buy(positions: &mut HashMap<String, VecDeque<LeaderLot>>, swap: &ObservedSwapRow) {
    if !swap.qty_in.is_finite()
        || !swap.qty_out.is_finite()
        || swap.qty_in <= EPSILON_QTY
        || swap.qty_out <= EPSILON_QTY
    {
        return;
    }
    positions
        .entry(swap.token_out.clone())
        .or_default()
        .push_back(LeaderLot {
            qty: swap.qty_out,
            cost_sol: swap.qty_in,
        });
}

fn record_sell(
    positions: &mut HashMap<String, VecDeque<LeaderLot>>,
    swap: &ObservedSwapRow,
    closes: &mut Vec<LeaderCloseFact>,
) {
    if !swap.qty_in.is_finite()
        || !swap.qty_out.is_finite()
        || swap.qty_in <= EPSILON_QTY
        || swap.qty_out <= EPSILON_QTY
    {
        return;
    }
    let Some(lots) = positions.get_mut(&swap.token_in) else {
        return;
    };
    let mut remaining_qty = swap.qty_in;
    let mut matched_qty = 0.0;
    let mut sell_pnl = 0.0;
    while remaining_qty > EPSILON_QTY {
        let Some(front) = lots.front_mut() else {
            break;
        };
        let take_qty = remaining_qty.min(front.qty);
        if take_qty <= EPSILON_QTY || front.qty <= EPSILON_QTY {
            break;
        }
        let sell_fraction = take_qty / swap.qty_in;
        let lot_fraction = take_qty / front.qty;
        let proceeds_sol = swap.qty_out * sell_fraction;
        let cost_sol = front.cost_sol * lot_fraction;
        sell_pnl += proceeds_sol - cost_sol;
        matched_qty += take_qty;
        front.qty -= take_qty;
        front.cost_sol -= cost_sol;
        remaining_qty -= take_qty;
        if front.qty <= EPSILON_QTY {
            lots.pop_front();
        }
    }
    if lots.is_empty() {
        positions.remove(&swap.token_in);
    }
    if matched_qty > EPSILON_QTY {
        closes.push(LeaderCloseFact {
            pnl_sol: sell_pnl,
            win: sell_pnl > 0.0,
        });
    }
}
