use super::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteBatchedDeleteSummary, SqliteStore,
    TokenMarketStats, WalletScoringBuyFactRow, WalletScoringCloseFactRow, WalletScoringDayRow,
    WalletScoringQualitySource, WalletScoringSnapshot,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, Connection, OptionalExtension};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Instant;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
const QUALITY_MAX_FETCH_PER_BATCH: usize = 20;
const QUALITY_RPC_BUDGET_MS: u64 = 1_500;

#[derive(Debug, Clone)]
struct OpenLotRow {
    buy_signature: String,
    qty: f64,
    cost_sol: f64,
    opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct QualitySnapshot {
    source: WalletScoringQualitySource,
    token_age_seconds: Option<u64>,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
}

#[derive(Debug, Clone)]
struct QualityCacheRowLocal {
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct QualityFetchBudget {
    rpc_attempted: usize,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct QualityCacheUpsert {
    mint: String,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PreparedBuyFact {
    market_stats: TokenMarketStats,
    quality: QualitySnapshot,
    quality_cache_upsert: Option<QualityCacheUpsert>,
    rug_check_after_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PreparedScoringSwap {
    swap: SwapEvent,
    buy_fact: Option<PreparedBuyFact>,
}

#[derive(Debug, Clone)]
struct CarryoverLotRow {
    qty: f64,
    cost_sol: f64,
    oldest_opened_ts: DateTime<Utc>,
}

fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn cmp_swap_order(a: &SwapEvent, b: &SwapEvent) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn cmp_cursor_order(a: &DiscoveryRuntimeCursor, b: &DiscoveryRuntimeCursor) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn parse_day(raw: &str, field: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("invalid {field} date value: {raw}"))
}

fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}

fn token_market_stats_on_conn(
    conn: &Connection,
    token: &str,
    as_of: DateTime<Utc>,
) -> Result<TokenMarketStats> {
    let as_of_raw = as_of.to_rfc3339();

    let first_seen_raw: Option<String> = conn
        .query_row(
            "SELECT MIN(ts)
             FROM (
                SELECT ts FROM observed_swaps WHERE token_in = ?1 AND ts <= ?2
                UNION ALL
                SELECT ts FROM observed_swaps WHERE token_out = ?1 AND ts <= ?2
             )",
            params![token, &as_of_raw],
            |row| row.get(0),
        )
        .context("failed querying token first_seen for discovery scoring")?;
    let first_seen = first_seen_raw
        .as_deref()
        .map(|raw| parse_ts(raw, "wallet_scoring token first_seen"))
        .transpose()?;

    let holders_proxy_raw: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM (
                SELECT DISTINCT wallet_id
                FROM observed_swaps
                WHERE token_in = ?1
                  AND ts <= ?2
                UNION
                SELECT DISTINCT wallet_id
                FROM observed_swaps
                WHERE token_out = ?1
                  AND ts <= ?2
             )",
            params![token, &as_of_raw],
            |row| row.get(0),
        )
        .context("failed querying token holders proxy for discovery scoring")?;

    let window_start = (as_of - Duration::minutes(5)).to_rfc3339();
    let window_end = as_of.to_rfc3339();
    let (volume_5m_sol, liquidity_sol_proxy, unique_traders_5m_raw): (f64, f64, i64) = conn
        .query_row(
            "SELECT
                COALESCE(SUM(sol_notional), 0.0) AS volume_5m_sol,
                COALESCE(MAX(sol_notional), 0.0) AS liquidity_sol_proxy,
                COUNT(DISTINCT wallet_id) AS unique_traders_5m
             FROM (
                SELECT wallet_id, qty_out AS sol_notional
                FROM observed_swaps
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, qty_in AS sol_notional
                FROM observed_swaps
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )",
            params![token, SOL_MINT, window_start, window_end],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .context("failed querying token 5m market stats for discovery scoring")?;

    Ok(TokenMarketStats {
        first_seen,
        holders_proxy: holders_proxy_raw.max(0) as u64,
        liquidity_sol_proxy,
        volume_5m_sol,
        unique_traders_5m: unique_traders_5m_raw.max(0) as u64,
    })
}

fn load_token_quality_cache_on_conn(
    conn: &Connection,
    mint: &str,
) -> Result<Option<QualityCacheRowLocal>> {
    let row: Option<(Option<i64>, Option<f64>, Option<i64>, String)> = conn
        .query_row(
            "SELECT holders, liquidity_sol, token_age_seconds, fetched_at
             FROM token_quality_cache
             WHERE mint = ?1",
            params![mint],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .context("failed querying token_quality_cache in discovery scoring write path")?;

    row.map(
        |(holders_raw, liquidity_sol, token_age_seconds_raw, fetched_at_raw)| -> Result<_> {
            Ok(QualityCacheRowLocal {
                holders: holders_raw.map(|value| value.max(0) as u64),
                liquidity_sol,
                token_age_seconds: token_age_seconds_raw.map(|value| value.max(0) as u64),
                fetched_at: parse_ts(&fetched_at_raw, "token_quality_cache.fetched_at")?,
            })
        },
    )
    .transpose()
}

fn upsert_token_quality_cache_on_conn(
    conn: &Connection,
    mint: &str,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO token_quality_cache(
            mint,
            holders,
            liquidity_sol,
            token_age_seconds,
            fetched_at
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(mint) DO UPDATE SET
            holders = excluded.holders,
            liquidity_sol = excluded.liquidity_sol,
            token_age_seconds = excluded.token_age_seconds,
            fetched_at = excluded.fetched_at",
        params![
            mint,
            holders.map(|value| value as i64),
            liquidity_sol,
            token_age_seconds.map(|value| value as i64),
            fetched_at.to_rfc3339(),
        ],
    )
    .context("failed upserting token_quality_cache row in discovery scoring write path")?;
    Ok(())
}

fn resolve_quality_snapshot_on_conn(
    conn: &Connection,
    mint: &str,
    signal_ts: DateTime<Utc>,
    config: &DiscoveryAggregateWriteConfig,
    budget: &mut QualityFetchBudget,
) -> Result<(QualitySnapshot, Option<QualityCacheUpsert>)> {
    let cached = load_token_quality_cache_on_conn(conn, mint)?;
    let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
    if let Some(row) = cached.as_ref() {
        if signal_ts.signed_duration_since(row.fetched_at) <= ttl {
            return Ok((
                QualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: row.token_age_seconds,
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None,
            ));
        }
    }

    let Some(helius_http_url) = config.helius_http_url.as_deref() else {
        return Ok((
            match cached {
                Some(row) => QualitySnapshot {
                    source: WalletScoringQualitySource::Stale,
                    token_age_seconds: row.token_age_seconds.map(|age| {
                        age.saturating_add(
                            signal_ts
                                .signed_duration_since(row.fetched_at)
                                .num_seconds()
                                .max(0) as u64,
                        )
                    }),
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None => QualitySnapshot {
                    source: WalletScoringQualitySource::Missing,
                    token_age_seconds: None,
                    holders: None,
                    liquidity_sol: None,
                },
            },
            None,
        ));
    };

    if budget.rpc_attempted >= QUALITY_MAX_FETCH_PER_BATCH {
        return Ok((
            match cached {
                Some(row) => QualitySnapshot {
                    source: WalletScoringQualitySource::Stale,
                    token_age_seconds: row.token_age_seconds.map(|age| {
                        age.saturating_add(
                            signal_ts
                                .signed_duration_since(row.fetched_at)
                                .num_seconds()
                                .max(0) as u64,
                        )
                    }),
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None => QualitySnapshot {
                    source: WalletScoringQualitySource::Deferred,
                    token_age_seconds: None,
                    holders: None,
                    liquidity_sol: None,
                },
            },
            None,
        ));
    }

    let started_at = budget.started_at.get_or_insert_with(Instant::now);
    if started_at.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
        return Ok((
            match cached {
                Some(row) => QualitySnapshot {
                    source: WalletScoringQualitySource::Stale,
                    token_age_seconds: row.token_age_seconds.map(|age| {
                        age.saturating_add(
                            signal_ts
                                .signed_duration_since(row.fetched_at)
                                .num_seconds()
                                .max(0) as u64,
                        )
                    }),
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None => QualitySnapshot {
                    source: WalletScoringQualitySource::Deferred,
                    token_age_seconds: None,
                    holders: None,
                    liquidity_sol: None,
                },
            },
            None,
        ));
    }

    budget.rpc_attempted = budget.rpc_attempted.saturating_add(1);
    let fetched = SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        QUALITY_RPC_TIMEOUT_MS,
        QUALITY_MAX_SIGNATURE_PAGES,
        config.min_token_age_hint_seconds,
    );
    match fetched {
        Ok(fetched) => Ok((
            QualitySnapshot {
                source: WalletScoringQualitySource::Fresh,
                token_age_seconds: fetched.token_age_seconds,
                holders: fetched.holders,
                liquidity_sol: fetched.liquidity_sol,
            },
            Some(QualityCacheUpsert {
                mint: mint.to_string(),
                holders: fetched.holders,
                liquidity_sol: fetched.liquidity_sol,
                token_age_seconds: fetched.token_age_seconds,
                fetched_at: signal_ts,
            }),
        )),
        Err(_) => Ok((
            match cached {
                Some(row) => QualitySnapshot {
                    source: WalletScoringQualitySource::Stale,
                    token_age_seconds: row.token_age_seconds.map(|age| {
                        age.saturating_add(
                            signal_ts
                                .signed_duration_since(row.fetched_at)
                                .num_seconds()
                                .max(0) as u64,
                        )
                    }),
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None => QualitySnapshot {
                    source: WalletScoringQualitySource::Missing,
                    token_age_seconds: None,
                    holders: None,
                    liquidity_sol: None,
                },
            },
            None,
        )),
    }
}

fn upsert_wallet_scoring_day_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let buy_notional = if is_sol_buy(swap) {
        swap.amount_in.max(0.0)
    } else {
        0.0
    };
    conn.execute(
        "INSERT INTO wallet_scoring_days(
            wallet_id,
            activity_day,
            first_seen,
            last_seen,
            trades,
            spent_sol,
            max_buy_notional_sol
         ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)
         ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
            first_seen = CASE
                WHEN excluded.first_seen < wallet_scoring_days.first_seen
                    THEN excluded.first_seen
                ELSE wallet_scoring_days.first_seen
            END,
            last_seen = CASE
                WHEN excluded.last_seen > wallet_scoring_days.last_seen
                    THEN excluded.last_seen
                ELSE wallet_scoring_days.last_seen
            END,
            trades = wallet_scoring_days.trades + 1,
            spent_sol = wallet_scoring_days.spent_sol + excluded.spent_sol,
            max_buy_notional_sol = MAX(
                wallet_scoring_days.max_buy_notional_sol,
                excluded.max_buy_notional_sol
            )",
        params![
            &swap.wallet,
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.to_rfc3339(),
            buy_notional,
            buy_notional,
        ],
    )
    .context("failed upserting wallet_scoring_days row")?;
    Ok(())
}

fn upsert_wallet_scoring_tx_minute_on_conn(
    conn: &Connection,
    wallet_id: &str,
    minute_bucket: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO wallet_scoring_tx_minutes(wallet_id, minute_bucket, tx_count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(wallet_id, minute_bucket) DO UPDATE SET
            tx_count = wallet_scoring_tx_minutes.tx_count + 1",
        params![wallet_id, minute_bucket],
    )
    .context("failed upserting wallet_scoring_tx_minutes row")?;
    Ok(())
}

fn prepare_discovery_scoring_swaps(
    conn: &Connection,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
) -> Result<Vec<PreparedScoringSwap>> {
    if swaps.is_empty() {
        return Ok(Vec::new());
    }

    let mut ordered = swaps.to_vec();
    ordered.sort_by(cmp_swap_order);
    let mut budget = QualityFetchBudget::default();
    let mut prepared = Vec::with_capacity(ordered.len());
    for swap in ordered {
        let buy_fact = if is_sol_buy(&swap) {
            let token = swap.token_out.as_str();
            let market_stats = token_market_stats_on_conn(conn, token, swap.ts_utc)?;
            let (quality, quality_cache_upsert) =
                resolve_quality_snapshot_on_conn(conn, token, swap.ts_utc, config, &mut budget)?;
            Some(PreparedBuyFact {
                market_stats,
                quality,
                quality_cache_upsert,
                rug_check_after_ts: swap.ts_utc
                    + Duration::seconds(config.rug_lookahead_seconds.max(1) as i64),
            })
        } else {
            None
        };
        prepared.push(PreparedScoringSwap { swap, buy_fact });
    }
    Ok(prepared)
}

fn insert_wallet_scoring_buy_fact_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
    prepared: &PreparedBuyFact,
) -> Result<()> {
    let token = swap.token_out.as_str();

    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_buy_facts(
            buy_signature,
            wallet_id,
            token,
            ts,
            activity_day,
            notional_sol,
            market_volume_5m_sol,
            market_unique_traders_5m,
            market_liquidity_proxy_sol,
            quality_source,
            quality_token_age_seconds,
            quality_holders,
            quality_liquidity_sol,
            rug_check_after_ts,
            rug_volume_lookahead_sol,
            rug_unique_traders_lookahead
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, NULL)",
        params![
            &swap.signature,
            &swap.wallet,
            token,
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            swap.amount_in.max(0.0),
            prepared.market_stats.volume_5m_sol.max(0.0),
            prepared.market_stats.unique_traders_5m as i64,
            prepared.market_stats.liquidity_sol_proxy.max(0.0),
            match prepared.quality.source {
                WalletScoringQualitySource::Fresh => "fresh",
                WalletScoringQualitySource::Stale => "stale",
                WalletScoringQualitySource::Deferred => "deferred",
                WalletScoringQualitySource::Missing => "missing",
            },
            prepared.quality.token_age_seconds.map(|value| value as i64),
            prepared.quality.holders.map(|value| value as i64),
            prepared.quality.liquidity_sol,
            prepared.rug_check_after_ts.to_rfc3339(),
        ],
    )
    .context("failed inserting wallet_scoring_buy_facts row")?;

    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_open_lots(
            buy_signature,
            wallet_id,
            token,
            qty,
            cost_sol,
            opened_ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            &swap.signature,
            &swap.wallet,
            token,
            swap.amount_out.max(0.0),
            swap.amount_in.max(0.0),
            swap.ts_utc.to_rfc3339(),
        ],
    )
    .context("failed inserting wallet_scoring_open_lots row")?;
    Ok(())
}

fn load_wallet_scoring_open_lots_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<Vec<OpenLotRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             WHERE wallet_id = ?1
               AND token = ?2
             ORDER BY opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots query")?;
    let mut rows = stmt
        .query(params![wallet_id, token])
        .context("failed querying wallet_scoring_open_lots")?;
    let mut lots = Vec::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots rows")?
    {
        let opened_ts_raw: String = row
            .get(3)
            .context("failed reading wallet_scoring_open_lots.opened_ts")?;
        lots.push(OpenLotRow {
            buy_signature: row
                .get(0)
                .context("failed reading wallet_scoring_open_lots.buy_signature")?,
            qty: row
                .get(1)
                .context("failed reading wallet_scoring_open_lots.qty")?,
            cost_sol: row
                .get(2)
                .context("failed reading wallet_scoring_open_lots.cost_sol")?,
            opened_ts: parse_ts(&opened_ts_raw, "wallet_scoring_open_lots.opened_ts")?,
        });
    }
    Ok(lots)
}

fn load_wallet_scoring_carryover_lot_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<Option<CarryoverLotRow>> {
    let row: Option<(f64, f64, String)> = conn
        .query_row(
            "SELECT qty, cost_sol, oldest_opened_ts
             FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![wallet_id, token],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed querying wallet_scoring_carryover_lots")?;
    row.map(|(qty, cost_sol, oldest_opened_ts_raw)| {
        Ok(CarryoverLotRow {
            qty,
            cost_sol,
            oldest_opened_ts: parse_ts(
                &oldest_opened_ts_raw,
                "wallet_scoring_carryover_lots.oldest_opened_ts",
            )?,
        })
    })
    .transpose()
}

fn apply_wallet_scoring_carryover_sell_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
    segment_index: i64,
) -> Result<i64> {
    let token = swap.token_in.as_str();
    let Some(carryover) = load_wallet_scoring_carryover_lot_on_conn(conn, &swap.wallet, token)?
    else {
        return Ok(0);
    };
    let qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || carryover.qty <= 1e-12 {
        return Ok(0);
    }

    let take_qty = qty_remaining.min(carryover.qty);
    let lot_fraction = take_qty / carryover.qty;
    let cost_part = carryover.cost_sol * lot_fraction;
    let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
    let pnl_sol = proceeds_part - cost_part;
    let remaining_qty = (carryover.qty - take_qty).max(0.0);
    let remaining_cost = (carryover.cost_sol - cost_part).max(0.0);

    if remaining_qty <= 1e-12 {
        conn.execute(
            "DELETE FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token],
        )
        .context("failed deleting consumed wallet_scoring_carryover_lot")?;
    } else {
        conn.execute(
            "UPDATE wallet_scoring_carryover_lots
             SET qty = ?3, cost_sol = ?4
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token, remaining_qty, remaining_cost],
        )
        .context("failed updating partially consumed wallet_scoring_carryover_lot")?;
    }

    let hold_seconds = (swap.ts_utc - carryover.oldest_opened_ts)
        .num_seconds()
        .max(0);
    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_close_facts(
            sell_signature,
            segment_index,
            wallet_id,
            token,
            closed_ts,
            activity_day,
            pnl_sol,
            hold_seconds,
            win
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            &swap.signature,
            segment_index,
            &swap.wallet,
            token,
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            pnl_sol,
            hold_seconds,
            if pnl_sol > 0.0 { 1 } else { 0 },
        ],
    )
    .context("failed inserting wallet_scoring_close_facts row for carryover lot")?;

    Ok(1)
}

fn apply_wallet_scoring_sell_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let token = swap.token_in.as_str();
    let lots = load_wallet_scoring_open_lots_on_conn(conn, &swap.wallet, token)?;
    if lots.is_empty() {
        let _ = apply_wallet_scoring_carryover_sell_on_conn(conn, swap, 0)?;
        return Ok(());
    }

    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        return Ok(());
    }

    let mut segment_index = 0i64;
    for lot in lots {
        if qty_remaining <= 1e-12 {
            break;
        }
        if lot.qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting empty wallet_scoring_open_lot")?;
            continue;
        }

        let take_qty = qty_remaining.min(lot.qty);
        let lot_fraction = take_qty / lot.qty;
        let cost_part = lot.cost_sol * lot_fraction;
        let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
        let pnl_sol = proceeds_part - cost_part;
        let remaining_qty = (lot.qty - take_qty).max(0.0);
        let remaining_cost = (lot.cost_sol - cost_part).max(0.0);

        if remaining_qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting consumed wallet_scoring_open_lot")?;
        } else {
            conn.execute(
                "UPDATE wallet_scoring_open_lots
                 SET qty = ?2, cost_sol = ?3
                 WHERE buy_signature = ?1",
                params![&lot.buy_signature, remaining_qty, remaining_cost],
            )
            .context("failed updating partially consumed wallet_scoring_open_lot")?;
        }

        let hold_seconds = (swap.ts_utc - lot.opened_ts).num_seconds().max(0);
        conn.execute(
            "INSERT OR IGNORE INTO wallet_scoring_close_facts(
                sell_signature,
                segment_index,
                wallet_id,
                token,
                closed_ts,
                activity_day,
                pnl_sol,
                hold_seconds,
                win
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                &swap.signature,
                segment_index,
                &swap.wallet,
                token,
                swap.ts_utc.to_rfc3339(),
                swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
                pnl_sol,
                hold_seconds,
                if pnl_sol > 0.0 { 1 } else { 0 },
            ],
        )
        .context("failed inserting wallet_scoring_close_facts row")?;

        qty_remaining -= take_qty;
        segment_index += 1;
    }

    if qty_remaining > 1e-12 {
        let carryover_sell = SwapEvent {
            amount_in: qty_remaining,
            ..swap.clone()
        };
        let _ = apply_wallet_scoring_carryover_sell_on_conn(conn, &carryover_sell, segment_index)?;
    }

    Ok(())
}

fn rug_lookahead_stats_on_conn(
    conn: &Connection,
    token: &str,
    buy_ts: DateTime<Utc>,
    lookahead_end: DateTime<Utc>,
) -> Result<(f64, u32)> {
    let (volume_sol, unique_traders_raw): (f64, i64) = conn
        .query_row(
            "SELECT
                COALESCE(SUM(sol_notional), 0.0) AS volume_sol,
                COUNT(DISTINCT wallet_id) AS unique_traders
             FROM (
                SELECT wallet_id, qty_out AS sol_notional
                FROM observed_swaps
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, qty_in AS sol_notional
                FROM observed_swaps
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )",
            params![
                token,
                SOL_MINT,
                buy_ts.to_rfc3339(),
                lookahead_end.to_rfc3339(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("failed querying discovery scoring rug lookahead stats")?;
    Ok((volume_sol.max(0.0), unique_traders_raw.max(0) as u32))
}

fn finalize_mature_rug_facts_on_conn(conn: &Connection, watermark_ts: DateTime<Utc>) -> Result<()> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, token, ts, rug_check_after_ts
             FROM wallet_scoring_buy_facts
             WHERE rug_volume_lookahead_sol IS NULL
               AND rug_check_after_ts <= ?1
             ORDER BY rug_check_after_ts ASC, ts ASC",
        )
        .context("failed preparing pending rug facts query")?;
    let mut rows = stmt
        .query(params![watermark_ts.to_rfc3339()])
        .context("failed querying pending rug facts")?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().context("failed iterating pending rug facts")? {
        let buy_ts_raw: String = row
            .get(2)
            .context("failed reading wallet_scoring_buy_facts.ts")?;
        let check_after_raw: String = row
            .get(3)
            .context("failed reading wallet_scoring_buy_facts.rug_check_after_ts")?;
        pending.push((
            row.get::<_, String>(0)
                .context("failed reading wallet_scoring_buy_facts.buy_signature")?,
            row.get::<_, String>(1)
                .context("failed reading wallet_scoring_buy_facts.token")?,
            parse_ts(&buy_ts_raw, "wallet_scoring_buy_facts.ts")?,
            parse_ts(
                &check_after_raw,
                "wallet_scoring_buy_facts.rug_check_after_ts",
            )?,
        ));
    }

    for (buy_signature, token, buy_ts, check_after_ts) in pending {
        let (volume_sol, unique_traders) =
            rug_lookahead_stats_on_conn(conn, &token, buy_ts, check_after_ts)?;
        conn.execute(
            "UPDATE wallet_scoring_buy_facts
             SET rug_volume_lookahead_sol = ?2,
                 rug_unique_traders_lookahead = ?3
             WHERE buy_signature = ?1",
            params![buy_signature, volume_sol, unique_traders as i64],
        )
        .context("failed updating matured rug facts")?;
    }
    Ok(())
}

fn apply_discovery_scoring_swaps_on_conn(
    conn: &Connection,
    prepared_swaps: &[PreparedScoringSwap],
) -> Result<()> {
    for prepared in prepared_swaps {
        let swap = &prepared.swap;
        upsert_wallet_scoring_day_on_conn(conn, swap)?;
        upsert_wallet_scoring_tx_minute_on_conn(
            conn,
            &swap.wallet,
            swap.ts_utc.timestamp().div_euclid(60),
        )?;
        if let Some(buy_fact) = prepared.buy_fact.as_ref() {
            if let Some(cache_upsert) = buy_fact.quality_cache_upsert.as_ref() {
                upsert_token_quality_cache_on_conn(
                    conn,
                    &cache_upsert.mint,
                    cache_upsert.holders,
                    cache_upsert.liquidity_sol,
                    cache_upsert.token_age_seconds,
                    cache_upsert.fetched_at,
                )?;
            }
            insert_wallet_scoring_buy_fact_on_conn(conn, swap, buy_fact)?;
        } else if is_sol_sell(swap) {
            apply_wallet_scoring_sell_on_conn(conn, swap)?;
        }
    }
    Ok(())
}

impl SqliteStore {
    fn upsert_discovery_scoring_state_ts(
        &self,
        state_key: &str,
        value: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring state update", |conn| {
            conn.execute(
                "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                params![state_key, value.to_rfc3339(), Utc::now().to_rfc3339()],
            )
            .with_context(|| format!("failed upserting discovery_scoring_state.{state_key}"))?;
            Ok(0usize)
        })?;
        Ok(())
    }

    fn load_discovery_scoring_state_ts(&self, state_key: &str) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = ?1",
                params![state_key],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))?;
        raw.map(|raw| parse_ts(&raw, &format!("discovery_scoring_state.{state_key}")))
            .transpose()
    }

    pub fn apply_discovery_scoring_batch(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<()> {
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        self.with_immediate_transaction_retry("discovery scoring batch", |conn| {
            apply_discovery_scoring_swaps_on_conn(conn, &prepared)
        })
    }

    pub fn finalize_discovery_scoring_rug_facts(&self, watermark_ts: DateTime<Utc>) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring rug finalize", |conn| {
            finalize_mature_rug_facts_on_conn(conn, watermark_ts)?;
            Ok(0usize)
        })?;
        Ok(())
    }

    pub fn reset_discovery_scoring_tables(&self) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring reset", |conn| {
            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_open_lots", [])
                .context("failed clearing wallet_scoring_open_lots")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;
            Ok(0usize)
        })?;
        Ok(())
    }

    pub fn set_discovery_scoring_covered_since(&self, covered_since: DateTime<Utc>) -> Result<()> {
        self.upsert_discovery_scoring_state_ts("covered_since_ts", covered_since)
    }

    pub fn set_discovery_scoring_backfill_progress(
        &self,
        start_ts: DateTime<Utc>,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill progress update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_progress_start_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![start_ts.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_progress_start_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_progress_cursor_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.ts_utc.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_progress_cursor_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_progress_cursor_slot', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.slot.to_string(), &now],
                )
                .context(
                    "failed upserting discovery_scoring_state.backfill_progress_cursor_slot",
                )?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_progress_cursor_signature', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![&cursor.signature, &now],
                )
                .context(
                    "failed upserting discovery_scoring_state.backfill_progress_cursor_signature",
                )?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_materialization_gap_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let existing = self.load_discovery_scoring_materialization_gap_cursor()?;
        if existing
            .as_ref()
            .is_some_and(|current| cmp_cursor_order(current, cursor) != Ordering::Greater)
        {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('materialization_gap_since_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.ts_utc.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.materialization_gap_since_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('materialization_gap_since_slot', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.slot.to_string(), &now],
                )
                .context(
                    "failed upserting discovery_scoring_state.materialization_gap_since_slot",
                )?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('materialization_gap_since_signature', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![&cursor.signature, &now],
                )
                .context(
                    "failed upserting discovery_scoring_state.materialization_gap_since_signature",
                )?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_covered_through_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let existing = self.load_discovery_scoring_covered_through_cursor()?;
        if existing
            .as_ref()
            .is_some_and(|current| cmp_cursor_order(current, cursor) != Ordering::Less)
        {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring covered_through cursor update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.ts_utc.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_slot', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.slot.to_string(), &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_slot")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_signature', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![&cursor.signature, &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_signature")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &self,
        observed_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let Some(gap_cursor) = self.load_discovery_scoring_materialization_gap_cursor()? else {
            return Ok(());
        };
        if cmp_cursor_order(observed_cursor, &gap_cursor) != Ordering::Equal {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN (
                    'materialization_gap_since_ts',
                    'materialization_gap_since_slot',
                    'materialization_gap_since_signature'
                 )",
                    [],
                )
                .context("failed clearing discovery_scoring_state.materialization_gap cursor")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_backfill_source_protection(
        &self,
        protect_since: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill source protection",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_protect_since_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![protect_since.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_protect_since_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_protect_expires_at', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![expires_at.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_protect_expires_at")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_backfill_source_protection(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill source protection clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN ('backfill_protect_since_ts', 'backfill_protect_expires_at')",
                    [],
                )
                .context("failed clearing discovery scoring backfill source protection")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_backfill_progress(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill progress clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN (
                    'backfill_progress_start_ts',
                    'backfill_progress_cursor_ts',
                    'backfill_progress_cursor_slot',
                    'backfill_progress_cursor_signature'
                 )",
                    [],
                )
                .context("failed clearing discovery scoring backfill progress")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn load_discovery_scoring_covered_since(&self) -> Result<Option<DateTime<Utc>>> {
        self.load_discovery_scoring_state_ts("covered_since_ts")
    }

    pub fn load_discovery_scoring_backfill_progress(
        &self,
    ) -> Result<Option<(DateTime<Utc>, DiscoveryRuntimeCursor)>> {
        let Some(start_ts) = self.load_discovery_scoring_state_ts("backfill_progress_start_ts")?
        else {
            return Ok(None);
        };
        let cursor_ts = self.load_discovery_scoring_state_ts("backfill_progress_cursor_ts")?;
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'backfill_progress_cursor_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.backfill_progress_cursor_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'backfill_progress_cursor_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.backfill_progress_cursor_signature",
            )?;
        match (cursor_ts, slot_raw, signature) {
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.backfill_progress_cursor_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some((
                    start_ts,
                    DiscoveryRuntimeCursor {
                        ts_utc,
                        slot,
                        signature,
                    },
                )))
            }
            _ => Ok(None),
        }
    }

    pub fn load_discovery_scoring_materialization_gap_cursor(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let Some(ts_utc) = self.load_discovery_scoring_state_ts("materialization_gap_since_ts")?
        else {
            return Ok(None);
        };
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'materialization_gap_since_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.materialization_gap_since_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'materialization_gap_since_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.materialization_gap_since_signature",
            )?;
        match (slot_raw, signature) {
            (Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.materialization_gap_since_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some(DiscoveryRuntimeCursor {
                    ts_utc,
                    slot,
                    signature,
                }))
            }
            _ => Ok(Some(DiscoveryRuntimeCursor {
                ts_utc,
                slot: 0,
                signature: String::new(),
            })),
        }
    }

    pub fn load_discovery_scoring_covered_through(&self) -> Result<Option<DateTime<Utc>>> {
        self.load_discovery_scoring_state_ts("covered_through_ts")
    }

    pub fn load_discovery_scoring_covered_through_cursor(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let Some(ts_utc) = self.load_discovery_scoring_covered_through()? else {
            return Ok(None);
        };
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'covered_through_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.covered_through_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'covered_through_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.covered_through_signature")?;
        let Some(slot_raw) = slot_raw else {
            return Ok(None);
        };
        let Some(signature) = signature else {
            return Ok(None);
        };
        let slot = slot_raw.parse::<u64>().with_context(|| {
            format!("invalid discovery_scoring_state.covered_through_slot value: {slot_raw}")
        })?;
        Ok(Some(DiscoveryRuntimeCursor {
            ts_utc,
            slot,
            signature,
        }))
    }

    pub fn load_discovery_scoring_backfill_protected_since(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>> {
        let Some(expires_at) =
            self.load_discovery_scoring_state_ts("backfill_protect_expires_at")?
        else {
            return Ok(None);
        };
        if expires_at < now {
            return Ok(None);
        }
        self.load_discovery_scoring_state_ts("backfill_protect_since_ts")
    }

    pub fn discovery_scoring_ready_for_window(
        &self,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        max_lag: Duration,
    ) -> Result<bool> {
        let Some(covered_since) = self.load_discovery_scoring_covered_since()? else {
            return Ok(false);
        };
        if self
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some()
        {
            return Ok(false);
        }
        let Some(covered_through) = self.load_discovery_scoring_covered_through_cursor()? else {
            return Ok(false);
        };
        Ok(covered_since <= window_start && covered_through.ts_utc + max_lag >= now)
    }

    pub fn prune_discovery_scoring_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.prune_discovery_scoring_before_batched(cutoff, usize::MAX)
            .map(|summary| summary.deleted_rows)
    }

    pub fn prune_discovery_scoring_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<SqliteBatchedDeleteSummary> {
        let mut summary = SqliteBatchedDeleteSummary::default();
        loop {
            let deleted = self.prune_discovery_scoring_before_batch(cutoff, batch_size)?;
            if deleted == 0 {
                break;
            }
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
    }

    pub fn prune_discovery_scoring_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<usize> {
        let cutoff_day = cutoff.date_naive().format("%Y-%m-%d").to_string();
        let cutoff_ts = cutoff.to_rfc3339();
        let cutoff_minute = cutoff.timestamp().div_euclid(60);
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("discovery scoring retention prune batch", |conn| {
            let mut deleted = 0usize;
            let mut remaining = batch_limit;
            if remaining <= 0 {
                return Ok(0usize);
            }

            let buy_deleted = conn
                .execute(
                    "DELETE FROM wallet_scoring_buy_facts
                         WHERE rowid IN (
                             SELECT rowid
                             FROM wallet_scoring_buy_facts
                             WHERE ts < ?1
                             ORDER BY ts ASC, buy_signature ASC
                             LIMIT ?2
                         )",
                    params![&cutoff_ts, remaining],
                )
                .context("failed pruning wallet_scoring_buy_facts")?;
            deleted += buy_deleted;
            remaining -= buy_deleted as i64;

            if remaining > 0 {
                let close_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_close_facts
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_close_facts
                                 WHERE closed_ts < ?1
                                 ORDER BY closed_ts ASC, sell_signature ASC, segment_index ASC
                                 LIMIT ?2
                             )",
                        params![&cutoff_ts, remaining],
                    )
                    .context("failed pruning wallet_scoring_close_facts")?;
                deleted += close_deleted;
                remaining -= close_deleted as i64;
            }

            if remaining > 0 {
                let tx_minutes_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_tx_minutes
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_tx_minutes
                                 WHERE minute_bucket < ?1
                                 ORDER BY minute_bucket ASC, wallet_id ASC
                                 LIMIT ?2
                             )",
                        params![cutoff_minute, remaining],
                    )
                    .context("failed pruning wallet_scoring_tx_minutes")?;
                deleted += tx_minutes_deleted;
                remaining -= tx_minutes_deleted as i64;
            }

            if remaining > 0 {
                let days_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_days
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_days
                                 WHERE activity_day < ?1
                                 ORDER BY activity_day ASC, wallet_id ASC
                                 LIMIT ?2
                             )",
                        params![&cutoff_day, remaining],
                    )
                    .context("failed pruning wallet_scoring_days")?;
                deleted += days_deleted;
            }
            Ok(deleted)
        })
    }

    pub fn has_wallet_scoring_data_since(&self, window_start: DateTime<Utc>) -> Result<bool> {
        let exists = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM wallet_scoring_days
                    WHERE activity_day >= ?1
                )",
                params![window_start.date_naive().format("%Y-%m-%d").to_string()],
                |row| row.get::<_, i64>(0),
            )
            .context("failed querying wallet_scoring_days existence")?;
        Ok(exists != 0)
    }

    pub fn load_wallet_scoring_days_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringDayRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, activity_day, first_seen, last_seen, trades, spent_sol, max_buy_notional_sol
                 FROM wallet_scoring_days
                 WHERE activity_day >= ?1
                 ORDER BY activity_day ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_days query")?;
        let mut rows = stmt
            .query(params![window_start
                .date_naive()
                .format("%Y-%m-%d")
                .to_string()])
            .context("failed querying wallet_scoring_days")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_days rows")?
        {
            let activity_day_raw: String = row
                .get(1)
                .context("failed reading wallet_scoring_days.activity_day")?;
            let first_seen_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_days.first_seen")?;
            let last_seen_raw: String = row
                .get(3)
                .context("failed reading wallet_scoring_days.last_seen")?;
            let trades_raw: i64 = row
                .get(4)
                .context("failed reading wallet_scoring_days.trades")?;
            out.push(WalletScoringDayRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_days.wallet_id")?,
                activity_day: parse_day(&activity_day_raw, "wallet_scoring_days.activity_day")?,
                first_seen: parse_ts(&first_seen_raw, "wallet_scoring_days.first_seen")?,
                last_seen: parse_ts(&last_seen_raw, "wallet_scoring_days.last_seen")?,
                trades: trades_raw.max(0) as u32,
                spent_sol: row
                    .get(5)
                    .context("failed reading wallet_scoring_days.spent_sol")?,
                max_buy_notional_sol: row
                    .get(6)
                    .context("failed reading wallet_scoring_days.max_buy_notional_sol")?,
            });
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_buy_facts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringBuyFactRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, token, ts, notional_sol,
                        market_volume_5m_sol, market_unique_traders_5m, market_liquidity_proxy_sol,
                        quality_source, quality_token_age_seconds, quality_holders, quality_liquidity_sol,
                        rug_check_after_ts, rug_volume_lookahead_sol, rug_unique_traders_lookahead
                 FROM wallet_scoring_buy_facts
                 WHERE ts >= ?1
                 ORDER BY ts ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_buy_facts query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying wallet_scoring_buy_facts")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_buy_facts rows")?
        {
            let ts_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_buy_facts.ts")?;
            let source_raw: String = row
                .get(7)
                .context("failed reading wallet_scoring_buy_facts.quality_source")?;
            let rug_check_after_raw: String = row
                .get(11)
                .context("failed reading wallet_scoring_buy_facts.rug_check_after_ts")?;
            let market_unique_traders_raw: i64 = row
                .get(5)
                .context("failed reading wallet_scoring_buy_facts.market_unique_traders_5m")?;
            let rug_unique_traders_raw: Option<i64> = row
                .get(13)
                .context("failed reading wallet_scoring_buy_facts.rug_unique_traders_lookahead")?;
            out.push(WalletScoringBuyFactRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_buy_facts.wallet_id")?,
                token: row
                    .get(1)
                    .context("failed reading wallet_scoring_buy_facts.token")?,
                ts: parse_ts(&ts_raw, "wallet_scoring_buy_facts.ts")?,
                notional_sol: row
                    .get(3)
                    .context("failed reading wallet_scoring_buy_facts.notional_sol")?,
                market_volume_5m_sol: row
                    .get(4)
                    .context("failed reading wallet_scoring_buy_facts.market_volume_5m_sol")?,
                market_unique_traders_5m: market_unique_traders_raw.max(0) as u32,
                market_liquidity_proxy_sol: row.get(6).context(
                    "failed reading wallet_scoring_buy_facts.market_liquidity_proxy_sol",
                )?,
                quality_source: match source_raw.as_str() {
                    "fresh" => WalletScoringQualitySource::Fresh,
                    "stale" => WalletScoringQualitySource::Stale,
                    "deferred" => WalletScoringQualitySource::Deferred,
                    "missing" => WalletScoringQualitySource::Missing,
                    other => {
                        return Err(anyhow!(
                            "invalid wallet_scoring_buy_facts.quality_source value: {other}"
                        ));
                    }
                },
                quality_token_age_seconds: row
                    .get::<_, Option<i64>>(8)
                    .context("failed reading wallet_scoring_buy_facts.quality_token_age_seconds")?
                    .map(|value| value.max(0) as u64),
                quality_holders: row
                    .get::<_, Option<i64>>(9)
                    .context("failed reading wallet_scoring_buy_facts.quality_holders")?
                    .map(|value| value.max(0) as u64),
                quality_liquidity_sol: row
                    .get(10)
                    .context("failed reading wallet_scoring_buy_facts.quality_liquidity_sol")?,
                rug_check_after_ts: parse_ts(
                    &rug_check_after_raw,
                    "wallet_scoring_buy_facts.rug_check_after_ts",
                )?,
                rug_volume_lookahead_sol: row
                    .get(12)
                    .context("failed reading wallet_scoring_buy_facts.rug_volume_lookahead_sol")?,
                rug_unique_traders_lookahead: rug_unique_traders_raw
                    .map(|value| value.max(0) as u32),
            });
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_close_facts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringCloseFactRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, token, closed_ts, pnl_sol, hold_seconds, win
                 FROM wallet_scoring_close_facts
                 WHERE closed_ts >= ?1
                 ORDER BY closed_ts ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_close_facts query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying wallet_scoring_close_facts")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_close_facts rows")?
        {
            let closed_ts_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_close_facts.closed_ts")?;
            let hold_seconds_raw: i64 = row
                .get(4)
                .context("failed reading wallet_scoring_close_facts.hold_seconds")?;
            let win_raw: i64 = row
                .get(5)
                .context("failed reading wallet_scoring_close_facts.win")?;
            out.push(WalletScoringCloseFactRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_close_facts.wallet_id")?,
                token: row
                    .get(1)
                    .context("failed reading wallet_scoring_close_facts.token")?,
                closed_ts: parse_ts(&closed_ts_raw, "wallet_scoring_close_facts.closed_ts")?,
                pnl_sol: row
                    .get(3)
                    .context("failed reading wallet_scoring_close_facts.pnl_sol")?,
                hold_seconds: hold_seconds_raw.max(0),
                win: win_raw != 0,
            });
        }
        Ok(out)
    }

    pub fn wallet_scoring_max_tx_counts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, MAX(tx_count)
                 FROM wallet_scoring_tx_minutes
                 WHERE minute_bucket >= ?1
                 GROUP BY wallet_id",
            )
            .context("failed to prepare wallet_scoring_tx_minutes max query")?;
        let mut rows = stmt
            .query(params![window_start.timestamp().div_euclid(60)])
            .context("failed querying wallet_scoring_tx_minutes maxima")?;
        let mut out = HashMap::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_tx_minutes maxima")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading wallet_scoring_tx_minutes.wallet_id")?;
            let max_count_raw: i64 = row
                .get(1)
                .context("failed reading wallet_scoring_tx_minutes max(tx_count)")?;
            out.insert(wallet_id, max_count_raw.max(0) as u32);
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_snapshot_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<WalletScoringSnapshot> {
        self.conn
            .execute_batch("BEGIN DEFERRED TRANSACTION")
            .context("failed to open deferred wallet_scoring snapshot transaction")?;
        let snapshot_result = (|| -> Result<WalletScoringSnapshot> {
            Ok(WalletScoringSnapshot {
                days: self.load_wallet_scoring_days_since(window_start)?,
                buy_facts: self.load_wallet_scoring_buy_facts_since(window_start)?,
                close_facts: self.load_wallet_scoring_close_facts_since(window_start)?,
                max_tx_counts: self.wallet_scoring_max_tx_counts_since(window_start)?,
            })
        })();

        match snapshot_result {
            Ok(snapshot) => {
                self.conn
                    .execute_batch("COMMIT")
                    .context("failed to commit deferred wallet_scoring snapshot transaction")?;
                Ok(snapshot)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}
