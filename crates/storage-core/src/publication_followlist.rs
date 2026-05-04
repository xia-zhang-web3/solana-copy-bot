fn upsert_wallets(conn: &rusqlite::Connection, wallets: &[WalletUpsertRow]) -> Result<()> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(wallet_id) DO UPDATE SET
            first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
            last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
            status = excluded.status",
    )?;
    for wallet in wallets {
        stmt.execute(params![
            &wallet.wallet_id,
            wallet.first_seen.to_rfc3339(),
            wallet.last_seen.to_rfc3339(),
            &wallet.status,
        ])?;
    }
    Ok(())
}

fn insert_metrics(conn: &rusqlite::Connection, metrics: &[WalletMetricRow]) -> Result<()> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO wallet_metrics(
            wallet_id, window_start, pnl, win_rate, trades, closed_trades,
            hold_median_seconds, score, buy_total, tradable_ratio, rug_ratio
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )?;
    for metric in metrics {
        stmt.execute(params![
            &metric.wallet_id,
            metric.window_start.to_rfc3339(),
            metric.pnl,
            metric.win_rate,
            metric.trades as i64,
            metric.closed_trades as i64,
            metric.hold_median_seconds,
            metric.score,
            metric.buy_total as i64,
            metric.tradable_ratio,
            metric.rug_ratio,
        ])?;
    }
    Ok(())
}

fn update_followlist(
    conn: &rusqlite::Connection,
    desired_wallets: &[String],
    allow_activate: bool,
    allow_deactivate: bool,
    now: DateTime<Utc>,
    reason: &str,
) -> Result<FollowlistUpdateResult> {
    let desired = desired_wallets
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let current = active_wallets_on_conn(conn)?;
    let now_raw = now.to_rfc3339();
    let mut result = FollowlistUpdateResult::default();
    if allow_deactivate {
        for wallet_id in current {
            if !desired.contains(wallet_id.as_str()) {
                result.deactivated += conn.execute(
                    "UPDATE followlist SET active = 0, removed_at = ?1, reason = ?2
                     WHERE wallet_id = ?3 AND active = 1",
                    params![&now_raw, reason, &wallet_id],
                )?;
            }
        }
    }
    if allow_activate {
        for wallet_id in desired_wallets {
            result.activated += conn.execute(
                "INSERT OR IGNORE INTO followlist(wallet_id, added_at, reason, active)
                 VALUES (?1, ?2, ?3, 1)",
                params![wallet_id, &now_raw, reason],
            )?;
        }
    }
    Ok(result)
}

fn active_wallets_on_conn(conn: &rusqlite::Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT wallet_id FROM followlist WHERE active = 1")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed collecting active followlist wallets")
}
