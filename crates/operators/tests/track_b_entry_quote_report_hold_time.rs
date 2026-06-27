use anyhow::Result;
use chrono::{TimeZone, Utc};
use copybot_operators::track_b_entry_quote_report::{build_report, Cli};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

#[test]
fn market_exit_quote_makes_market_bucket_fully_executable() -> Result<()> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    insert_event(
        &conn,
        "market-exit",
        "signal-market-exit",
        "w4",
        "token-exit",
        2.0,
        1.0,
        0.2,
    )?;
    insert_metric_rank(&conn, "w4", 16)?;
    let close_id = insert_close(
        &conn,
        "sell-market-exit",
        "w4",
        "token-exit",
        "market",
        1.0,
        1.2,
        0.2,
    )?;
    insert_market_exit_quote(&conn, close_id, "w4", "token-exit", 0.75, 1.0)?;
    drop(conn);

    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");
    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .expect("market bucket should exist");
    let fully = summary
        .by_exit_executability
        .iter()
        .find(|row| row.bucket == "fully_executable")
        .expect("fully executable bucket should exist");

    assert_eq!(summary.counts.market_exit_quote_events, 1);
    assert_eq!(summary.counts.market_exit_missing_quote_events, 0);
    assert_eq!(market.events, 1);
    assert_eq!(market.fully_executable_events, 1);
    assert_eq!(market.market_exit_quote_events, 1);
    assert_eq!(market.fully_executable_coverage, Some(1.0));
    let rank_16_30 = summary
        .by_rank_cohort
        .iter()
        .find(|row| row.cohort == "rank_16_30")
        .expect("rank 16-30 cohort should exist");
    assert_eq!(rank_16_30.events, 1);
    assert!((market.entry_adjusted_pnl_sol + 0.4).abs() < 1e-9);
    assert!((market.fully_executable_pnl_sol.expect("full pnl") + 0.55).abs() < 1e-9);
    assert!(
        (market
            .fully_executable_shadow_pnl_sol
            .expect("matched shadow pnl")
            - 0.2)
            .abs()
            < 1e-9
    );
    assert!(
        (market
            .fully_executable_delta_sol
            .expect("matched executable delta")
            + 0.75)
            .abs()
            < 1e-9
    );
    assert_eq!(market.fully_executable_delta_stats.count, 1);
    assert!((market.fully_executable_delta_stats.p50.expect("delta p50") + 0.75).abs() < 1e-9);
    assert_eq!(fully.events, 1);
    assert!((fully.fully_executable_pnl_sol.expect("full bucket pnl") + 0.55).abs() < 1e-9);
    Ok(())
}

#[test]
fn fully_executable_delta_uses_matched_shadow_subset() -> Result<()> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    insert_event(
        &conn,
        "executable-market",
        "signal-executable-market",
        "w-exec",
        "token-exec",
        2.0,
        1.0,
        0.2,
    )?;
    let close_id = insert_close(
        &conn,
        "sell-executable-market",
        "w-exec",
        "token-exec",
        "market",
        1.0,
        1.2,
        0.2,
    )?;
    insert_market_exit_quote(&conn, close_id, "w-exec", "token-exec", 0.75, 1.0)?;

    insert_event(
        &conn,
        "paper-only-market",
        "signal-paper-only-market",
        "w-paper",
        "token-paper",
        1.0,
        1.0,
        0.2,
    )?;
    insert_close(
        &conn,
        "sell-paper-only-market",
        "w-paper",
        "token-paper",
        "market",
        1.0,
        11.0,
        10.0,
    )?;
    drop(conn);

    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");
    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .expect("market bucket should exist");

    assert_eq!(market.events, 2);
    assert_eq!(market.fully_executable_events, 1);
    assert_eq!(market.fully_executable_coverage, Some(0.5));
    assert!((market.shadow_pnl_sol - 10.2).abs() < 1e-9);
    assert!((market.fully_executable_pnl_sol.expect("full pnl") + 0.55).abs() < 1e-9);
    assert!(
        (market
            .fully_executable_shadow_pnl_sol
            .expect("matched shadow pnl")
            - 0.2)
            .abs()
            < 1e-9
    );
    assert!(
        (market
            .fully_executable_delta_sol
            .expect("matched executable delta")
            + 0.75)
            .abs()
            < 1e-9
    );
    Ok(())
}

#[test]
fn splits_gap_by_hold_time_bucket() -> Result<()> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    insert_event(
        &conn,
        "fast-hold",
        "signal-fast",
        "w-fast",
        "token-fast",
        1.0,
        1.0,
        0.2,
    )?;
    insert_close_with_times(
        &conn,
        "sell-fast",
        "w-fast",
        "token-fast",
        "market",
        1.0,
        1.1,
        0.1,
        "2026-06-23T00:00:00+00:00",
        "2026-06-23T00:04:00+00:00",
    )?;
    insert_event(
        &conn,
        "slow-hold",
        "signal-slow",
        "w-slow",
        "token-slow",
        1.0,
        1.0,
        0.2,
    )?;
    insert_close_with_times(
        &conn,
        "sell-slow",
        "w-slow",
        "token-slow",
        "market",
        1.0,
        1.3,
        0.3,
        "2026-06-23T00:00:00+00:00",
        "2026-06-23T01:00:00+00:00",
    )?;
    drop(conn);

    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");
    let under_5m = summary
        .by_hold_time_bucket
        .iter()
        .find(|row| row.bucket == "under_5m")
        .expect("under_5m bucket");
    let one_to_six_hours = summary
        .by_hold_time_bucket
        .iter()
        .find(|row| row.bucket == "1h_to_6h")
        .expect("1h_to_6h bucket");

    assert_eq!(under_5m.events, 1);
    assert_eq!(one_to_six_hours.events, 1);
    assert!((under_5m.shadow_pnl_sol - 0.1).abs() < 1e-9);
    assert!((one_to_six_hours.shadow_pnl_sol - 0.3).abs() < 1e-9);
    Ok(())
}

fn test_cli(path: &std::path::Path) -> Cli {
    Cli {
        db_path: Some(path.to_path_buf()),
        since: Some(Utc.with_ymd_and_hms(2026, 6, 23, 0, 0, 0).unwrap()),
        until: Some(Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap()),
        limit: 100,
        close_match_limit: 16,
        ..Cli::default()
    }
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE execution_quote_canary_events(
            event_id TEXT PRIMARY KEY,
            signal_id TEXT,
            shadow_closed_trade_id INTEGER,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            side TEXT NOT NULL,
            quote_status TEXT NOT NULL,
            request_ts TEXT NOT NULL,
            signal_ts TEXT,
            decision_delay_ms INTEGER,
            quote_latency_ms INTEGER,
            leader_notional_sol REAL,
            quote_in_amount_raw TEXT,
            quote_out_amount_raw TEXT,
            quote_response_json TEXT,
            quote_price_sol REAL,
            shadow_price_sol REAL,
            slippage_bps REAL,
            price_impact_pct REAL,
            route_plan_json TEXT,
            priority_fee_status TEXT,
            priority_fee_lamports INTEGER,
            priority_fee_json TEXT,
            decision_status TEXT,
            decision_reason TEXT,
            error TEXT
        );
        CREATE INDEX idx_execution_quote_canary_events_side_request_ts
            ON execution_quote_canary_events(side, request_ts);
        CREATE TABLE wallet_metrics(id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL, window_start TEXT NOT NULL, score REAL NOT NULL DEFAULT 0);
        CREATE INDEX idx_wallet_metrics_window_start ON wallet_metrics(window_start);
        CREATE TABLE shadow_closed_trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            qty REAL NOT NULL,
            entry_cost_sol REAL NOT NULL,
            exit_value_sol REAL NOT NULL,
            pnl_sol REAL NOT NULL,
            opened_ts TEXT NOT NULL,
            closed_ts TEXT NOT NULL,
            close_context TEXT
        );
        CREATE INDEX idx_shadow_closed_trades_wallet_closed_ts
            ON shadow_closed_trades(wallet_id, closed_ts);
        ",
    )?;
    Ok(())
}

fn insert_metric_rank(conn: &Connection, wallet: &str, rank: u64) -> Result<()> {
    for index in 1..rank {
        conn.execute(
            "INSERT INTO wallet_metrics(wallet_id, window_start, score) VALUES (?1, ?2, ?3)",
            params![
                format!("rank-before-{index:02}"),
                "2026-06-23T00:00:00+00:00",
                10_000.0 - index as f64
            ],
        )?;
    }
    conn.execute(
        "INSERT INTO wallet_metrics(wallet_id, window_start, score) VALUES (?1, ?2, ?3)",
        params![wallet, "2026-06-23T00:00:00+00:00", 1.0],
    )?;
    Ok(())
}

fn insert_event(
    conn: &Connection,
    suffix: &str,
    signal_id: &str,
    wallet: &str,
    token: &str,
    quote_price: f64,
    shadow_price: f64,
    impact: f64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status,
            request_ts, signal_ts, quote_price_sol, shadow_price_sol, price_impact_pct
        ) VALUES (?1, ?2, ?3, ?4, 'buy', 'ok', ?5, ?6, ?7, ?8, ?9)",
        params![
            format!("quote:entry-shadow-diag:{suffix}"),
            signal_id,
            wallet,
            token,
            "2026-06-23T00:10:00+00:00",
            "2026-06-23T00:00:00+00:00",
            quote_price,
            shadow_price,
            impact
        ],
    )?;
    Ok(())
}

fn insert_close(
    conn: &Connection,
    signal_id: &str,
    wallet: &str,
    token: &str,
    context: &str,
    entry_cost: f64,
    exit_value: f64,
    pnl: f64,
) -> Result<i64> {
    insert_close_with_times(
        conn,
        signal_id,
        wallet,
        token,
        context,
        entry_cost,
        exit_value,
        pnl,
        "2026-06-23T00:00:00+00:00",
        "2026-06-23T01:00:00+00:00",
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_close_with_times(
    conn: &Connection,
    signal_id: &str,
    wallet: &str,
    token: &str,
    context: &str,
    entry_cost: f64,
    exit_value: f64,
    pnl: f64,
    opened_ts: &str,
    closed_ts: &str,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol,
            pnl_sol, opened_ts, closed_ts, close_context
        ) VALUES (?1, ?2, ?3, 1.0, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            signal_id, wallet, token, entry_cost, exit_value, pnl, opened_ts, closed_ts, context,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

fn insert_market_exit_quote(
    conn: &Connection,
    close_id: i64,
    wallet: &str,
    token: &str,
    quote_price: f64,
    shadow_price: f64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
            decision_delay_ms, quote_latency_ms, quote_price_sol, shadow_price_sol
        ) VALUES (?1, ?2, ?3, 'sell', 'ok', ?4, ?5, 2000, 100, ?6, ?7)",
        params![
            format!("quote:market-exit-shadow-diag:{close_id}"),
            wallet,
            token,
            "2026-06-23T01:00:02+00:00",
            "2026-06-23T01:00:00+00:00",
            quote_price,
            shadow_price,
        ],
    )?;
    Ok(())
}
