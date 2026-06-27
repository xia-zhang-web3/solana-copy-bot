use anyhow::Result;
use chrono::{TimeZone, Utc};
use copybot_operators::track_b_entry_quote_report::{build_report, Cli};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

#[test]
fn splits_exit_executability_and_excludes_contaminated_ratios() -> Result<()> {
    let db = seed_db()?;
    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");

    assert_eq!(summary.counts.total_events, 5);
    assert_eq!(summary.counts.ok_events, 4);
    assert_eq!(summary.counts.error_events, 1);
    assert_eq!(summary.counts.closed_events, 3);
    assert_eq!(summary.counts.clean_closed_usable_events, 2);
    assert_eq!(summary.counts.contaminated_ratio_events, 1);
    assert_eq!(summary.counts.multi_close_match_events, 1);
    assert_eq!(summary.counts.truncated_at_close_match_limit_events, 0);

    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .expect("market bucket should exist");
    let stale = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "stale_quote_price")
        .expect("stale quote bucket should exist");
    let fully = summary
        .by_exit_executability
        .iter()
        .find(|row| row.bucket == "fully_executable")
        .expect("fully executable bucket should exist");
    let hybrid = summary
        .by_exit_executability
        .iter()
        .find(|row| row.bucket == "hybrid_paper_exit")
        .expect("hybrid bucket should exist");

    assert_eq!(market.events, 1);
    assert_eq!(stale.events, 1);
    assert_eq!(fully.events, 1);
    assert_eq!(hybrid.events, 1);
    assert_eq!(market.fully_executable_events, 0);
    assert_eq!(market.fully_executable_coverage, Some(0.0));
    assert_eq!(market.fully_executable_pnl_sol, None);
    assert_eq!(market.fully_executable_shadow_pnl_sol, None);
    assert_eq!(market.fully_executable_delta_sol, None);
    assert_eq!(market.market_exit_missing_quote_events, 2);
    assert!((stale.entry_adjusted_pnl_sol + 0.4).abs() < 1e-9);
    assert!((market.shadow_pnl_sol - 0.3).abs() < 1e-9);

    let ratio_110 = summary
        .quote_shadow_ratio_sweep
        .iter()
        .find(|row| (row.threshold_gt - 1.10).abs() < 1e-9)
        .expect("ratio threshold should exist");
    assert_eq!(ratio_110.rejected_events, 2);
    assert_eq!(ratio_110.rejected_market_events, 1);
    assert_eq!(ratio_110.rejected_stale_quote_events, 1);
    Ok(())
}

#[test]
fn mixed_close_context_is_reported_separately() -> Result<()> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    insert_event(
        &conn,
        "mixed",
        "signal-mixed",
        "w2",
        "token-m",
        1.2,
        1.0,
        0.2,
    )?;
    let market_close_id = insert_close(&conn, "sell-a", "w2", "token-m", "market", 1.0, 1.1, 0.1)?;
    insert_market_exit_quote(&conn, market_close_id, "w2", "token-m", 1.0, 1.0)?;
    insert_close(
        &conn,
        "sell-b",
        "w2",
        "token-m",
        "stale_quote_price",
        0.5,
        0.2,
        -0.3,
    )?;
    drop(conn);

    let report = build_report(
        test_cli(db.path()),
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");

    assert_eq!(summary.counts.clean_closed_usable_events, 1);
    assert_eq!(summary.counts.multi_close_match_events, 1);
    assert_eq!(summary.counts.mixed_close_context_events, 1);
    assert_eq!(
        summary
            .by_close_bucket
            .iter()
            .find(|row| row.bucket == "mixed")
            .expect("mixed bucket should exist")
            .events,
        1
    );
    assert_eq!(
        summary
            .by_exit_executability
            .iter()
            .find(|row| row.bucket == "mixed_ambiguous")
            .expect("mixed executability bucket should exist")
            .events,
        1
    );
    assert_eq!(
        summary
            .by_exit_executability
            .iter()
            .find(|row| row.bucket == "fully_executable")
            .expect("fully executable bucket should exist")
            .events,
        0
    );
    Ok(())
}

#[test]
fn reports_close_match_limit_hits() -> Result<()> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    insert_event(&conn, "cap", "signal-cap", "w3", "token-cap", 1.2, 1.0, 0.2)?;
    insert_close(&conn, "sell-a", "w3", "token-cap", "market", 1.0, 1.1, 0.1)?;
    insert_close(&conn, "sell-b", "w3", "token-cap", "market", 0.5, 0.6, 0.1)?;
    drop(conn);

    let report = build_report(
        Cli {
            close_match_limit: 1,
            ..test_cli(db.path())
        },
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");

    assert_eq!(summary.counts.closed_events, 1);
    assert_eq!(summary.counts.multi_close_match_events, 0);
    assert_eq!(summary.counts.truncated_at_close_match_limit_events, 1);
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

fn seed_db() -> Result<NamedTempFile> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    insert_event(
        &conn,
        "market",
        "signal-buy-market",
        "w1",
        "token-a",
        1.25,
        1.0,
        0.2,
    )?;
    insert_close(&conn, "sell-a", "w1", "token-a", "market", 1.0, 1.2, 0.2)?;
    insert_close(&conn, "sell-b", "w1", "token-a", "market", 0.5, 0.6, 0.1)?;
    insert_event(
        &conn,
        "stale",
        "signal-buy-stale",
        "w1",
        "token-b",
        2.0,
        1.0,
        0.2,
    )?;
    insert_close(
        &conn,
        "sell-c",
        "w1",
        "token-b",
        "stale_quote_price",
        1.0,
        1.2,
        0.2,
    )?;
    insert_event(
        &conn,
        "contaminated",
        "signal-buy-contaminated",
        "w1",
        "token-c",
        100.0,
        1.0,
        0.2,
    )?;
    insert_close(
        &conn,
        "sell-d",
        "w1",
        "token-c",
        "stale_quote_price",
        1.0,
        1.1,
        0.1,
    )?;
    insert_event(&conn, "open", "signal-open", "w1", "token-d", 1.0, 1.0, 0.0)?;
    insert_error_event(&conn, "error", "signal-error", "w1", "token-e")?;
    drop(conn);
    Ok(db)
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

fn insert_error_event(
    conn: &Connection,
    suffix: &str,
    signal_id: &str,
    wallet: &str,
    token: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status, request_ts, signal_ts
        ) VALUES (?1, ?2, ?3, ?4, 'buy', 'error', ?5, ?6)",
        params![
            format!("quote:entry-shadow-diag:{suffix}"),
            signal_id,
            wallet,
            token,
            "2026-06-23T00:11:00+00:00",
            "2026-06-23T00:00:00+00:00",
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
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol,
            pnl_sol, opened_ts, closed_ts, close_context
        ) VALUES (?1, ?2, ?3, 1.0, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            signal_id,
            wallet,
            token,
            entry_cost,
            exit_value,
            pnl,
            "2026-06-23T00:00:00+00:00",
            "2026-06-23T01:00:00+00:00",
            context,
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
    insert_market_exit_quote_status(
        conn,
        close_id,
        wallet,
        token,
        "ok",
        Some(quote_price),
        Some(shadow_price),
        2_000,
    )
}

fn insert_market_exit_quote_status(
    conn: &Connection,
    close_id: i64,
    wallet: &str,
    token: &str,
    status: &str,
    quote_price: Option<f64>,
    shadow_price: Option<f64>,
    delay_ms: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
            decision_delay_ms, quote_latency_ms, quote_price_sol, shadow_price_sol
        ) VALUES (?1, ?2, ?3, 'sell', ?4, ?5, ?6, ?7, 100, ?8, ?9)",
        params![
            format!("quote:market-exit-shadow-diag:{close_id}"),
            wallet,
            token,
            status,
            "2026-06-23T01:00:02+00:00",
            "2026-06-23T01:00:00+00:00",
            delay_ms,
            quote_price,
            shadow_price,
        ],
    )?;
    Ok(())
}
