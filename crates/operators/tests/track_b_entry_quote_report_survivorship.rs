use anyhow::Result;
use chrono::{TimeZone, Utc};
use copybot_operators::track_b_entry_quote_report::{build_report, Cli};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

#[test]
fn market_exit_error_books_zero_exit_not_missing() -> Result<()> {
    let db = TestDb::new()?;
    db.insert_entry("dead", "signal-dead", "w5", "token-dead")?;
    let close_id = db.insert_market_close("sell-dead", "w5", "token-dead", 1.0, 1.4, 0.4)?;
    db.insert_market_exit_quote(
        close_id,
        "w5",
        "token-dead",
        "error",
        Some(r#"{"errorCode":"TOKEN_NOT_TRADABLE"}"#),
        None,
        None,
        2_000,
    )?;

    let summary = build_report(test_cli(db.path()), report_time())
        .summary
        .expect("report should load");
    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .unwrap();

    assert_eq!(summary.counts.market_exit_error_events, 1);
    assert_eq!(summary.counts.market_exit_dead_error_events, 1);
    assert_eq!(summary.counts.market_exit_transient_error_events, 0);
    assert_eq!(summary.counts.market_exit_zero_exit_events, 1);
    assert_eq!(summary.counts.market_exit_missing_quote_events, 0);
    assert_eq!(market.fully_executable_events, 1);
    assert!((market.fully_executable_pnl_sol.expect("zero exit pnl") + 1.0).abs() < 1e-9);
    Ok(())
}

#[test]
fn market_exit_transient_error_is_no_data_not_zero_exit() -> Result<()> {
    let db = TestDb::new()?;
    db.insert_entry("transient", "signal-transient", "w7", "token-transient")?;
    let close_id =
        db.insert_market_close("sell-transient", "w7", "token-transient", 1.0, 1.4, 0.4)?;
    db.insert_market_exit_quote(
        close_id,
        "w7",
        "token-transient",
        "error",
        Some(r#"{"errorCode":"CANNOT_COMPUTE_OTHER_AMOUNT_THRESHOLD"}"#),
        None,
        None,
        2_000,
    )?;

    let summary = build_report(test_cli(db.path()), report_time())
        .summary
        .expect("report should load");
    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .unwrap();

    assert_eq!(summary.counts.market_exit_error_events, 1);
    assert_eq!(summary.counts.market_exit_dead_error_events, 0);
    assert_eq!(summary.counts.market_exit_transient_error_events, 1);
    assert_eq!(summary.counts.market_exit_zero_exit_events, 0);
    assert_eq!(summary.counts.market_exit_missing_quote_events, 1);
    assert_eq!(market.fully_executable_events, 0);
    assert_eq!(market.fully_executable_pnl_sol, None);
    Ok(())
}

#[test]
fn max_market_exit_delay_excludes_late_quote_as_no_data() -> Result<()> {
    let db = TestDb::new()?;
    db.insert_entry("late", "signal-late", "w6", "token-late")?;
    let close_id = db.insert_market_close("sell-late", "w6", "token-late", 1.0, 1.2, 0.2)?;
    db.insert_market_exit_quote(
        close_id,
        "w6",
        "token-late",
        "ok",
        None,
        Some(1.0),
        Some(1.0),
        60_000,
    )?;

    let summary = build_report(
        Cli {
            max_market_exit_delay_ms: Some(30_000),
            ..test_cli(db.path())
        },
        report_time(),
    )
    .summary
    .expect("report should load");
    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .unwrap();

    assert_eq!(summary.counts.market_exit_missing_quote_events, 1);
    assert_eq!(market.fully_executable_events, 0);
    Ok(())
}

#[test]
fn market_exit_ratio_outlier_is_no_data_not_executable() -> Result<()> {
    let db = TestDb::new()?;
    db.insert_entry("outlier", "signal-outlier", "w8", "token-outlier")?;
    let close_id = db.insert_market_close("sell-outlier", "w8", "token-outlier", 1.0, 1.2, 0.2)?;
    db.insert_market_exit_quote(
        close_id,
        "w8",
        "token-outlier",
        "ok",
        None,
        Some(20.0),
        Some(1.0),
        2_000,
    )?;

    let summary = build_report(test_cli(db.path()), report_time())
        .summary
        .expect("report should load");
    let market = summary
        .by_close_bucket
        .iter()
        .find(|row| row.bucket == "market")
        .unwrap();

    assert_eq!(summary.counts.market_exit_ratio_outlier_events, 1);
    assert_eq!(summary.counts.market_exit_missing_quote_events, 1);
    assert_eq!(summary.counts.market_exit_quote_events, 0);
    assert_eq!(market.market_exit_ratio_outlier_events, 1);
    assert_eq!(market.fully_executable_events, 0);
    assert_eq!(market.fully_executable_pnl_sol, None);
    Ok(())
}

struct TestDb {
    file: NamedTempFile,
    conn: Connection,
}

impl TestDb {
    fn new() -> Result<Self> {
        let file = NamedTempFile::new()?;
        let conn = Connection::open(file.path())?;
        create_schema(&conn)?;
        Ok(Self { file, conn })
    }

    fn path(&self) -> &std::path::Path {
        self.file.path()
    }

    fn insert_entry(&self, suffix: &str, signal_id: &str, wallet: &str, token: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO execution_quote_canary_events(
                event_id, signal_id, wallet_id, token, side, quote_status,
                request_ts, signal_ts, quote_price_sol, shadow_price_sol, price_impact_pct
            ) VALUES (?1, ?2, ?3, ?4, 'buy', 'ok', ?5, ?6, 1.0, 1.0, 0.2)",
            params![
                format!("quote:entry-shadow-diag:{suffix}"),
                signal_id,
                wallet,
                token,
                "2026-06-23T00:10:00+00:00",
                "2026-06-23T00:00:00+00:00",
            ],
        )?;
        Ok(())
    }

    fn insert_market_close(
        &self,
        signal_id: &str,
        wallet: &str,
        token: &str,
        entry_cost: f64,
        exit_value: f64,
        pnl: f64,
    ) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol,
                pnl_sol, opened_ts, closed_ts, close_context
            ) VALUES (?1, ?2, ?3, 1.0, ?4, ?5, ?6, ?7, ?8, 'market')",
            params![
                signal_id,
                wallet,
                token,
                entry_cost,
                exit_value,
                pnl,
                "2026-06-23T00:00:00+00:00",
                "2026-06-23T01:00:00+00:00",
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn insert_market_exit_quote(
        &self,
        close_id: i64,
        wallet: &str,
        token: &str,
        status: &str,
        error: Option<&str>,
        quote_price: Option<f64>,
        shadow_price: Option<f64>,
        delay_ms: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO execution_quote_canary_events(
            event_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
                decision_delay_ms, quote_latency_ms, quote_price_sol, shadow_price_sol, error
            ) VALUES (?1, ?2, ?3, 'sell', ?4, ?5, ?6, ?7, 100, ?8, ?9, ?10)",
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
                error,
            ],
        )?;
        Ok(())
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
            quote_price_sol REAL,
            shadow_price_sol REAL,
            price_impact_pct REAL,
            error TEXT
        );
        CREATE INDEX idx_execution_quote_canary_events_side_request_ts
            ON execution_quote_canary_events(side, request_ts);
        CREATE TABLE wallet_metrics(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL,
            window_start TEXT NOT NULL,
            score REAL NOT NULL DEFAULT 0
        );
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

fn report_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap()
}
