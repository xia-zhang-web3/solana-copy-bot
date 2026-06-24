use anyhow::Result;
use chrono::{TimeZone, Utc};
use copybot_operators::track_b_entry_quote_report::{build_report, Cli};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

#[test]
fn stamped_discovery_rank_survives_empty_wallet_metrics() -> Result<()> {
    let db = NamedTempFile::new()?;
    let conn = Connection::open(db.path())?;
    create_schema(&conn)?;
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status,
            request_ts, signal_ts, discovery_rank, discovery_rank_cohort,
            discovery_rank_window_start, quote_price_sol, shadow_price_sol, price_impact_pct
        ) VALUES (
            'quote:entry-shadow-diag:signal-stamped', 'signal-stamped', 'wallet-b',
            'token-b', 'buy', 'ok', ?1, ?2, 16, 'rank_16_30', ?3, 1.1, 1.0, 0.2
        )",
        params![
            "2026-06-23T00:10:00+00:00",
            "2026-06-23T00:00:00+00:00",
            "2026-06-23T00:00:00+00:00",
        ],
    )?;
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol,
            pnl_sol, opened_ts, closed_ts, close_context
        ) VALUES (
            'sell-stamped', 'wallet-b', 'token-b', 1.0, 1.0, 1.2, 0.2,
            ?1, ?2, 'stale_quote_price'
        )",
        params!["2026-06-23T00:00:00+00:00", "2026-06-23T01:00:00+00:00",],
    )?;
    drop(conn);

    let report = build_report(
        Cli {
            db_path: Some(db.path().to_path_buf()),
            since: Some(Utc.with_ymd_and_hms(2026, 6, 23, 0, 0, 0).unwrap()),
            until: Some(Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap()),
            json: true,
            ..Cli::default()
        },
        Utc.with_ymd_and_hms(2026, 6, 24, 0, 0, 0).unwrap(),
    );
    let summary = report.summary.expect("report should load");

    let cohort = summary
        .by_rank_cohort
        .iter()
        .find(|row| row.cohort == "rank_16_30")
        .expect("stamped rank 16 should classify into cohort B");
    assert_eq!(cohort.rank_min, Some(16));
    assert_eq!(cohort.rank_max, Some(30));
    assert_eq!(cohort.events, 1);
    assert!(
        summary
            .by_rank_cohort
            .iter()
            .all(|row| row.cohort != "unranked" || row.events == 0),
        "stamped events must not depend on wallet_metrics fallback"
    );
    assert!(summary
        .caveats
        .iter()
        .any(|line| line.contains("prefers discovery_rank")));
    Ok(())
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
            discovery_rank INTEGER,
            discovery_rank_cohort TEXT,
            discovery_rank_window_start TEXT,
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
