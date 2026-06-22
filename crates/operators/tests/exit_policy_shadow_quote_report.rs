use chrono::{Duration, TimeZone, Utc};
use copybot_operators::exit_policy_shadow_quote_report::{build_report, parse_args_from};
use rusqlite::{params, Connection};
use tempfile::tempdir;

#[test]
fn report_summarizes_blind_and_conditional_benefit() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("runtime.db");
    let conn = Connection::open(&db_path).unwrap();
    create_schema(&conn);

    let opened = Utc.with_ymd_and_hms(2026, 6, 20, 12, 0, 0).unwrap();
    let trigger = opened + Duration::minutes(30);
    insert_event(
        &conn,
        "quote:exit-policy:backstop30m:1",
        trigger,
        "ok",
        None,
        Some(0.8),
        Some(1.0),
        Some(10_000),
    );
    insert_close(
        &conn,
        "sig-1",
        trigger + Duration::minutes(10),
        10.0,
        5.0,
        "stale_quote_price",
    );

    let opened2 = opened + Duration::minutes(1);
    let trigger2 = opened2 + Duration::minutes(30);
    insert_event(
        &conn,
        "quote:exit-policy:backstop30m:2",
        trigger2,
        "ok",
        None,
        Some(1.1),
        Some(1.0),
        Some(10_000),
    );
    insert_close(
        &conn,
        "sig-2",
        trigger2 + Duration::minutes(10),
        10.0,
        12.0,
        "market",
    );

    let opened3 = opened + Duration::minutes(2);
    let trigger3 = opened3 + Duration::minutes(30);
    insert_event(
        &conn,
        "quote:exit-policy:backstop30m:3",
        trigger3,
        "ok",
        None,
        Some(0.4),
        Some(1.0),
        Some(10_000),
    );
    insert_close(
        &conn,
        "sig-3",
        trigger3 + Duration::minutes(10),
        10.0,
        0.0,
        "stale_terminal_zero_price",
    );

    let trigger4 = opened + Duration::minutes(32);
    insert_event(
        &conn,
        "quote:exit-policy:backstop30m:4",
        trigger4,
        "error",
        Some("NO_ROUTES_FOUND"),
        None,
        Some(1.0),
        Some(10_000),
    );

    let cli = parse_args_from([
        "--db-path",
        db_path.to_str().unwrap(),
        "--json",
        "--since",
        "2026-06-20T11:00:00Z",
        "--until",
        "2026-06-20T14:00:00Z",
        "--exit-cost-sol",
        "0",
        "--thresholds",
        "0.9",
    ])
    .unwrap();
    let report = build_report(cli, Utc.with_ymd_and_hms(2026, 6, 20, 14, 0, 0).unwrap());

    let counts = report.counts.as_ref().unwrap();
    assert_eq!(counts.total_events, 4);
    assert_eq!(counts.ok_events, 3);
    assert_eq!(counts.error_events, 1);
    assert_eq!(counts.future_close_matched_events, 3);

    let benefit = report.benefit.as_ref().unwrap();
    assert_eq!(benefit.event_count, 3);
    assert!((benefit.gross_benefit_sol - 6.0).abs() < 0.000001);

    let sweep = &report.conditional_sweeps[0];
    assert_eq!(sweep.all.event_count, 2);
    assert!((sweep.all.gross_benefit_sol - 7.0).abs() < 0.000001);
    let report_json = serde_json::to_value(&report).unwrap();
    assert_bucket(&report_json, "executable_vs_executable", 1);
    assert_bucket(&report_json, "executable_vs_paper", 1);
    assert_bucket(&report_json, "executable_vs_zero", 1);
    assert_eq!(report.by_error_class[0].error_class, "no_route");
}

fn assert_bucket(report_json: &serde_json::Value, name: &str, event_count: u64) {
    let bucket = report_json["by_comparability_bucket"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["bucket"] == name)
        .unwrap_or_else(|| panic!("missing bucket {name}"));
    assert_eq!(bucket["event_count"], event_count);
}

fn create_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE execution_quote_canary_events (
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
        CREATE TABLE shadow_closed_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            close_context TEXT,
            qty REAL NOT NULL,
            entry_cost_sol REAL NOT NULL,
            exit_value_sol REAL NOT NULL,
            pnl_sol REAL NOT NULL,
            opened_ts TEXT NOT NULL,
            closed_ts TEXT NOT NULL
        );
        CREATE INDEX idx_shadow_closed_trades_wallet_closed_ts
            ON shadow_closed_trades(wallet_id, closed_ts);",
    )
    .unwrap();
}

fn insert_event(
    conn: &Connection,
    event_id: &str,
    signal_ts: chrono::DateTime<Utc>,
    quote_status: &str,
    error: Option<&str>,
    quote_price_sol: Option<f64>,
    shadow_price_sol: Option<f64>,
    decision_delay_ms: Option<i64>,
) {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
            decision_delay_ms, quote_price_sol, shadow_price_sol, error
         ) VALUES (?1, 'wallet', 'token', 'sell', ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            event_id,
            quote_status,
            signal_ts.to_rfc3339(),
            signal_ts.to_rfc3339(),
            decision_delay_ms,
            quote_price_sol,
            shadow_price_sol,
            error,
        ],
    )
    .unwrap();
}

fn insert_close(
    conn: &Connection,
    signal_id: &str,
    closed_ts: chrono::DateTime<Utc>,
    qty: f64,
    exit_value_sol: f64,
    close_context: &str,
) {
    let opened_ts = closed_ts - Duration::minutes(40);
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, close_context, qty, entry_cost_sol, exit_value_sol,
            pnl_sol, opened_ts, closed_ts
         ) VALUES (?1, 'wallet', 'token', ?2, ?3, 10.0, ?4, ?5, ?6, ?7)",
        params![
            signal_id,
            close_context,
            qty,
            exit_value_sol,
            exit_value_sol - 10.0,
            opened_ts.to_rfc3339(),
            closed_ts.to_rfc3339(),
        ],
    )
    .unwrap();
}
