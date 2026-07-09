#![allow(dead_code)]

use chrono::{DateTime, Duration, TimeZone, Utc};
use copybot_operators::first_sell_full_exit_report::{build_report, Cli};
use copybot_operators::first_sell_full_exit_report_types::{FirstSellAuditSummary, PolicySummary};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

const CAP_LAMPORTS: u64 = 500_000;

pub(crate) fn run_report(db: &NamedTempFile, base: DateTime<Utc>) -> FirstSellAuditSummary {
    let report = build_report(cli(db, base), base + Duration::hours(1));
    assert_eq!(report.reason_class, "first_sell_full_exit_report_loaded");
    report.summary.unwrap()
}

pub(crate) fn cli(db: &NamedTempFile, base: DateTime<Utc>) -> Cli {
    Cli {
        db_path: Some(db.path().to_path_buf()),
        json: true,
        since: Some(base - Duration::seconds(1)),
        until: Some(base + Duration::seconds(30)),
        outcome_until: Some(base + Duration::minutes(5)),
        priority_fee_cap_lamports: Some(CAP_LAMPORTS),
        expected_entry_lamports: Some(10_000_000),
        max_entry_slippage_bps: Some(100),
        ..Cli::default()
    }
}

pub(crate) fn policy<'a>(summary: &'a FirstSellAuditSummary, name: &str) -> &'a PolicySummary {
    summary
        .policies
        .iter()
        .find(|policy| policy.policy == name)
        .unwrap_or_else(|| panic!("missing policy {name}"))
}

pub(crate) fn assert_sol(actual: Option<f64>, expected: f64) {
    assert!((actual.unwrap() - expected).abs() < 0.000000001);
}

pub(crate) fn database() -> (NamedTempFile, Connection) {
    let db = NamedTempFile::new().unwrap();
    let conn = Connection::open(db.path()).unwrap();
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
            quote_latency_ms INTEGER,
            quote_in_amount_raw TEXT,
            quote_out_amount_raw TEXT,
            quote_response_json TEXT,
            quote_price_sol REAL,
            slippage_bps REAL,
            route_plan_json TEXT,
            discovery_rank INTEGER,
            source_cohort TEXT,
            priority_fee_status TEXT,
            priority_fee_lamports INTEGER,
            decision_status TEXT,
            error TEXT
        );
        CREATE INDEX idx_execution_quote_canary_events_side_request_ts
            ON execution_quote_canary_events(side, request_ts);
        CREATE TABLE execution_quote_canary_shadow_gate_events (
            signal_id TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            side TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            recorded_ts TEXT NOT NULL
        );
        CREATE TABLE copy_signals (
            signal_id TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            side TEXT NOT NULL,
            token TEXT NOT NULL,
            notional_sol REAL NOT NULL,
            ts TEXT NOT NULL,
            status TEXT NOT NULL
        );
        CREATE INDEX idx_copy_signals_status_ts ON copy_signals(status, ts);
        CREATE TABLE shadow_closed_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            wallet_id TEXT NOT NULL,
            token TEXT NOT NULL,
            close_context TEXT,
            qty REAL NOT NULL,
            qty_raw TEXT,
            entry_cost_sol REAL NOT NULL,
            exit_value_sol REAL NOT NULL,
            exit_value_lamports INTEGER,
            pnl_sol REAL NOT NULL,
            opened_ts TEXT NOT NULL,
            closed_ts TEXT NOT NULL
        );
        CREATE INDEX idx_shadow_closed_trades_closed_ts
            ON shadow_closed_trades(closed_ts);",
    )
    .unwrap();
    (db, conn)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_entry(
    conn: &Connection,
    signal_id: &str,
    wallet_id: &str,
    token: &str,
    at: DateTime<Utc>,
    in_lamports: u64,
    out_raw: u64,
    priority_fee: Option<u64>,
    source: &str,
    rank: u64,
) {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
            quote_latency_ms, quote_in_amount_raw, quote_out_amount_raw, quote_response_json,
            quote_price_sol, slippage_bps, route_plan_json, priority_fee_status,
            priority_fee_lamports, decision_status
         ) VALUES (?1, ?2, ?3, ?4, 'buy', 'ok', ?5, ?5, 500, ?6, ?7, '{}',
                   0.00001, 50.0, '{}', ?8, ?9, 'would_execute')",
        params![
            format!("quote:entry:{signal_id}"),
            signal_id,
            wallet_id,
            token,
            at.to_rfc3339(),
            in_lamports.to_string(),
            out_raw.to_string(),
            priority_fee.map(|_| "ok"),
            priority_fee,
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO execution_quote_canary_shadow_gate_events(
            signal_id, wallet_id, token, side, status, recorded_ts
         ) VALUES (?1, ?2, ?3, 'buy', 'shadow_recorded', ?4)",
        params![
            signal_id,
            wallet_id,
            token,
            (at + Duration::milliseconds(100)).to_rfc3339()
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
         VALUES (?1, ?2, 'buy', ?3, 0.01, ?4, 'shadow_recorded')",
        params![signal_id, wallet_id, token, at.to_rfc3339()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
            quote_in_amount_raw, quote_out_amount_raw, discovery_rank, source_cohort
         ) VALUES (?1, ?2, ?3, ?4, 'buy', 'ok', ?5, ?5, ?6, ?7, ?8, ?9)",
        params![
            format!("quote:entry-shadow-diag:{signal_id}"),
            signal_id,
            wallet_id,
            token,
            at.to_rfc3339(),
            in_lamports.to_string(),
            out_raw.to_string(),
            rank,
            source,
        ],
    )
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_sell(
    conn: &Connection,
    signal_id: &str,
    wallet_id: &str,
    token: &str,
    at: DateTime<Utc>,
    quote_status: &str,
    quote_in_raw: Option<u64>,
    quote_out_lamports: Option<u64>,
    priority_fee: Option<u64>,
    error: Option<&str>,
) {
    conn.execute(
        "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
         VALUES (?1, ?2, 'sell', ?3, 0.0, ?4, 'shadow_recorded')",
        params![signal_id, wallet_id, token, at.to_rfc3339()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status, request_ts, signal_ts,
            quote_in_amount_raw, quote_out_amount_raw, quote_price_sol, route_plan_json,
            priority_fee_status, priority_fee_lamports, decision_status, error
         ) VALUES (?1, ?2, ?3, ?4, 'sell', ?5, ?6, ?6, ?7, ?8, 0.1, '{}',
                   ?9, ?10, 'would_execute', ?11)",
        params![
            format!("quote:close:{signal_id}"),
            signal_id,
            wallet_id,
            token,
            quote_status,
            at.to_rfc3339(),
            quote_in_raw.map(|value| value.to_string()),
            quote_out_lamports.map(|value| value.to_string()),
            priority_fee.map(|_| "ok"),
            priority_fee,
            error,
        ],
    )
    .unwrap();
}

pub(crate) fn ts(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .unwrap()
}
