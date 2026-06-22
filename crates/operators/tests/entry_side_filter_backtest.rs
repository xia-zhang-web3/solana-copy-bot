use chrono::{Duration, TimeZone, Utc};
use copybot_operators::entry_side_filter_backtest::{build_report, parse_args_from};
use rusqlite::{params, Connection};
use tempfile::tempdir;

#[test]
fn backtest_uses_prior_evidence_without_future_leakage() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("runtime.db");
    let conn = Connection::open(&db_path).unwrap();
    create_schema(&conn);

    let base = Utc.with_ymd_and_hms(2026, 6, 22, 12, 0, 0).unwrap();
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "prior-wallet-a-rug",
            wallet_id: "wallet-a",
            token: "token-prior",
            close_context: "stale_quote_price",
            pnl_sol: -0.20,
            opened_ts: base - Duration::hours(3),
            closed_ts: base - Duration::hours(2) - Duration::minutes(50),
        },
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "prior-wallet-c-stale-market",
            wallet_id: "wallet-c",
            token: "token-c-prior",
            close_context: "stale_market_price",
            pnl_sol: -0.40,
            opened_ts: base - Duration::hours(3),
            closed_ts: base - Duration::hours(2) - Duration::minutes(45),
        },
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "prior-token-d-rug",
            wallet_id: "wallet-other",
            token: "token-d",
            close_context: "stale_terminal_zero_price",
            pnl_sol: -0.50,
            opened_ts: base - Duration::hours(3),
            closed_ts: base - Duration::hours(2) - Duration::minutes(40),
        },
    );

    let eval_a_opened = base - Duration::hours(1);
    insert_observed_buy(
        &conn,
        "obs-a",
        "wallet-a",
        "token-a",
        eval_a_opened - Duration::seconds(20),
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "eval-wallet-a",
            wallet_id: "wallet-a",
            token: "token-a",
            close_context: "market",
            pnl_sol: -0.30,
            opened_ts: eval_a_opened,
            closed_ts: eval_a_opened + Duration::minutes(30),
        },
    );

    let eval_b_opened = base - Duration::minutes(55);
    insert_observed_buy(
        &conn,
        "obs-b",
        "wallet-b",
        "token-b",
        eval_b_opened - Duration::seconds(20),
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "eval-wallet-b",
            wallet_id: "wallet-b",
            token: "token-b",
            close_context: "market",
            pnl_sol: 0.50,
            opened_ts: eval_b_opened,
            closed_ts: eval_b_opened + Duration::minutes(30),
        },
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "future-wallet-b-rug",
            wallet_id: "wallet-b",
            token: "token-b-later",
            close_context: "stale_quote_price",
            pnl_sol: -0.40,
            opened_ts: base,
            closed_ts: base + Duration::minutes(30),
        },
    );

    let eval_c_opened = base - Duration::minutes(50);
    insert_observed_buy(
        &conn,
        "obs-c",
        "wallet-c",
        "token-c",
        eval_c_opened - Duration::seconds(20),
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "eval-wallet-c",
            wallet_id: "wallet-c",
            token: "token-c",
            close_context: "market",
            pnl_sol: -0.25,
            opened_ts: eval_c_opened,
            closed_ts: eval_c_opened + Duration::minutes(30),
        },
    );

    let eval_d_opened = base - Duration::minutes(45);
    insert_observed_buy(
        &conn,
        "obs-d",
        "wallet-d",
        "token-d",
        eval_d_opened - Duration::seconds(20),
    );
    insert_trade(
        &conn,
        TradeRow {
            signal_id: "eval-token-d",
            wallet_id: "wallet-d",
            token: "token-d",
            close_context: "market",
            pnl_sol: -0.60,
            opened_ts: eval_d_opened,
            closed_ts: eval_d_opened + Duration::minutes(30),
        },
    );

    drop(conn);
    let cli = parse_args_from([
        "--db-path",
        db_path.to_str().unwrap(),
        "--json",
        "--since",
        "2026-06-22T10:00:00Z",
        "--until",
        "2026-06-22T13:00:00Z",
        "--history-limit",
        "100",
        "--min-wallet-history-closes",
        "1",
        "--wallet-rug-rate-thresholds",
        "0.50",
        "--wallet-tail-rate-thresholds",
        "1.00",
        "--token-age-minutes",
        "5",
        "--leader-lag-seconds",
        "60",
    ])
    .unwrap();
    let report = build_report(cli, base + Duration::hours(1));
    assert_eq!(report.reason_class, "entry_side_filter_backtest_loaded");
    let report_json = serde_json::to_value(&report).unwrap();
    let summary = &report_json["summary"];

    assert_eq!(summary["point_in_time"], true);
    assert_eq!(summary["metric_basis"], "shadow_outcome_not_executable");
    assert_eq!(summary["baseline"]["event_count"], 5);

    let wallet_rug = find_gate(summary, "wallet_rug_rate");
    assert_eq!(wallet_rug["rejected"]["event_count"], 1);
    assert!((wallet_rug["shadow_delta_if_rejected_sol"].as_f64().unwrap() - 0.30).abs() < 0.000001);

    let token_bad = find_gate(summary, "token_seen_before_bad");
    assert_eq!(token_bad["rejected"]["event_count"], 1);
    assert!((token_bad["shadow_delta_if_rejected_sol"].as_f64().unwrap() - 0.60).abs() < 0.000001);
}

fn find_gate<'a>(summary: &'a serde_json::Value, gate: &str) -> &'a serde_json::Value {
    summary["gates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|report| report["gate"] == gate)
        .unwrap_or_else(|| panic!("missing gate {gate}"))
}

fn create_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE shadow_closed_trades (
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
        CREATE INDEX idx_shadow_closed_trades_closed_ts
            ON shadow_closed_trades(closed_ts);
        CREATE INDEX idx_shadow_closed_trades_wallet_closed_ts
            ON shadow_closed_trades(wallet_id, closed_ts);
        CREATE TABLE observed_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            dex TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            qty_in REAL NOT NULL,
            qty_out REAL NOT NULL,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX idx_observed_swaps_token_in_ts
            ON observed_swaps(token_in, ts);
        CREATE INDEX idx_observed_swaps_token_out_ts
            ON observed_swaps(token_out, ts);
        CREATE INDEX idx_observed_swaps_wallet_ts
            ON observed_swaps(wallet_id, ts);",
    )
    .unwrap();
}

struct TradeRow {
    signal_id: &'static str,
    wallet_id: &'static str,
    token: &'static str,
    close_context: &'static str,
    pnl_sol: f64,
    opened_ts: chrono::DateTime<Utc>,
    closed_ts: chrono::DateTime<Utc>,
}

fn insert_trade(conn: &Connection, row: TradeRow) {
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, close_context, qty, entry_cost_sol, exit_value_sol,
            pnl_sol, opened_ts, closed_ts
         ) VALUES (?1, ?2, ?3, ?4, 1.0, 1.0, ?5, ?6, ?7, ?8)",
        params![
            row.signal_id,
            row.wallet_id,
            row.token,
            row.close_context,
            1.0 + row.pnl_sol,
            row.pnl_sol,
            row.opened_ts.to_rfc3339(),
            row.closed_ts.to_rfc3339(),
        ],
    )
    .unwrap();
}

fn insert_observed_buy(
    conn: &Connection,
    signature: &str,
    wallet_id: &str,
    token: &str,
    ts: chrono::DateTime<Utc>,
) {
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES (?1, ?2, 'pumpswap', 'SOL', ?3, 1.0, 1.0, 1, ?4)",
        params![signature, wallet_id, token, ts.to_rfc3339()],
    )
    .unwrap();
}
