#[path = "first_sell_full_exit_report/support.rs"]
mod support;

use chrono::Duration;
use rusqlite::params;
use support::{database, insert_entry, insert_sell, policy, run_report, ts};

#[test]
fn exit_quote_missing_daemon_metadata_stays_unknown() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-x",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        1,
    );
    insert_sell(
        &conn,
        "sell-a",
        "wallet-a",
        "token-x",
        base + Duration::minutes(1),
        "ok",
        Some(1_000),
        Some(15_000_000),
        Some(100_000),
        None,
    );
    conn.execute(
        "UPDATE execution_quote_canary_events
         SET route_plan_json = NULL
         WHERE event_id = 'quote:close:sell-a'",
        [],
    )
    .unwrap();
    drop(conn);

    let summary = run_report(&db, base);
    assert_eq!(summary.verdict, "fail_closed_incomplete_exit_coverage");
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.valued_exit_events, 0);
    assert_eq!(daemon.unknown_exit_events, 1);
    assert_eq!(
        daemon.no_data_reasons[0].reason,
        "first_exit_quote_not_daemon_buildable"
    );
}

#[test]
fn stale_quote_sidecar_without_daemon_metadata_stays_unknown() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-stale",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        1,
    );
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, close_context, qty, qty_raw, entry_cost_sol,
            exit_value_sol, exit_value_lamports, pnl_sol, opened_ts, closed_ts
         ) VALUES ('stale-close-a', 'wallet-a', 'token-stale', 'stale_quote_price',
                   1.0, '1000', 0.01, 0.02, 20000000, 0.01, ?1, ?2)",
        params![
            base.to_rfc3339(),
            (base + Duration::minutes(1)).to_rfc3339()
        ],
    )
    .unwrap();
    drop(conn);

    let summary = run_report(&db, base);
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.valued_exit_events, 0);
    assert_eq!(daemon.unknown_exit_events, 1);
    assert_eq!(
        daemon.no_data_reasons[0].reason,
        "missing_daemon_buildable_stale_exit_quote"
    );
}
