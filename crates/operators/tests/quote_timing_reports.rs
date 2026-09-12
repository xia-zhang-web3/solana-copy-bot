#[path = "first_sell_full_exit_report/support.rs"]
mod support;
use chrono::Duration;
use support::{database, insert_entry, insert_sell, run_report, ts};

#[test]
fn historical_first_sell_model_keeps_financial_population_and_exposes_actual_coverage() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-timing",
        "wallet",
        "token",
        base,
        10_000_000,
        1000,
        Some(1000),
        "baseline",
        3,
    );
    insert_sell(
        &conn,
        "sell-timing",
        "wallet",
        "token",
        base + Duration::minutes(1),
        "ok",
        Some(1000),
        Some(15_000_000),
        Some(1000),
        None,
    );
    let legacy = run_report(&db, base);
    assert_eq!(legacy.coverage.unknown_http_entry_events, 1);
    assert_eq!(legacy.coverage.actual_http_entry_events, 0);
    assert!(legacy
        .entry_gate_model
        .earliest_open_basis
        .contains("historical model"));
    conn.execute_batch(
        "ALTER TABLE execution_quote_canary_events ADD COLUMN http_request_started_ts TEXT",
    )
    .unwrap();
    conn.execute(
        "UPDATE execution_quote_canary_events SET http_request_started_ts=?1 WHERE side='buy'",
        [(base + Duration::minutes(2)).to_rfc3339()],
    )
    .unwrap();
    let actual = run_report(&db, base);
    assert_eq!(actual.coverage.actual_http_entry_events, 1);
    assert_eq!(actual.coverage.actual_entry_ready_events, 1);
    assert_eq!(actual.coverage.unknown_http_entry_events, 0);
    assert_eq!(
        legacy.coverage.loaded_entry_events,
        actual.coverage.loaded_entry_events
    );
    assert_eq!(
        legacy.policies, actual.policies,
        "HTTP coverage cannot redefine historical financial replay"
    );
}
