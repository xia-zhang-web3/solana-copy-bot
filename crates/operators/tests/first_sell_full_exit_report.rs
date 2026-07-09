#[path = "first_sell_full_exit_report/support.rs"]
mod support;

use chrono::Duration;
use copybot_operators::first_sell_full_exit_report::build_report;
use rusqlite::params;
use support::{assert_sol, cli, database, insert_entry, insert_sell, policy, run_report, ts};

#[test]
fn report_separates_token_first_scaled_exit_from_origin_wallet_exact_exit() {
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
        Some(700_000),
        "baseline",
        3,
    );
    insert_sell(
        &conn,
        "sell-cross",
        "wallet-b",
        "token-x",
        base + Duration::minutes(1),
        "ok",
        Some(500),
        Some(6_000_000),
        Some(300_000),
        None,
    );
    insert_sell(
        &conn,
        "sell-origin",
        "wallet-a",
        "token-x",
        base + Duration::minutes(2),
        "ok",
        Some(1_000),
        Some(15_000_000),
        Some(600_000),
        None,
    );
    drop(conn);

    let summary = run_report(&db, base);
    assert_eq!(
        summary.verdict,
        "fail_closed_insufficient_exact_full_amount_quotes"
    );
    assert_eq!(summary.observed_entry_notionals.len(), 1);
    assert_eq!(
        summary.observed_entry_notionals[0].entry_in_lamports,
        10_000_000
    );

    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.cross_wallet_triggers, 1);
    assert_eq!(daemon.exact_amount_events, 0);
    assert_eq!(daemon.scaled_up_events, 1);
    assert_sol(
        daemon.amount_scaled_estimate.planned_net_existing_ata_sol,
        0.00119,
    );
    assert_sol(
        daemon.amount_scaled_estimate.planned_net_new_ata_cash_sol,
        -0.00084928,
    );
    assert_eq!(daemon.by_source_cohort[0].cohort, "baseline");
    assert_eq!(daemon.by_rank_cohort[0].cohort, "rank_1_15");

    let origin = policy(&summary, "origin_wallet_first_sell");
    assert_eq!(origin.cross_wallet_triggers, 0);
    assert_eq!(origin.exact_amount_events, 1);
    assert_eq!(origin.scaled_up_events, 0);
    assert_sol(origin.exact_amount.planned_net_existing_ata_sol, 0.00399);
}

#[test]
fn report_never_replaces_the_first_failed_token_trigger_with_a_later_quote() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-y",
        base,
        10_000_000,
        1_000,
        Some(500_000),
        "baseline",
        20,
    );
    insert_sell(
        &conn,
        "sell-cross-error",
        "wallet-b",
        "token-y",
        base + Duration::minutes(1),
        "error",
        None,
        None,
        None,
        Some("temporary quote timeout"),
    );
    insert_sell(
        &conn,
        "sell-origin-ok",
        "wallet-a",
        "token-y",
        base + Duration::minutes(2),
        "ok",
        Some(1_000),
        Some(15_000_000),
        Some(500_000),
        None,
    );
    drop(conn);

    let summary = run_report(&db, base);
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.unknown_exit_events, 1);
    assert_eq!(daemon.amount_scaled_estimate.gross_events, 0);
    assert_eq!(daemon.missing_entry_priority_samples, 0);
    assert_eq!(daemon.missing_exit_priority_samples, 1);
    assert_eq!(daemon.no_data_reasons[0].reason, "first_exit_quote_error");

    let origin = policy(&summary, "origin_wallet_first_sell");
    assert_eq!(origin.exact_amount_events, 1);
    assert_eq!(origin.missing_entry_priority_samples, 0);
    assert_eq!(origin.missing_exit_priority_samples, 0);
    assert_sol(origin.exact_amount.planned_net_existing_ata_sol, 0.00399);
}

#[test]
fn report_books_a_terminal_stale_close_as_a_zero_exit_fallback() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-dead",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "slow_hold",
        35,
    );
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, close_context, qty, qty_raw, entry_cost_sol,
            exit_value_sol, exit_value_lamports, pnl_sol, opened_ts, closed_ts
         ) VALUES ('buy-a', 'wallet-a', 'token-dead', 'stale_terminal_zero_price', 1.0,
                   '1000', 0.01, 0.0, 0, -0.01, ?1, ?2)",
        params![
            base.to_rfc3339(),
            (base + Duration::minutes(1)).to_rfc3339()
        ],
    )
    .unwrap();
    drop(conn);

    let summary = run_report(&db, base);
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.terminal_zero_events, 1);
    assert_eq!(daemon.missing_exit_priority_samples, 1);
    assert_sol(
        daemon.strict_unscaled.planned_net_existing_ata_sol,
        -0.01061,
    );
    assert_eq!(daemon.by_source_cohort[0].cohort, "slow_hold");
    assert_eq!(daemon.by_rank_cohort[0].cohort, "rank_gt_30");
}

#[test]
fn daemon_policy_uses_an_earlier_cross_wallet_stale_trigger() {
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
        2,
    );
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, close_context, qty, qty_raw, entry_cost_sol,
            exit_value_sol, exit_value_lamports, pnl_sol, opened_ts, closed_ts
         ) VALUES ('stale-close-cross', 'wallet-b', 'token-stale',
                   'stale_terminal_zero_price', 1.0, '500', 0.01, 0.0, 0, -0.01, ?1, ?2)",
        params![
            (base - Duration::hours(1)).to_rfc3339(),
            (base + Duration::minutes(1)).to_rfc3339()
        ],
    )
    .unwrap();
    insert_sell(
        &conn,
        "sell-origin",
        "wallet-a",
        "token-stale",
        base + Duration::minutes(2),
        "ok",
        Some(1_000),
        Some(15_000_000),
        Some(300_000),
        None,
    );
    drop(conn);

    let summary = run_report(&db, base);
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.terminal_zero_events, 1);
    assert_eq!(daemon.cross_wallet_triggers, 1);
    assert_sol(
        daemon.strict_unscaled.planned_net_existing_ata_sol,
        -0.01061,
    );

    let origin = policy(&summary, "origin_wallet_first_sell");
    assert_eq!(origin.exact_amount_events, 1);
    assert_sol(origin.exact_amount.planned_net_existing_ata_sol, 0.00459);
}

#[test]
fn daemon_policy_ignores_a_sell_before_the_entry_quote_completed() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-timing",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        2,
    );
    insert_sell(
        &conn,
        "sell-too-early",
        "wallet-b",
        "token-timing",
        base + Duration::milliseconds(100),
        "ok",
        Some(1_000),
        Some(1_000_000),
        Some(100_000),
        None,
    );
    insert_sell(
        &conn,
        "sell-after-ready",
        "wallet-a",
        "token-timing",
        base + Duration::seconds(1),
        "ok",
        Some(1_000),
        Some(15_000_000),
        Some(300_000),
        None,
    );
    drop(conn);

    let summary = run_report(&db, base);
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.cross_wallet_triggers, 0);
    assert_eq!(daemon.exact_amount_events, 1);
    assert_sol(daemon.exact_amount.planned_net_existing_ata_sol, 0.00459);
}

#[test]
fn no_route_keeps_the_virtual_position_open_and_fails_coverage() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-no-route",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        2,
    );
    insert_sell(
        &conn,
        "sell-no-route",
        "wallet-a",
        "token-no-route",
        base + Duration::minutes(1),
        "error",
        None,
        None,
        Some(100_000),
        Some("NO_ROUTES_FOUND"),
    );
    drop(conn);

    let summary = run_report(&db, base);
    assert_eq!(summary.verdict, "fail_closed_incomplete_exit_coverage");
    let daemon = policy(&summary, "daemon_token_first_sell");
    assert_eq!(daemon.terminal_zero_events, 0);
    assert_eq!(daemon.unknown_exit_events, 1);
    assert!(!daemon.complete_quote_backed_exit_coverage);
    assert_eq!(
        daemon.no_data_reasons[0].reason,
        "first_exit_unlanded_position_remains_open"
    );
}

#[test]
fn report_excludes_a_quote_that_the_shadow_gate_dropped() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-dropped",
        "wallet-a",
        "token-x",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        1,
    );
    conn.execute(
        "UPDATE execution_quote_canary_shadow_gate_events
         SET status = 'shadow_dropped', reason = 'risk_gate'
         WHERE signal_id = 'buy-dropped'",
        [],
    )
    .unwrap();
    drop(conn);

    let summary = run_report(&db, base);
    assert_eq!(summary.coverage.eligible_entry_events, 0);
    assert_eq!(summary.coverage.shadow_dropped_entry_events, 1);
    assert_eq!(policy(&summary, "daemon_token_first_sell").entries, 0);
}

#[test]
fn report_excludes_an_entry_missing_required_priority_fee_metadata() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-no-fee",
        "wallet-a",
        "token-x",
        base,
        10_000_000,
        1_000,
        None,
        "baseline",
        1,
    );
    drop(conn);

    let summary = run_report(&db, base);
    assert_eq!(summary.coverage.eligible_entry_events, 0);
    assert_eq!(summary.coverage.entry_gate_rejected_events, 1);
    assert_eq!(summary.verdict, "fail_closed_no_eligible_entries");
}

#[test]
fn report_fails_closed_when_entry_ready_time_is_unknown() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-no-timing",
        "wallet-a",
        "token-x",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        1,
    );
    conn.execute(
        "UPDATE execution_quote_canary_events
         SET quote_latency_ms = NULL
         WHERE event_id = 'quote:entry:buy-no-timing'",
        [],
    )
    .unwrap();
    drop(conn);

    let summary = run_report(&db, base);
    assert_eq!(summary.coverage.entry_timing_unknown_events, 1);
    assert_eq!(summary.verdict, "fail_closed_incomplete_entry_coverage");
}

#[test]
fn report_fails_closed_when_the_entry_query_is_truncated() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    for suffix in ["a", "b"] {
        insert_entry(
            &conn,
            &format!("buy-{suffix}"),
            &format!("wallet-{suffix}"),
            &format!("token-{suffix}"),
            base,
            10_000_000,
            1_000,
            Some(100_000),
            "baseline",
            1,
        );
    }
    drop(conn);

    let mut cli = cli(&db, base);
    cli.entry_limit = 1;
    let report = build_report(cli, base + Duration::hours(1));
    assert_eq!(report.reason_class, "first_sell_full_exit_report_loaded");
    let summary = report.summary.unwrap();
    assert!(summary.coverage.entry_limit_hit);
    assert_eq!(summary.coverage.loaded_entry_events, 1);
    assert_eq!(summary.verdict, "fail_closed_truncated_evidence");
}

#[test]
fn report_defaults_historical_outcomes_to_the_report_as_of() {
    let (db, conn) = database();
    let base = ts(2026, 7, 8, 12, 0, 0);
    insert_entry(
        &conn,
        "buy-a",
        "wallet-a",
        "token-open",
        base,
        10_000_000,
        1_000,
        Some(100_000),
        "baseline",
        1,
    );
    drop(conn);

    let as_of = base + Duration::hours(1);
    let mut cli = cli(&db, base);
    cli.outcome_until = None;
    let report = build_report(cli, as_of);
    assert_eq!(report.params.as_ref().unwrap().outcome_until, as_of);
    assert!(report
        .summary
        .unwrap()
        .minimum_eligible_entry_followup_seconds
        .is_some_and(|seconds| seconds > 0));
}
