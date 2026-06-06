use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, SqliteStore, SHADOW_CLOSE_CONTEXT_STALE_MARKET_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

#[test]
fn quote_pnl_summary_counts_executable_quote_adjusted_pnl() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-counted")?;
    let opened = ts("2026-06-02T12:00:00Z");
    let closed = opened + Duration::seconds(30);
    store.insert_shadow_closed_trade_exact(
        "sell-counted",
        "leader-wallet",
        "TokenMint",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.10,
        0.13,
        0.03,
        opened,
        closed,
    )?;
    let close_id = close_id_for_signal(&store, "sell-counted")?;
    store.record_execution_quote_canary_event(&buy_quote(
        "quote:buy:counted",
        "buy-counted",
        "TokenMint",
        opened,
        "would_execute",
    ))?;
    store.record_execution_quote_canary_event(&sell_quote_with_decision(
        "quote:sell:counted",
        close_id,
        "TokenMint",
        opened,
        "50",
        "125000000",
        "would_force_exit",
    ))?;

    let summary =
        store.execution_canary_quote_pnl_summary(closed, opened - Duration::seconds(1), 10)?;
    let trade = summary.trades.first().expect("trade should be reported");

    assert_eq!(summary.total_closed_trades, 1);
    assert_eq!(summary.shadow_close_breakdown.total_closed_trades, 1);
    assert_eq!(summary.shadow_close_breakdown.market_closed_trades, 1);
    assert_eq!(summary.shadow_close_breakdown.stale_closed_trades, 0);
    assert_eq!(summary.matched_quote_trades, 1);
    assert_eq!(summary.pnl_counted_trades, 1);
    assert_eq!(summary.skipped_trades, 0);
    assert_eq!(summary.quote_win_count, 1);
    assert_close(summary.shadow_pnl_sol, 0.03);
    assert_close(summary.quote_adjusted_pnl_sol, 0.025);
    assert_close(summary.quote_adjusted_pnl_after_priority_fee_sol, 0.02498);
    assert_close(summary.quote_vs_shadow_delta_sol, -0.005);
    assert_close(summary.quote_after_fee_vs_shadow_delta_sol, -0.00502);
    assert_eq!(summary.priority_fee_lamports_sum, 20_000);
    assert_eq!(summary.force_exit_counted_trades, 1);
    assert_eq!(summary.force_exit_skipped_entry_trades, 0);
    assert_eq!(summary.quote_diagnostics.entry_all.events, 1);
    assert_eq!(summary.quote_diagnostics.entry_counted.events, 1);
    assert_eq!(summary.quote_diagnostics.entry_skipped.events, 0);
    assert_eq!(summary.readiness_gate.status, "blocked");
    assert!(!summary.readiness_gate.can_start_tiny_execution);
    assert_eq!(
        readiness_check_status(&summary.readiness_gate, "market_closed_trades"),
        Some("block")
    );
    assert_close(summary.quote_diagnostics.entry_all.slippage_bps_avg, 15.0);
    assert_close(summary.quote_diagnostics.exit_all.slippage_bps_avg, 20.0);
    assert_eq!(trade.status, "pnl_counted");
    assert_close(trade.closed_qty_ratio.expect("ratio"), 0.5);
    assert_eq!(trade.entry_decision_delay_ms, Some(10));
    assert_eq!(trade.entry_quote_latency_ms, Some(20));
    assert_eq!(trade.entry_route_labels, vec!["Metis".to_string()]);
    Ok(())
}

#[test]
fn quote_pnl_readiness_gate_allows_clean_tiny_execution_window() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-readiness-ready")?;
    let opened = ts("2026-06-02T13:00:00Z");
    for index in 0..30 {
        let token = format!("ReadyToken{index}");
        let signal_id = format!("sell-ready-{index}");
        let trade_opened = opened + Duration::seconds(index);
        let closed = trade_opened + Duration::seconds(30);
        store.insert_shadow_closed_trade_exact(
            &signal_id,
            "leader-wallet",
            &token,
            50.0,
            Some(TokenQuantity::new(50, 0)),
            0.10,
            0.13,
            0.03,
            trade_opened,
            closed,
        )?;
        let close_id = close_id_for_signal(&store, &signal_id)?;
        store.record_execution_quote_canary_event(&buy_quote(
            &format!("quote:buy:ready:{index}"),
            &format!("buy-ready-{index}"),
            &token,
            trade_opened,
            "would_execute",
        ))?;
        store.record_execution_quote_canary_event(&sell_quote(
            &format!("quote:sell:ready:{index}"),
            close_id,
            &token,
            trade_opened,
            "50",
            "125000000",
        ))?;
    }

    let summary = store.execution_canary_quote_pnl_summary(
        opened + Duration::seconds(90),
        opened - Duration::seconds(1),
        100,
    )?;

    assert_eq!(summary.total_closed_trades, 30);
    assert_eq!(summary.shadow_close_breakdown.market_closed_trades, 30);
    assert_eq!(summary.pnl_counted_trades, 30);
    assert_eq!(summary.skipped_trades, 0);
    assert_eq!(summary.unknown_trades, 0);
    assert_eq!(summary.readiness_gate.status, "ready");
    assert!(summary.readiness_gate.can_start_tiny_execution);
    assert_eq!(summary.readiness_gate.blocker_count, 0);
    assert_eq!(summary.readiness_gate.warning_count, 0);
    assert_close(summary.readiness_gate.skip_rate_pct, 0.0);
    assert_close(summary.readiness_gate.non_ok_priority_fee_rate_pct, 0.0);
    Ok(())
}

#[test]
fn quote_pnl_summary_breaks_out_stale_shadow_closes() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-stale-breakdown")?;
    let opened = ts("2026-06-02T12:05:00Z");
    let closed = opened + Duration::seconds(30);
    store.insert_shadow_closed_trade_exact(
        "sell-market",
        "leader-wallet",
        "MarketToken",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.10,
        0.13,
        0.03,
        opened,
        closed,
    )?;
    store.insert_shadow_closed_trade_exact_with_context(
        "sell-stale",
        "leader-wallet",
        "StaleToken",
        100.0,
        Some(TokenQuantity::new(100, 0)),
        0.20,
        0.0,
        -0.20,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        opened,
        closed + Duration::seconds(1),
    )?;
    store.insert_shadow_closed_trade_exact(
        "stale-close-legacy-market",
        "leader-wallet",
        "StaleMarketToken",
        100.0,
        Some(TokenQuantity::new(100, 0)),
        0.20,
        6.20,
        6.00,
        opened,
        closed + Duration::seconds(2),
    )?;
    let close_id = close_id_for_signal(&store, "sell-market")?;
    store.record_execution_quote_canary_event(&buy_quote(
        "quote:buy:market",
        "buy-market",
        "MarketToken",
        opened,
        "would_execute",
    ))?;
    store.record_execution_quote_canary_event(&sell_quote(
        "quote:sell:market",
        close_id,
        "MarketToken",
        opened,
        "50",
        "125000000",
    ))?;

    let summary =
        store.execution_canary_quote_pnl_summary(closed, opened - Duration::seconds(1), 10)?;
    let breakdown = &summary.shadow_close_breakdown;
    let stale_context = breakdown
        .contexts
        .iter()
        .find(|context| context.close_context == SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE)
        .expect("stale context should be reported");
    let stale_market_context = breakdown
        .contexts
        .iter()
        .find(|context| context.close_context == SHADOW_CLOSE_CONTEXT_STALE_MARKET_PRICE)
        .expect("stale market context should be reported");

    assert_eq!(summary.total_closed_trades, 1);
    assert_close(summary.shadow_pnl_sol, 0.03);
    assert_eq!(breakdown.total_closed_trades, 3);
    assert_eq!(breakdown.market_closed_trades, 1);
    assert_eq!(breakdown.stale_closed_trades, 2);
    assert_eq!(breakdown.stale_rug_like_closed_trades, 1);
    assert_eq!(breakdown.non_market_closed_trades, 2);
    assert_close(breakdown.total_pnl_sol, 5.83);
    assert_close(breakdown.market_pnl_sol, 0.03);
    assert_close(breakdown.stale_pnl_sol, 5.80);
    assert_close(breakdown.stale_rug_like_pnl_sol, -0.20);
    assert_close(breakdown.non_market_pnl_sol, 5.80);
    assert_eq!(stale_context.closed_trades, 1);
    assert_close(stale_context.pnl_sol, -0.20);
    assert_eq!(stale_context.rug_like_closed_trades, 1);
    assert_close(stale_context.rug_like_pnl_sol, -0.20);
    assert_eq!(stale_market_context.closed_trades, 1);
    assert_close(stale_market_context.pnl_sol, 6.00);
    assert_eq!(stale_market_context.rug_like_closed_trades, 0);
    assert_close(stale_market_context.rug_like_pnl_sol, 0.0);
    Ok(())
}

#[test]
fn quote_pnl_summary_excludes_entry_would_skip_from_quote_pnl() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-skipped")?;
    let opened = ts("2026-06-02T12:10:00Z");
    let closed = opened + Duration::seconds(20);
    store.insert_shadow_closed_trade_exact(
        "sell-skipped",
        "leader-wallet",
        "SkipToken",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.10,
        0.11,
        0.01,
        opened,
        closed,
    )?;
    let close_id = close_id_for_signal(&store, "sell-skipped")?;
    store.record_execution_quote_canary_event(&buy_quote(
        "quote:buy:skipped",
        "buy-skipped",
        "SkipToken",
        opened,
        "would_skip",
    ))?;
    store.record_execution_quote_canary_event(&sell_quote_with_decision(
        "quote:sell:skipped",
        close_id,
        "SkipToken",
        opened,
        "50",
        "125000000",
        "would_force_exit",
    ))?;

    let summary =
        store.execution_canary_quote_pnl_summary(closed, opened - Duration::seconds(1), 10)?;
    let trade = summary.trades.first().expect("trade should be reported");

    assert_eq!(summary.total_closed_trades, 1);
    assert_eq!(summary.matched_quote_trades, 1);
    assert_eq!(summary.pnl_counted_trades, 0);
    assert_eq!(summary.skipped_trades, 1);
    assert_eq!(summary.unknown_trades, 0);
    assert_close(summary.quote_adjusted_pnl_sol, 0.0);
    assert_close(summary.skipped_shadow_pnl_sol, 0.01);
    assert_close(summary.skipped_counterfactual_pnl_sol, 0.025);
    assert_close(
        summary.skipped_counterfactual_pnl_after_priority_fee_sol,
        0.02498,
    );
    assert_close(
        summary.skipped_counterfactual_after_fee_vs_shadow_delta_sol,
        0.01498,
    );
    assert_eq!(summary.force_exit_counted_trades, 0);
    assert_eq!(summary.force_exit_skipped_entry_trades, 1);
    assert_eq!(summary.quote_diagnostics.entry_skipped.events, 1);
    assert_eq!(summary.quote_diagnostics.exit_skipped_entry.events, 1);
    assert_close(
        summary.quote_diagnostics.entry_skipped.slippage_bps_avg,
        15.0,
    );
    assert_eq!(trade.status, "would_skip");
    assert_eq!(trade.reason, "inside_test_skip_limit");
    assert_eq!(trade.exit_decision_delay_ms, Some(10));
    assert_eq!(trade.exit_route_labels, vec!["Metis".to_string()]);
    assert_close(trade.skipped_counterfactual_pnl_sol.expect("pnl"), 0.025);
    assert_close(
        trade
            .skipped_counterfactual_pnl_after_priority_fee_sol
            .expect("pnl after fee"),
        0.02498,
    );
    Ok(())
}

#[test]
fn quote_pnl_summary_scales_sell_quote_when_close_qty_exceeds_entry_quote() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-scaled-exit")?;
    let opened = ts("2026-06-02T12:20:00Z");
    let closed = opened + Duration::seconds(20);
    store.insert_shadow_closed_trade_exact(
        "sell-scaled",
        "leader-wallet",
        "ScaledToken",
        120.0,
        Some(TokenQuantity::new(120, 0)),
        0.20,
        0.24,
        0.04,
        opened,
        closed,
    )?;
    let close_id = close_id_for_signal(&store, "sell-scaled")?;
    store.record_execution_quote_canary_event(&buy_quote(
        "quote:buy:scaled",
        "buy-scaled",
        "ScaledToken",
        opened,
        "would_execute",
    ))?;
    store.record_execution_quote_canary_event(&sell_quote(
        "quote:sell:scaled",
        close_id,
        "ScaledToken",
        opened,
        "120",
        "240000000",
    ))?;

    let summary =
        store.execution_canary_quote_pnl_summary(closed, opened - Duration::seconds(1), 10)?;
    let trade = summary.trades.first().expect("trade should be reported");

    assert_eq!(summary.pnl_counted_trades, 1);
    assert_eq!(trade.status, "pnl_counted");
    assert_eq!(trade.reason, "ok_scaled_to_entry_qty");
    assert_close(trade.closed_qty_ratio.expect("ratio"), 1.0);
    assert_close(summary.quote_adjusted_pnl_sol, 0.0);
    assert_close(summary.quote_adjusted_pnl_after_priority_fee_sol, -0.00002);
    Ok(())
}

fn close_id_for_signal(store: &SqliteStore, signal_id: &str) -> Result<i64> {
    Ok(store
        .list_execution_quote_canary_close_candidates_for_signal(signal_id, 10)?
        .into_iter()
        .next()
        .expect("close candidate")
        .id)
}

fn buy_quote(
    event_id: &str,
    signal_id: &str,
    token: &str,
    opened: DateTime<Utc>,
    decision_status: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: Some(signal_id.to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: opened + Duration::milliseconds(10),
        signal_ts: Some(opened),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("100".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.002),
        shadow_price_sol: Some(0.002),
        slippage_bps: Some(15.0),
        price_impact_pct: Some(0.02),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some("{\"recommended\":10000}".to_string()),
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some("inside_test_skip_limit".to_string()),
        error: None,
    }
}

fn sell_quote(
    event_id: &str,
    close_id: i64,
    token: &str,
    opened: DateTime<Utc>,
    in_raw: &str,
    out_raw: &str,
) -> ExecutionQuoteCanaryEventInsert {
    sell_quote_with_decision(
        event_id,
        close_id,
        token,
        opened,
        in_raw,
        out_raw,
        "would_execute",
    )
}

fn sell_quote_with_decision(
    event_id: &str,
    close_id: i64,
    token: &str,
    opened: DateTime<Utc>,
    in_raw: &str,
    out_raw: &str,
    decision_status: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: Some(format!("sell:{close_id}")),
        shadow_closed_trade_id: Some(close_id),
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "sell".to_string(),
        quote_status: "ok".to_string(),
        request_ts: opened + Duration::milliseconds(30),
        signal_ts: Some(opened + Duration::seconds(30)),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.125),
        quote_in_amount_raw: Some(in_raw.to_string()),
        quote_out_amount_raw: Some(out_raw.to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0025),
        shadow_price_sol: Some(0.0026),
        slippage_bps: Some(20.0),
        price_impact_pct: Some(0.03),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some("{\"recommended\":10000}".to_string()),
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some("inside_sell_limit".to_string()),
        error: None,
    }
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
}

fn readiness_check_status<'a>(
    gate: &'a copybot_storage_core::ExecutionCanaryQuoteReadinessGate,
    name: &str,
) -> Option<&'a str> {
    gate.checks
        .iter()
        .find(|check| check.name == name)
        .map(|check| check.status.as_str())
}
