use super::*;

#[test]
fn market_exit_shadow_quote_contract_runs_without_canary_enabled() {
    let mut config = ExecutionConfig::default();
    config.market_exit_shadow_quote_enabled = true;
    config.market_exit_shadow_quote_batch_limit = 0;

    let error = crate::config_contract::validate_execution_canary_contract(&config)
        .expect_err("market exit diagnostics must validate outside canary mode");

    assert!(error
        .to_string()
        .contains("market_exit_shadow_quote_batch_limit"));
}

#[test]
fn market_exit_shadow_quote_events_do_not_replace_canonical_close_quotes() -> Result<()> {
    let db_path = unique_execution_canary_test_path("market-exit-diag-isolated");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.insert_shadow_closed_trade_exact(
        "close-market-diag",
        "leader-wallet",
        "TokenMint",
        12.34,
        Some(TokenQuantity::new(1_234, 2)),
        0.20,
        0.24,
        0.04,
        now - chrono::Duration::minutes(2),
        now - chrono::Duration::seconds(20),
    )?;

    let candidates = store
        .list_execution_quote_canary_close_candidates(now - chrono::Duration::minutes(5), 10)?;
    assert_eq!(candidates.len(), 1);
    let close = candidates[0].clone();

    store.record_execution_quote_canary_event(&sell_quote_event(
        &crate::market_exit_shadow_quote::market_exit_shadow_quote_event_id(close.id),
        None,
        None,
        now,
    ))?;
    let candidates = store
        .list_execution_quote_canary_close_candidates(now - chrono::Duration::minutes(5), 10)?;
    assert_eq!(
        candidates.len(),
        1,
        "diagnostic market exit quote must not block canonical close quote"
    );

    let canonical_event_id = format!("quote:close:{}", close.id);
    store.record_execution_quote_canary_event(&sell_quote_event(
        &canonical_event_id,
        Some(&close.signal_id),
        Some(close.id),
        now,
    ))?;
    let candidates = store
        .list_execution_quote_canary_close_candidates(now - chrono::Duration::minutes(5), 10)?;
    assert!(
        candidates.is_empty(),
        "canonical close quote should block close quote retry"
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn sell_quote_event(
    event_id: &str,
    signal_id: Option<&str>,
    shadow_closed_trade_id: Option<i64>,
    now: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: signal_id.map(ToString::to_string),
        shadow_closed_trade_id,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "sell".to_string(),
        quote_status: "ok".to_string(),
        request_ts: now,
        signal_ts: Some(now),
        decision_delay_ms: Some(0),
        quote_latency_ms: Some(10),
        leader_notional_sol: Some(0.24),
        quote_in_amount_raw: Some("1234".to_string()),
        quote_out_amount_raw: Some("240000000".to_string()),
        quote_response_json: Some("{\"routePlan\":[]}".to_string()),
        quote_price_sol: Some(0.0194),
        shadow_price_sol: Some(0.0194),
        slippage_bps: Some(0.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}
