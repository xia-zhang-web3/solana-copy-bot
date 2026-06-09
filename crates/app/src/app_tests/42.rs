use chrono::Utc;
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;

#[test]
fn priority_fee_timeout_is_non_blocking_canary_metadata() {
    let timeout = anyhow::anyhow!(
        "priority fee canary request failed: error sending request: operation timed out"
    );
    assert!(crate::execution_quote_canary_priority_fee::is_priority_fee_transient_error(&timeout));

    let now = Utc::now();
    let mut event = ExecutionQuoteCanaryEventInsert {
        event_id: "quote:priority-timeout".to_string(),
        signal_id: Some("sig".to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        quote_status: crate::execution_quote_canary_helpers::QUOTE_STATUS_OK.to_string(),
        request_ts: now,
        signal_ts: Some(now),
        decision_delay_ms: Some(0),
        quote_latency_ms: Some(12),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("1234000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.1),
        shadow_price_sol: Some(0.1),
        slippage_bps: Some(25.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: Some(
            crate::execution_quote_canary_helpers::QUOTE_STATUS_SKIPPED.to_string(),
        ),
        priority_fee_lamports: None,
        priority_fee_json: Some("{\"reason\":\"priority_fee_transient_unavailable\"}".to_string()),
        decision_status: None,
        decision_reason: None,
        error: Some("priority_fee: operation timed out".to_string()),
    };

    crate::execution_quote_canary_helpers::finalize_quote_decision(&mut event, 50);

    assert_eq!(
        event.decision_status.as_deref(),
        Some(crate::execution_quote_canary_helpers::DECISION_WOULD_EXECUTE)
    );
    assert_eq!(
        event.decision_reason.as_deref(),
        Some("within_slippage_limit")
    );
}

#[test]
fn sell_quote_with_missing_slippage_is_force_exit_not_unknown() {
    let now = Utc::now();
    let mut sell_event = ExecutionQuoteCanaryEventInsert {
        event_id: "quote:sell-missing-slippage".to_string(),
        signal_id: Some("sell-sig".to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "sell".to_string(),
        quote_status: crate::execution_quote_canary_helpers::QUOTE_STATUS_OK.to_string(),
        request_ts: now,
        signal_ts: Some(now),
        decision_delay_ms: Some(0),
        quote_latency_ms: Some(12),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("10000".to_string()),
        quote_out_amount_raw: Some("1000000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.1),
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: Some(
            crate::execution_quote_canary_helpers::QUOTE_STATUS_OK.to_string(),
        ),
        priority_fee_lamports: Some(1),
        priority_fee_json: Some("{}".to_string()),
        decision_status: None,
        decision_reason: None,
        error: None,
    };
    let mut buy_event = sell_event.clone();
    buy_event.side = "buy".to_string();

    crate::execution_quote_canary_helpers::finalize_quote_decision(&mut sell_event, 500);
    crate::execution_quote_canary_helpers::finalize_quote_decision(&mut buy_event, 500);

    assert_eq!(
        sell_event.decision_status.as_deref(),
        Some(crate::execution_quote_canary_helpers::DECISION_WOULD_FORCE_EXIT)
    );
    assert_eq!(
        sell_event.decision_reason.as_deref(),
        Some("exit_missing_slippage_force_exit")
    );
    assert_eq!(
        buy_event.decision_status.as_deref(),
        Some(crate::execution_quote_canary_helpers::DECISION_UNKNOWN)
    );
    assert_eq!(
        buy_event.decision_reason.as_deref(),
        Some("missing_slippage_bps")
    );
}

#[test]
fn priority_fee_base_rate_contract_accepts_250ms_interval() {
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_enabled = true;
    config.priority_fee_canary_enabled = true;
    config.priority_fee_canary_rpc_url = "https://example.com/rpc".to_string();
    config.priority_fee_canary_min_request_interval_ms = 250;
    config.priority_fee_canary_cache_ttl_ms = 1_000;

    crate::config_contract::validate_execution_canary_contract(&config)
        .expect("Base priority fee rate settings should be accepted");
}

#[test]
fn priority_fee_rate_contract_rejects_too_aggressive_interval() {
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_enabled = true;
    config.priority_fee_canary_enabled = true;
    config.priority_fee_canary_rpc_url = "https://example.com/rpc".to_string();
    config.priority_fee_canary_min_request_interval_ms = 50;

    let error = crate::config_contract::validate_execution_canary_contract(&config)
        .expect_err("too aggressive priority fee interval should be rejected");

    assert!(error
        .to_string()
        .contains("priority_fee_canary_min_request_interval_ms"));
}

#[test]
fn execution_canary_contract_accepts_live_tiny_daily_loss_budget() {
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_max_daily_loss_sol = 1.00;

    crate::config_contract::validate_execution_canary_contract(&config)
        .expect("live tiny daily loss budget should be accepted");

    config.canary_max_daily_loss_sol = 1.01;
    let error = crate::config_contract::validate_execution_canary_contract(&config)
        .expect_err("daily loss budget above live tiny cap should be rejected");

    assert!(error.to_string().contains("canary_max_daily_loss_sol"));
}

#[test]
fn swap_transaction_dry_run_requires_quote_canary_metadata() {
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_enabled = true;
    config.swap_transaction_dry_run_enabled = true;

    let error = crate::config_contract::validate_execution_canary_contract(&config)
        .expect_err("swap transaction dry-run needs quote canary metadata");

    assert!(error
        .to_string()
        .contains("swap_transaction_dry_run_enabled"));
}
