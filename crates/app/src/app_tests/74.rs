use super::*;

#[test]
fn confirmed_sell_accounting_uses_quote_input_amount_for_partial_wallet_sell() -> Result<()> {
    let db_path = unique_partial_sell_accounting_test_path("quote-input");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let signal = partial_sell_signal(now + chrono::Duration::minutes(1));
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now,
    )?;
    store.mark_execution_canary_built(&reserve.order.order_id, now)?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(&reserve.order.order_id, now, "tx-partial-sell")?;
    store.mark_execution_canary_confirmed(&reserve.order.order_id, now)?;
    let request = partial_sell_request(&reserve.order, &signal);

    let fill = crate::execution_submit_adapter::build_execution_confirmed_fill_from_request(
        &store, &request, now,
    )?;
    let outcome = crate::execution_submit_adapter::record_confirmed_fill_accounting(&store, fill)?;
    let remaining = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("partial sell should leave remaining position");

    assert_eq!(outcome.sell_closed, 1);
    assert_eq!(outcome.sell_partial, 1);
    assert_eq!(outcome.closed_qty, 4.0);
    assert_eq!(
        remaining.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(6_000, 3))
    );
    assert_eq!(
        remaining.cost_lamports,
        Some(copybot_core_types::Lamports::new(479_999_999))
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn partial_sell_request(
    order: &copybot_storage_core::ExecutionCanaryOrder,
    signal: &copybot_core_types::CopySignalRow,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: signal.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        route: crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN
            .to_string(),
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: signal.side.clone(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: "ExecutorPubkey".to_string(),
        entry_route_plan_json: None,
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_event_id: Some("quote:partial-sell".to_string()),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("4000".to_string()),
            quote_out_amount_raw: Some("480000000".to_string()),
            quote_response_json: Some(
                r#"{"inputMint":"TokenMint","inAmount":"4000","outputMint":"So11111111111111111111111111111111111111112","outAmount":"480000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#
                    .to_string(),
            ),
            quote_price_sol: Some(0.12),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(10_000),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            ..crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default()
        },
    }
}

fn partial_sell_signal(ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-partial-sell:leader-wallet:sell:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.48,
        notional_lamports: Some(copybot_core_types::Lamports::new(480_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_partial_sell_accounting_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-partial-sell-accounting-{name}-{nanos}.db"
    ))
}
