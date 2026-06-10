use super::*;

#[test]
fn public_execute_beats_metis_skip_for_live_execution_selection() -> Result<()> {
    let db_path = unique_provider_selection_path("public-execute-metis-skip");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = selector_signal(now);
    let event = quote_event(&signal, now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&event)?;
    record_provider_sample(
        &store,
        &event.event_id,
        copybot_storage_core::PROVIDER_GENERIC_METIS,
        "ok",
        Some(generic_quote_json()),
        "would_skip",
        "slippage_above_limit",
        Some(12_786.65),
        None,
    )?;
    record_provider_sample(
        &store,
        &event.event_id,
        copybot_storage_core::PROVIDER_GENERIC_PUBLIC,
        "ok",
        Some(generic_quote_json()),
        "would_execute",
        "within_slippage_limit",
        Some(-328.46),
        None,
    )?;
    record_provider_sample(
        &store,
        &event.event_id,
        copybot_storage_core::PROVIDER_PUMP_FUN_PAID,
        "error",
        None,
        "unknown",
        "quote_error",
        None,
        Some("pump.fun paid quote returned HTTP 400 Bad Request: bonding curve not found"),
    )?;

    let metadata =
        crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
            &store, event,
        )?;

    assert_eq!(
        metadata.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_PUBLIC)
    );
    assert_eq!(metadata.decision_status.as_deref(), Some("would_execute"));
    assert_eq!(
        metadata.decision_reason.as_deref(),
        Some("within_slippage_limit")
    );
    assert_eq!(metadata.slippage_bps, Some(-328.46));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn selector_signal(ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:selector:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn quote_event(
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: format!("quote:entry:{}", signal.signal_id),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(7),
        quote_latency_ms: Some(11),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: Some(generic_quote_json()),
        quote_price_sol: Some(0.081),
        shadow_price_sol: Some(0.08),
        slippage_bps: Some(125.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Amm"}}]"#.to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(22_000),
        priority_fee_json: Some(r#"{"recommended":22000}"#.to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error: None,
    }
}

fn record_provider_sample(
    store: &SqliteStore,
    event_id: &str,
    provider: &str,
    quote_status: &str,
    quote_response_json: Option<String>,
    decision_status: &str,
    decision_reason: &str,
    slippage_bps: Option<f64>,
    error: Option<&str>,
) -> Result<()> {
    store.record_execution_quote_canary_provider_sample(
        &copybot_storage_core::ExecutionQuoteCanaryProviderSampleInsert {
            event_id: event_id.to_string(),
            provider: provider.to_string(),
            side: "buy".to_string(),
            quote_status: quote_status.to_string(),
            request_ts: Utc::now(),
            quote_latency_ms: Some(9),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json,
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps,
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Amm"}}]"#.to_string()),
            decision_status: Some(decision_status.to_string()),
            decision_reason: Some(decision_reason.to_string()),
            error: error.map(ToString::to_string),
        },
    )?;
    Ok(())
}

fn generic_quote_json() -> String {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#.to_string()
}

fn unique_provider_selection_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("copybot-provider-selection-{name}-{nanos}.db"))
}
