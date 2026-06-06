use super::*;

#[tokio::test]
async fn execution_canary_state_machine_persists_build_plan_metadata() -> Result<()> {
    let db_path = unique_execution_build_metadata_test_path("persist-build-metadata");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-build-metadata:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now,
        status: "shadow_recorded".to_string(),
    };
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: "quote:entry:persisted".to_string(),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now + chrono::Duration::milliseconds(25),
            signal_ts: Some(now),
            decision_delay_ms: Some(25),
            quote_latency_ms: Some(15),
            leader_notional_sol: Some(0.2),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: None,
            quote_price_sol: Some(0.000081),
            shadow_price_sol: Some(0.00008),
            slippage_bps: Some(12.5),
            price_impact_pct: Some(0.04),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(12_345),
            priority_fee_json: Some("{\"recommended\":12345}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("inside_limits".to_string()),
            error: None,
        },
    )?;

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config,
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");
    let metadata = store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .expect("build metadata should be persisted");
    let report = store.execution_canary_status_report(now + chrono::Duration::seconds(1))?;

    assert_eq!(summary.built, 1);
    assert_eq!(metadata.client_order_id, order.client_order_id);
    assert_eq!(
        metadata.quote_source.as_deref(),
        Some("execution_quote_canary_event")
    );
    assert_eq!(
        metadata.quote_event_id.as_deref(),
        Some("quote:entry:persisted")
    );
    assert_eq!(metadata.quote_in_amount_raw.as_deref(), Some("10000000"));
    assert_eq!(metadata.quote_out_amount_raw.as_deref(), Some("123456"));
    assert_eq!(metadata.priority_fee_lamports, Some(12_345));
    assert_eq!(
        report
            .latest_build_plan_metadata
            .as_ref()
            .map(|latest| latest.order_id.as_str()),
        Some(order.order_id.as_str())
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn unique_execution_build_metadata_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-build-metadata-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
