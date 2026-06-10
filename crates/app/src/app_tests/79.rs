use super::*;

#[tokio::test]
async fn already_confirmed_tiny_buy_reconciliation_backfills_missing_position() -> Result<()> {
    let db_path = unique_backfill_test_path("confirmed-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_backfill_order(&store, "confirmed-buy", "buy", "tx-buy", now)?;
    record_backfill_build_metadata(&store, &order_id, now, 0.1)?;
    store.mark_execution_canary_confirmed(&order_id, now + chrono::Duration::seconds(4))?;

    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let outcome = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &store,
        &backfill_reconcile_config(),
        &order_id,
        &reqwest::Client::new(),
        "",
        now + chrono::Duration::seconds(5),
        1_000,
    )
    .await?;
    let position = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("already confirmed buy should backfill owned position");

    assert_eq!(outcome.confirmation_confirmed, 1);
    assert_eq!(outcome.buy_opened, 1);
    assert_eq!(outcome.buy_existing, 0);
    assert_eq!(
        outcome.reason.as_deref(),
        Some("submitted_reconcile_already_confirmed")
    );
    assert_eq!(position.position_id, format!("exec-canary-pos:{order_id}"));
    assert_eq!(
        position.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3))
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn backfill_reconcile_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_buy_size_sol = 1.0;
    config.canary_wallet_pubkey = "DryRunWallet11111111111111111111111111111111".to_string();
    config
}

fn submitted_backfill_order(
    store: &SqliteStore,
    name: &str,
    side: &str,
    tx_signature: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let signal = backfill_signal(name, side, now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(2),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(3),
        tx_signature,
    )?;
    Ok(reserve.order.order_id)
}

fn record_backfill_build_metadata(
    store: &SqliteStore,
    order_id: &str,
    now: chrono::DateTime<Utc>,
    quote_price_sol: f64,
) -> Result<()> {
    let order = store
        .load_execution_canary_order(order_id)?
        .expect("order should exist");
    store.record_execution_canary_build_plan_metadata(
        &copybot_storage_core::ExecutionCanaryBuildPlanMetadata {
            order_id: order.order_id,
            signal_id: order.signal_id,
            client_order_id: order.client_order_id,
            recorded_ts: now,
            quote_source: Some("test".to_string()),
            quote_event_id: Some("quote:test".to_string()),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("1000000000".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(
                r#"{"quote":{"outAmountUi":10.0,"meta":{"outDecimals":3}}}"#.to_string(),
            ),
            quote_price_sol: Some(quote_price_sol),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_source: Some("test".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(10_000),
            priority_fee_json: Some("{\"recommended\":10000}".to_string()),
            slippage_bps: Some(10.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
        },
    )?;
    Ok(())
}

fn backfill_signal(
    name: &str,
    side: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-backfill:leader-wallet:{name}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(copybot_core_types::Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_backfill_test_path(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-confirmed-backfill-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
