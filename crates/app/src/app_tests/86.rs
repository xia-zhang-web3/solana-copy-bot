use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};

#[tokio::test]
async fn terminal_token_not_tradable_sell_writes_off_open_position_after_retry_budget() -> Result<()>
{
    let db_path = unique_token_not_tradable_test_path("terminal-token-not-tradable");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    let signal = token_not_tradable_sell_signal(token, now + chrono::Duration::seconds(1));
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now,
    )?;
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&token_not_tradable_sell_quote(
        "quote:close:terminal-token-not-tradable",
        &signal,
        now + chrono::Duration::seconds(2),
    ))?;
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now + chrono::Duration::seconds(3),
    )?;
    mark_token_not_tradable_failures_to_terminal(&store, &reserve.order.order_id, now)?;
    let config = token_not_tradable_config("http://127.0.0.1:9");

    let summary = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &store,
        "quote:close:terminal-token-not-tradable",
        now + chrono::Duration::seconds(9),
    )
    .await?
    .expect("terminal token-not-tradable sell should be processed");
    let order = store
        .load_execution_canary_order(&reserve.order.order_id)?
        .expect("terminal token-not-tradable order should remain recorded");

    assert_eq!(
        summary.skipped_reason,
        Some("terminal_failed_sell_no_route_written_off")
    );
    assert_eq!(summary.sell_closed, 1);
    assert_eq!(summary.open_positions, 0);
    assert_eq!(
        summary.last_close_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_POSITION_CLOSE_CLOSED)
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE)
    );
    let error = order.simulation_error.as_deref().unwrap_or_default();
    assert!(error.contains("terminal_failed_sell_no_route_written_off"));
    assert!(error.contains("TOKEN_NOT_TRADABLE"));
    assert!(store.load_execution_canary_open_position(token)?.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn mark_token_not_tradable_failures_to_terminal(
    store: &SqliteStore,
    order_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    for offset in [4, 6, 8] {
        store.mark_execution_canary_failed(
            order_id,
            now + chrono::Duration::seconds(offset),
            copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED,
            "owned_sell_quote_failed: quote canary returned HTTP 400 Bad Request: {\"error\":\"The token TokenMint is not tradable\",\"errorCode\":\"TOKEN_NOT_TRADABLE\"}; pump.fun owned sell fallback failed: pump.fun paid quote returned HTTP 400 Bad Request: {\"error\":\"Bonding curve for mint not found\"}",
        )?;
        if offset < 8 {
            store.mark_execution_canary_failed_build_retry_candidate(
                order_id,
                now + chrono::Duration::seconds(offset + 1),
                "retry_failed_sell_with_owned_position_amount",
            )?;
        }
    }
    Ok(())
}

fn token_not_tradable_config(base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = base_url.to_string();
    config.quote_canary_base_url = base_url.to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.max_submit_attempts = 3;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn token_not_tradable_sell_signal(token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-token-not-tradable-sell:leader:{token}"),
        wallet_id: "leader".to_string(),
        side: "sell".to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn token_not_tradable_sell_quote(
    event_id: &str,
    signal: &CopySignalRow,
    request_ts: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: Some(42),
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "sell".to_string(),
        quote_status: "ok".to_string(),
        request_ts,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(1000),
        quote_latency_ms: Some(50),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("10000".to_string()),
        quote_out_amount_raw: Some("11000000".to_string()),
        quote_response_json: Some("{\"loadedLongtailToken\":true}".to_string()),
        quote_price_sol: Some(0.0011),
        shadow_price_sol: Some(0.001),
        slippage_bps: Some(50.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error: None,
    }
}

fn unique_token_not_tradable_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-token-not-tradable-{name}-{}-{nanos}",
        std::process::id()
    ))
}
