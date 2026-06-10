use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};

#[tokio::test]
async fn terminal_no_route_sell_does_not_write_off_newer_recovery_open_position() -> Result<()> {
    let db_path = unique_stale_terminal_guard_test_path("terminal-no-route-stale-recovery");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    let signal = retry_candidate_sell_signal(token, now + chrono::Duration::seconds(1));
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now,
    )?;
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&retry_candidate_sell_quote(
        "quote:close:terminal-no-route-stale-recovery",
        &signal,
        now + chrono::Duration::seconds(2),
    ))?;
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now + chrono::Duration::seconds(3),
    )?;
    mark_no_route_failures_to_terminal(&store, &reserve.order.order_id, now)?;
    store.close_execution_canary_open_position(
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.0,
        1e-12,
        now + chrono::Duration::seconds(10),
    )?;
    store.record_execution_canary_open_position(
        "recovery-orphan:TokenMint:newer",
        token,
        20.0,
        Some(TokenQuantity::new(20_000, 3)),
        0.02,
        now + chrono::Duration::seconds(20),
    )?;
    let config = retry_candidate_sell_config();

    let summary = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &store,
        "quote:close:terminal-no-route-stale-recovery",
        now + chrono::Duration::seconds(30),
    )
    .await?
    .expect("terminal no-route sell should be inspected");
    let order = store
        .load_execution_canary_order(&reserve.order.order_id)?
        .expect("terminal no-route order should remain recorded");

    assert_eq!(
        summary.skipped_reason,
        Some("terminal_failed_sell_stale_position_signal")
    );
    assert_eq!(summary.sell_closed, 0);
    assert_eq!(summary.open_positions, 1);
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED)
    );
    let open = store
        .load_execution_canary_open_position(token)?
        .expect("newer recovery position must remain open");
    assert_eq!(
        open.position_id,
        "exec-canary-pos:recovery-orphan:TokenMint:newer"
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn retry_candidate_sell_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_base_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.max_submit_attempts = 3;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn retry_candidate_sell_signal(token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-stale-terminal-sell:leader:{token}"),
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

fn retry_candidate_sell_quote(
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

fn mark_no_route_failures_to_terminal(
    store: &SqliteStore,
    order_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    for offset in [4, 6, 8] {
        store.mark_execution_canary_failed(
            order_id,
            now + chrono::Duration::seconds(offset),
            copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED,
            "owned_sell_quote_failed: NO_ROUTES_FOUND",
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

fn unique_stale_terminal_guard_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-stale-terminal-guard-{name}-{}-{nanos}",
        std::process::id()
    ))
}
