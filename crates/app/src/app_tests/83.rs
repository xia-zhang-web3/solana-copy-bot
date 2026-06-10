use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_tick_reconciles_submitted_buy_before_orphan_recovery() -> Result<()> {
    let db_path = unique_tiny_tick_ordering_path("submitted-buy-before-recovery");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    let historical_opened_ts = now - chrono::Duration::hours(2);
    store.record_execution_canary_open_position(
        "historical-confirmed-buy",
        token,
        1.0,
        Some(TokenQuantity::new(1_000, 3)),
        0.01,
        historical_opened_ts,
    )?;
    store.close_execution_canary_open_position(
        token,
        1.0,
        Some(TokenQuantity::new(1_000, 3)),
        0.001,
        1e-9,
        now - chrono::Duration::hours(1),
    )?;
    let signal = tiny_tick_ordering_buy_signal(token, now);
    store.insert_copy_signal(&signal)?;
    let order_id =
        mark_tiny_tick_ordering_submitted_order(&store, &signal, now, "tx-tiny-tick-ordering-buy")?;
    let (rpc_url, server) =
        serve_tiny_tick_confirmation_and_wallet("tx-tiny-tick-ordering-buy", token).await?;
    let runner = ExecutionCanaryRunner::new(tiny_tick_ordering_config(&rpc_url));

    let summary = runner
        .process_tick(&store, now + chrono::Duration::seconds(5))
        .await?;
    server.await??;
    let position = store
        .load_execution_canary_open_position(token)?
        .expect("submitted buy should be accounted as the order position");

    assert_eq!(summary.state_machine_existing, 1);
    assert_eq!(summary.orphan_recovery_recovered, 0);
    assert_eq!(position.position_id, format!("exec-canary-pos:{order_id}"));
    assert_eq!(position.qty_exact, Some(TokenQuantity::new(10_000, 3)));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn mark_tiny_tick_ordering_submitted_order(
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: chrono::DateTime<Utc>,
    tx_signature: &str,
) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now,
    )?;
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
    store.record_execution_canary_build_plan_metadata(
        &copybot_storage_core::ExecutionCanaryBuildPlanMetadata {
            order_id: reserve.order.order_id.clone(),
            signal_id: signal.signal_id.clone(),
            client_order_id: reserve.order.client_order_id.clone(),
            recorded_ts: now,
            quote_source: Some("test".to_string()),
            quote_event_id: Some(format!("quote:entry:{}", signal.signal_id)),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(
                r#"{"loadedLongtailToken":true,"outAmountUi":10.0,"meta":{"outDecimals":3}}"#
                    .to_string(),
            ),
            quote_price_sol: Some(0.001),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_source: Some("test".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some("{\"recommended\":22000}".to_string()),
            slippage_bps: Some(100.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
        },
    )?;
    Ok(reserve.order.order_id)
}

async fn serve_tiny_tick_confirmation_and_wallet(
    tx_signature: &'static str,
    token: &'static str,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await?;
            let mut buffer = [0_u8; 4096];
            let read = socket.read(&mut buffer).await?;
            let request = String::from_utf8_lossy(&buffer[..read]);
            let body = if request.contains("getSignatureStatuses") {
                format!(
                    r#"{{"jsonrpc":"2.0","id":"execution-confirmation","result":{{"value":[{{"slot":44,"confirmations":null,"err":null,"confirmationStatus":"finalized"}}]}}}}"#
                )
            } else if request.contains("getTransaction") {
                r#"{"jsonrpc":"2.0","id":"execution-confirmed-fill","result":null}"#.to_string()
            } else {
                assert!(request.contains("getTokenAccountsByOwner"));
                format!(
                    r#"{{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{{"value":[{{"account":{{"data":{{"parsed":{{"info":{{"mint":"{token}","tokenAmount":{{"amount":"10000","decimals":3,"uiAmountString":"10"}}}}}}}}}}}}]}}}}"#
                )
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await?;
        }
        assert!(!tx_signature.is_empty());
        Ok(())
    });
    Ok((base_url, server))
}

fn tiny_tick_ordering_config(rpc_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = rpc_url.to_string();
    config.priority_fee_canary_rpc_url = rpc_url.to_string();
    config.canary_buy_size_sol = 0.01;
    config.canary_batch_limit = 1;
    config.canary_max_open_positions = 10;
    config.canary_max_signal_age_seconds = 60;
    config.canary_max_daily_loss_sol = 2.0;
    config.quote_canary_enabled = false;
    config.quote_canary_timeout_ms = 1_000;
    config.swap_transaction_dry_run_enabled = true;
    config.submit_timeout_ms = 1_000;
    config.max_confirm_seconds = 2;
    config
}

fn tiny_tick_ordering_buy_signal(token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-tiny-tick-ordering:leader:{token}"),
        wallet_id: "leader".to_string(),
        side: "buy".to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_tiny_tick_ordering_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-tiny-tick-ordering-{name}-{}-{nanos}",
        std::process::id()
    ))
}
