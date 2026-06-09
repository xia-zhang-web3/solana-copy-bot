use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_submit_reconciliation_confirmed_orphan_sell_marks_no_position() -> Result<()> {
    let db_path = unique_orphan_sell_test_path("confirmed-orphan-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_orphan_sell_order(&store, now)?;
    record_orphan_sell_build_metadata(&store, &order_id, now)?;
    let rpc_url = serve_orphan_sell_confirmation("tx-orphan-sell").await?;

    let outcome = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &store,
        &orphan_sell_config(),
        &order_id,
        &reqwest::Client::new(),
        &rpc_url,
        now + chrono::Duration::seconds(7),
        1_000,
    )
    .await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("orphan sell order should exist");

    assert_eq!(outcome.confirmation_confirmed, 1);
    assert_eq!(outcome.sell_no_position, 1);
    assert_eq!(outcome.sell_closed, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn submitted_orphan_sell_order(store: &SqliteStore, now: chrono::DateTime<Utc>) -> Result<String> {
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-orphan-sell:leader-wallet:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(copybot_core_types::Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now,
        status: "shadow_recorded".to_string(),
    };
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
        "tx-orphan-sell",
    )?;
    Ok(reserve.order.order_id)
}

fn record_orphan_sell_build_metadata(
    store: &SqliteStore,
    order_id: &str,
    now: chrono::DateTime<Utc>,
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
            quote_event_id: Some("quote:orphan-sell".to_string()),
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("10000".to_string()),
            quote_out_amount_raw: Some("1200000000".to_string()),
            quote_response_json: Some(
                r#"{"inputMint":"TokenMint","inAmount":"10000","inAmountUi":10.0,"outputMint":"So11111111111111111111111111111111111111112","outAmount":"1200000000","priceImpactPct":"0.01","meta":{"inDecimals":3}}"#.to_string(),
            ),
            quote_price_sol: Some(0.12),
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
    Ok(())
}

fn orphan_sell_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_buy_size_sol = 1.0;
    config.canary_wallet_pubkey = "DryRunWallet11111111111111111111111111111111".to_string();
    config.quote_canary_sell_slippage_bps = 500;
    config
}

async fn serve_orphan_sell_confirmation(expected_signature: &str) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let expected_signature = expected_signature.to_string();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("http request");
        let mut buffer = [0_u8; 8192];
        let read = socket.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("\"method\":\"getSignatureStatuses\""));
        assert!(request.contains(&expected_signature));
        write_orphan_sell_http_response(
            socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":902,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    Ok(rpc_url)
}

async fn write_orphan_sell_http_response(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn unique_orphan_sell_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-orphan-sell-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
