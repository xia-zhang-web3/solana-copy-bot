use super::*;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_submit_route_confirmed_sell_closes_owned_position() -> Result<()> {
    let db_path = unique_tiny_sell_route_test_path("confirmed-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_sell_route_keypair(24);
    let keypair_path = write_tiny_sell_route_keypair_file("confirmed-sell", &keypair.bytes)?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let signal = tiny_sell_route_signal("confirmed-sell", now + chrono::Duration::minutes(1));
    store.insert_copy_signal(&signal)?;
    let quote_event_id = format!("quote:close:{}", signal.signal_id);
    record_tiny_sell_route_quote(&store, &signal, &quote_event_id, now)?;
    let (base_url, server) =
        serve_sell_build_submit_and_confirm(&keypair.public_key, "tx-tiny-route-sell").await?;
    let mut config = tiny_sell_route_config(&keypair.pubkey, &keypair_path, &base_url, &base_url);
    config.pretrade_max_priority_fee_lamports = 10_000;

    let summary = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &store,
        &quote_event_id,
        now + chrono::Duration::minutes(1),
    )
    .await?
    .expect("sell quote event should be processed");
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("tiny sell route should reserve an order");
    let metadata = store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .expect("tiny sell build metadata should be recorded");

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.sell_execute, 1);
    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.sell_closed, 1);
    assert_eq!(summary.failed, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(metadata.priority_fee_lamports, Some(10_000));
    assert_eq!(order.tx_signature.as_deref(), Some("tx-tiny-route-sell"));
    assert!(store
        .load_execution_canary_open_position(&signal.token)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_route_reconciles_existing_submitted_sell() -> Result<()> {
    let db_path = unique_tiny_sell_route_test_path("reconcile-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_sell_route_keypair(26);
    let keypair_path = write_tiny_sell_route_keypair_file("reconcile-sell", &keypair.bytes)?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let signal = tiny_sell_route_signal("reconcile-sell", now + chrono::Duration::minutes(1));
    store.insert_copy_signal(&signal)?;
    let quote_event_id = format!("quote:close:{}", signal.signal_id);
    record_tiny_sell_route_quote(&store, &signal, &quote_event_id, now)?;
    mark_tiny_sell_route_submitted_order(
        &store,
        &signal,
        now + chrono::Duration::minutes(1),
        "tx-tiny-route-reconcile-sell",
    )?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("tiny sell route should reserve an order");

    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert!(store
        .load_execution_canary_open_position(&signal.token)?
        .is_some());

    let (confirm_url, confirm_server) =
        serve_tiny_sell_route_confirmation_only("tx-tiny-route-reconcile-sell").await?;
    let reconcile_config =
        tiny_sell_route_config(&keypair.pubkey, &keypair_path, &confirm_url, &confirm_url);
    let second = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &reconcile_config,
        &store,
        &quote_event_id,
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(2),
    )
    .await?
    .expect("sell quote event should be reconciled");
    confirm_server.await?;
    let confirmed = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("tiny sell route order should still exist");

    assert_eq!(second.existing, 1);
    assert_eq!(second.reserved, 0);
    assert_eq!(second.built, 0);
    assert_eq!(second.sell_closed, 1);
    assert_eq!(
        confirmed.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert!(store
        .load_execution_canary_open_position(&signal.token)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_tick_sweeps_existing_submitted_sell() -> Result<()> {
    let db_path = unique_tiny_sell_route_test_path("sweep-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_sell_route_keypair(28);
    let keypair_path = write_tiny_sell_route_keypair_file("sweep-sell", &keypair.bytes)?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let signal = tiny_sell_route_signal("sweep-sell", now + chrono::Duration::minutes(1));
    store.insert_copy_signal(&signal)?;
    let order_id = mark_tiny_sell_route_submitted_order(
        &store,
        &signal,
        now + chrono::Duration::minutes(1),
        "tx-tiny-route-sweep-sell",
    )?;
    let (confirm_url, confirm_server) =
        serve_tiny_sell_route_confirmation_only("tx-tiny-route-sweep-sell").await?;
    let mut config =
        tiny_sell_route_config(&keypair.pubkey, &keypair_path, &confirm_url, &confirm_url);
    config.quote_canary_enabled = false;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner
        .process_tick(
            &store,
            now + chrono::Duration::minutes(1) + chrono::Duration::seconds(5),
        )
        .await?;
    confirm_server.await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("swept sell order should exist");

    assert_eq!(summary.state_machine_existing, 1);
    assert_eq!(summary.state_machine_reserved, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert!(store
        .load_execution_canary_open_position(&signal.token)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

async fn serve_sell_build_submit_and_confirm(
    first_signer: &[u8; 32],
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let serialized_transaction = serialized_sell_route_legacy_transaction(*first_signer);
    let server = tokio::spawn(async move {
        let balance = read_tiny_sell_route_http_request(&listener).await;
        assert!(balance
            .body
            .contains("\"method\":\"getTokenAccountsByOwner\""));
        write_tiny_sell_route_http_response(
            balance.socket,
            r#"{"jsonrpc":"2.0","id":"execution-canary-owned-sell-balance","result":{"value":[{"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"10000","decimals":3}}}}}}]}}"#,
        )
        .await;

        let quote = read_tiny_sell_route_http_request(&listener).await;
        assert!(quote.body.starts_with("GET /quote?"));
        assert!(quote.body.contains("amount=10000"));
        write_tiny_sell_route_http_response(
            quote.socket,
            r#"{"inputMint":"TokenMint","inAmount":"10000","outputMint":"So11111111111111111111111111111111111111112","outAmount":"1200000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
        )
        .await;

        let swap_instructions = read_tiny_sell_route_http_request(&listener).await;
        assert!(swap_instructions
            .body
            .starts_with("POST /swap-instructions "));
        assert!(swap_instructions.body.contains("\"inAmount\":\"10000\""));
        assert!(swap_instructions
            .body
            .contains("\"prioritizationFeeLamports\":10000"));
        write_tiny_sell_route_http_response(
            swap_instructions.socket,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#,
        )
        .await;

        let swap = read_tiny_sell_route_http_request(&listener).await;
        assert!(swap.body.starts_with("POST /swap "));
        write_tiny_sell_route_http_response(
            swap.socket,
            &format!(r#"{{"swapTransaction":"{serialized_transaction}","simulationError":null}}"#),
        )
        .await;

        let rpc_simulation = read_tiny_sell_route_http_request(&listener).await;
        assert!(rpc_simulation
            .body
            .contains("\"method\":\"simulateTransaction\""));
        write_tiny_sell_route_http_response(
            rpc_simulation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":null,"logs":[]}}}"#,
        )
        .await;

        let submit = read_tiny_sell_route_http_request(&listener).await;
        assert!(submit.body.contains("\"method\":\"sendTransaction\""));
        write_tiny_sell_route_http_response(
            submit.socket,
            &format!(r#"{{"jsonrpc":"2.0","id":"execution-submit","result":"{tx}"}}"#),
        )
        .await;

        let confirmation = read_tiny_sell_route_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx));
        write_tiny_sell_route_http_response(
            confirmation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":43,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    Ok((base_url, server))
}

async fn serve_tiny_sell_route_confirmation_only(
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let server = tokio::spawn(async move {
        let confirmation = read_tiny_sell_route_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx));
        write_tiny_sell_route_http_response(
            confirmation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":45,"confirmations":null,"err":null,"confirmationStatus":"confirmed"}]}}"#,
        )
        .await;
    });
    Ok((base_url, server))
}

fn mark_tiny_sell_route_submitted_order(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
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
            quote_event_id: Some(format!("quote:close:{}", signal.signal_id)),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("200000".to_string()),
            quote_out_amount_raw: Some("24000000000".to_string()),
            quote_response_json: Some(
                r#"{"inputMint":"TokenMint","inAmount":"200000","outputMint":"So11111111111111111111111111111111111111112","outAmount":"24000000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#
                    .to_string(),
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
    Ok(reserve.order.order_id)
}

struct TinySellRouteHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_tiny_sell_route_http_request(
    listener: &tokio::net::TcpListener,
) -> TinySellRouteHttpRequest {
    let (mut socket, _) = listener
        .accept()
        .await
        .expect("tiny sell route HTTP request");
    let mut buffer = [0_u8; 16384];
    let read = socket.read(&mut buffer).await.expect("read request");
    TinySellRouteHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_tiny_sell_route_http_response(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn tiny_sell_route_config(
    pubkey: &str,
    keypair_path: &Path,
    quote_base_url: &str,
    rpc_url: &str,
) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = pubkey.to_string();
    config.execution_signer_pubkey = pubkey.to_string();
    config.execution_signer_keypair_path = keypair_path.to_string_lossy().to_string();
    config.submit_adapter_http_url = rpc_url.to_string();
    config.canary_buy_size_sol = 0.01;
    config.canary_max_open_positions = 1;
    config.quote_canary_base_url = quote_base_url.to_string();
    config.quote_canary_sell_slippage_bps = 500;
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config.submit_timeout_ms = 1_000;
    config.max_confirm_seconds = 2;
    config
}

fn tiny_sell_route_signal(
    name: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-tiny-sell-route:leader-wallet:{name}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 1.2,
        notional_lamports: None,
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn record_tiny_sell_route_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    event_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: event_id.to_string(),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "sell".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: Some(8),
            quote_latency_ms: Some(12),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000".to_string()),
            quote_out_amount_raw: Some("1200000000".to_string()),
            quote_response_json: Some(
                r#"{"inputMint":"TokenMint","inAmount":"10000","outputMint":"So11111111111111111111111111111111111111112","outAmount":"1200000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#
                    .to_string(),
            ),
            quote_price_sol: Some(0.12),
            shadow_price_sol: Some(0.12),
            slippage_bps: Some(100.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some("{\"recommended\":22000}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

struct TinySellRouteKeypair {
    public_key: [u8; 32],
    pubkey: String,
    bytes: Vec<u8>,
}

fn tiny_sell_route_keypair(seed: u8) -> TinySellRouteKeypair {
    let secret = [seed; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key().to_bytes();
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&secret);
    bytes.extend_from_slice(&public_key);
    TinySellRouteKeypair {
        public_key,
        pubkey: bs58::encode(public_key).into_string(),
        bytes,
    }
}

fn write_tiny_sell_route_keypair_file(name: &str, bytes: &[u8]) -> Result<PathBuf> {
    let path = unique_tiny_sell_route_test_path(name).with_extension("json");
    std::fs::write(&path, serde_json::to_string(bytes)?)?;
    Ok(path)
}

fn serialized_sell_route_legacy_transaction(first_account_key: [u8; 32]) -> String {
    let mut transaction = vec![1_u8];
    transaction.extend_from_slice(&[0_u8; 64]);
    transaction.extend_from_slice(&[1_u8, 0, 0]);
    transaction.push(1);
    transaction.extend_from_slice(&first_account_key);
    transaction.extend_from_slice(&[9_u8; 32]);
    transaction.push(0);
    BASE64_STANDARD.encode(transaction)
}

fn unique_tiny_sell_route_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-tiny-sell-route-{name}-{}-{nanos}",
        std::process::id()
    ))
}
