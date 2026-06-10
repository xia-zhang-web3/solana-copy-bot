use super::*;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn tiny_submit_route_contract_rejects_missing_submit_url() -> Result<()> {
    let keypair = tiny_route_keypair(21);
    let keypair_path = write_tiny_route_keypair_file("contract-missing-url", &keypair.bytes)?;
    let mut config = tiny_route_config(
        &keypair.pubkey,
        &keypair_path,
        "http://127.0.0.1:1",
        "http://127.0.0.1:2",
    );
    config.submit_adapter_http_url.clear();

    let error = crate::config_contract::validate_execution_runtime_contract(&config, "dev")
        .expect_err("tiny submit must require a submit RPC URL");

    assert!(error
        .to_string()
        .contains("canary_tiny_submit_enabled requires submit_adapter_http_url"));

    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[test]
fn tiny_submit_route_contract_accepts_guarded_config() -> Result<()> {
    let keypair = tiny_route_keypair(22);
    let keypair_path = write_tiny_route_keypair_file("contract-ok", &keypair.bytes)?;
    let config = tiny_route_config(
        &keypair.pubkey,
        &keypair_path,
        "http://127.0.0.1:1",
        "http://127.0.0.1:2",
    );

    crate::config_contract::validate_execution_runtime_contract(&config, "dev")?;

    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_route_confirmed_buy_opens_position() -> Result<()> {
    let db_path = unique_tiny_route_test_path("confirmed-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_route_keypair(23);
    let keypair_path = write_tiny_route_keypair_file("confirmed-buy", &keypair.bytes)?;
    let now = Utc::now();
    let signal = tiny_route_signal("confirmed-buy", now);
    store.insert_copy_signal(&signal)?;
    record_tiny_route_quote(&store, &signal, now)?;
    let (base_url, server) =
        serve_metis_build_submit_and_confirm(&keypair.public_key, "tx-tiny-route-buy").await?;
    let config = tiny_route_config(&keypair.pubkey, &keypair_path, &base_url, &base_url);
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner.process_tick(&store, now).await?;
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("tiny route should reserve an order");
    let position = store
        .load_execution_canary_open_position(&signal.token)?
        .expect("confirmed tiny BUY should open accounting position");

    assert_eq!(summary.candidates, 1);
    assert_eq!(summary.state_machine_reserved, 1);
    assert_eq!(summary.state_machine_built, 1);
    assert_eq!(summary.state_machine_simulated, 1);
    assert_eq!(summary.state_machine_submit_disabled, 0);
    assert_eq!(summary.state_machine_failed, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(order.tx_signature.as_deref(), Some("tx-tiny-route-buy"));
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    assert_eq!(
        position.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3))
    );
    assert!((position.qty - 10.0).abs() < 1e-9);
    assert!((position.cost_sol - 0.01).abs() < 1e-9);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_route_reconciles_existing_submitted_buy() -> Result<()> {
    let db_path = unique_tiny_route_test_path("reconcile-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_route_keypair(25);
    let keypair_path = write_tiny_route_keypair_file("reconcile-buy", &keypair.bytes)?;
    let now = Utc::now();
    let signal = tiny_route_signal("reconcile-buy", now);
    store.insert_copy_signal(&signal)?;
    record_tiny_route_quote(&store, &signal, now)?;
    let order_id =
        mark_tiny_route_submitted_order(&store, &signal, now, "tx-tiny-route-reconcile-buy")?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("tiny route should reserve an order");

    assert_eq!(order.order_id, order_id);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert!(store
        .load_execution_canary_open_position(&signal.token)?
        .is_none());

    let (confirm_url, confirm_server) =
        serve_tiny_route_confirmation_only("tx-tiny-route-reconcile-buy").await?;
    let mut reconcile_config =
        tiny_route_config(&keypair.pubkey, &keypair_path, &confirm_url, &confirm_url);
    reconcile_config.quote_canary_enabled = false;
    let second = crate::execution_canary_route::process_canary_state_machine_for_route(
        &reconcile_config,
        &store,
        &signal,
        now + chrono::Duration::seconds(2),
    )
    .await?;
    confirm_server.await?;
    let confirmed = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("tiny route order should still exist");

    assert_eq!(second.existing, 1);
    assert_eq!(second.reserved, 0);
    assert_eq!(second.built, 0);
    assert_eq!(
        confirmed.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_tick_sweeps_existing_submitted_buy() -> Result<()> {
    let db_path = unique_tiny_route_test_path("sweep-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_route_keypair(27);
    let keypair_path = write_tiny_route_keypair_file("sweep-buy", &keypair.bytes)?;
    let now = Utc::now();
    let signal = tiny_route_signal("sweep-buy", now);
    store.insert_copy_signal(&signal)?;
    let order_id =
        mark_tiny_route_submitted_order(&store, &signal, now, "tx-tiny-route-sweep-buy")?;
    let (confirm_url, confirm_server) =
        serve_tiny_route_confirmation_only("tx-tiny-route-sweep-buy").await?;
    let mut config = tiny_route_config(&keypair.pubkey, &keypair_path, &confirm_url, &confirm_url);
    config.quote_canary_enabled = false;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner
        .process_tick(&store, now + chrono::Duration::seconds(5))
        .await?;
    confirm_server.await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("swept order should exist");

    assert_eq!(summary.state_machine_existing, 1);
    assert_eq!(summary.state_machine_reserved, 0);
    assert_eq!(summary.state_machine_built, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

async fn serve_metis_build_submit_and_confirm(
    first_signer: &[u8; 32],
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let serialized_transaction = serialized_legacy_transaction(*first_signer);
    let server = tokio::spawn(async move {
        let quote = read_tiny_route_http_request(&listener).await;
        assert!(quote.body.starts_with("GET /quote?"));
        write_tiny_route_http_response(
            quote.socket,
            r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
        )
        .await;

        let swap_instructions = read_tiny_route_http_request(&listener).await;
        assert!(swap_instructions
            .body
            .starts_with("POST /swap-instructions "));
        assert!(swap_instructions.body.contains("\"quoteResponse\""));
        assert!(swap_instructions
            .body
            .contains("\"prioritizationFeeLamports\":22000"));
        write_tiny_route_http_response(
            swap_instructions.socket,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#,
        )
        .await;

        let swap = read_tiny_route_http_request(&listener).await;
        assert!(swap.body.starts_with("POST /swap "));
        assert!(swap.body.contains("\"userPublicKey\""));
        write_tiny_route_http_response(
            swap.socket,
            &format!(r#"{{"swapTransaction":"{serialized_transaction}","simulationError":null}}"#),
        )
        .await;

        let rpc_simulation = read_tiny_route_http_request(&listener).await;
        assert!(rpc_simulation
            .body
            .contains("\"method\":\"simulateTransaction\""));
        assert!(rpc_simulation.body.contains("\"sigVerify\":false"));
        write_tiny_route_http_response(
            rpc_simulation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":null,"logs":[]}}}"#,
        )
        .await;

        let submit = read_tiny_route_http_request(&listener).await;
        assert!(submit.body.contains("\"method\":\"sendTransaction\""));
        write_tiny_route_http_response(
            submit.socket,
            &format!(r#"{{"jsonrpc":"2.0","id":"execution-submit","result":"{tx}"}}"#),
        )
        .await;

        let confirmation = read_tiny_route_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx));
        write_tiny_route_http_response(
            confirmation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":42,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    Ok((base_url, server))
}

async fn serve_tiny_route_confirmation_only(
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let server = tokio::spawn(async move {
        let confirmation = read_tiny_route_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx));
        write_tiny_route_http_response(
            confirmation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":44,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    Ok((base_url, server))
}

fn mark_tiny_route_submitted_order(
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
            quote_event_id: Some(format!("quote:entry:{}", signal.signal_id)),
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

struct TinyRouteHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_tiny_route_http_request(listener: &tokio::net::TcpListener) -> TinyRouteHttpRequest {
    let (mut socket, _) = listener.accept().await.expect("tiny route HTTP request");
    let mut buffer = [0_u8; 16384];
    let read = socket.read(&mut buffer).await.expect("read request");
    TinyRouteHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_tiny_route_http_response(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn tiny_route_config(
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
    config.canary_batch_limit = 1;
    config.canary_max_open_positions = 1;
    config.canary_max_signal_age_seconds = 60;
    config.canary_max_daily_loss_sol = 0.02;
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = quote_base_url.to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.quote_canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config.submit_timeout_ms = 1_000;
    config.max_confirm_seconds = 2;
    config
}

fn tiny_route_signal(name: &str, ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-tiny-route:leader-wallet:{name}:TokenMint"),
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

fn record_tiny_route_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: Some(8),
            quote_latency_ms: Some(12),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(
                r#"{"loadedLongtailToken":true,"outAmountUi":10.0,"meta":{"outDecimals":3}}"#
                    .to_string(),
            ),
            quote_price_sol: Some(0.001),
            shadow_price_sol: Some(0.001),
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

struct TinyRouteKeypair {
    public_key: [u8; 32],
    pubkey: String,
    bytes: Vec<u8>,
}

fn tiny_route_keypair(seed: u8) -> TinyRouteKeypair {
    let secret = [seed; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key().to_bytes();
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&secret);
    bytes.extend_from_slice(&public_key);
    TinyRouteKeypair {
        public_key,
        pubkey: bs58::encode(public_key).into_string(),
        bytes,
    }
}

fn write_tiny_route_keypair_file(name: &str, bytes: &[u8]) -> Result<PathBuf> {
    let path = unique_tiny_route_test_path(name).with_extension("json");
    std::fs::write(&path, serde_json::to_string(bytes)?)?;
    Ok(path)
}

fn serialized_legacy_transaction(first_account_key: [u8; 32]) -> String {
    let mut transaction = vec![1_u8];
    transaction.extend_from_slice(&[0_u8; 64]);
    transaction.extend_from_slice(&[1_u8, 0, 0]);
    transaction.push(1);
    transaction.extend_from_slice(&first_account_key);
    transaction.extend_from_slice(&[9_u8; 32]);
    transaction.push(0);
    BASE64_STANDARD.encode(transaction)
}

fn unique_tiny_route_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-tiny-route-{name}-{}-{nanos}",
        std::process::id()
    ))
}
