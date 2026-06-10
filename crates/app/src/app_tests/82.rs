use super::*;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use ed25519_dalek::SigningKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_submit_sweep_retries_transient_failed_buy_simulation() -> Result<()> {
    let db_path = unique_buy_retry_test_path("missing-account");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = buy_retry_keypair(82);
    let keypair_path = write_buy_retry_keypair_file("missing-account", &keypair.bytes)?;
    let now = Utc::now();
    let signal = buy_retry_signal("missing-account", now);
    store.insert_copy_signal(&signal)?;
    record_buy_retry_quote(&store, &signal, now)?;
    let order_id = mark_failed_buy_simulation_order(&store, &signal, now)?;

    let (base_url, server) =
        serve_buy_retry_build_submit_and_confirm(&keypair.public_key, "tx-buy-retry").await?;
    let mut config = buy_retry_config(&base_url);
    config.canary_wallet_pubkey = keypair.pubkey.clone();
    config.execution_signer_pubkey = keypair.pubkey.clone();
    config.execution_signer_keypair_path = keypair_path.to_string_lossy().to_string();

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(5),
    )
    .await?
    .expect("tiny submit route should sweep failed buy simulation");
    server.await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("retry order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(order.attempt, 2);
    assert_eq!(order.tx_signature.as_deref(), Some("tx-buy-retry"));
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_sweep_skips_failed_buy_retry_after_later_sell_signal() -> Result<()> {
    let db_path = unique_buy_retry_test_path("stale-after-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = buy_retry_signal("stale-after-sell", now);
    store.insert_copy_signal(&signal)?;
    record_buy_retry_quote(&store, &signal, now)?;
    let order_id = mark_failed_buy_simulation_order(&store, &signal, now)?;
    store.insert_copy_signal(&CopySignalRow {
        signal_id: "shadow:sig-buy-retry-stale-after-sell:leader-wallet:sell:TokenMint".to_string(),
        side: "sell".to_string(),
        ts: now + chrono::Duration::seconds(4),
        ..signal.clone()
    })?;

    let config = buy_retry_config("http://127.0.0.1:1");
    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(5),
    )
    .await?
    .expect("tiny submit route should run reconciliation sweep");
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("failed order should remain present");

    assert_eq!(summary.existing, 0);
    assert_eq!(summary.simulated, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(order.attempt, 1);
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn mark_failed_buy_simulation_order(
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now,
    )?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(1))?;
    let error = "swap transaction dry-run simulation error source=metis_no_shared_accounts shared_accounts_disabled=true: {\"error\":\"Error processing Instruction 5: An account required by the instruction is missing\",\"errorCode\":\"TRANSACTION_ERROR\"}";
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(2),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED,
        Some(error),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(3),
        copybot_storage_core::EXECUTION_ERROR_SIMULATION_FAILED,
        error,
    )?;
    Ok(reserve.order.order_id)
}

async fn serve_buy_retry_build_submit_and_confirm(
    first_signer: &[u8; 32],
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let serialized_transaction = serialized_buy_retry_transaction(*first_signer);
    let server = tokio::spawn(async move {
        let quote = read_buy_retry_http_request(&listener).await;
        assert!(quote.body.starts_with("GET /quote?"));
        write_buy_retry_http_response(quote.socket, fresh_buy_retry_quote_json()).await;

        let instructions = read_buy_retry_http_request(&listener).await;
        assert!(instructions.body.starts_with("POST /swap-instructions "));
        write_buy_retry_http_response(instructions.socket, valid_buy_retry_instructions_json())
            .await;

        let swap = read_buy_retry_http_request(&listener).await;
        assert!(swap.body.starts_with("POST /swap "));
        write_buy_retry_http_response(
            swap.socket,
            &format!(r#"{{"swapTransaction":"{serialized_transaction}","simulationError":null}}"#),
        )
        .await;

        let submit = read_buy_retry_http_request(&listener).await;
        assert!(submit.body.contains("\"method\":\"sendTransaction\""));
        write_buy_retry_http_response(
            submit.socket,
            &format!(r#"{{"jsonrpc":"2.0","id":"execution-submit","result":"{tx}"}}"#),
        )
        .await;

        let confirmation = read_buy_retry_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx));
        write_buy_retry_http_response(
            confirmation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":82,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    Ok((base_url, server))
}

struct BuyRetryHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_buy_retry_http_request(listener: &tokio::net::TcpListener) -> BuyRetryHttpRequest {
    let (mut socket, _) = listener.accept().await.expect("buy retry HTTP request");
    let mut buffer = [0_u8; 16384];
    let read = socket.read(&mut buffer).await.expect("read request");
    BuyRetryHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_buy_retry_http_response(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn record_buy_retry_quote(
    store: &SqliteStore,
    signal: &CopySignalRow,
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
            decision_delay_ms: Some(5),
            quote_latency_ms: Some(10),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(original_buy_retry_quote_json().to_string()),
            quote_price_sol: Some(0.001),
            shadow_price_sol: Some(0.00099),
            slippage_bps: Some(100.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Amm"}}]"#.to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some(r#"{"recommended":22000}"#.to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn original_buy_retry_quote_json() -> &'static str {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}],"meta":{"outDecimals":3}}"#
}

fn fresh_buy_retry_quote_json() -> &'static str {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#
}

fn valid_buy_retry_instructions_json() -> &'static str {
    r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#
}

struct BuyRetryKeypair {
    public_key: [u8; 32],
    pubkey: String,
    bytes: Vec<u8>,
}

fn buy_retry_keypair(seed: u8) -> BuyRetryKeypair {
    let secret = [seed; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key().to_bytes();
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&secret);
    bytes.extend_from_slice(&public_key);
    BuyRetryKeypair {
        public_key,
        pubkey: bs58::encode(public_key).into_string(),
        bytes,
    }
}

fn write_buy_retry_keypair_file(name: &str, bytes: &[u8]) -> Result<PathBuf> {
    let path = unique_buy_retry_test_path(name).with_extension("json");
    std::fs::write(&path, serde_json::to_string(bytes)?)?;
    Ok(path)
}

fn serialized_buy_retry_transaction(first_account_key: [u8; 32]) -> String {
    let mut transaction = vec![1_u8];
    transaction.extend_from_slice(&[0_u8; 64]);
    transaction.extend_from_slice(&[1_u8, 0, 0]);
    transaction.push(1);
    transaction.extend_from_slice(&first_account_key);
    transaction.extend_from_slice(&[9_u8; 32]);
    transaction.push(0);
    BASE64_STANDARD.encode(transaction)
}

fn buy_retry_config(base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.quote_canary_base_url = base_url.to_string();
    config.submit_adapter_http_url = base_url.to_string();
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config.quote_canary_buy_slippage_bps = 500;
    config.max_submit_attempts = 2;
    config.max_confirm_seconds = 2;
    config.canary_batch_limit = 2;
    config
}

fn buy_retry_signal(name: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-buy-retry:{name}:leader-wallet:buy:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_buy_retry_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-buy-retry-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
