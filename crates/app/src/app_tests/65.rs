use super::*;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_submit_sweep_retries_submitted_unknown_without_signature_after_timeout() -> Result<()>
{
    let db_path = unique_tiny_timeout_route_test_path("unknown-retry");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = tiny_timeout_signal("unknown-retry", now);
    store.insert_copy_signal(&signal)?;
    let order_id = mark_tiny_timeout_submitted_unknown(&store, &signal, now)?;
    let mut config = tiny_timeout_config("http://127.0.0.1:1");
    config.max_submit_attempts = 2;

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(6),
    )
    .await?
    .expect("tiny submit route should sweep submitted orders");
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.submit_timeout_retry, 1);
    assert_eq!(summary.submit_timeout_expire_unsafe, 0);
    assert_eq!(summary.simulated, 1);
    assert_eq!(
        summary.last_confirm_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_CONFIRM_DECISION_RETRY)
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
    );
    assert_eq!(order.attempt, 2);
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_sweep_replays_retry_ready_simulated_order() -> Result<()> {
    let db_path = unique_tiny_timeout_route_test_path("retry-replay");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_timeout_keypair(66);
    let keypair_path = write_tiny_timeout_keypair_file("retry-replay", &keypair.bytes)?;
    let now = Utc::now();
    let signal = tiny_timeout_signal("retry-replay", now);
    store.insert_copy_signal(&signal)?;
    let order_id = mark_tiny_timeout_submitted_unknown(&store, &signal, now)?;
    record_tiny_timeout_build_metadata(&store, &order_id, &signal, now)?;
    let mut config = tiny_timeout_config("http://127.0.0.1:1");
    config.max_submit_attempts = 2;

    crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(6),
    )
    .await?
    .expect("first sweep should mark retry-ready simulated order");
    let retry_ready = store
        .load_execution_canary_order(&order_id)?
        .expect("retry-ready order should exist");
    assert_eq!(retry_ready.attempt, 2);
    assert_eq!(
        retry_ready.simulation_error.as_deref(),
        Some("retry_after_unknown_submit_timeout")
    );

    let (base_url, server) =
        serve_tiny_retry_build_submit_and_confirm(&keypair.public_key, "tx-retry-replay").await?;
    config.quote_canary_base_url = base_url.clone();
    config.submit_adapter_http_url = base_url;
    config.canary_wallet_pubkey = keypair.pubkey.clone();
    config.execution_signer_pubkey = keypair.pubkey.clone();
    config.execution_signer_keypair_path = keypair_path.to_string_lossy().to_string();
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(7),
    )
    .await?
    .expect("second sweep should replay retry-ready order");
    server.await?;
    let confirmed = store
        .load_execution_canary_order(&order_id)?
        .expect("confirmed order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(
        confirmed.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(confirmed.attempt, 2);
    assert_eq!(confirmed.tx_signature.as_deref(), Some("tx-retry-replay"));
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_sweep_replays_rpc_not_sent_retry_ready_order() -> Result<()> {
    let db_path = unique_tiny_timeout_route_test_path("rpc-not-sent-retry");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let keypair = tiny_timeout_keypair(67);
    let keypair_path = write_tiny_timeout_keypair_file("rpc-not-sent-retry", &keypair.bytes)?;
    let now = Utc::now();
    let signal = tiny_timeout_signal("rpc-not-sent-retry", now);
    store.insert_copy_signal(&signal)?;
    let order_id = mark_tiny_timeout_simulated(&store, &signal, now)?;
    record_tiny_timeout_build_metadata(&store, &order_id, &signal, now)?;
    store.mark_execution_canary_retry_after_submit_not_sent(
        &order_id,
        now + chrono::Duration::seconds(3),
        "retry_after_rpc_submit_not_sent:rpc_send_transaction_error",
    )?;
    let (base_url, server) =
        serve_tiny_retry_build_submit_and_confirm(&keypair.public_key, "tx-rpc-retry").await?;
    let mut config = tiny_timeout_config(&base_url);
    config.max_submit_attempts = 2;
    config.quote_canary_base_url = base_url.clone();
    config.submit_adapter_http_url = base_url;
    config.canary_wallet_pubkey = keypair.pubkey.clone();
    config.execution_signer_pubkey = keypair.pubkey.clone();
    config.execution_signer_keypair_path = keypair_path.to_string_lossy().to_string();
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(4),
    )
    .await?
    .expect("sweep should replay rpc-not-sent retry-ready order");
    server.await?;
    let confirmed = store
        .load_execution_canary_order(&order_id)?
        .expect("confirmed order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(
        confirmed.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(confirmed.attempt, 2);
    assert_eq!(confirmed.tx_signature.as_deref(), Some("tx-rpc-retry"));

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_sweep_expires_unknown_without_signature_when_retry_budget_is_exhausted(
) -> Result<()> {
    let db_path = unique_tiny_timeout_route_test_path("retry-budget");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = tiny_timeout_signal("retry-budget", now);
    store.insert_copy_signal(&signal)?;
    let order_id = mark_tiny_timeout_submitted_unknown(&store, &signal, now)?;
    let mut config = tiny_timeout_config("http://127.0.0.1:1");
    config.max_submit_attempts = 1;

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(6),
    )
    .await?
    .expect("tiny submit route should sweep submitted orders");
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.submit_timeout_retry, 0);
    assert_eq!(summary.expired, 1);
    assert_eq!(
        summary.skipped_reason,
        Some("submit_retry_budget_exhausted")
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
    );
    assert_eq!(order.attempt, 1);
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_sweep_expires_signed_pending_after_timeout_without_retry() -> Result<()> {
    let db_path = unique_tiny_timeout_route_test_path("signed-expire");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = tiny_timeout_signal("signed-expire", now);
    store.insert_copy_signal(&signal)?;
    let order_id = mark_tiny_timeout_submitted_signed(&store, &signal, now, "tx-signed-pending")?;
    record_tiny_timeout_build_metadata(&store, &order_id, &signal, now)?;
    let (rpc_url, server) = serve_tiny_timeout_pending_confirmation("tx-signed-pending").await?;
    let config = tiny_timeout_config(&rpc_url);

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(6),
    )
    .await?
    .expect("tiny submit route should sweep submitted orders");
    server.await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.submit_timeout_retry, 0);
    assert_eq!(summary.submit_timeout_expire_unsafe, 1);
    assert_eq!(summary.expired, 1);
    assert_eq!(
        summary.last_confirm_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE)
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
    );
    assert_eq!(order.tx_signature.as_deref(), Some("tx-signed-pending"));
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn mark_tiny_timeout_submitted_unknown(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let order_id = mark_tiny_timeout_simulated(store, signal, now)?;
    store.mark_execution_canary_submitted_unknown(
        &order_id,
        now + chrono::Duration::seconds(3),
        "submitted_without_signature",
    )?;
    Ok(order_id)
}

fn mark_tiny_timeout_submitted_signed(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    tx_signature: &str,
) -> Result<String> {
    let order_id = mark_tiny_timeout_simulated(store, signal, now)?;
    store.mark_execution_canary_submitted(
        &order_id,
        now + chrono::Duration::seconds(3),
        tx_signature,
    )?;
    Ok(order_id)
}

fn mark_tiny_timeout_simulated(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
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
    Ok(reserve.order.order_id)
}

fn record_tiny_timeout_build_metadata(
    store: &SqliteStore,
    order_id: &str,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    store.record_execution_canary_build_plan_metadata(
        &copybot_storage_core::ExecutionCanaryBuildPlanMetadata {
            order_id: order_id.to_string(),
            signal_id: signal.signal_id.clone(),
            client_order_id: format!("copybot:canary:{}", signal.signal_id),
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
    Ok(())
}

async fn serve_tiny_timeout_pending_confirmation(
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("confirmation request");
        let mut buffer = [0_u8; 8192];
        let read = socket.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("\"method\":\"getSignatureStatuses\""));
        assert!(request.contains(&tx));
        let body = r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[null]}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });
    Ok((rpc_url, server))
}

async fn serve_tiny_retry_build_submit_and_confirm(
    first_signer: &[u8; 32],
    tx_signature: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let tx = tx_signature.to_string();
    let serialized_transaction = serialized_tiny_timeout_transaction(*first_signer);
    let server = tokio::spawn(async move {
        let swap_instructions = read_tiny_timeout_http_request(&listener).await;
        assert!(swap_instructions
            .body
            .starts_with("POST /swap-instructions "));
        write_tiny_timeout_http_response(
            swap_instructions.socket,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#,
        )
        .await;

        let swap = read_tiny_timeout_http_request(&listener).await;
        assert!(swap.body.starts_with("POST /swap "));
        write_tiny_timeout_http_response(
            swap.socket,
            &format!(r#"{{"swapTransaction":"{serialized_transaction}","simulationError":null}}"#),
        )
        .await;

        let submit = read_tiny_timeout_http_request(&listener).await;
        assert!(submit.body.contains("\"method\":\"sendTransaction\""));
        write_tiny_timeout_http_response(
            submit.socket,
            &format!(r#"{{"jsonrpc":"2.0","id":"execution-submit","result":"{tx}"}}"#),
        )
        .await;

        let confirmation = read_tiny_timeout_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx));
        write_tiny_timeout_http_response(
            confirmation.socket,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":44,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    Ok((base_url, server))
}

struct TinyTimeoutHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_tiny_timeout_http_request(
    listener: &tokio::net::TcpListener,
) -> TinyTimeoutHttpRequest {
    let (mut socket, _) = listener.accept().await.expect("tiny timeout HTTP request");
    let mut buffer = [0_u8; 16384];
    let read = socket.read(&mut buffer).await.expect("read request");
    TinyTimeoutHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_tiny_timeout_http_response(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

struct TinyTimeoutKeypair {
    public_key: [u8; 32],
    pubkey: String,
    bytes: Vec<u8>,
}

fn tiny_timeout_keypair(seed: u8) -> TinyTimeoutKeypair {
    let secret = [seed; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key().to_bytes();
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&secret);
    bytes.extend_from_slice(&public_key);
    TinyTimeoutKeypair {
        public_key,
        pubkey: bs58::encode(public_key).into_string(),
        bytes,
    }
}

fn write_tiny_timeout_keypair_file(name: &str, bytes: &[u8]) -> Result<PathBuf> {
    let path = unique_tiny_timeout_route_test_path(name).with_extension("json");
    std::fs::write(&path, serde_json::to_string(bytes)?)?;
    Ok(path)
}

fn serialized_tiny_timeout_transaction(first_account_key: [u8; 32]) -> String {
    let mut transaction = vec![1_u8];
    transaction.extend_from_slice(&[0_u8; 64]);
    transaction.extend_from_slice(&[1_u8, 0, 0]);
    transaction.push(1);
    transaction.extend_from_slice(&first_account_key);
    transaction.extend_from_slice(&[9_u8; 32]);
    transaction.push(0);
    BASE64_STANDARD.encode(transaction)
}

fn tiny_timeout_config(rpc_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "DryRunWallet11111111111111111111111111111111".to_string();
    config.execution_signer_pubkey = config.canary_wallet_pubkey.clone();
    config.execution_signer_keypair_path = "/tmp/copybot-test-keypair.json".to_string();
    config.submit_adapter_http_url = rpc_url.to_string();
    config.swap_transaction_dry_run_enabled = true;
    config.max_confirm_seconds = 2;
    config.canary_batch_limit = 2;
    config
}

fn tiny_timeout_signal(name: &str, ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-tiny-timeout:{name}:TokenMint"),
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

fn unique_tiny_timeout_route_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-tiny-timeout-route-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
