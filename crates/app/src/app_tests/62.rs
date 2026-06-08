use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_submit_confirm_path_gate_disabled_stops_before_confirmation() -> Result<()> {
    let db_path = unique_tiny_path_test_path("gate-disabled");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = tiny_path_request(&store, "gate-disabled", "buy", 0.01, now)?;
    let adapter = TinyPathSubmitReadyAdapter;
    let envelope = tiny_path_envelope(&request)?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: false,
        submit_timeout_ms: 1_000,
    };
    let transport =
        crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(String::new());

    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path(
        &store,
        &adapter,
        &request,
        &envelope,
        &gate,
        &transport,
        &reqwest::Client::new(),
        "",
        now,
        1_000,
    )
    .await?;

    assert_eq!(outcome.submit_disabled, 1);
    assert_eq!(outcome.submit_ready_rejected, 1);
    assert_eq!(outcome.confirmation_confirmed, 0);
    assert_eq!(outcome.confirmation_pending, 0);
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_confirm_path_confirmed_buy_opens_position() -> Result<()> {
    let db_path = unique_tiny_path_test_path("confirmed-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = tiny_path_request(&store, "confirmed-buy", "buy", 0.01, now)?;
    let adapter = TinyPathSubmitReadyAdapter;
    let envelope = tiny_path_envelope(&request)?;
    let (rpc_url, server) = serve_submit_then_confirm(
        "tx-confirmed-buy",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":10,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
    )
    .await?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: true,
        submit_timeout_ms: 1_000,
    };
    let transport =
        crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url.clone());

    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path(
        &store,
        &adapter,
        &request,
        &envelope,
        &gate,
        &transport,
        &reqwest::Client::new(),
        &rpc_url,
        now + chrono::Duration::seconds(2),
        1_000,
    )
    .await?;
    server.await?;
    let position = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("confirmed buy should open owned position");

    assert_eq!(outcome.submitted, 1);
    assert_eq!(outcome.confirmation_confirmed, 1);
    assert_eq!(outcome.buy_opened, 1);
    assert_eq!(
        position.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3))
    );
    assert!((position.qty - 10.0).abs() < 1e-9);
    assert!((position.cost_sol - 0.1).abs() < 1e-9);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_confirm_path_pending_buy_does_not_open_fill() -> Result<()> {
    let db_path = unique_tiny_path_test_path("pending-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = tiny_path_request(&store, "pending-buy", "buy", 0.01, now)?;
    let adapter = TinyPathSubmitReadyAdapter;
    let envelope = tiny_path_envelope(&request)?;
    let (rpc_url, server) = serve_submit_then_confirm(
        "tx-pending-buy",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[null]}}"#,
    )
    .await?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: true,
        submit_timeout_ms: 1_000,
    };
    let transport =
        crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url.clone());

    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path(
        &store,
        &adapter,
        &request,
        &envelope,
        &gate,
        &transport,
        &reqwest::Client::new(),
        &rpc_url,
        now + chrono::Duration::seconds(2),
        1_000,
    )
    .await?;
    server.await?;

    assert_eq!(outcome.submitted, 1);
    assert_eq!(outcome.confirmation_confirmed, 0);
    assert_eq!(outcome.confirmation_pending, 1);
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_confirm_path_confirmed_sell_closes_owned_position() -> Result<()> {
    let db_path = unique_tiny_path_test_path("confirmed-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let request = tiny_path_request(
        &store,
        "confirmed-sell",
        "sell",
        0.1,
        now + chrono::Duration::minutes(1),
    )?;
    let adapter = TinyPathSubmitReadyAdapter;
    let envelope = tiny_path_envelope(&request)?;
    let (rpc_url, server) = serve_submit_then_confirm(
        "tx-confirmed-sell",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":20,"confirmations":null,"err":null,"confirmationStatus":"confirmed"}]}}"#,
    )
    .await?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: true,
        submit_timeout_ms: 1_000,
    };
    let transport =
        crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url.clone());

    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path(
        &store,
        &adapter,
        &request,
        &envelope,
        &gate,
        &transport,
        &reqwest::Client::new(),
        &rpc_url,
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(2),
        1_000,
    )
    .await?;
    server.await?;

    assert_eq!(outcome.confirmation_confirmed, 1);
    assert_eq!(outcome.sell_closed, 1);
    assert!((outcome.tx_signature.as_deref() == Some("tx-confirmed-sell")));
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[derive(Debug, Clone)]
struct TinyPathSubmitReadyAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for TinyPathSubmitReadyAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        _plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async {
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: None,
            })
        })
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        Ok(
            crate::execution_submit_adapter::ExecutionSubmitPlan::submit_disabled(
                request,
                "tiny_path_test_default_disabled".to_string(),
            ),
        )
    }

    fn plan_submit_with_envelope(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        envelope: &crate::execution_signing_envelope::ExecutionSigningEnvelope,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        let intent = crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
            request,
            envelope,
            "rpc_send_transaction".to_string(),
        )?;
        Ok(crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitReady(intent))
    }
}

async fn serve_submit_then_confirm(
    tx_signature: &str,
    confirmation_body: &str,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let tx_signature = tx_signature.to_string();
    let confirmation_body = confirmation_body.to_string();
    let server = tokio::spawn(async move {
        let submit = read_http_request(&listener).await;
        assert!(submit.body.contains("\"method\":\"sendTransaction\""));
        write_http_response(
            submit.socket,
            &format!(r#"{{"jsonrpc":"2.0","id":"execution-submit","result":"{tx_signature}"}}"#),
        )
        .await;

        let confirmation = read_http_request(&listener).await;
        assert!(confirmation
            .body
            .contains("\"method\":\"getSignatureStatuses\""));
        assert!(confirmation.body.contains(&tx_signature));
        write_http_response(confirmation.socket, &confirmation_body).await;
    });
    Ok((rpc_url, server))
}

struct TinyPathHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_http_request(listener: &tokio::net::TcpListener) -> TinyPathHttpRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.expect("read request");
    TinyPathHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_http_response(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn tiny_path_request(
    store: &SqliteStore,
    name: &str,
    side: &str,
    quote_price_sol: f64,
    now: chrono::DateTime<Utc>,
) -> Result<crate::execution_submit_adapter::ExecutionSubmitRequest> {
    let signal = tiny_path_signal(name, side, now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now)?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    Ok(crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: reserve.order.order_id,
        signal_id: signal.signal_id,
        client_order_id: reserve.order.client_order_id,
        attempt: reserve.order.attempt,
        route: "metis-canary".to_string(),
        wallet_id: signal.wallet_id,
        token: signal.token,
        side: signal.side,
        buy_size_sol: 0.1,
        slippage_tolerance_bps: 500,
        wallet_pubkey: "DryRunWallet11111111111111111111111111111111".to_string(),
        metadata: tiny_path_metadata(quote_price_sol),
    })
}

fn tiny_path_metadata(
    quote_price_sol: f64,
) -> crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
    crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
        quote_out_amount_raw: Some("10000".to_string()),
        quote_response_json: Some(
            r#"{"quote":{"outAmountUi":10.0,"meta":{"outDecimals":3}}}"#.to_string(),
        ),
        quote_price_sol: Some(quote_price_sol),
        ..crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default()
    }
}

fn tiny_path_envelope(
    request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
) -> Result<crate::execution_signing_envelope::ExecutionSigningEnvelope> {
    let plan = crate::execution_submit_adapter::NoSubmitExecutionAdapter
        .build_transaction_plan(request)?;
    crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
        request,
        &plan,
        crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
            signed_transaction_base64: "AQIDBA==".to_string(),
            tx_signature_hint: Some("tx-hint-from-envelope".to_string()),
        },
    )
}

fn tiny_path_signal(
    name: &str,
    side: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-tiny-path:{name}:{side}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_tiny_path_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-tiny-path-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
