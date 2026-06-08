use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn tiny_submit_gate_disabled_records_not_sent_without_http() -> Result<()> {
    let db_path = unique_tiny_submit_test_path("gate-disabled");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = tiny_submit_request(&store, "gate-disabled", now)?;
    let adapter = TinySubmitReadyAdapter {
        tx_signature_hint: Some("tx-hint-gate-disabled"),
    };
    let envelope = tiny_submit_envelope(&request)?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: false,
        submit_timeout_ms: 1_000,
    };
    let transport =
        crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(String::new());

    let outcome = crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
        &store, &adapter, &request, &envelope, &gate, &transport, now,
    )
    .await?;
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .expect("order should exist");

    assert_eq!(outcome.submit_disabled, 1);
    assert_eq!(outcome.submit_ready_rejected, 1);
    assert_eq!(outcome.skipped_reason, Some("tiny_submit_gate_disabled"));
    assert!(outcome
        .reason
        .as_deref()
        .unwrap_or_default()
        .contains("tiny_submit_gate_disabled"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_gate_allowed_records_rpc_signature() -> Result<()> {
    let db_path = unique_tiny_submit_test_path("rpc-ok");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_tiny_submit_request(&listener).await;
        assert!(request.body.contains("\"method\":\"sendTransaction\""));
        assert!(request.body.contains("\"AQIDBA==\""));
        write_tiny_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","result":"tx-tiny-submit-ok"}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = tiny_submit_request(&store, "rpc-ok", now)?;
    let adapter = TinySubmitReadyAdapter {
        tx_signature_hint: Some("tx-hint-rpc-ok"),
    };
    let envelope = tiny_submit_envelope(&request)?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: true,
        submit_timeout_ms: 1_000,
    };
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
        &store, &adapter, &request, &envelope, &gate, &transport, now,
    )
    .await?;
    server.await?;
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .expect("order should exist");

    assert_eq!(outcome.submitted, 1);
    assert_eq!(outcome.tx_signature.as_deref(), Some("tx-tiny-submit-ok"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(order.tx_signature.as_deref(), Some("tx-tiny-submit-ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_gate_allowed_records_rpc_error_as_not_sent() -> Result<()> {
    let db_path = unique_tiny_submit_test_path("rpc-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_tiny_submit_request(&listener).await;
        assert!(request.body.contains("\"sendTransaction\""));
        write_tiny_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","error":{"code":-32002,"message":"Transaction simulation failed"}}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = tiny_submit_request(&store, "rpc-error", now)?;
    let adapter = TinySubmitReadyAdapter {
        tx_signature_hint: Some("tx-hint-rpc-error"),
    };
    let envelope = tiny_submit_envelope(&request)?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: true,
        submit_timeout_ms: 1_000,
    };
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
        &store, &adapter, &request, &envelope, &gate, &transport, now,
    )
    .await?;
    server.await?;
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .expect("order should exist");

    assert_eq!(outcome.submit_disabled, 1);
    assert!(outcome
        .reason
        .as_deref()
        .unwrap_or_default()
        .contains("rpc_send_transaction_error"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_gate_timeout_with_hint_records_submitted_unknown() -> Result<()> {
    let db_path = unique_tiny_submit_test_path("rpc-timeout");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_tiny_submit_request(&listener).await;
        assert!(request.body.contains("\"sendTransaction\""));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        write_tiny_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","result":"too-late"}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = tiny_submit_request(&store, "rpc-timeout", now)?;
    let adapter = TinySubmitReadyAdapter {
        tx_signature_hint: Some("tx-hint-timeout"),
    };
    let envelope = tiny_submit_envelope(&request)?;
    let gate = crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
        allow_rpc_submit: true,
        submit_timeout_ms: 20,
    };
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
        &store, &adapter, &request, &envelope, &gate, &transport, now,
    )
    .await?;
    server.await?;
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .expect("order should exist");

    assert_eq!(outcome.submitted, 1);
    assert_eq!(outcome.tx_signature.as_deref(), Some("tx-hint-timeout"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(order.tx_signature.as_deref(), Some("tx-hint-timeout"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[derive(Debug, Clone)]
struct TinySubmitReadyAdapter {
    tx_signature_hint: Option<&'static str>,
}

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for TinySubmitReadyAdapter {
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
                "tiny_submit_test_default_disabled".to_string(),
            ),
        )
    }

    fn plan_submit_with_envelope(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        envelope: &crate::execution_signing_envelope::ExecutionSigningEnvelope,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        let mut intent =
            crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
                request,
                envelope,
                "rpc_send_transaction".to_string(),
            )?;
        intent.tx_signature_hint = self.tx_signature_hint.map(ToString::to_string);
        Ok(crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitReady(intent))
    }
}

struct TinySubmitHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_tiny_submit_request(listener: &tokio::net::TcpListener) -> TinySubmitHttpRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.expect("read request");
    TinySubmitHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_tiny_submit_status(mut socket: tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn tiny_submit_request(
    store: &SqliteStore,
    name: &str,
    now: chrono::DateTime<Utc>,
) -> Result<crate::execution_submit_adapter::ExecutionSubmitRequest> {
    let signal = tiny_submit_signal(name, now);
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
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: "DryRunWallet11111111111111111111111111111111".to_string(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default(),
    })
}

fn tiny_submit_envelope(
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

fn tiny_submit_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-tiny-submit:{signal_id}:TokenMint"),
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

fn unique_tiny_submit_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-tiny-submit-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
