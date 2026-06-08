use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn rpc_submit_transport_posts_send_transaction_and_returns_signature() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_submit_request(&listener).await;
        assert!(request.body.starts_with("POST / "));
        assert!(request.body.contains("\"method\":\"sendTransaction\""));
        assert!(request.body.contains("\"AQIDBA==\""));
        assert!(request.body.contains("\"encoding\":\"base64\""));
        assert!(request.body.contains("\"skipPreflight\":false"));
        assert!(request
            .body
            .contains("\"preflightCommitment\":\"confirmed\""));
        assert!(request.body.contains("\"maxRetries\":0"));
        write_rpc_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","result":"tx-rpc-submit"}"#,
        )
        .await;
    });
    let attempt = rpc_submit_attempt(1_000, Some("tx-hint"));
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = transport.submit(&attempt).await?;
    server.await?;

    assert_eq!(
        outcome,
        crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::SubmittedUnknown {
            idempotency_key: attempt.idempotency_key.clone(),
            tx_signature: Some("tx-rpc-submit".to_string()),
        }
    );
    Ok(())
}

#[tokio::test]
async fn rpc_submit_transport_json_rpc_error_is_not_sent() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_submit_request(&listener).await;
        assert!(request.body.contains("\"sendTransaction\""));
        write_rpc_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","error":{"code":-32002,"message":"Transaction simulation failed"}}"#,
        )
        .await;
    });
    let attempt = rpc_submit_attempt(1_000, Some("tx-hint"));
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = transport.submit(&attempt).await?;
    server.await?;

    let crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::NotSent {
        idempotency_key,
        reason,
    } = outcome
    else {
        panic!("JSON-RPC error should not be marked submitted");
    };
    assert_eq!(idempotency_key, attempt.idempotency_key);
    assert!(reason.starts_with("rpc_send_transaction_error:"));
    assert!(reason.contains("Transaction simulation failed"));
    Ok(())
}

#[tokio::test]
async fn rpc_submit_transport_timeout_becomes_unknown_with_signature_hint() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_submit_request(&listener).await;
        assert!(request.body.contains("\"sendTransaction\""));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        write_rpc_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","result":"too-late"}"#,
        )
        .await;
    });
    let attempt = rpc_submit_attempt(20, Some("tx-hint-timeout"));
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = transport.submit(&attempt).await?;
    server.await?;

    assert_eq!(
        outcome,
        crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::SubmittedUnknown {
            idempotency_key: attempt.idempotency_key.clone(),
            tx_signature: Some("tx-hint-timeout".to_string()),
        }
    );
    Ok(())
}

#[tokio::test]
async fn rpc_submit_transport_timeout_without_hint_stays_unknown_without_signature() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_submit_request(&listener).await;
        assert!(request.body.contains("\"sendTransaction\""));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        write_rpc_submit_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-submit","result":"too-late"}"#,
        )
        .await;
    });
    let attempt = rpc_submit_attempt(20, None);
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(rpc_url);

    let outcome = transport.submit(&attempt).await?;
    server.await?;

    assert_eq!(
        outcome,
        crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::SubmittedUnknown {
            idempotency_key: attempt.idempotency_key.clone(),
            tx_signature: None,
        }
    );
    Ok(())
}

#[tokio::test]
async fn rpc_submit_transport_rejects_empty_rpc_url() {
    let attempt = rpc_submit_attempt(1_000, Some("tx-hint"));
    let transport =
        crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(String::new());

    let error = transport
        .submit(&attempt)
        .await
        .expect_err("empty RPC URL must fail before HTTP");

    assert!(error
        .to_string()
        .contains("submit RPC URL must be non-empty"));
}

struct RpcSubmitRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_rpc_submit_request(listener: &tokio::net::TcpListener) -> RpcSubmitRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.expect("read request");
    RpcSubmitRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_rpc_submit_status(mut socket: tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

fn rpc_submit_attempt(
    timeout_ms: u64,
    tx_signature_hint: Option<&str>,
) -> crate::execution_submit_adapter::ExecutionSubmitTransportAttempt {
    crate::execution_submit_adapter::ExecutionSubmitTransportAttempt {
        idempotency_key: "copybot:submit:client-order:attempt:1".to_string(),
        submit_route: "rpc_send_transaction".to_string(),
        signed_transaction_base64: "AQIDBA==".to_string(),
        tx_signature_hint: tx_signature_hint.map(ToString::to_string),
        timeout_ms,
        requested_at: Utc::now(),
    }
}
