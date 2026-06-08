use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn rpc_confirmation_confirms_finalized_signature() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_confirmation_request(&listener).await;
        assert!(request.body.starts_with("POST / "));
        assert!(request.body.contains("\"method\":\"getSignatureStatuses\""));
        assert!(request.body.contains("\"tx-finalized\""));
        assert!(request.body.contains("\"searchTransactionHistory\":true"));
        write_rpc_confirmation_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"context":{"slot":776},"value":[{"slot":777,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = rpc_confirmation_request("order-finalized", "tx-finalized", now);

    let outcome = crate::execution_submit_adapter::fetch_rpc_signature_confirmation(
        &reqwest::Client::new(),
        &rpc_url,
        &request,
        now + chrono::Duration::seconds(3),
        1_000,
    )
    .await?;
    server.await?;

    let crate::execution_submit_adapter::ExecutionConfirmationTrackerOutcome::Confirmed(proof) =
        outcome
    else {
        panic!("expected confirmed outcome");
    };
    assert_eq!(proof.tx_signature, "tx-finalized");
    assert_eq!(proof.confirmation_status, "finalized");
    assert_eq!(proof.slot, Some(777));
    assert_eq!(proof.confirmed_at, now + chrono::Duration::seconds(3));
    Ok(())
}

#[tokio::test]
async fn rpc_confirmation_reports_pending_when_status_missing() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_confirmation_request(&listener).await;
        assert!(request.body.contains("\"tx-missing\""));
        write_rpc_confirmation_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"context":{"slot":900},"value":[null]}}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = rpc_confirmation_request("order-missing", "tx-missing", now);

    let outcome = crate::execution_submit_adapter::fetch_rpc_signature_confirmation(
        &reqwest::Client::new(),
        &rpc_url,
        &request,
        now,
        1_000,
    )
    .await?;
    server.await?;

    assert_eq!(
        outcome,
        crate::execution_submit_adapter::ExecutionConfirmationTrackerOutcome::Pending {
            tx_signature: "tx-missing".to_string(),
            reason: "rpc_signature_status_missing".to_string(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn rpc_confirmation_keeps_processed_signature_pending() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_confirmation_request(&listener).await;
        assert!(request.body.contains("\"tx-processed\""));
        write_rpc_confirmation_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":1000,"confirmations":1,"err":null,"confirmationStatus":"processed"}]}}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = rpc_confirmation_request("order-processed", "tx-processed", now);

    let outcome = crate::execution_submit_adapter::fetch_rpc_signature_confirmation(
        &reqwest::Client::new(),
        &rpc_url,
        &request,
        now,
        1_000,
    )
    .await?;
    server.await?;

    assert_eq!(
        outcome,
        crate::execution_submit_adapter::ExecutionConfirmationTrackerOutcome::Pending {
            tx_signature: "tx-processed".to_string(),
            reason: "rpc_confirmation_pending_processed".to_string(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn rpc_confirmation_rejects_transaction_error() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_rpc_confirmation_request(&listener).await;
        assert!(request.body.contains("\"tx-error\""));
        write_rpc_confirmation_status(
            request.socket,
            200,
            r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":1001,"confirmations":null,"err":{"InstructionError":[0,"Custom"]},"confirmationStatus":"finalized"}]}}"#,
        )
        .await;
    });
    let now = Utc::now();
    let request = rpc_confirmation_request("order-error", "tx-error", now);

    let error = crate::execution_submit_adapter::fetch_rpc_signature_confirmation(
        &reqwest::Client::new(),
        &rpc_url,
        &request,
        now,
        1_000,
    )
    .await
    .expect_err("transaction error must not be confirmed");
    server.await?;

    assert!(format!("{error:#}").contains("confirmation RPC transaction_error"));
    Ok(())
}

struct RpcConfirmationRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_rpc_confirmation_request(
    listener: &tokio::net::TcpListener,
) -> RpcConfirmationRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.expect("read request");
    RpcConfirmationRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_rpc_confirmation_status(mut socket: tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn rpc_confirmation_request(
    order_id: &str,
    tx_signature: &str,
    requested_at: chrono::DateTime<Utc>,
) -> crate::execution_submit_adapter::ExecutionConfirmationRequest {
    crate::execution_submit_adapter::ExecutionConfirmationRequest {
        order_id: order_id.to_string(),
        tx_signature: tx_signature.to_string(),
        requested_at,
    }
}
