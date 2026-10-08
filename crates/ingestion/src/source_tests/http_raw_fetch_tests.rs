//! Raw fetch is bounded RPC evidence; the ordered consumer alone normalizes it.
use super::ConfirmedHttpRecovery;
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn serve(
    deadline: Option<u64>,
    body_bytes: Option<usize>,
    chunked: bool,
) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0; 4096];
        let (end, length) = loop {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let length = String::from_utf8_lossy(&bytes[..end])
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < end + length {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
        let request: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
        assert_eq!(request["method"], "getBlock");
        assert_eq!(request["params"][1]["commitment"], "confirmed");
        assert_eq!(request["params"][1]["rewards"], true);
        let mut body = format!(
            "{{\n\"jsonrpc\":\"2.0\",\"id\":{},\"result\": null\n}}",
            request["id"]
        )
        .into_bytes();
        if let Some(length) = body_bytes {
            let prefix = format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":null,\"padding\":\"",
                request["id"]
            );
            body = prefix.into_bytes();
            body.resize(length - 2, b'x');
            body.extend_from_slice(b"\"}");
            assert_eq!(body.len(), length);
        }
        let deadline = deadline
            .map(|value| format!("X-Copybot-Session-Deadline-Unix-Ms: {value}\r\n"))
            .unwrap_or_default();
        let size = if chunked {
            "Transfer-Encoding: chunked\r\n".into()
        } else {
            format!("Content-Length: {}\r\n", body.len())
        };
        let header = format!("HTTP/1.1 200 OK\r\n{size}Connection: close\r\n{deadline}\r\n");
        socket.write_all(header.as_bytes()).await.unwrap();
        if chunked {
            for chunk in body.chunks(701) {
                let _ = socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await;
                let _ = socket.write_all(chunk).await;
                let _ = socket.write_all(b"\r\n").await;
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
        } else {
            socket.write_all(&body).await.unwrap();
        }
        body
    });
    (url, task)
}

#[tokio::test]
async fn raw_getblock_keeps_exact_bytes_and_defers_unconfirmed_refusal_to_ordered_normalization() {
    let (url, server) = serve(None, None, false).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 1024, Duration::from_secs(1)).unwrap();
    let raw = client.raw_block(10).await.unwrap();
    assert_eq!(raw.response.bytes, server.await.unwrap());
    assert!(client
        .normalize_raw_block(raw)
        .unwrap_err()
        .to_string()
        .contains("unavailable_or_unconfirmed"));
}

#[tokio::test]
async fn completed_raw_response_waiting_for_apply_cannot_outlive_original_session_deadline() {
    let deadline = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64)
        + 250;
    let (url, server) = serve(Some(deadline), None, false).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 1024, Duration::from_secs(1)).unwrap();
    let raw = client.raw_block(10).await.unwrap();
    server.await.unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(now) + 10)).await;
    let error = client.normalize_raw_block(raw).unwrap_err();
    assert!(error.to_string().contains("session_deadline_exhausted"));
}

#[tokio::test]
async fn chunked_raw_response_capacity_stays_inside_exact_limit_and_overrun_does_not_retry() {
    // A non-power-of-two limit catches the previous geometric Vec capacity
    // overshoot even when the body itself was exactly inside its byte cap.
    let limit = 4197;
    let (url, server) = serve(None, Some(limit), true).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, limit, Duration::from_secs(1)).unwrap();
    let raw = client.raw_block(10).await.unwrap();
    assert_eq!(raw.response.bytes.len(), limit);
    assert!(raw.response.bytes.capacity() <= limit);
    assert_eq!(raw.response.bytes, server.await.unwrap());
    let (url, server) = serve(None, Some(limit + 1), true).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, limit, Duration::from_secs(1)).unwrap();
    let error = client.raw_block(10).await.err().unwrap();
    assert!(error.to_string().contains("response_limit"));
    assert!(!super::delivery_error::retryable(&error));
    server.await.unwrap();
}

#[test]
fn raw_capacity_allocation_failure_is_terminal() {
    let error = super::delivery::bounded_response_buffer(usize::MAX).unwrap_err();
    assert!(error.to_string().contains("response_allocation"));
    assert!(!super::delivery_error::retryable(&error));
}
