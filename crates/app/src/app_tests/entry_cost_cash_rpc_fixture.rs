use super::failed_expense_runtime_tests::failure;
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) async fn cash_server(
    receipt: Value,
    failed: bool,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<String>>>)> {
    let expected_signature = receipt["result"]["transaction"]["signatures"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let mut calls = Vec::new();
        for expected in ["getSignatureStatuses", "getTransaction"] {
            let mut stream =
                tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
                    .await??
                    .0;
            let request = read_request(&mut stream).await?;
            assert_eq!(request["method"], expected);
            let signature = if expected == "getSignatureStatuses" {
                &request["params"][0][0]
            } else {
                &request["params"][0]
            };
            assert_eq!(signature, &expected_signature);
            calls.push(expected.into());
            let response = if expected == "getSignatureStatuses" {
                json!({"result":{"value":[{"slot":42,"confirmationStatus":"confirmed","err":if failed { failure() } else { Value::Null }}]}})
            } else {
                receipt.clone()
            };
            let body = response.to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
        }
        Ok(calls)
    });
    Ok((url, task))
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Result<Value> {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0_u8; 4096];
        let n = stream.read(&mut buf).await?;
        ensure!(
            n > 0 && bytes.len() + n < 65536,
            "invalid mock HTTP request"
        );
        bytes.extend_from_slice(&buf[..n]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|v| v.strip_prefix("content-length:"))
                .unwrap()
                .trim()
                .parse()?;
            if bytes.len() >= end + 4 + length {
                return Ok(serde_json::from_slice(&bytes[end + 4..end + 4 + length])?);
            }
        }
    }
}
