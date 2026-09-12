use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) fn valid() -> Value {
    json!({"jsonrpc":"2.0","id":"execution-swap-transaction-simulate",
        "result":{"context":{"slot":0},"value":{"err":null}}})
}

pub(super) struct Step {
    pub method: &'static str,
    pub body: String,
    pub status: u16,
    pub delay: Duration,
}
impl Step {
    pub fn json(method: &'static str, body: Value) -> Self {
        Self {
            method,
            body: body.to_string(),
            status: 200,
            delay: Duration::ZERO,
        }
    }
}

pub(super) struct Server {
    pub url: String,
    task: tokio::task::JoinHandle<Result<Vec<Value>>>,
}
impl Server {
    pub async fn new(steps: Vec<Step>) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for step in steps {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
                let (path, request) =
                    tokio::time::timeout(Duration::from_secs(3), read(&mut socket)).await??;
                if step.method.starts_with('/') {
                    ensure!(path == step.method, "unexpected path {path}");
                } else {
                    ensure!(request["method"] == step.method, "unexpected RPC {request}");
                }
                if step.method == "simulateTransaction" {
                    ensure!(request["jsonrpc"] == "2.0");
                    ensure!(request["id"] == "execution-swap-transaction-simulate");
                    ensure!(
                        request["params"][1]
                            == json!({"encoding":"base64", "sigVerify":false,
                        "replaceRecentBlockhash":true,"commitment":"confirmed"})
                    );
                }
                requests.push(request);
                if !step.delay.is_zero() {
                    tokio::time::sleep(step.delay).await;
                }
                // Timeout scenarios close without a write to the already timed-out client.
                if step.body.is_empty() {
                    continue;
                }
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {} Test\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
                            step.status,
                            step.body.len(),
                            step.body
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
            Ok(requests)
        });
        Ok(Self { url, task })
    }
    pub async fn finish(self) -> Result<Vec<Value>> {
        self.task.await?
    }
}

pub(super) async fn read(socket: &mut tokio::net::TcpStream) -> Result<(String, Value)> {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 8192];
        let count = socket.read(&mut buf).await?;
        ensure!(
            count > 0 && bytes.len() + count <= 65536,
            "invalid HTTP request"
        );
        bytes.extend_from_slice(&buf[..count]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let size = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|n| n.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + size {
                let path = headers
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .into();
                let body = if size == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[end + 4..end + 4 + size])?
                };
                return Ok((path, body));
            }
        }
    }
}
