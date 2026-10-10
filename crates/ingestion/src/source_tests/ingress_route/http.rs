//! Loopback JSON-RPC fixture; exposes only getBlocks/getBlock, never financial RPC.
use super::corpus::*;
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
pub async fn serve(listener: TcpListener, corpus: Arc<Corpus>, calls: Arc<Mutex<Vec<Value>>>) {
    while let Ok((socket, _)) = listener.accept().await {
        let corpus = corpus.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            if let Err(error) = request(socket, corpus, calls).await {
                panic!("local HTTP refused: {error:#}");
            }
        });
    }
}
async fn request(
    mut socket: tokio::net::TcpStream,
    corpus: Arc<Corpus>,
    calls: Arc<Mutex<Vec<Value>>>,
) -> Result<()> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    let (end, len) = loop {
        let n = socket.read(&mut chunk).await?;
        ensure!(n > 0, "request boundary");
        bytes.extend_from_slice(&chunk[..n]);
        ensure!(bytes.len() < 1 << 20, "request cap");
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = std::str::from_utf8(&bytes[..end])?;
            let len = header
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, len);
        }
    };
    while bytes.len() < end + len {
        let n = socket.read(&mut chunk).await?;
        ensure!(n > 0, "body boundary");
        bytes.extend_from_slice(&chunk[..n]);
    }
    let request: Value = serde_json::from_slice(&bytes[end..end + len])?;
    let result = match request["method"].as_str() {
        Some("getBlocks") => {
            let from = request["params"][0].as_u64().unwrap();
            let to = request["params"][1].as_u64().unwrap();
            ensure!(to - from <= 1024, "range cap");
            json!((FIRST..=corpus.latest())
                .filter(|s| *s >= from && *s <= to)
                .collect::<Vec<_>>())
        }
        Some("getBlock") => {
            ensure!(
                request["params"][1]["rewards"] == true
                    && request["params"][1]["maxSupportedTransactionVersion"] == 1,
                "full pair request"
            );
            let slot = request["params"][0].as_u64().unwrap();
            corpus.raw(slot)
        }
        _ => anyhow::bail!("financial/provider request forbidden"),
    };
    let body = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":result}))?;
    calls.lock().unwrap().push(json!({"method":request["method"],"params":request["params"],"response_bytes":body.len(),"observed_unix":chrono::Utc::now().timestamp_nanos_opt().unwrap() as f64/1e9}));
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(&body).await?;
    Ok(())
}
