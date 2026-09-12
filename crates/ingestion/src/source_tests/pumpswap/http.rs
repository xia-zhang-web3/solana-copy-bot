use crate::source::{helius_fetch, HeliusWsSource, LogsNotification, RawSwapObservation};
use anyhow::Result;
use copybot_config::IngestionConfig;
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

pub(super) fn fetch(
    f: &Value,
    config: &IngestionConfig,
) -> Result<(Option<RawSwapObservation>, Value)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let response = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":f["result"]}))?;
    let server = std::thread::spawn(move || -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, peer)) => {
                    assert!(peer.ip().is_loopback());
                    break stream;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2))
                }
                Err(error) => return Err(error.into()),
            }
        };
        // The listener polls nonblocking; accepted sockets must block for request bytes.
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        let mut wire = Vec::new();
        let (header, count) = loop {
            let mut buf = [0; 4096];
            let n = stream.read(&mut buf)?;
            anyhow::ensure!(n > 0, "empty fixture request");
            wire.extend_from_slice(&buf[..n]);
            if let Some(end) = wire.windows(4).position(|b| b == b"\r\n\r\n") {
                let text = String::from_utf8_lossy(&wire[..end]);
                let size = text
                    .lines()
                    .find_map(|s| {
                        s.to_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap();
                break (end + 4, size);
            }
        };
        while wire.len() < header + count {
            let mut buf = [0; 4096];
            let n = stream.read(&mut buf)?;
            anyhow::ensure!(n > 0);
            wire.extend_from_slice(&buf[..n]);
        }
        let request: Value = serde_json::from_slice(&wire[header..header + count])?;
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len())?;
        stream.write_all(&response)?;
        Ok(request)
    });
    let mut config = config.clone();
    config.helius_http_url = format!("http://{address}");
    config.helius_http_urls = vec![config.helius_http_url.clone()];
    config.tx_fetch_retries = 0;
    config.tx_request_timeout_ms = 3000;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let output = runtime.block_on(async {
        let source = HeliusWsSource::new(&config)?;
        helius_fetch::fetch_swap_with_retries(
            &source.runtime_config,
            LogsNotification {
                signature: f["signature"].as_str().unwrap().to_string(),
                slot: f["result"]["slot"].as_u64().unwrap(),
                arrival_seq: 1,
                logs: Vec::new(),
                is_failed: false,
                enqueued_at: Instant::now(),
            },
        )
        .await
    });
    let request = server.join().expect("fixture HTTP thread panicked")?;
    assert_eq!(request["method"], "getTransaction");
    assert_eq!(request["params"][0], f["signature"]);
    assert_eq!(request["params"][1]["encoding"], "jsonParsed");
    Ok((output?.map(|fetched| fetched.raw), request))
}
