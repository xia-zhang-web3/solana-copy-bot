use super::b58_fixture::SOL;
use super::*;
use serde_json::{json, Value};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub(super) async fn serve(
    listener: TcpListener,
    tokens: &[&str],
    origin: Instant,
    ready: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) -> Result<Vec<Value>> {
    let mut barrier = Some((ready, release));
    let mut traces = Vec::new();
    for token in tokens {
        let (mut socket, peer) = listener.accept().await?;
        assert!(peer.ip().is_loopback());
        let accept_utc = Utc::now();
        let accept_us = origin.elapsed().as_micros();
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            bytes.push(socket.read_u8().await?);
            anyhow::ensure!(bytes.len() <= 16_384, "bounded GET headers");
        }
        let request_utc = Utc::now();
        let request_us = origin.elapsed().as_micros();
        let request = String::from_utf8(bytes)?;
        let first_line = request.lines().next().unwrap();
        let path = first_line.split_whitespace().nth(1).unwrap();
        let query: BTreeMap<_, _> = path
            .split_once('?')
            .unwrap()
            .1
            .split('&')
            .map(|part| part.split_once('=').unwrap())
            .collect();
        assert!(path.starts_with("/quote?"));
        assert_eq!(query["swapMode"], "ExactIn");
        let sell = *token == "TokenS";
        assert_eq!(query["inputMint"], if sell { *token } else { SOL });
        assert_eq!(query["outputMint"], if sell { SOL } else { *token });
        assert_eq!(query["amount"], if sell { "1000000" } else { "200000000" });
        if let Some((ready, release)) = barrier.take() {
            ready
                .send(())
                .map_err(|_| anyhow::anyhow!("controller dropped"))?;
            release.await?;
        }
        let before_response_utc = Utc::now();
        let before_response_us = origin.elapsed().as_micros();
        let response_body = json!({"inAmount":query["amount"],
            "outAmount":if sell { "200000000" } else { "1000000" },
            "priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]})
        .to_string();
        let response = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", response_body.len(), response_body);
        socket.write_all(response.as_bytes()).await?;
        socket.shutdown().await?;
        traces.push(json!({"token":token,"request_line":first_line,"response_body":response_body,
            "accept_utc":accept_utc,"accept_us":accept_us,"request_utc":request_utc,"request_us":request_us,
            "before_response_utc":before_response_utc,"before_response_us":before_response_us,
            "response_complete_utc":Utc::now(),"response_complete_us":origin.elapsed().as_micros()}));
    }
    Ok(traces)
}
