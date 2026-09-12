use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Stamp {
    pub utc: DateTime<Utc>,
    pub mono_ns: u128,
}
impl Stamp {
    pub fn now(origin: Instant) -> Self {
        Self {
            utc: Utc::now(),
            mono_ns: origin.elapsed().as_nanos(),
        }
    }
    pub fn json(self) -> Value {
        let ms = self.utc.timestamp_millis();
        json!({"utc":self.utc.to_rfc3339_opts(chrono::SecondsFormat::Nanos,false),
            "unix_ns":self.utc.timestamp_nanos_opt().unwrap().to_string(),
            "monotonic_ns":self.mono_ns.to_string(),"floor_unix_ms":ms.to_string(),
            "ms_envelope_ns":[(ms*1_000_000).to_string(),((ms+1)*1_000_000).to_string()]})
    }
}

pub(crate) struct Reply {
    pub status: &'static str,
    pub body: String,
    pub advertised_extra: usize,
}
pub(crate) struct ServerResult {
    pub get: Stamp,
    pub body_release: Stamp,
    pub body_sent: Stamp,
    pub request: String,
}

pub(crate) async fn serve(
    listener: TcpListener,
    reply: Reply,
    origin: Instant,
    got: oneshot::Sender<Stamp>,
    release: oneshot::Receiver<()>,
) -> Result<ServerResult> {
    let (mut socket, peer) = listener.accept().await?;
    ensure!(peer.ip().is_loopback());
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await?);
        ensure!(bytes.len() < 16_384);
    }
    let request = String::from_utf8(bytes)?;
    ensure!(
        request.starts_with("GET /quote?"),
        "unexpected non-quote request"
    );
    let get = Stamp::now(origin);
    let headers = format!("HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",reply.status,reply.body.len()+reply.advertised_extra);
    socket.write_all(headers.as_bytes()).await?;
    got.send(get)
        .map_err(|_| anyhow::anyhow!("controller disappeared"))?;
    // No body byte can be written before the explicit release channel resolves.
    release.await?;
    let body_release = Stamp::now(origin);
    socket.write_all(reply.body.as_bytes()).await?;
    socket.shutdown().await?;
    let body_sent = Stamp::now(origin);
    // Listener stays open while checking that the producer made no optional request.
    ensure!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    Ok(ServerResult {
        get,
        body_release,
        body_sent,
        request,
    })
}

pub(crate) async fn later_ms(stamp: Stamp, origin: Instant) -> Result<Stamp> {
    let deadline = Instant::now() + Duration::from_millis(100);
    loop {
        let now = Stamp::now(origin);
        if now.utc.timestamp_millis() > stamp.utc.timestamp_millis() {
            return Ok(now);
        }
        ensure!(
            Instant::now() < deadline,
            "wall-clock millisecond failed to advance"
        );
        tokio::task::yield_now().await;
    }
}
