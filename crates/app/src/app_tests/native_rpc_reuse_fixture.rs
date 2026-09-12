use super::native_rpc_fixture::{read_request, success};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Barrier};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Default, Debug)]
pub(super) struct ReuseTrace {
    pub connections: usize,
    pub requests: Vec<(usize, Value)>,
}

pub(super) struct ReuseFixture {
    pub endpoint: String,
    stop: watch::Sender<bool>,
    server: JoinHandle<Result<ReuseTrace>>,
}

impl ReuseFixture {
    pub async fn start() -> Result<Self> {
        Self::with_rent(false).await
    }
    pub async fn with_rent(rent: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/reused", listener.local_addr()?);
        let (stop, mut stopped) = watch::channel(false);
        let barrier = Arc::new(Barrier::new(if rent { 3 } else { 2 }));
        let trace = Arc::new(Mutex::new(ReuseTrace::default()));
        let server = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let id = {
                            let mut trace = trace.lock().unwrap();
                            trace.connections += 1;
                            trace.connections
                        };
                        let (stopped, trace, barrier) = (stopped.clone(), trace.clone(), barrier.clone());
                        handlers.spawn(serve(stream, id, stopped, trace, barrier));
                    }
                }
            }
            let mut failure = None;
            while let Some(result) = handlers.join_next().await {
                if let Err(error) = result.map_err(anyhow::Error::from).and_then(|r| r) {
                    failure.get_or_insert(error);
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            let result = std::mem::take(&mut *trace.lock().unwrap());
            Ok(result)
        });
        Ok(Self {
            endpoint,
            stop,
            server,
        })
    }

    pub async fn finish(self) -> Result<ReuseTrace> {
        self.stop.send(true)?;
        self.server.await.context("reuse server panicked")?
    }
}

async fn serve(
    mut stream: TcpStream,
    id: usize,
    mut stopped: watch::Receiver<bool>,
    trace: Arc<Mutex<ReuseTrace>>,
    barrier: Arc<Barrier>,
) -> Result<()> {
    loop {
        let request = tokio::select! {
            _ = stopped.changed() => return Ok(()),
            request = tokio::time::timeout(Duration::from_secs(3), read_request(&mut stream)) => request??,
        };
        let sequence = {
            let mut trace = trace.lock().unwrap();
            let count = trace
                .requests
                .iter()
                .filter(|(_, r)| r["method"] == request["method"])
                .count();
            trace.requests.push((id, request.clone()));
            count as u64
        };
        tokio::time::timeout(Duration::from_secs(3), barrier.wait()).await?;
        let mut response = if request["method"] == "getMinimumBalanceForRentExemption" {
            json!({"jsonrpc":"2.0", "id":request["id"], "result":2_039_280 + sequence})
        } else {
            success(&request)
        };
        if request["method"] == "getMinimumBalanceForRentExemption" {
            // Scalar RPC intentionally has no context slot.
        } else if request["method"] == "getFeeForMessage" {
            response["result"]["context"]["slot"] = json!(70 + sequence);
            response["result"]["value"] = json!(19_000 + sequence);
        } else {
            response["result"]["context"]["slot"] = json!(72 + sequence);
            response["result"]["value"][0]["lamports"] = json!(u64::MAX - sequence);
        }
        let body = serde_json::to_vec(&response)?;
        let header = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n", body.len());
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(&body).await?;
        stream.flush().await?;
    }
}
