use super::{open_risk_sell_task_fixture::RpcTask, source_write_off_fixture::*};
use anyhow::{Context, Result};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub(super) struct QuoteServer {
    pub url: String,
    pub completed: Arc<Mutex<bool>>,
    pub after_quote_mutation: Arc<Mutex<Option<std::collections::BTreeMap<String, Vec<String>>>>>,
    pub task: RpcTask,
}
impl QuoteServer {
    pub async fn start(f: &Fixture, replace_on_quote: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let path = f.path.clone();
        let now = f.now;
        let signal_id = f.signal.signal_id.clone();
        let completed = Arc::new(Mutex::new(false));
        let done = completed.clone();
        let after_quote_mutation = Arc::new(Mutex::new(None));
        let captured = after_quote_mutation.clone();
        let task = RpcTask::new(tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_secs(5), async move {
                for n in 0..2 {
                    let (mut socket, _) = listener.accept().await?;
                    let mut bytes = Vec::new();
                    loop {
                        let mut chunk = [0; 4096]; let count = socket.read(&mut chunk).await?;
                        anyhow::ensure!(count>0, "incomplete HTTP request"); bytes.extend_from_slice(&chunk[..count]);
                        if let Some(end) = bytes.windows(4).position(|w|w==b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                            let length = header.lines().find_map(|l|l.strip_prefix("content-length:")).map(str::trim).unwrap_or("0").parse::<usize>()?;
                            if bytes.len() >= end+4+length { break; }
                        }
                    }
                    let request = String::from_utf8(bytes)?;
                    let (status, body) = if n==0 {
                        anyhow::ensure!(request.contains("getTokenAccountsByOwner"), "expected owned wallet request");
                        (200, r#"{"result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"mint","tokenAmount":{"amount":"1","decimals":3}}}}}}]}}"#)
                    } else {
                        anyhow::ensure!(request.starts_with("GET /quote?") && request.contains("amount=1&"), "expected actual dust quote");
                        if replace_on_quote { replace(&copybot_storage_core::SqliteStore::open(&path)?, now, 1)?; }
                        let db = copybot_storage_core::SqliteStore::open(&path)?;
                        let a = db.load_execution_canary_order_by_signal(&signal_id)?.context("actual quoted A missing")?;
                        *captured.lock().unwrap() = Some(snapshot_without_order(&rusqlite::Connection::open(&path)?, &a.order_id)?);
                        (400, r#"{"error":"No routes found","errorCode":"NO_ROUTES_FOUND"}"#)
                    };
                    let response = format!("HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len());
                    socket.write_all(response.as_bytes()).await?;
                }
                *done.lock().unwrap() = true;
                Ok::<_, anyhow::Error>(())
            }).await.context("write-off quote fixture deadline")?
        }));
        Ok(Self {
            url,
            completed,
            after_quote_mutation,
            task,
        })
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.task.finish().await?;
        anyhow::ensure!(
            *self.completed.lock().unwrap(),
            "quote fixture did not complete both requests"
        );
        Ok(())
    }
}
