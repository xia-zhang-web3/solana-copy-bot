//! Local TCP peer + captured consumer inputs. No private key or external transport.
use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_storage_core::ordered_sell_quote::*;
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
pub struct Request {
    pub query: HashMap<String, String>,
    pub reply: oneshot::Sender<Value>,
}
impl Request {
    pub fn answer(self) -> Result<()> {
        let body = json!({"inputMint":self.query["inputMint"],"outputMint":self.query["outputMint"],"inAmount":self.query["amount"],"outAmount":"1000000","swapMode":"ExactIn","routePlan":[]});
        self.reply
            .send(body)
            .map_err(|_| anyhow::anyhow!("server response receiver lost"))
    }
}
pub struct Server {
    pub url: String,
    rx: mpsc::Receiver<Request>,
    task: tokio::task::JoinHandle<Result<()>>,
    pub seen: std::sync::Arc<std::sync::Mutex<Vec<HashMap<String, String>>>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn new() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (tx, rx) = mpsc::channel(16);
        let seen = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
        let captured = seen.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut data = vec![];
                while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut buf = [0u8; 1024];
                    let n = socket.read(&mut buf).await?;
                    ensure!(n > 0 && data.len() < 16384, "bounded complete HTTP headers");
                    data.extend_from_slice(&buf[..n]);
                }
                let request = String::from_utf8(data)?;
                ensure!(
                    request.starts_with("GET /"),
                    "unexpected financial POST/RPC"
                );
                let path = request.split_whitespace().nth(1).unwrap();
                let query: HashMap<String, String> =
                    reqwest::Url::parse(&format!("http://localhost{path}"))?
                        .query_pairs()
                        .into_owned()
                        .collect();
                captured.lock().unwrap().push(query.clone());
                let (reply, wait) = oneshot::channel();
                tx.send(Request { query, reply }).await?;
                let body = wait.await?.to_string();
                let response=format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                socket.write_all(response.as_bytes()).await?;
            }
        });
        Ok(Self {
            url,
            rx,
            task,
            seen,
        })
    }
    pub async fn request(&mut self) -> Result<Request> {
        tokio::time::timeout(Duration::from_secs(3), self.rx.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("HTTP server stopped"))
    }
}
pub fn id(m: &Value) -> String {
    format!("source-sell:{}", m["sell"]["signature"].as_str().unwrap())
}
pub fn runner(
    db: &f::Db,
    m: &Value,
    url: &str,
    enabled: bool,
) -> Result<crate::execution_canary::ExecutionCanaryRunner> {
    let mut c = f::config(m);
    c.execution.quote_canary_enabled = enabled;
    c.execution.quote_canary_base_url = url.into();
    c.execution.quote_canary_timeout_ms = 4000;
    c.execution.canary_enabled = true;
    c.execution.canary_batch_limit = 4;
    c.execution.priority_fee_canary_enabled = false;
    c.execution.quote_canary_pump_fun_parallel_enabled = false;
    crate::execution_canary::ExecutionCanaryRunner::new(c.execution)
        .for_ingestion(&c.ingestion, &db.path.to_string_lossy())
}
pub async fn seeded(name: &str) -> Result<(f::Db, Value)> {
    let m = p::meta("direct")?;
    let db = f::Db::new(name)?;
    s::seed(&db, &m)?;
    p::stage(&db, "direct", &m, p::frames(&m), name, false).await?;
    ensure!(
        db.sql.query_row(
            "SELECT count(*) FROM ordered_source_sell_intents",
            [],
            |r| r.get::<_, i64>(0)
        )? == 1
    );
    Ok((db, m))
}
pub async fn tick(r: &crate::execution_canary::ExecutionCanaryRunner, db: &f::Db) -> Result<()> {
    tokio::time::timeout(
        Duration::from_millis(250),
        r.process_tick(&db.store, Utc::now()),
    )
    .await??;
    Ok(())
}
pub async fn result(db: &f::Db, m: &Value) -> Result<QuoteObservation> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(r) = db
                .store
                .load_strict_sell_quote(&id(m), p::limits(), Utc::now())?
            {
                return Ok(r);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
}
pub async fn quote(db: &f::Db, m: &Value) -> Result<QuoteObservation> {
    let mut server = Server::new().await?;
    let r = runner(db, m, &server.url, true)?;
    let before = s::snapshot(db)?;
    tick(&r, db).await?;
    server.request().await?.answer()?;
    let result = result(db, m).await?;
    ensure!(result.outcome == QuoteOutcome::Current, "{result:?}");
    ensure!(s::snapshot(db)? == before);
    ensure!(server.seen.lock().unwrap().len() == 1);
    Ok(result)
}
pub fn first(
    db: &f::Db,
    m: &Value,
) -> Result<copybot_storage_core::association_sell_preparation::FirstBinding> {
    let wire: String = db.sql.query_row(
        "SELECT first_binding FROM association_sell_preparations WHERE signature=?1",
        [m["sell"]["signature"].as_str().unwrap()],
        |r| r.get(0),
    )?;
    Ok(serde_json::from_str(&wire)?)
}
