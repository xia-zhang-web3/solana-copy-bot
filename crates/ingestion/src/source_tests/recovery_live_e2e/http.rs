//! Loopback-only saved-body service with real elapsed delays and owned cancellation.
use super::corpus::Corpus;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};

#[derive(Default)]
pub(super) struct Counts {
    pub blocks: AtomicUsize,
    pub bytes: AtomicUsize,
    pub active: AtomicUsize,
    pub max_active: AtomicUsize,
    pub preparing: AtomicUsize,
    pub max_preparing: AtomicUsize,
    pub prepared_blocks: AtomicUsize,
    pub preparation_ns: AtomicU64,
    pub preparation_ns_max: AtomicU64,
    pub preparation_deadline_overruns: AtomicUsize,
    pub service_delay_ms: AtomicU64,
    pub deadline_lateness_ns: AtomicU64,
    pub deadline_lateness_ns_max: AtomicU64,
    pub response_floor_checks: AtomicUsize,
    pub response_floor_violations: AtomicUsize,
}
impl Counts {
    pub fn witness(&self) -> Value {
        json!({
            "prepared_blocks": self.prepared_blocks.load(Ordering::Relaxed),
            "max_preparing": self.max_preparing.load(Ordering::Relaxed),
            "preparation_us_total": self.preparation_ns.load(Ordering::Relaxed) / 1000,
            "preparation_us_max": self.preparation_ns_max.load(Ordering::Relaxed) / 1000,
            "preparation_deadline_overruns": self.preparation_deadline_overruns.load(Ordering::Relaxed),
            "recorded_service_delay_ms_total": self.service_delay_ms.load(Ordering::Relaxed),
            "deadline_lateness_us_total": self.deadline_lateness_ns.load(Ordering::Relaxed) / 1000,
            "deadline_lateness_us_max": self.deadline_lateness_ns_max.load(Ordering::Relaxed) / 1000,
            "response_floor_checks": self.response_floor_checks.load(Ordering::Relaxed),
            "response_floor_violations": self.response_floor_violations.load(Ordering::Relaxed),
        })
    }
}
struct Active(Arc<Counts>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
struct Preparing(Arc<Counts>);
impl Drop for Preparing {
    fn drop(&mut self) {
        self.0.preparing.fetch_sub(1, Ordering::Relaxed);
    }
}
pub(super) struct Server {
    pub url: String,
    pub counts: Arc<Counts>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn start(corpus: Arc<Corpus>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let counts = Arc::new(Counts::default());
        let own = counts.clone();
        let preparation_slots = Arc::new(Semaphore::new(10));
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    socket = listener.accept() => {
                        let (socket, _) = socket.unwrap();
                        let c = corpus.clone(); let n = own.clone();
                        let p = preparation_slots.clone();
                        children.spawn(async move { serve(socket, c, n, p).await });
                    }
                    done = children.join_next(), if !children.is_empty() => { done.unwrap().unwrap().unwrap(); }
                }
            }
        });
        Ok(Self { url, counts, task })
    }
}
async fn serve(
    mut socket: TcpStream,
    corpus: Arc<Corpus>,
    counts: Arc<Counts>,
    preparation_slots: Arc<Semaphore>,
) -> Result<()> {
    let request_started = Instant::now();
    let mut request = Vec::new();
    let mut chunk = [0; 4096];
    let (end, size) = loop {
        let n = socket.read(&mut chunk).await?;
        ensure!(n != 0, "fixture request EOF");
        request.extend_from_slice(&chunk[..n]);
        ensure!(request.len() <= 1 << 20, "request cap");
        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = std::str::from_utf8(&request[..end])?;
            let size = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .context("request length")?;
            break (end + 4, size);
        }
    };
    while request.len() < end + size {
        let n = socket.read(&mut chunk).await?;
        ensure!(n != 0, "fixture request EOF");
        request.extend_from_slice(&chunk[..n]);
        ensure!(request.len() <= 1 << 20, "request cap");
    }
    let r: Value = serde_json::from_slice(&request[end..end + size])?;
    let id = r["id"].as_u64().context("request id")?;
    let body = if r["method"] == "getBlocks" {
        ensure!(
            r["params"][2]["commitment"] == "confirmed",
            "list commitment"
        );
        let lo = r["params"][0].as_u64().unwrap();
        let hi = r["params"][1].as_u64().unwrap();
        serde_json::to_vec(
            &json!({"jsonrpc":"2.0","id":id,"result":corpus.records.range(lo..=hi).map(|(s,_)| *s).collect::<Vec<_>>()}),
        )?
    } else {
        ensure!(r["method"] == "getBlock", "unexpected method");
        ensure!(
            r["params"][1]["commitment"] == "confirmed"
                && r["params"][1]["maxSupportedTransactionVersion"] == 1
                && r["params"][1]["rewards"] == true,
            "actual full block request"
        );
        let slot = r["params"][0].as_u64().unwrap();
        let delay_ms = corpus.delay_ms(slot);
        let deadline = request_started + Duration::from_millis(delay_ms);
        // This lease stays with blocking preparation and its returned bytes until
        // transmission ends. Cancelling the async owner cannot accumulate jobs.
        let preparation_permit = preparation_slots.acquire_owned().await?;
        let active = counts.active.fetch_add(1, Ordering::Relaxed) + 1;
        counts.max_active.fetch_max(active, Ordering::Relaxed);
        let _active = Active(counts.clone());
        counts
            .service_delay_ms
            .fetch_add(delay_ms, Ordering::Relaxed);
        let preparation_started = Instant::now();
        let preparing_counts = counts.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            let n = preparing_counts.preparing.fetch_add(1, Ordering::Relaxed) + 1;
            preparing_counts
                .max_preparing
                .fetch_max(n, Ordering::Relaxed);
            let _preparing = Preparing(preparing_counts.clone());
            let body = corpus.adapted(slot, id);
            let finished = Instant::now();
            let ns: u64 = finished
                .duration_since(preparation_started)
                .as_nanos()
                .try_into()?;
            preparing_counts
                .preparation_ns
                .fetch_add(ns, Ordering::Relaxed);
            preparing_counts
                .preparation_ns_max
                .fetch_max(ns, Ordering::Relaxed);
            if finished > deadline {
                preparing_counts
                    .preparation_deadline_overruns
                    .fetch_add(1, Ordering::Relaxed);
            }
            let body = body?;
            preparing_counts
                .prepared_blocks
                .fetch_add(1, Ordering::Relaxed);
            Ok::<_, anyhow::Error>((body, preparation_permit))
        });
        // The archived service elapsed already includes provider/body work. Local
        // disk verification overlaps it; headers require BOTH prep and its floor.
        tokio::time::sleep_until(deadline.into()).await;
        let (body, _prepared_permit) = prepared.await??;
        let ready = Instant::now();
        counts.response_floor_checks.fetch_add(1, Ordering::Relaxed);
        if ready < deadline {
            counts
                .response_floor_violations
                .fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("saved response service floor violated");
        }
        let late_ns: u64 = ready.duration_since(deadline).as_nanos().try_into()?;
        counts
            .deadline_lateness_ns
            .fetch_add(late_ns, Ordering::Relaxed);
        counts
            .deadline_lateness_ns_max
            .fetch_max(late_ns, Ordering::Relaxed);
        counts.blocks.fetch_add(1, Ordering::Relaxed);
        counts.bytes.fetch_add(body.len(), Ordering::Relaxed);
        // Keep this charge through body transmission, just as the client request.
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await?;
        socket.write_all(&body).await?;
        socket.shutdown().await?;
        return Ok(());
    };
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await?;
    socket.write_all(&body).await?;
    socket.shutdown().await?;
    Ok(())
}
