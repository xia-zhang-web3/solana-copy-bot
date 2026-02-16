use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::IngestionConfig;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::{self, Interval};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct RawSwapObservation {
    pub signature: String,
    pub slot: u64,
    pub signer: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub program_ids: Vec<String>,
    pub dex_hint: String,
    pub ts_utc: DateTime<Utc>,
}

pub enum IngestionSource {
    Mock(MockSource),
    HeliusWs(HeliusWsSource),
}

impl IngestionSource {
    pub fn from_config(config: &IngestionConfig) -> Result<Self> {
        match config.source.to_lowercase().as_str() {
            "mock" => Ok(Self::Mock(MockSource::new(
                config.mock_interval_ms,
                config
                    .raydium_program_ids
                    .first()
                    .cloned()
                    .unwrap_or_default(),
            ))),
            "helius" | "helius_ws" => Ok(Self::HeliusWs(HeliusWsSource::new(config)?)),
            other => Err(anyhow!("unknown ingestion.source: {other}")),
        }
    }

    pub async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        match self {
            Self::Mock(source) => source.next_observation().await,
            Self::HeliusWs(source) => source.next_observation().await,
        }
    }
}

pub struct MockSource {
    interval: Interval,
    sequence: u64,
    session_tag: String,
    raydium_program_id: String,
}

impl MockSource {
    pub fn new(interval_ms: u64, raydium_program_id: String) -> Self {
        let session_tag = format!("{}-{}", Utc::now().timestamp_millis(), std::process::id());
        Self {
            interval: time::interval(Duration::from_millis(interval_ms.max(100))),
            sequence: 0,
            session_tag,
            raydium_program_id,
        }
    }

    async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        self.interval.tick().await;
        self.sequence = self.sequence.saturating_add(1);
        let n = self.sequence;

        Ok(Some(RawSwapObservation {
            signature: format!("mock-{}-sig-{n}", self.session_tag),
            slot: 1_000_000 + n,
            signer: "MockLeaderWallet1111111111111111111111111111".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: format!("MockTokenMint{n}"),
            amount_in: 0.5,
            amount_out: 1_000.0 + (n as f64),
            program_ids: vec![self.raydium_program_id.clone()],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        }))
    }
}

#[derive(Debug, Clone)]
struct LogsNotification {
    signature: String,
    slot: u64,
    arrival_seq: u64,
    logs: Vec<String>,
    is_failed: bool,
}

#[derive(Debug, Clone)]
struct FetchedObservation {
    raw: RawSwapObservation,
    arrival_seq: u64,
    fetch_latency_ms: u64,
}

#[derive(Debug)]
struct ReorderEntry {
    raw: RawSwapObservation,
    enqueued_at: Instant,
}

type HeliusWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const WS_IDLE_TIMEOUT_SECS: u64 = 45;
const TELEMETRY_SAMPLE_CAPACITY: usize = 4096;

#[derive(Debug)]
struct IngestionTelemetry {
    ws_notifications_seen: AtomicU64,
    ws_notifications_enqueued: AtomicU64,
    ws_notifications_backpressured: AtomicU64,
    ws_notifications_dropped: AtomicU64,
    fetch_inflight: AtomicU64,
    fetch_success: AtomicU64,
    fetch_failed: AtomicU64,
    rpc_429: AtomicU64,
    rpc_5xx: AtomicU64,
    fetch_latency_ms_samples: Mutex<VecDeque<u64>>,
    ingestion_lag_ms_samples: Mutex<VecDeque<u64>>,
    reorder_hold_ms_samples: Mutex<VecDeque<u64>>,
    max_reorder_buffer_size: AtomicUsize,
    last_report_ms: AtomicI64,
}

impl Default for IngestionTelemetry {
    fn default() -> Self {
        Self {
            ws_notifications_seen: AtomicU64::new(0),
            ws_notifications_enqueued: AtomicU64::new(0),
            ws_notifications_backpressured: AtomicU64::new(0),
            ws_notifications_dropped: AtomicU64::new(0),
            fetch_inflight: AtomicU64::new(0),
            fetch_success: AtomicU64::new(0),
            fetch_failed: AtomicU64::new(0),
            rpc_429: AtomicU64::new(0),
            rpc_5xx: AtomicU64::new(0),
            fetch_latency_ms_samples: Mutex::new(VecDeque::with_capacity(
                TELEMETRY_SAMPLE_CAPACITY,
            )),
            ingestion_lag_ms_samples: Mutex::new(VecDeque::with_capacity(
                TELEMETRY_SAMPLE_CAPACITY,
            )),
            reorder_hold_ms_samples: Mutex::new(VecDeque::with_capacity(TELEMETRY_SAMPLE_CAPACITY)),
            max_reorder_buffer_size: AtomicUsize::new(0),
            last_report_ms: AtomicI64::new(0),
        }
    }
}

impl IngestionTelemetry {
    fn push_fetch_latency(&self, value: u64) {
        if let Ok(mut guard) = self.fetch_latency_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    fn push_ingestion_lag(&self, value: u64) {
        if let Ok(mut guard) = self.ingestion_lag_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    fn push_reorder_hold(&self, value: u64) {
        if let Ok(mut guard) = self.reorder_hold_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    fn note_reorder_buffer_size(&self, size: usize) {
        let _ = self
            .max_reorder_buffer_size
            .fetch_max(size, Ordering::Relaxed);
    }

    fn maybe_report(
        &self,
        report_seconds: u64,
        ws_to_fetch_queue_depth: usize,
        fetch_to_output_queue_depth: usize,
        reorder_buffer_size: usize,
    ) {
        let report_seconds = report_seconds.max(5);
        let now_ms = Utc::now().timestamp_millis();
        let last = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < (report_seconds as i64 * 1_000) {
            return;
        }
        if self
            .last_report_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let fetch_samples = self
            .fetch_latency_ms_samples
            .lock()
            .ok()
            .map(|values| values.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let lag_samples = self
            .ingestion_lag_ms_samples
            .lock()
            .ok()
            .map(|values| values.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let hold_samples = self
            .reorder_hold_ms_samples
            .lock()
            .ok()
            .map(|values| values.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        info!(
            ws_notifications_seen = self.ws_notifications_seen.load(Ordering::Relaxed),
            ws_notifications_enqueued = self.ws_notifications_enqueued.load(Ordering::Relaxed),
            ws_notifications_backpressured =
                self.ws_notifications_backpressured.load(Ordering::Relaxed),
            ws_notifications_dropped = self.ws_notifications_dropped.load(Ordering::Relaxed),
            ws_to_fetch_queue_depth,
            fetch_to_output_queue_depth,
            fetch_concurrency_inflight = self.fetch_inflight.load(Ordering::Relaxed),
            fetch_success = self.fetch_success.load(Ordering::Relaxed),
            fetch_failed = self.fetch_failed.load(Ordering::Relaxed),
            rpc_429 = self.rpc_429.load(Ordering::Relaxed),
            rpc_5xx = self.rpc_5xx.load(Ordering::Relaxed),
            fetch_latency_ms_p50 = percentile(&fetch_samples, 0.50),
            fetch_latency_ms_p95 = percentile(&fetch_samples, 0.95),
            fetch_latency_ms_p99 = percentile(&fetch_samples, 0.99),
            ingestion_lag_ms_p50 = percentile(&lag_samples, 0.50),
            ingestion_lag_ms_p95 = percentile(&lag_samples, 0.95),
            ingestion_lag_ms_p99 = percentile(&lag_samples, 0.99),
            reorder_hold_ms_p95 = percentile(&hold_samples, 0.95),
            reorder_buffer_size,
            reorder_buffer_max = self.max_reorder_buffer_size.load(Ordering::Relaxed),
            "ingestion pipeline metrics"
        );
    }
}

struct HeliusPipeline {
    output_rx: mpsc::Receiver<FetchedObservation>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    fetch_to_output_depth: Arc<AtomicUsize>,
    ws_reader_task: JoinHandle<()>,
    fetcher_tasks: Vec<JoinHandle<()>>,
}

impl Drop for HeliusPipeline {
    fn drop(&mut self) {
        self.ws_reader_task.abort();
        for task in &self.fetcher_tasks {
            task.abort();
        }
    }
}

struct HeliusRuntimeConfig {
    ws_url: String,
    http_urls: Vec<String>,
    http_url_rr: AtomicUsize,
    reconnect_initial_ms: u64,
    reconnect_max_ms: u64,
    tx_fetch_retries: u32,
    tx_fetch_retry_delay_ms: u64,
    seen_signatures_limit: usize,
    interested_program_ids: HashSet<String>,
    raydium_program_ids: HashSet<String>,
    pumpswap_program_ids: HashSet<String>,
    http_client: Client,
    telemetry: Arc<IngestionTelemetry>,
}

impl HeliusRuntimeConfig {
    fn next_http_url(&self) -> &str {
        let len = self.http_urls.len();
        let index = self.http_url_rr.fetch_add(1, Ordering::Relaxed) % len;
        &self.http_urls[index]
    }
}

pub struct HeliusWsSource {
    runtime_config: Arc<HeliusRuntimeConfig>,
    fetch_concurrency: usize,
    ws_queue_capacity: usize,
    output_queue_capacity: usize,
    reorder_hold_ms: u64,
    reorder_max_buffer: usize,
    telemetry_report_seconds: u64,
    pipeline: Option<HeliusPipeline>,
    reorder_buffer: BTreeMap<(u64, u64, String), ReorderEntry>,
}

enum OutputRecvOutcome {
    Item(FetchedObservation),
    ChannelClosed,
    TimedOut,
}

impl HeliusWsSource {
    pub fn new(config: &IngestionConfig) -> Result<Self> {
        let mut interested_program_ids: HashSet<String> =
            config.subscribe_program_ids.iter().cloned().collect();
        if interested_program_ids.is_empty() {
            interested_program_ids.extend(config.raydium_program_ids.iter().cloned());
            interested_program_ids.extend(config.pumpswap_program_ids.iter().cloned());
        }

        if interested_program_ids.is_empty() {
            return Err(anyhow!(
                "helius_ws requires program IDs in subscribe_program_ids/raydium_program_ids/pumpswap_program_ids"
            ));
        }

        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.tx_request_timeout_ms.max(500)))
            .build()
            .context("failed building reqwest client")?;

        let mut candidates = Vec::new();
        for url in &config.helius_http_urls {
            let trimmed = url.trim();
            if !trimmed.is_empty() && !candidates.iter().any(|existing| existing == trimmed) {
                candidates.push(trimmed.to_string());
            }
        }
        if candidates.is_empty() {
            let trimmed = config.helius_http_url.trim();
            if !trimmed.is_empty() {
                candidates.push(trimmed.to_string());
            }
        }

        let mut http_urls = Vec::new();
        for candidate in candidates {
            let parsed = match Url::parse(&candidate) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(
                        url = %candidate,
                        error = %error,
                        "dropping invalid ingestion HTTP URL"
                    );
                    continue;
                }
            };

            let scheme = parsed.scheme();
            if scheme != "http" && scheme != "https" {
                warn!(
                    url = %candidate,
                    scheme = %scheme,
                    "dropping ingestion HTTP URL with unsupported scheme"
                );
                continue;
            }
            if parsed.host_str().is_none() {
                warn!(
                    url = %candidate,
                    "dropping ingestion HTTP URL without host"
                );
                continue;
            }

            if !http_urls.iter().any(|existing| existing == &candidate) {
                http_urls.push(candidate);
            }
        }
        if http_urls.is_empty() {
            return Err(anyhow!(
                "no valid ingestion HTTP URL configured (check helius_http_url / helius_http_urls)"
            ));
        }

        let runtime_config = HeliusRuntimeConfig {
            ws_url: config.helius_ws_url.clone(),
            http_urls,
            http_url_rr: AtomicUsize::new(0),
            reconnect_initial_ms: config.reconnect_initial_ms.max(200),
            reconnect_max_ms: config
                .reconnect_max_ms
                .max(config.reconnect_initial_ms.max(200)),
            tx_fetch_retries: config.tx_fetch_retries,
            tx_fetch_retry_delay_ms: config.tx_fetch_retry_delay_ms.max(50),
            seen_signatures_limit: config.seen_signatures_limit.max(500),
            interested_program_ids,
            raydium_program_ids: config.raydium_program_ids.iter().cloned().collect(),
            pumpswap_program_ids: config.pumpswap_program_ids.iter().cloned().collect(),
            http_client,
            telemetry: Arc::new(IngestionTelemetry::default()),
        };

        Ok(Self {
            runtime_config: Arc::new(runtime_config),
            fetch_concurrency: config.fetch_concurrency.max(1),
            ws_queue_capacity: config.ws_queue_capacity.max(128),
            output_queue_capacity: config.output_queue_capacity.max(64),
            reorder_hold_ms: config.reorder_hold_ms.max(1),
            reorder_max_buffer: config.reorder_max_buffer.max(16),
            telemetry_report_seconds: config.telemetry_report_seconds.max(5),
            pipeline: None,
            reorder_buffer: BTreeMap::new(),
        })
    }

    async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        loop {
            self.ensure_pipeline_running()?;

            if let Some(raw) = self.pop_ready_observation() {
                self.maybe_report_pipeline_metrics();
                return Ok(Some(raw));
            }

            let wait_for_ready = self.reorder_wait_duration();
            match self.recv_from_pipeline(wait_for_ready).await {
                OutputRecvOutcome::Item(item) => {
                    self.push_reorder_entry(item);
                    self.maybe_report_pipeline_metrics();
                }
                OutputRecvOutcome::TimedOut => {
                    self.maybe_report_pipeline_metrics();
                    continue;
                }
                OutputRecvOutcome::ChannelClosed => {
                    warn!("ingestion pipeline output channel closed; restarting pipeline");
                    self.pipeline = None;
                    if let Some(raw) = self.pop_earliest_observation() {
                        self.maybe_report_pipeline_metrics();
                        return Ok(Some(raw));
                    }
                    self.maybe_report_pipeline_metrics();
                    continue;
                }
            }
        }
    }

    fn ensure_pipeline_running(&mut self) -> Result<()> {
        let needs_restart = self
            .pipeline
            .as_ref()
            .map(|pipeline| {
                pipeline.ws_reader_task.is_finished()
                    || pipeline.fetcher_tasks.iter().any(|task| task.is_finished())
            })
            .unwrap_or(true);
        if needs_restart {
            if self.pipeline.is_some() {
                warn!("ingestion pipeline became unhealthy; recreating pipeline tasks");
            }
            self.pipeline = Some(self.spawn_pipeline()?);
        }
        Ok(())
    }

    fn spawn_pipeline(&self) -> Result<HeliusPipeline> {
        if self.runtime_config.ws_url.contains("REPLACE_ME")
            || self
                .runtime_config
                .http_urls
                .iter()
                .any(|url| url.contains("REPLACE_ME"))
            || self.runtime_config.http_urls.is_empty()
        {
            return Err(anyhow!(
                "configure ingestion.helius_ws_url and ingestion.helius_http_url / ingestion.helius_http_urls with real API key(s)"
            ));
        }

        let (sig_tx, sig_rx) = mpsc::channel::<LogsNotification>(self.ws_queue_capacity);
        let (out_tx, out_rx) = mpsc::channel::<FetchedObservation>(self.output_queue_capacity);
        let ws_to_fetch_depth = Arc::new(AtomicUsize::new(0));
        let fetch_to_output_depth = Arc::new(AtomicUsize::new(0));

        let ws_reader_task = {
            let runtime_config = Arc::clone(&self.runtime_config);
            let ws_to_fetch_depth = Arc::clone(&ws_to_fetch_depth);
            tokio::spawn(async move {
                ws_reader_loop(runtime_config, sig_tx, ws_to_fetch_depth).await;
            })
        };

        let shared_sig_rx = Arc::new(AsyncMutex::new(sig_rx));
        let mut fetcher_tasks = Vec::with_capacity(self.fetch_concurrency);
        for worker_id in 0..self.fetch_concurrency {
            let runtime_config = Arc::clone(&self.runtime_config);
            let shared_sig_rx = Arc::clone(&shared_sig_rx);
            let out_tx = out_tx.clone();
            let ws_to_fetch_depth = Arc::clone(&ws_to_fetch_depth);
            let fetch_to_output_depth = Arc::clone(&fetch_to_output_depth);
            fetcher_tasks.push(tokio::spawn(async move {
                fetch_worker_loop(
                    worker_id,
                    runtime_config,
                    shared_sig_rx,
                    out_tx,
                    ws_to_fetch_depth,
                    fetch_to_output_depth,
                )
                .await;
            }));
        }
        drop(out_tx);

        Ok(HeliusPipeline {
            output_rx: out_rx,
            ws_to_fetch_depth,
            fetch_to_output_depth,
            ws_reader_task,
            fetcher_tasks,
        })
    }

    async fn recv_from_pipeline(&mut self, wait: Option<Duration>) -> OutputRecvOutcome {
        let Some(pipeline) = self.pipeline.as_mut() else {
            return OutputRecvOutcome::ChannelClosed;
        };

        if let Some(wait) = wait {
            match time::timeout(wait, pipeline.output_rx.recv()).await {
                Ok(Some(item)) => {
                    decrement_atomic_usize(&pipeline.fetch_to_output_depth);
                    OutputRecvOutcome::Item(item)
                }
                Ok(None) => OutputRecvOutcome::ChannelClosed,
                Err(_) => OutputRecvOutcome::TimedOut,
            }
        } else {
            match pipeline.output_rx.recv().await {
                Some(item) => {
                    decrement_atomic_usize(&pipeline.fetch_to_output_depth);
                    OutputRecvOutcome::Item(item)
                }
                None => OutputRecvOutcome::ChannelClosed,
            }
        }
    }

    fn push_reorder_entry(&mut self, fetched: FetchedObservation) {
        self.runtime_config
            .telemetry
            .push_fetch_latency(fetched.fetch_latency_ms);

        let key = (
            fetched.raw.slot,
            fetched.arrival_seq,
            fetched.raw.signature.clone(),
        );
        self.reorder_buffer.entry(key).or_insert(ReorderEntry {
            raw: fetched.raw,
            enqueued_at: Instant::now(),
        });
        self.runtime_config
            .telemetry
            .note_reorder_buffer_size(self.reorder_buffer.len());
    }

    fn pop_ready_observation(&mut self) -> Option<RawSwapObservation> {
        if self.reorder_buffer.is_empty() {
            return None;
        }

        let first_key = self.reorder_buffer.keys().next()?.clone();
        let first_entry = self.reorder_buffer.get(&first_key)?;
        let hold_elapsed = first_entry.enqueued_at.elapsed();
        let hold_target = Duration::from_millis(self.reorder_hold_ms.max(1));

        let should_release =
            self.reorder_buffer.len() > self.reorder_max_buffer || hold_elapsed >= hold_target;
        if !should_release {
            return None;
        }

        let entry = self.reorder_buffer.remove(&first_key)?;
        let hold_ms = entry.enqueued_at.elapsed().as_millis() as u64;
        self.runtime_config.telemetry.push_reorder_hold(hold_ms);

        let lag_ms = (Utc::now() - entry.raw.ts_utc).num_milliseconds().max(0) as u64;
        self.runtime_config.telemetry.push_ingestion_lag(lag_ms);
        Some(entry.raw)
    }

    fn pop_earliest_observation(&mut self) -> Option<RawSwapObservation> {
        let first_key = self.reorder_buffer.keys().next()?.clone();
        let entry = self.reorder_buffer.remove(&first_key)?;
        let hold_ms = entry.enqueued_at.elapsed().as_millis() as u64;
        self.runtime_config.telemetry.push_reorder_hold(hold_ms);

        let lag_ms = (Utc::now() - entry.raw.ts_utc).num_milliseconds().max(0) as u64;
        self.runtime_config.telemetry.push_ingestion_lag(lag_ms);
        Some(entry.raw)
    }

    fn reorder_wait_duration(&self) -> Option<Duration> {
        let first_entry = self.reorder_buffer.values().next()?;
        if self.reorder_buffer.len() > self.reorder_max_buffer {
            return Some(Duration::from_millis(0));
        }

        let hold_target = Duration::from_millis(self.reorder_hold_ms.max(1));
        let elapsed = first_entry.enqueued_at.elapsed();
        if elapsed >= hold_target {
            Some(Duration::from_millis(0))
        } else {
            Some(hold_target - elapsed)
        }
    }

    fn maybe_report_pipeline_metrics(&self) {
        let (ws_depth, output_depth) = self
            .pipeline
            .as_ref()
            .map(|pipeline| {
                (
                    pipeline.ws_to_fetch_depth.load(Ordering::Relaxed),
                    pipeline.fetch_to_output_depth.load(Ordering::Relaxed),
                )
            })
            .unwrap_or((0, 0));
        self.runtime_config.telemetry.maybe_report(
            self.telemetry_report_seconds,
            ws_depth,
            output_depth,
            self.reorder_buffer.len(),
        );
    }

    fn parse_logs_notification(text: &str) -> Option<LogsNotification> {
        let value: Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(error) => {
                debug!(error = %error, "skipping invalid ws message json");
                return None;
            }
        };

        if let (Some(id), Some(result)) = (value.get("id"), value.get("result")) {
            if id.is_number() && result.is_number() {
                debug!(id = ?id, subscription = ?result, "logsSubscribe acknowledged");
            }
            return None;
        }

        let method = value.get("method").and_then(Value::as_str)?;
        if method != "logsNotification" {
            return None;
        }

        let params = value.get("params")?;
        let result = params.get("result")?;
        let context = result.get("context")?;
        let event = result.get("value")?;

        let signature = event.get("signature")?.as_str()?.to_string();
        let slot = context
            .get("slot")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let logs = event
            .get("logs")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let is_failed = event.get("err").map(|err| !err.is_null()).unwrap_or(false);

        Some(LogsNotification {
            signature,
            slot,
            arrival_seq: 0,
            logs,
            is_failed,
        })
    }

    fn extract_account_keys(result: &Value) -> Vec<(String, bool)> {
        result
            .pointer("/transaction/message/accountKeys")
            .and_then(Value::as_array)
            .map(|keys| {
                keys.iter()
                    .filter_map(|item| {
                        if let Some(pubkey) = item.as_str() {
                            return Some((pubkey.to_string(), false));
                        }
                        let pubkey = item.get("pubkey").and_then(Value::as_str)?;
                        let signer = item.get("signer").and_then(Value::as_bool).unwrap_or(false);
                        Some((pubkey.to_string(), signer))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn extract_program_ids(result: &Value, meta: &Value, logs_hint: &[String]) -> HashSet<String> {
        let mut set = HashSet::new();

        if let Some(ixs) = result
            .pointer("/transaction/message/instructions")
            .and_then(Value::as_array)
        {
            for ix in ixs {
                if let Some(program_id) = ix.get("programId").and_then(Value::as_str) {
                    set.insert(program_id.to_string());
                }
            }
        }

        if let Some(inner) = meta.get("innerInstructions").and_then(Value::as_array) {
            for group in inner {
                if let Some(ixs) = group.get("instructions").and_then(Value::as_array) {
                    for ix in ixs {
                        if let Some(program_id) = ix.get("programId").and_then(Value::as_str) {
                            set.insert(program_id.to_string());
                        }
                    }
                }
            }
        }

        for log in logs_hint.iter().chain(
            Self::value_to_string_vec(meta.get("logMessages"))
                .unwrap_or_default()
                .iter(),
        ) {
            if let Some(program_id) = Self::extract_program_id_from_log(log) {
                set.insert(program_id);
            }
        }

        set
    }

    fn extract_program_id_from_log(log: &str) -> Option<String> {
        let mut parts = log.split_whitespace();
        if parts.next()? != "Program" {
            return None;
        }
        let program_id = parts.next()?.trim();
        if program_id.is_empty() {
            None
        } else {
            Some(program_id.to_string())
        }
    }

    fn infer_swap_from_json_balances(
        meta: &Value,
        signer_index: usize,
        signer: &str,
    ) -> Option<(String, f64, String, f64)> {
        const TOKEN_EPS: f64 = 1e-12;
        const SOL_EPS: f64 = 1e-8;
        let mut mint_deltas: HashMap<String, f64> = HashMap::new();

        let pre = meta
            .get("preTokenBalances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let post = meta
            .get("postTokenBalances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        for item in pre {
            if item.get("owner").and_then(Value::as_str) == Some(signer) {
                let mint = item.get("mint").and_then(Value::as_str)?.to_string();
                let amount = Self::parse_ui_amount_json(item.get("uiTokenAmount"))?;
                *mint_deltas.entry(mint).or_default() -= amount;
            }
        }
        for item in post {
            if item.get("owner").and_then(Value::as_str) == Some(signer) {
                let mint = item.get("mint").and_then(Value::as_str)?.to_string();
                let amount = Self::parse_ui_amount_json(item.get("uiTokenAmount"))?;
                *mint_deltas.entry(mint).or_default() += amount;
            }
        }

        let mut token_in_candidates = Vec::new();
        let mut token_out_candidates = Vec::new();
        for (mint, delta) in &mint_deltas {
            if *delta < -TOKEN_EPS {
                token_in_candidates.push((mint.clone(), delta.abs()));
            } else if *delta > TOKEN_EPS {
                token_out_candidates.push((mint.clone(), *delta));
            }
        }
        token_in_candidates
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        token_out_candidates
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let sol_token_delta = mint_deltas.get(SOL_MINT).copied().unwrap_or(0.0);
        if sol_token_delta < -TOKEN_EPS {
            if let Some((out_mint, out_amt)) = Self::dominant_non_sol_leg(&token_out_candidates) {
                return Some((
                    SOL_MINT.to_string(),
                    sol_token_delta.abs(),
                    out_mint,
                    out_amt,
                ));
            }
        }
        if sol_token_delta > TOKEN_EPS {
            if let Some((in_mint, in_amt)) = Self::dominant_non_sol_leg(&token_in_candidates) {
                return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_token_delta));
            }
        }

        let sol_delta = Self::signer_sol_delta(meta, signer_index).unwrap_or(0.0);
        if sol_delta < -SOL_EPS {
            if let Some((out_mint, out_amt)) = Self::dominant_non_sol_leg(&token_out_candidates) {
                return Some((SOL_MINT.to_string(), sol_delta.abs(), out_mint, out_amt));
            }
        }
        if sol_delta > SOL_EPS {
            if let Some((in_mint, in_amt)) = Self::dominant_non_sol_leg(&token_in_candidates) {
                return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_delta));
            }
        }

        if sol_delta.abs() <= SOL_EPS && sol_token_delta.abs() <= TOKEN_EPS {
            let token_in_non_sol: Vec<_> = token_in_candidates
                .iter()
                .filter(|(mint, _)| mint != SOL_MINT)
                .cloned()
                .collect();
            let token_out_non_sol: Vec<_> = token_out_candidates
                .iter()
                .filter(|(mint, _)| mint != SOL_MINT)
                .cloned()
                .collect();
            if token_in_non_sol.len() == 1 && token_out_non_sol.len() == 1 {
                let (in_mint, in_amt) = token_in_non_sol[0].clone();
                let (out_mint, out_amt) = token_out_non_sol[0].clone();
                if in_mint != out_mint {
                    return Some((in_mint, in_amt, out_mint, out_amt));
                }
            }
        }

        None
    }

    fn signer_sol_delta(meta: &Value, signer_index: usize) -> Option<f64> {
        let pre_sol = meta
            .get("preBalances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.get(signer_index))
            .and_then(Value::as_u64)
            .map(|lamports| lamports as f64 / 1_000_000_000.0)?;
        let post_sol = meta
            .get("postBalances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.get(signer_index))
            .and_then(Value::as_u64)
            .map(|lamports| lamports as f64 / 1_000_000_000.0)?;
        Some(post_sol - pre_sol)
    }

    fn dominant_non_sol_leg(entries: &[(String, f64)]) -> Option<(String, f64)> {
        const EPS: f64 = 1e-12;
        const SECOND_LEG_AMBIGUITY_RATIO: f64 = 0.15;
        let non_sol: Vec<(String, f64)> = entries
            .iter()
            .filter(|(mint, value)| mint != SOL_MINT && *value > EPS)
            .cloned()
            .collect();
        let (primary_mint, primary_value) = non_sol.first()?.clone();
        if non_sol.len() >= 2 {
            let second_value = non_sol[1].1;
            if second_value > primary_value * SECOND_LEG_AMBIGUITY_RATIO {
                return None;
            }
        }
        Some((primary_mint, primary_value))
    }

    fn parse_ui_amount_json(ui_amount: Option<&Value>) -> Option<f64> {
        let ui_amount = ui_amount?;
        if let Some(amount) = ui_amount.get("uiAmountString").and_then(Value::as_str) {
            return amount.parse::<f64>().ok();
        }
        if let Some(amount) = ui_amount.get("uiAmount").and_then(Value::as_f64) {
            return Some(amount);
        }
        let raw = ui_amount.get("amount").and_then(Value::as_str)?;
        let decimals = ui_amount.get("decimals").and_then(Value::as_u64)?;
        if decimals > 18 {
            return None;
        }
        let raw = raw.parse::<f64>().ok()?;
        Some(raw / 10f64.powi(decimals as i32))
    }

    fn value_to_string_vec(value: Option<&Value>) -> Option<Vec<String>> {
        Some(
            value?
                .as_array()?
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect(),
        )
    }

    fn detect_dex_hint(
        program_ids: &HashSet<String>,
        logs: &[String],
        raydium_program_ids: &HashSet<String>,
        pumpswap_program_ids: &HashSet<String>,
    ) -> String {
        if program_ids
            .iter()
            .any(|program| raydium_program_ids.contains(program))
            || logs
                .iter()
                .any(|log| log.to_ascii_lowercase().contains("raydium"))
        {
            return "raydium".to_string();
        }
        if program_ids
            .iter()
            .any(|program| pumpswap_program_ids.contains(program))
            || logs
                .iter()
                .any(|log| log.to_ascii_lowercase().contains("pump"))
        {
            return "pumpswap".to_string();
        }
        "unknown".to_string()
    }
}

async fn ws_reader_loop(
    runtime_config: Arc<HeliusRuntimeConfig>,
    sig_tx: mpsc::Sender<LogsNotification>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
) {
    let mut request_id: u64 = 1000;
    let mut arrival_seq: u64 = 0;
    let mut ws: Option<HeliusWsStream> = None;
    let mut next_backoff_ms = runtime_config.reconnect_initial_ms;
    let mut seen_signatures_queue: VecDeque<String> = VecDeque::new();
    let mut seen_signatures_set: HashSet<String> = HashSet::new();

    loop {
        if ws.is_none() {
            match connect_ws_stream(&runtime_config, &mut request_id).await {
                Ok(stream) => {
                    ws = Some(stream);
                    next_backoff_ms = runtime_config.reconnect_initial_ms;
                }
                Err(error) => {
                    warn!(error = ?error, "helius ws connect failed");
                    sleep_with_backoff(
                        &mut next_backoff_ms,
                        runtime_config.reconnect_initial_ms,
                        runtime_config.reconnect_max_ms,
                    )
                    .await;
                    continue;
                }
            }
        }

        let next_message = {
            let ws_stream = ws.as_mut().expect("ws stream present after connect");
            time::timeout(Duration::from_secs(WS_IDLE_TIMEOUT_SECS), ws_stream.next()).await
        };

        match next_message {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Some(mut notification) = HeliusWsSource::parse_logs_notification(&text) {
                    runtime_config
                        .telemetry
                        .ws_notifications_seen
                        .fetch_add(1, Ordering::Relaxed);
                    if notification.is_failed {
                        continue;
                    }
                    if is_seen_signature(&seen_signatures_set, &notification.signature) {
                        continue;
                    }

                    arrival_seq = arrival_seq.saturating_add(1);
                    notification.arrival_seq = arrival_seq;
                    let signature = notification.signature.clone();

                    match sig_tx.try_send(notification) {
                        Ok(()) => {
                            mark_seen_signature(
                                &mut seen_signatures_set,
                                &mut seen_signatures_queue,
                                runtime_config.seen_signatures_limit,
                                signature,
                            );
                            increment_atomic_usize(&ws_to_fetch_depth);
                            runtime_config
                                .telemetry
                                .ws_notifications_enqueued
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Full(notification)) => {
                            runtime_config
                                .telemetry
                                .ws_notifications_backpressured
                                .fetch_add(1, Ordering::Relaxed);
                            match sig_tx.send(notification).await {
                                Ok(()) => {
                                    mark_seen_signature(
                                        &mut seen_signatures_set,
                                        &mut seen_signatures_queue,
                                        runtime_config.seen_signatures_limit,
                                        signature,
                                    );
                                    increment_atomic_usize(&ws_to_fetch_depth);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_enqueued
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_dropped
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!(
                                        "signature queue receiver dropped while backpressured; stopping ws reader"
                                    );
                                    break;
                                }
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            runtime_config
                                .telemetry
                                .ws_notifications_dropped
                                .fetch_add(1, Ordering::Relaxed);
                            warn!("signature queue receiver dropped; stopping ws reader");
                            break;
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                if let Some(ws_stream) = ws.as_mut() {
                    if let Err(error) = ws_stream.send(Message::Pong(payload)).await {
                        warn!(error = %error, "failed to send ws pong");
                        ws = None;
                        sleep_with_backoff(
                            &mut next_backoff_ms,
                            runtime_config.reconnect_initial_ms,
                            runtime_config.reconnect_max_ms,
                        )
                        .await;
                    }
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                warn!(?frame, "helius ws closed");
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => {
                warn!(error = %error, "helius ws stream error");
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
            Ok(None) => {
                warn!("helius ws stream ended");
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
            Err(_) => {
                warn!(
                    idle_timeout_seconds = WS_IDLE_TIMEOUT_SECS,
                    "helius ws idle timeout, reconnecting"
                );
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
        }
    }
}

async fn fetch_worker_loop(
    worker_id: usize,
    runtime_config: Arc<HeliusRuntimeConfig>,
    shared_sig_rx: Arc<AsyncMutex<mpsc::Receiver<LogsNotification>>>,
    out_tx: mpsc::Sender<FetchedObservation>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    fetch_to_output_depth: Arc<AtomicUsize>,
) {
    loop {
        let notification = {
            let mut guard = shared_sig_rx.lock().await;
            guard.recv().await
        };

        let Some(notification) = notification else {
            debug!(
                worker_id,
                "fetch worker exiting because signature channel closed"
            );
            return;
        };
        decrement_atomic_usize(&ws_to_fetch_depth);

        match fetch_swap_with_retries(runtime_config.as_ref(), notification).await {
            Ok(Some(fetched)) => {
                if out_tx.send(fetched).await.is_err() {
                    warn!(worker_id, "output channel closed; stopping fetch worker");
                    return;
                }
                increment_atomic_usize(&fetch_to_output_depth);
            }
            Ok(None) => {
                runtime_config
                    .telemetry
                    .fetch_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                runtime_config
                    .telemetry
                    .fetch_failed
                    .fetch_add(1, Ordering::Relaxed);
                warn!(worker_id, error = %error, "fetch worker failed to parse transaction");
            }
        }
    }
}

async fn connect_ws_stream(
    runtime_config: &HeliusRuntimeConfig,
    request_id: &mut u64,
) -> Result<HeliusWsStream> {
    let (mut ws, _response) = connect_async(&runtime_config.ws_url)
        .await
        .with_context(|| format!("failed connecting to {}", runtime_config.ws_url))?;

    for program_id in &runtime_config.interested_program_ids {
        *request_id = request_id.saturating_add(1);
        let request = json!({
            "jsonrpc": "2.0",
            "id": *request_id,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [program_id]},
                {"commitment": "confirmed"}
            ]
        });
        ws.send(Message::Text(request.to_string().into()))
            .await
            .with_context(|| format!("failed sending logsSubscribe for {program_id}"))?;
    }

    info!(
        ws_url = %runtime_config.ws_url,
        http_endpoints = runtime_config.http_urls.len(),
        programs = runtime_config.interested_program_ids.len(),
        "helius ws connected and subscriptions sent"
    );

    Ok(ws)
}

async fn fetch_swap_with_retries(
    runtime_config: &HeliusRuntimeConfig,
    notification: LogsNotification,
) -> Result<Option<FetchedObservation>> {
    for attempt in 0..=runtime_config.tx_fetch_retries {
        let started = Instant::now();
        runtime_config
            .telemetry
            .fetch_inflight
            .fetch_add(1, Ordering::Relaxed);
        let result = fetch_swap_from_signature(
            runtime_config,
            &notification.signature,
            notification.slot,
            &notification.logs,
        )
        .await;
        runtime_config
            .telemetry
            .fetch_inflight
            .fetch_sub(1, Ordering::Relaxed);

        match result {
            Ok(Some(raw)) => {
                runtime_config
                    .telemetry
                    .fetch_success
                    .fetch_add(1, Ordering::Relaxed);
                let fetch_latency_ms = started.elapsed().as_millis() as u64;
                return Ok(Some(FetchedObservation {
                    raw,
                    arrival_seq: notification.arrival_seq,
                    fetch_latency_ms,
                }));
            }
            // No parsed swap / null transaction is a terminal outcome for this signature.
            // Retrying these amplifies backlog pressure without improving success rate.
            Ok(None) => return Ok(None),
            Err(error) => {
                // Keep high-signal warnings only for the terminal retry attempt.
                if attempt >= runtime_config.tx_fetch_retries {
                    warn!(
                        error = %error,
                        signature = %notification.signature,
                        attempt,
                        "tx fetch attempt failed"
                    );
                } else {
                    debug!(
                        error = %error,
                        signature = %notification.signature,
                        attempt,
                        "tx fetch attempt failed"
                    );
                }
            }
        }

        if attempt < runtime_config.tx_fetch_retries {
            time::sleep(Duration::from_millis(
                runtime_config.tx_fetch_retry_delay_ms,
            ))
            .await;
        }
    }

    Ok(None)
}

async fn fetch_swap_from_signature(
    runtime_config: &HeliusRuntimeConfig,
    signature: &str,
    slot_hint: u64,
    logs_hint: &[String],
) -> Result<Option<RawSwapObservation>> {
    let http_url = runtime_config.next_http_url().to_string();
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    });

    let response = runtime_config
        .http_client
        .post(&http_url)
        .json(&request)
        .send()
        .await
        .with_context(|| format!("failed getTransaction POST for {signature} via {http_url}"))?;

    let status = response.status();
    if status.as_u16() == 429 {
        runtime_config
            .telemetry
            .rpc_429
            .fetch_add(1, Ordering::Relaxed);
    }
    if status.is_server_error() {
        runtime_config
            .telemetry
            .rpc_5xx
            .fetch_add(1, Ordering::Relaxed);
    }

    let response: Value = response
        .error_for_status()
        .with_context(|| {
            format!("non-success getTransaction status for {signature} via {http_url}")
        })?
        .json()
        .await
        .with_context(|| {
            format!("failed parsing getTransaction json for {signature} via {http_url}")
        })?;

    if response.get("error").is_some() {
        debug!(signature, error = ?response.get("error"), "rpc returned error");
        return Ok(None);
    }

    let result = match response.get("result") {
        Some(value) if !value.is_null() => value,
        _ => return Ok(None),
    };
    let meta = match result.get("meta") {
        Some(value) if !value.is_null() => value,
        _ => return Ok(None),
    };
    if meta.get("err").map(|err| !err.is_null()).unwrap_or(false) {
        return Ok(None);
    }

    let account_keys = HeliusWsSource::extract_account_keys(result);
    if account_keys.is_empty() {
        return Ok(None);
    }
    let signer_index = account_keys
        .iter()
        .position(|(_, is_signer)| *is_signer)
        .unwrap_or(0);
    let signer = account_keys
        .get(signer_index)
        .map(|(pubkey, _)| pubkey.clone())
        .ok_or_else(|| anyhow!("missing signer in parsed account keys"))?;

    let mut program_ids = HeliusWsSource::extract_program_ids(result, meta, logs_hint);
    if program_ids.is_empty() {
        program_ids.extend(runtime_config.interested_program_ids.iter().cloned());
    } else if !program_ids
        .iter()
        .any(|program| runtime_config.interested_program_ids.contains(program))
    {
        return Ok(None);
    }

    let (token_in, amount_in, token_out, amount_out) =
        match HeliusWsSource::infer_swap_from_json_balances(meta, signer_index, &signer) {
            Some(value) => value,
            None => return Ok(None),
        };

    let block_time = result.get("blockTime").and_then(Value::as_i64);
    let ts_utc = block_time
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);
    let slot = result
        .get("slot")
        .and_then(Value::as_u64)
        .unwrap_or(slot_hint);
    let logs = HeliusWsSource::value_to_string_vec(meta.get("logMessages"))
        .unwrap_or_else(|| logs_hint.to_vec());
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        &runtime_config.raydium_program_ids,
        &runtime_config.pumpswap_program_ids,
    );

    Ok(Some(RawSwapObservation {
        signature: signature.to_string(),
        slot,
        signer,
        token_in,
        token_out,
        amount_in,
        amount_out,
        program_ids: program_ids.into_iter().collect(),
        dex_hint,
        ts_utc,
    }))
}

fn is_seen_signature(seen_signatures_set: &HashSet<String>, signature: &str) -> bool {
    seen_signatures_set.contains(signature)
}

fn mark_seen_signature(
    seen_signatures_set: &mut HashSet<String>,
    seen_signatures_queue: &mut VecDeque<String>,
    seen_signatures_limit: usize,
    signature: String,
) {
    if seen_signatures_set.insert(signature.clone()) {
        seen_signatures_queue.push_back(signature);
    }
    while seen_signatures_queue.len() > seen_signatures_limit {
        if let Some(removed) = seen_signatures_queue.pop_front() {
            seen_signatures_set.remove(&removed);
        }
    }
}

async fn sleep_with_backoff(next_backoff_ms: &mut u64, initial_ms: u64, max_ms: u64) {
    let delay = (*next_backoff_ms).clamp(initial_ms, max_ms);
    time::sleep(Duration::from_millis(delay)).await;
    *next_backoff_ms = delay.saturating_mul(2).min(max_ms);
}

fn push_sample(samples: &mut VecDeque<u64>, value: u64, cap: usize) {
    if samples.len() >= cap {
        let _ = samples.pop_front();
    }
    samples.push_back(value);
}

fn percentile(values: &[u64], q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

fn increment_atomic_usize(counter: &AtomicUsize) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn decrement_atomic_usize(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn token_balance(owner: &str, mint: &str, amount: &str) -> Value {
        json!({
            "owner": owner,
            "mint": mint,
            "uiTokenAmount": {
                "uiAmountString": amount,
                "decimals": 6
            }
        })
    }

    #[test]
    fn infer_swap_prefers_sol_leg_with_lamport_delta() {
        let signer = "Leader111111111111111111111111111111111";
        let meta = json!({
            "preTokenBalances": [token_balance(signer, "TokenMintA", "0")],
            "postTokenBalances": [token_balance(signer, "TokenMintA", "100")],
            "preBalances": [2_000_000_000u64],
            "postBalances": [1_000_000_000u64]
        });

        let inferred = HeliusWsSource::infer_swap_from_json_balances(&meta, 0, signer)
            .expect("expected SOL buy inference");
        assert_eq!(inferred.0, SOL_MINT);
        assert!((inferred.1 - 1.0).abs() < 1e-9);
        assert_eq!(inferred.2, "TokenMintA");
        assert!((inferred.3 - 100.0).abs() < 1e-9);
    }

    #[test]
    fn infer_swap_drops_ambiguous_multi_output_tx() {
        let signer = "Leader111111111111111111111111111111111";
        let meta = json!({
            "preTokenBalances": [
                token_balance(signer, "TokenMintA", "0"),
                token_balance(signer, "TokenMintB", "0")
            ],
            "postTokenBalances": [
                token_balance(signer, "TokenMintA", "100"),
                token_balance(signer, "TokenMintB", "40")
            ],
            "preBalances": [2_000_000_000u64],
            "postBalances": [1_000_000_000u64]
        });

        let inferred = HeliusWsSource::infer_swap_from_json_balances(&meta, 0, signer);
        assert!(inferred.is_none(), "ambiguous multi-hop should be rejected");
    }

    #[test]
    fn reorder_releases_oldest_slot_signature() {
        let telemetry = Arc::new(IngestionTelemetry::default());
        let runtime_config = Arc::new(HeliusRuntimeConfig {
            ws_url: "wss://example".to_string(),
            http_urls: vec!["https://example".to_string()],
            http_url_rr: AtomicUsize::new(0),
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            tx_fetch_retries: 1,
            tx_fetch_retry_delay_ms: 50,
            seen_signatures_limit: 100,
            interested_program_ids: HashSet::new(),
            raydium_program_ids: HashSet::new(),
            pumpswap_program_ids: HashSet::new(),
            http_client: Client::new(),
            telemetry,
        });

        let mut source = HeliusWsSource {
            runtime_config,
            fetch_concurrency: 1,
            ws_queue_capacity: 16,
            output_queue_capacity: 16,
            reorder_hold_ms: 1,
            reorder_max_buffer: 8,
            telemetry_report_seconds: 30,
            pipeline: None,
            reorder_buffer: BTreeMap::new(),
        };

        source.push_reorder_entry(FetchedObservation {
            raw: RawSwapObservation {
                signature: "b".to_string(),
                slot: 20,
                signer: "w".to_string(),
                token_in: SOL_MINT.to_string(),
                token_out: "t".to_string(),
                amount_in: 1.0,
                amount_out: 100.0,
                program_ids: vec![],
                dex_hint: "raydium".to_string(),
                ts_utc: Utc::now(),
            },
            arrival_seq: 2,
            fetch_latency_ms: 10,
        });
        source.push_reorder_entry(FetchedObservation {
            raw: RawSwapObservation {
                signature: "a".to_string(),
                slot: 10,
                signer: "w".to_string(),
                token_in: SOL_MINT.to_string(),
                token_out: "t".to_string(),
                amount_in: 1.0,
                amount_out: 100.0,
                program_ids: vec![],
                dex_hint: "raydium".to_string(),
                ts_utc: Utc::now(),
            },
            arrival_seq: 1,
            fetch_latency_ms: 10,
        });

        // Force early release via buffer cap branch.
        source.reorder_max_buffer = 1;
        let first = source
            .pop_ready_observation()
            .expect("first observation should be released");
        assert_eq!(first.slot, 10);
        assert_eq!(first.signature, "a");
    }

    #[test]
    fn reorder_uses_arrival_sequence_within_same_slot() {
        let telemetry = Arc::new(IngestionTelemetry::default());
        let runtime_config = Arc::new(HeliusRuntimeConfig {
            ws_url: "wss://example".to_string(),
            http_urls: vec!["https://example".to_string()],
            http_url_rr: AtomicUsize::new(0),
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            tx_fetch_retries: 1,
            tx_fetch_retry_delay_ms: 50,
            seen_signatures_limit: 100,
            interested_program_ids: HashSet::new(),
            raydium_program_ids: HashSet::new(),
            pumpswap_program_ids: HashSet::new(),
            http_client: Client::new(),
            telemetry,
        });

        let mut source = HeliusWsSource {
            runtime_config,
            fetch_concurrency: 1,
            ws_queue_capacity: 16,
            output_queue_capacity: 16,
            reorder_hold_ms: 1,
            reorder_max_buffer: 8,
            telemetry_report_seconds: 30,
            pipeline: None,
            reorder_buffer: BTreeMap::new(),
        };

        source.push_reorder_entry(FetchedObservation {
            raw: RawSwapObservation {
                // Lexicographically smaller signature should NOT win inside same slot.
                signature: "A-signature".to_string(),
                slot: 50,
                signer: "wallet".to_string(),
                token_in: SOL_MINT.to_string(),
                token_out: "mint".to_string(),
                amount_in: 1.0,
                amount_out: 100.0,
                program_ids: vec![],
                dex_hint: "raydium".to_string(),
                ts_utc: Utc::now(),
            },
            arrival_seq: 2,
            fetch_latency_ms: 5,
        });
        source.push_reorder_entry(FetchedObservation {
            raw: RawSwapObservation {
                signature: "Z-signature".to_string(),
                slot: 50,
                signer: "wallet".to_string(),
                token_in: SOL_MINT.to_string(),
                token_out: "mint".to_string(),
                amount_in: 1.0,
                amount_out: 100.0,
                program_ids: vec![],
                dex_hint: "raydium".to_string(),
                ts_utc: Utc::now(),
            },
            arrival_seq: 1,
            fetch_latency_ms: 5,
        });

        source.reorder_max_buffer = 1;
        let first = source.pop_ready_observation().expect("first observation");
        assert_eq!(first.signature, "Z-signature");
    }

    #[test]
    fn parse_logs_notification_ignores_subscribe_ack() {
        let ack = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "result": 99,
        })
        .to_string();

        assert!(HeliusWsSource::parse_logs_notification(&ack).is_none());
    }
}
