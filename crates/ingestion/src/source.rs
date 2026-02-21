use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::IngestionConfig;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Interval};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update, CommitmentLevel, CompiledInstruction, InnerInstruction,
    Message as SolMessage, SubscribeRequest, SubscribeRequestFilterTransactions,
    SubscribeRequestPing, SubscribeUpdate, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo, TransactionStatusMeta, UiTokenAmount,
};

mod core;
mod queue;
mod rate_limit;
mod reorder;
mod telemetry;

use self::core::{
    compute_retry_delay, decrement_atomic_usize, effective_per_endpoint_rps_limit,
    increment_atomic_usize, is_seen_signature, mark_seen_signature,
    normalize_program_ids_or_fallback, parse_retry_after, prune_seen_signatures,
    sleep_with_backoff,
};
use self::queue::{OverflowQueue, QueueOverflowPolicy, QueuePushResult};
use self::rate_limit::{HeliusEndpoint, TokenBucketLimiter};
use self::reorder::{ReorderBuffer, ReorderRelease};
#[cfg(test)]
use self::telemetry::classify_parse_reject_reason;
use self::telemetry::IngestionTelemetry;

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

#[derive(Debug, Clone, Copy)]
pub struct IngestionRuntimeSnapshot {
    pub ts_utc: DateTime<Utc>,
    pub ws_notifications_enqueued: u64,
    pub ws_notifications_replaced_oldest: u64,
    pub grpc_message_total: u64,
    pub grpc_transaction_updates_total: u64,
    pub parse_rejected_total: u64,
    pub grpc_decode_errors: u64,
    pub rpc_429: u64,
    pub rpc_5xx: u64,
    pub ingestion_lag_ms_p95: u64,
}

pub enum IngestionSource {
    Mock(MockSource),
    HeliusWs(HeliusWsSource),
    YellowstoneGrpc(YellowstoneGrpcSource),
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
            "yellowstone" | "yellowstone_grpc" => {
                Ok(Self::YellowstoneGrpc(YellowstoneGrpcSource::new(config)?))
            }
            other => Err(anyhow!("unknown ingestion.source: {other}")),
        }
    }

    pub async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        match self {
            Self::Mock(source) => source.next_observation().await,
            Self::HeliusWs(source) => source.next_observation().await,
            Self::YellowstoneGrpc(source) => source.next_observation().await,
        }
    }

    pub fn runtime_snapshot(&self) -> Option<IngestionRuntimeSnapshot> {
        match self {
            Self::Mock(_) => None,
            Self::HeliusWs(source) => Some(source.runtime_snapshot()),
            Self::YellowstoneGrpc(source) => Some(source.runtime_snapshot()),
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
    enqueued_at: Instant,
}

#[derive(Debug, Clone)]
struct FetchedObservation {
    raw: RawSwapObservation,
    arrival_seq: u64,
    fetch_latency_ms: u64,
}

type HeliusWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const WS_IDLE_TIMEOUT_SECS: u64 = 45;
const TELEMETRY_SAMPLE_CAPACITY: usize = 4096;

type NotificationQueue = OverflowQueue<LogsNotification>;
type RawObservationQueue = OverflowQueue<FetchedObservation>;

#[derive(Debug, Clone)]
struct SeenSignatureEntry {
    signature: String,
    seen_at: Instant,
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
    http_endpoints: Vec<Arc<HeliusEndpoint>>,
    http_endpoint_rr: AtomicUsize,
    global_http_limiter: Option<Arc<TokenBucketLimiter>>,
    reconnect_initial_ms: u64,
    reconnect_max_ms: u64,
    tx_fetch_retries: u32,
    tx_fetch_retry_base_ms: u64,
    tx_fetch_retry_max_ms: u64,
    tx_fetch_retry_jitter_ms: u64,
    seen_signatures_limit: usize,
    seen_signatures_ttl: Duration,
    prefetch_stale_drop: Option<Duration>,
    interested_program_ids: HashSet<String>,
    raydium_program_ids: HashSet<String>,
    pumpswap_program_ids: HashSet<String>,
    http_client: Client,
    telemetry: Arc<IngestionTelemetry>,
}

impl HeliusRuntimeConfig {
    fn next_http_endpoint(&self) -> Arc<HeliusEndpoint> {
        let len = self.http_endpoints.len();
        let index = self.http_endpoint_rr.fetch_add(1, Ordering::Relaxed) % len;
        Arc::clone(&self.http_endpoints[index])
    }
}

pub struct HeliusWsSource {
    runtime_config: Arc<HeliusRuntimeConfig>,
    fetch_concurrency: usize,
    ws_queue_capacity: usize,
    queue_overflow_policy: QueueOverflowPolicy,
    output_queue_capacity: usize,
    reorder: ReorderBuffer,
    telemetry_report_seconds: u64,
    pipeline: Option<HeliusPipeline>,
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
            if !(candidate.starts_with("http://") || candidate.starts_with("https://")) {
                warn!(
                    url = %candidate,
                    "dropping ingestion HTTP URL without explicit http(s):// prefix"
                );
                continue;
            }

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

        let endpoint_rps_limit = effective_per_endpoint_rps_limit(
            config.per_endpoint_rpc_rps_limit,
            config.global_rpc_rps_limit,
            http_urls.len(),
        );
        if endpoint_rps_limit != config.per_endpoint_rpc_rps_limit {
            warn!(
                configured_per_endpoint_rps = config.per_endpoint_rpc_rps_limit,
                effective_per_endpoint_rps = endpoint_rps_limit,
                global_rps = config.global_rpc_rps_limit,
                endpoint_count = http_urls.len(),
                "adjusted per-endpoint RPC limiter to avoid self-throttling with a single endpoint"
            );
        }
        let endpoint_burst = endpoint_rps_limit.max(1);
        let http_endpoints = http_urls
            .into_iter()
            .map(|url| {
                Arc::new(HeliusEndpoint {
                    url,
                    limiter: TokenBucketLimiter::new(endpoint_rps_limit, endpoint_burst),
                })
            })
            .collect::<Vec<_>>();
        let global_rps_limit = config.global_rpc_rps_limit;
        let global_http_limiter =
            TokenBucketLimiter::new(global_rps_limit, global_rps_limit.max(1));
        let raw_queue_policy = config.queue_overflow_policy.trim();
        let queue_overflow_policy = QueueOverflowPolicy::parse(raw_queue_policy);
        let normalized_queue_policy = raw_queue_policy.to_ascii_lowercase();
        if !raw_queue_policy.is_empty()
            && normalized_queue_policy != "block"
            && normalized_queue_policy != "drop_oldest"
            && normalized_queue_policy != "drop-oldest"
        {
            warn!(
                policy = %raw_queue_policy,
                "unknown ingestion.queue_overflow_policy; falling back to block"
            );
        }

        let runtime_config = HeliusRuntimeConfig {
            ws_url: config.helius_ws_url.clone(),
            http_endpoints,
            http_endpoint_rr: AtomicUsize::new(0),
            global_http_limiter,
            reconnect_initial_ms: config.reconnect_initial_ms.max(200),
            reconnect_max_ms: config
                .reconnect_max_ms
                .max(config.reconnect_initial_ms.max(200)),
            tx_fetch_retries: config.tx_fetch_retries,
            tx_fetch_retry_base_ms: config.tx_fetch_retry_delay_ms.max(50),
            tx_fetch_retry_max_ms: config
                .tx_fetch_retry_max_ms
                .max(config.tx_fetch_retry_delay_ms.max(50)),
            tx_fetch_retry_jitter_ms: config.tx_fetch_retry_jitter_ms,
            seen_signatures_limit: config.seen_signatures_limit.max(500),
            seen_signatures_ttl: Duration::from_millis(config.seen_signatures_ttl_ms.max(1_000)),
            prefetch_stale_drop: if config.prefetch_stale_drop_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(config.prefetch_stale_drop_ms.max(1)))
            },
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
            queue_overflow_policy,
            output_queue_capacity: config.output_queue_capacity.max(64),
            reorder: ReorderBuffer::new(
                config.reorder_hold_ms.max(1),
                config.reorder_max_buffer.max(16),
            ),
            telemetry_report_seconds: config.telemetry_report_seconds.max(5),
            pipeline: None,
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
                .http_endpoints
                .iter()
                .any(|endpoint| endpoint.url.contains("REPLACE_ME"))
            || self.runtime_config.http_endpoints.is_empty()
        {
            return Err(anyhow!(
                "configure ingestion.helius_ws_url and ingestion.helius_http_url / ingestion.helius_http_urls with real API key(s)"
            ));
        }

        let notification_queue = Arc::new(NotificationQueue::new(self.ws_queue_capacity));
        let (out_tx, out_rx) = mpsc::channel::<FetchedObservation>(self.output_queue_capacity);
        let ws_to_fetch_depth = Arc::new(AtomicUsize::new(0));
        let fetch_to_output_depth = Arc::new(AtomicUsize::new(0));

        let ws_reader_task = {
            let runtime_config = Arc::clone(&self.runtime_config);
            let notification_queue = Arc::clone(&notification_queue);
            let ws_to_fetch_depth = Arc::clone(&ws_to_fetch_depth);
            let queue_overflow_policy = self.queue_overflow_policy;
            tokio::spawn(async move {
                ws_reader_loop(
                    runtime_config,
                    notification_queue,
                    ws_to_fetch_depth,
                    queue_overflow_policy,
                )
                .await;
            })
        };

        let mut fetcher_tasks = Vec::with_capacity(self.fetch_concurrency);
        for worker_id in 0..self.fetch_concurrency {
            let runtime_config = Arc::clone(&self.runtime_config);
            let notification_queue = Arc::clone(&notification_queue);
            let out_tx = out_tx.clone();
            let ws_to_fetch_depth = Arc::clone(&ws_to_fetch_depth);
            let fetch_to_output_depth = Arc::clone(&fetch_to_output_depth);
            fetcher_tasks.push(tokio::spawn(async move {
                fetch_worker_loop(
                    worker_id,
                    runtime_config,
                    notification_queue,
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
        self.reorder.push(fetched);
        self.runtime_config
            .telemetry
            .note_reorder_buffer_size(self.reorder.len());
    }

    fn pop_ready_observation(&mut self) -> Option<RawSwapObservation> {
        self.reorder
            .pop_ready()
            .map(|release| self.apply_reorder_release(release))
    }

    fn pop_earliest_observation(&mut self) -> Option<RawSwapObservation> {
        self.reorder
            .pop_earliest()
            .map(|release| self.apply_reorder_release(release))
    }

    fn reorder_wait_duration(&self) -> Option<Duration> {
        self.reorder.wait_duration()
    }

    fn apply_reorder_release(&self, release: ReorderRelease) -> RawSwapObservation {
        self.runtime_config
            .telemetry
            .push_reorder_hold(release.hold_ms);
        self.runtime_config
            .telemetry
            .push_ingestion_lag(release.lag_ms);
        release.raw
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
            self.reorder.len(),
        );
    }

    fn runtime_snapshot(&self) -> IngestionRuntimeSnapshot {
        self.runtime_config.telemetry.snapshot()
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
            enqueued_at: Instant::now(),
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

struct YellowstonePipeline {
    output_queue: Arc<RawObservationQueue>,
    output_queue_depth: Arc<AtomicUsize>,
    stream_task: JoinHandle<()>,
}

impl Drop for YellowstonePipeline {
    fn drop(&mut self) {
        self.stream_task.abort();
    }
}

struct YellowstoneRuntimeConfig {
    grpc_url: String,
    x_token: String,
    connect_timeout_ms: u64,
    subscribe_timeout_ms: u64,
    reconnect_initial_ms: u64,
    reconnect_max_ms: u64,
    stream_buffer_capacity: usize,
    seen_signatures_limit: usize,
    seen_signatures_ttl: Duration,
    interested_program_ids: HashSet<String>,
    raydium_program_ids: HashSet<String>,
    pumpswap_program_ids: HashSet<String>,
    telemetry: Arc<IngestionTelemetry>,
}

pub struct YellowstoneGrpcSource {
    runtime_config: Arc<YellowstoneRuntimeConfig>,
    queue_overflow_policy: QueueOverflowPolicy,
    reorder: ReorderBuffer,
    telemetry_report_seconds: u64,
    pipeline: Option<YellowstonePipeline>,
}

enum YellowstoneRecvOutcome {
    Item(FetchedObservation),
    QueueClosed,
    TimedOut,
}

enum YellowstoneParsedUpdate {
    Observation(RawSwapObservation),
    Ping,
}

impl YellowstoneGrpcSource {
    pub fn new(config: &IngestionConfig) -> Result<Self> {
        let mut interested_program_ids: HashSet<String> =
            config.yellowstone_program_ids.iter().cloned().collect();
        if interested_program_ids.is_empty() {
            interested_program_ids.extend(config.subscribe_program_ids.iter().cloned());
        }
        if interested_program_ids.is_empty() {
            interested_program_ids.extend(config.raydium_program_ids.iter().cloned());
            interested_program_ids.extend(config.pumpswap_program_ids.iter().cloned());
        }

        if interested_program_ids.is_empty() {
            return Err(anyhow!(
                "yellowstone_grpc requires at least one program id (yellowstone_program_ids / subscribe_program_ids / raydium+pumpswap)"
            ));
        }

        let raw_queue_policy = config.queue_overflow_policy.trim();
        let queue_overflow_policy = QueueOverflowPolicy::parse(raw_queue_policy);
        let normalized_queue_policy = raw_queue_policy.to_ascii_lowercase();
        if !raw_queue_policy.is_empty()
            && normalized_queue_policy != "block"
            && normalized_queue_policy != "drop_oldest"
            && normalized_queue_policy != "drop-oldest"
        {
            warn!(
                policy = %raw_queue_policy,
                "unknown ingestion.queue_overflow_policy; falling back to block"
            );
        }

        let grpc_url = config.yellowstone_grpc_url.trim();
        if grpc_url.is_empty()
            || grpc_url.contains("REPLACE_ME")
            || !(grpc_url.starts_with("http://") || grpc_url.starts_with("https://"))
        {
            return Err(anyhow!(
                "yellowstone_grpc requires ingestion.yellowstone_grpc_url with explicit http(s):// endpoint"
            ));
        }

        let x_token = config.yellowstone_x_token.trim();
        if x_token.is_empty() || x_token.contains("REPLACE_ME") {
            return Err(anyhow!(
                "yellowstone_grpc requires ingestion.yellowstone_x_token (x-token auth)"
            ));
        }

        let runtime_config = YellowstoneRuntimeConfig {
            grpc_url: grpc_url.to_string(),
            x_token: x_token.to_string(),
            connect_timeout_ms: config.yellowstone_connect_timeout_ms.max(500),
            subscribe_timeout_ms: config.yellowstone_subscribe_timeout_ms.max(1_000),
            reconnect_initial_ms: config.yellowstone_reconnect_initial_ms.max(200),
            reconnect_max_ms: config
                .yellowstone_reconnect_max_ms
                .max(config.yellowstone_reconnect_initial_ms.max(200)),
            stream_buffer_capacity: config.yellowstone_stream_buffer_capacity.max(64),
            seen_signatures_limit: config.seen_signatures_limit.max(500),
            seen_signatures_ttl: Duration::from_millis(config.seen_signatures_ttl_ms.max(1_000)),
            interested_program_ids,
            raydium_program_ids: config.raydium_program_ids.iter().cloned().collect(),
            pumpswap_program_ids: config.pumpswap_program_ids.iter().cloned().collect(),
            telemetry: Arc::new(IngestionTelemetry::default()),
        };

        Ok(Self {
            runtime_config: Arc::new(runtime_config),
            queue_overflow_policy,
            reorder: ReorderBuffer::new(
                config.reorder_hold_ms.max(1),
                config.reorder_max_buffer.max(16),
            ),
            telemetry_report_seconds: config.telemetry_report_seconds.max(5),
            pipeline: None,
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
                YellowstoneRecvOutcome::Item(item) => {
                    self.push_reorder_entry(item);
                    self.maybe_report_pipeline_metrics();
                }
                YellowstoneRecvOutcome::TimedOut => {
                    self.maybe_report_pipeline_metrics();
                    continue;
                }
                YellowstoneRecvOutcome::QueueClosed => {
                    warn!("yellowstone stream queue closed; restarting pipeline");
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
            .map(|pipeline| pipeline.stream_task.is_finished())
            .unwrap_or(true);
        if needs_restart {
            if self.pipeline.is_some() {
                warn!("yellowstone ingestion pipeline became unhealthy; recreating stream task");
            }
            self.pipeline = Some(self.spawn_pipeline()?);
        }
        Ok(())
    }

    fn spawn_pipeline(&self) -> Result<YellowstonePipeline> {
        if self.runtime_config.grpc_url.trim().is_empty()
            || self.runtime_config.grpc_url.contains("REPLACE_ME")
            || self.runtime_config.x_token.trim().is_empty()
        {
            return Err(anyhow!(
                "configure ingestion.yellowstone_grpc_url and ingestion.yellowstone_x_token with real QuickNode credentials"
            ));
        }

        let output_queue = Arc::new(RawObservationQueue::new(
            self.runtime_config.stream_buffer_capacity,
        ));
        let output_queue_depth = Arc::new(AtomicUsize::new(0));
        let stream_task = {
            let runtime_config = Arc::clone(&self.runtime_config);
            let output_queue = Arc::clone(&output_queue);
            let output_queue_depth = Arc::clone(&output_queue_depth);
            let queue_overflow_policy = self.queue_overflow_policy;
            tokio::spawn(async move {
                yellowstone_stream_loop(
                    runtime_config,
                    output_queue,
                    output_queue_depth,
                    queue_overflow_policy,
                )
                .await;
            })
        };

        Ok(YellowstonePipeline {
            output_queue,
            output_queue_depth,
            stream_task,
        })
    }

    async fn recv_from_pipeline(&mut self, wait: Option<Duration>) -> YellowstoneRecvOutcome {
        let Some(pipeline) = self.pipeline.as_ref() else {
            return YellowstoneRecvOutcome::QueueClosed;
        };

        if let Some(wait) = wait {
            match time::timeout(wait, pipeline.output_queue.pop()).await {
                Ok(Some(item)) => {
                    decrement_atomic_usize(&pipeline.output_queue_depth);
                    YellowstoneRecvOutcome::Item(item)
                }
                Ok(None) => YellowstoneRecvOutcome::QueueClosed,
                Err(_) => YellowstoneRecvOutcome::TimedOut,
            }
        } else {
            match pipeline.output_queue.pop().await {
                Some(item) => {
                    decrement_atomic_usize(&pipeline.output_queue_depth);
                    YellowstoneRecvOutcome::Item(item)
                }
                None => YellowstoneRecvOutcome::QueueClosed,
            }
        }
    }

    fn push_reorder_entry(&mut self, fetched: FetchedObservation) {
        self.reorder.push(fetched);
        self.runtime_config
            .telemetry
            .note_reorder_buffer_size(self.reorder.len());
    }

    fn pop_ready_observation(&mut self) -> Option<RawSwapObservation> {
        self.reorder
            .pop_ready()
            .map(|release| self.apply_reorder_release(release))
    }

    fn pop_earliest_observation(&mut self) -> Option<RawSwapObservation> {
        self.reorder
            .pop_earliest()
            .map(|release| self.apply_reorder_release(release))
    }

    fn reorder_wait_duration(&self) -> Option<Duration> {
        self.reorder.wait_duration()
    }

    fn apply_reorder_release(&self, release: ReorderRelease) -> RawSwapObservation {
        self.runtime_config
            .telemetry
            .push_reorder_hold(release.hold_ms);
        self.runtime_config
            .telemetry
            .push_ingestion_lag(release.lag_ms);
        release.raw
    }

    fn maybe_report_pipeline_metrics(&self) {
        let queue_depth = self
            .pipeline
            .as_ref()
            .map(|pipeline| pipeline.output_queue_depth.load(Ordering::Relaxed))
            .unwrap_or(0);
        self.runtime_config.telemetry.maybe_report(
            self.telemetry_report_seconds,
            queue_depth,
            0,
            self.reorder.len(),
        );
    }

    fn runtime_snapshot(&self) -> IngestionRuntimeSnapshot {
        self.runtime_config.telemetry.snapshot()
    }
}

async fn yellowstone_stream_loop(
    runtime_config: Arc<YellowstoneRuntimeConfig>,
    output_queue: Arc<RawObservationQueue>,
    output_queue_depth: Arc<AtomicUsize>,
    queue_overflow_policy: QueueOverflowPolicy,
) {
    let mut next_backoff_ms = runtime_config.reconnect_initial_ms;
    let mut arrival_seq: u64 = 0;
    let mut seen_signatures_queue: VecDeque<SeenSignatureEntry> = VecDeque::new();
    let mut seen_signatures_map: HashMap<String, Instant> = HashMap::new();

    loop {
        let subscribe_request = build_yellowstone_subscribe_request(runtime_config.as_ref());
        let builder = match GeyserGrpcClient::build_from_shared(runtime_config.grpc_url.clone()) {
            Ok(builder) => builder,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, "invalid yellowstone endpoint");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };
        let builder = match builder.x_token(Some(runtime_config.x_token.as_str())) {
            Ok(builder) => builder,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, "invalid yellowstone x-token metadata");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };
        let use_tls = runtime_config
            .grpc_url
            .trim()
            .to_ascii_lowercase()
            .starts_with("https://");
        let builder = if use_tls {
            let tls_config = ClientTlsConfig::new().with_native_roots();
            match builder.tls_config(tls_config) {
                Ok(builder) => builder,
                Err(error) => {
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(error = ?error, "invalid yellowstone TLS config");
                    sleep_with_backoff(
                        &mut next_backoff_ms,
                        runtime_config.reconnect_initial_ms,
                        runtime_config.reconnect_max_ms,
                    )
                    .await;
                    continue;
                }
            }
        } else {
            builder
        };
        let mut client = match builder
            .connect_timeout(Duration::from_millis(runtime_config.connect_timeout_ms))
            .timeout(Duration::from_millis(runtime_config.subscribe_timeout_ms))
            .http2_adaptive_window(true)
            .tcp_nodelay(true)
            .connect()
            .await
        {
            Ok(client) => client,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = ?error, "failed connecting yellowstone endpoint");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };

        let (mut subscribe_tx, mut stream) = match client.subscribe().await {
            Ok(parts) => parts,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, "failed opening yellowstone subscription stream");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };
        if let Err(error) = subscribe_tx.send(subscribe_request).await {
            runtime_config
                .telemetry
                .reconnect_count
                .fetch_add(1, Ordering::Relaxed);
            warn!(error = %error, "failed sending yellowstone subscribe request");
            sleep_with_backoff(
                &mut next_backoff_ms,
                runtime_config.reconnect_initial_ms,
                runtime_config.reconnect_max_ms,
            )
            .await;
            continue;
        };
        next_backoff_ms = runtime_config.reconnect_initial_ms;

        loop {
            let next_message =
                time::timeout(Duration::from_secs(WS_IDLE_TIMEOUT_SECS), stream.next()).await;
            match next_message {
                Ok(Some(Ok(update))) => {
                    let is_transaction_update = matches!(
                        update.update_oneof.as_ref(),
                        Some(subscribe_update::UpdateOneof::Transaction(_))
                    );
                    runtime_config
                        .telemetry
                        .grpc_message_total
                        .fetch_add(1, Ordering::Relaxed);
                    if is_transaction_update {
                        runtime_config
                            .telemetry
                            .grpc_transaction_updates_total
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    match parse_yellowstone_update(update, runtime_config.as_ref()) {
                        Ok(Some(YellowstoneParsedUpdate::Observation(raw))) => {
                            let now = Instant::now();
                            prune_seen_signatures(
                                &mut seen_signatures_map,
                                &mut seen_signatures_queue,
                                runtime_config.seen_signatures_limit,
                                runtime_config.seen_signatures_ttl,
                                now,
                            );

                            if is_seen_signature(
                                &seen_signatures_map,
                                &raw.signature,
                                runtime_config.seen_signatures_ttl,
                                now,
                            ) {
                                continue;
                            }

                            arrival_seq = arrival_seq.saturating_add(1);
                            let signature = raw.signature.clone();
                            let fetched = FetchedObservation {
                                raw,
                                arrival_seq,
                                fetch_latency_ms: 0,
                            };

                            match output_queue.push(fetched, queue_overflow_policy).await {
                                Some(QueuePushResult::Enqueued { backpressured }) => {
                                    mark_seen_signature(
                                        &mut seen_signatures_map,
                                        &mut seen_signatures_queue,
                                        runtime_config.seen_signatures_limit,
                                        runtime_config.seen_signatures_ttl,
                                        signature,
                                        now,
                                    );
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_seen
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_enqueued
                                        .fetch_add(1, Ordering::Relaxed);
                                    if backpressured {
                                        runtime_config
                                            .telemetry
                                            .ws_notifications_backpressured
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    increment_atomic_usize(&output_queue_depth);
                                }
                                Some(QueuePushResult::ReplacedOldest) => {
                                    mark_seen_signature(
                                        &mut seen_signatures_map,
                                        &mut seen_signatures_queue,
                                        runtime_config.seen_signatures_limit,
                                        runtime_config.seen_signatures_ttl,
                                        signature,
                                        now,
                                    );
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_seen
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_enqueued
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_backpressured
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_replaced_oldest
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                None => {
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_dropped
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!("yellowstone output queue closed; stopping stream loop");
                                    output_queue.close().await;
                                    return;
                                }
                            }
                        }
                        Ok(Some(YellowstoneParsedUpdate::Ping)) => {
                            let ping_request = SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            };
                            if let Err(error) = subscribe_tx.send(ping_request).await {
                                runtime_config
                                    .telemetry
                                    .reconnect_count
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(error = %error, "failed sending yellowstone ping response");
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            runtime_config.telemetry.note_parse_rejected(&error);
                            debug!(error = %error, "failed parsing yellowstone transaction update");
                        }
                    }
                }
                Ok(Some(Err(error))) => {
                    runtime_config
                        .telemetry
                        .grpc_decode_errors
                        .fetch_add(1, Ordering::Relaxed);
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(error = %error, "yellowstone stream update error");
                    break;
                }
                Ok(None) => {
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!("yellowstone stream ended");
                    break;
                }
                Err(_) => {
                    runtime_config
                        .telemetry
                        .stream_gap_detected
                        .fetch_add(1, Ordering::Relaxed);
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        idle_timeout_seconds = WS_IDLE_TIMEOUT_SECS,
                        "yellowstone stream idle timeout; reconnecting"
                    );
                    break;
                }
            }
        }

        sleep_with_backoff(
            &mut next_backoff_ms,
            runtime_config.reconnect_initial_ms,
            runtime_config.reconnect_max_ms,
        )
        .await;
    }
}

fn build_yellowstone_subscribe_request(
    runtime_config: &YellowstoneRuntimeConfig,
) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "copybot-swaps".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: runtime_config
                .interested_program_ids
                .iter()
                .cloned()
                .collect(),
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn parse_yellowstone_update(
    update: SubscribeUpdate,
    runtime_config: &YellowstoneRuntimeConfig,
) -> Result<Option<YellowstoneParsedUpdate>> {
    let created_at = update.created_at.clone();
    let Some(update_oneof) = update.update_oneof else {
        return Ok(None);
    };
    match update_oneof {
        subscribe_update::UpdateOneof::Transaction(transaction_update) => {
            parse_yellowstone_transaction_update(transaction_update, created_at, runtime_config)
                .map(|raw| raw.map(YellowstoneParsedUpdate::Observation))
        }
        subscribe_update::UpdateOneof::Ping(_) => Ok(Some(YellowstoneParsedUpdate::Ping)),
        _ => Ok(None),
    }
}

fn parse_yellowstone_transaction_update(
    tx_update: SubscribeUpdateTransaction,
    created_at: Option<yellowstone_grpc_proto::prost_types::Timestamp>,
    runtime_config: &YellowstoneRuntimeConfig,
) -> Result<Option<RawSwapObservation>> {
    if tx_update.slot == 0 {
        return Err(anyhow!("missing slot in yellowstone update"));
    }
    let Some(tx_info) = tx_update.transaction else {
        return Err(anyhow!("missing status in yellowstone update"));
    };
    if tx_info.is_vote {
        return Ok(None);
    }

    let Some(meta) = tx_info.meta.as_ref() else {
        return Err(anyhow!("missing status in yellowstone update"));
    };
    if tx_meta_has_error(meta) {
        return Ok(None);
    }

    let Some(transaction) = tx_info.transaction.as_ref() else {
        return Err(anyhow!("missing signer in yellowstone update"));
    };
    let Some(message) = transaction.message.as_ref() else {
        return Err(anyhow!("missing signer in yellowstone update"));
    };

    let account_keys = proto_account_keys(message, meta);
    if account_keys.is_empty() {
        return Err(anyhow!("missing signer in yellowstone update"));
    }

    let signer_index = 0;
    let signer = account_keys.get(signer_index).cloned().unwrap_or_default();
    if signer.is_empty() {
        return Err(anyhow!("missing signer in yellowstone update"));
    }

    let program_ids = match normalize_program_ids_or_fallback(
        extract_program_ids_from_proto(message, meta, &account_keys),
        &runtime_config.interested_program_ids,
        runtime_config.telemetry.as_ref(),
        "missing program ids in yellowstone update",
    )? {
        Some(value) => value,
        None => return Ok(None),
    };

    let (token_in, amount_in, token_out, amount_out) =
        match infer_swap_from_proto_balances(meta, signer_index, &signer) {
            Some(value) => value,
            None => return Ok(None),
        };
    if amount_in <= 0.0 || amount_out <= 0.0 {
        return Ok(None);
    }

    let signature = decode_signature_from_proto(&tx_info)
        .ok_or_else(|| anyhow!("missing transaction signature in yellowstone update"))?;
    let logs = meta.log_messages.clone();
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        &runtime_config.raydium_program_ids,
        &runtime_config.pumpswap_program_ids,
    );

    let ts_utc = created_at
        .as_ref()
        .and_then(|timestamp| {
            if timestamp.nanos < 0 || timestamp.nanos >= 1_000_000_000 {
                return None;
            }
            DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        })
        .unwrap_or_else(Utc::now);

    Ok(Some(RawSwapObservation {
        signature,
        slot: tx_update.slot,
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

fn tx_meta_has_error(meta: &TransactionStatusMeta) -> bool {
    meta.err.as_ref().is_some_and(|err| !err.err.is_empty())
}

fn decode_signature_from_proto(tx_info: &SubscribeUpdateTransactionInfo) -> Option<String> {
    if !tx_info.signature.is_empty() {
        return Some(bs58::encode(&tx_info.signature).into_string());
    }
    tx_info
        .transaction
        .as_ref()
        .and_then(|tx| tx.signatures.first())
        .map(|sig| bs58::encode(sig).into_string())
}

fn proto_account_keys(message: &SolMessage, meta: &TransactionStatusMeta) -> Vec<String> {
    let mut out = message
        .account_keys
        .iter()
        .map(|raw| bs58::encode(raw).into_string())
        .collect::<Vec<_>>();
    out.extend(
        meta.loaded_writable_addresses
            .iter()
            .map(|raw| bs58::encode(raw).into_string()),
    );
    out.extend(
        meta.loaded_readonly_addresses
            .iter()
            .map(|raw| bs58::encode(raw).into_string()),
    );
    out
}

fn extract_program_ids_from_proto(
    message: &SolMessage,
    meta: &TransactionStatusMeta,
    account_keys: &[String],
) -> HashSet<String> {
    let mut set = HashSet::new();
    for instruction in &message.instructions {
        if let Some(program) =
            decode_program_id_from_compiled_instruction(instruction, account_keys)
        {
            set.insert(program);
        }
    }
    for group in &meta.inner_instructions {
        for instruction in &group.instructions {
            if let Some(program) =
                decode_program_id_from_inner_instruction(instruction, account_keys)
            {
                set.insert(program);
            }
        }
    }
    for log in &meta.log_messages {
        if let Some(program_id) = HeliusWsSource::extract_program_id_from_log(log) {
            set.insert(program_id);
        }
    }
    set
}

fn decode_program_id_from_compiled_instruction(
    instruction: &CompiledInstruction,
    account_keys: &[String],
) -> Option<String> {
    account_keys
        .get(instruction.program_id_index as usize)
        .cloned()
}

fn decode_program_id_from_inner_instruction(
    instruction: &InnerInstruction,
    account_keys: &[String],
) -> Option<String> {
    account_keys
        .get(instruction.program_id_index as usize)
        .cloned()
}

fn infer_swap_from_proto_balances(
    meta: &TransactionStatusMeta,
    signer_index: usize,
    signer: &str,
) -> Option<(String, f64, String, f64)> {
    const TOKEN_EPS: f64 = 1e-12;
    const SOL_EPS: f64 = 1e-8;
    let mut mint_deltas: HashMap<String, f64> = HashMap::new();

    for item in &meta.pre_token_balances {
        if item.owner == signer {
            let Some(amount) = parse_proto_ui_amount(item.ui_token_amount.as_ref()) else {
                continue;
            };
            *mint_deltas.entry(item.mint.clone()).or_default() -= amount;
        }
    }
    for item in &meta.post_token_balances {
        if item.owner == signer {
            let Some(amount) = parse_proto_ui_amount(item.ui_token_amount.as_ref()) else {
                continue;
            };
            *mint_deltas.entry(item.mint.clone()).or_default() += amount;
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
    token_in_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    token_out_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let sol_token_delta = mint_deltas.get(SOL_MINT).copied().unwrap_or(0.0);
    if sol_token_delta < -TOKEN_EPS {
        if let Some((out_mint, out_amt)) =
            HeliusWsSource::dominant_non_sol_leg(&token_out_candidates)
        {
            return Some((
                SOL_MINT.to_string(),
                sol_token_delta.abs(),
                out_mint,
                out_amt,
            ));
        }
    }
    if sol_token_delta > TOKEN_EPS {
        if let Some((in_mint, in_amt)) = HeliusWsSource::dominant_non_sol_leg(&token_in_candidates)
        {
            return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_token_delta));
        }
    }

    let sol_delta = signer_sol_delta_from_proto(meta, signer_index).unwrap_or(0.0);
    if sol_delta < -SOL_EPS {
        if let Some((out_mint, out_amt)) =
            HeliusWsSource::dominant_non_sol_leg(&token_out_candidates)
        {
            return Some((SOL_MINT.to_string(), sol_delta.abs(), out_mint, out_amt));
        }
    }
    if sol_delta > SOL_EPS {
        if let Some((in_mint, in_amt)) = HeliusWsSource::dominant_non_sol_leg(&token_in_candidates)
        {
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

fn parse_proto_ui_amount(ui_amount: Option<&UiTokenAmount>) -> Option<f64> {
    let ui_amount = ui_amount?;
    if !ui_amount.ui_amount_string.is_empty() {
        return ui_amount.ui_amount_string.parse::<f64>().ok();
    }
    if !ui_amount.amount.is_empty() {
        let raw = ui_amount.amount.parse::<f64>().ok()?;
        return Some(raw / 10f64.powi(ui_amount.decimals as i32));
    }
    if ui_amount.ui_amount.is_finite() {
        return Some(ui_amount.ui_amount);
    }
    None
}

fn signer_sol_delta_from_proto(meta: &TransactionStatusMeta, signer_index: usize) -> Option<f64> {
    let pre_sol = *meta.pre_balances.get(signer_index)? as f64 / 1_000_000_000.0;
    let post_sol = *meta.post_balances.get(signer_index)? as f64 / 1_000_000_000.0;
    Some(post_sol - pre_sol)
}

async fn ws_reader_loop(
    runtime_config: Arc<HeliusRuntimeConfig>,
    notification_queue: Arc<NotificationQueue>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    queue_overflow_policy: QueueOverflowPolicy,
) {
    let mut request_id: u64 = 1000;
    let mut arrival_seq: u64 = 0;
    let mut ws: Option<HeliusWsStream> = None;
    let mut next_backoff_ms = runtime_config.reconnect_initial_ms;
    let mut seen_signatures_queue: VecDeque<SeenSignatureEntry> = VecDeque::new();
    let mut seen_signatures_map: HashMap<String, Instant> = HashMap::new();

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
                    let now = Instant::now();
                    prune_seen_signatures(
                        &mut seen_signatures_map,
                        &mut seen_signatures_queue,
                        runtime_config.seen_signatures_limit,
                        runtime_config.seen_signatures_ttl,
                        now,
                    );
                    if is_seen_signature(
                        &seen_signatures_map,
                        &notification.signature,
                        runtime_config.seen_signatures_ttl,
                        now,
                    ) {
                        continue;
                    }

                    arrival_seq = arrival_seq.saturating_add(1);
                    notification.arrival_seq = arrival_seq;
                    notification.enqueued_at = now;
                    let signature = notification.signature.clone();

                    match notification_queue
                        .push(notification, queue_overflow_policy)
                        .await
                    {
                        Some(QueuePushResult::Enqueued { backpressured }) => {
                            mark_seen_signature(
                                &mut seen_signatures_map,
                                &mut seen_signatures_queue,
                                runtime_config.seen_signatures_limit,
                                runtime_config.seen_signatures_ttl,
                                signature,
                                now,
                            );
                            if backpressured {
                                runtime_config
                                    .telemetry
                                    .ws_notifications_backpressured
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            increment_atomic_usize(&ws_to_fetch_depth);
                            runtime_config
                                .telemetry
                                .ws_notifications_enqueued
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Some(QueuePushResult::ReplacedOldest) => {
                            mark_seen_signature(
                                &mut seen_signatures_map,
                                &mut seen_signatures_queue,
                                runtime_config.seen_signatures_limit,
                                runtime_config.seen_signatures_ttl,
                                signature,
                                now,
                            );
                            runtime_config
                                .telemetry
                                .ws_notifications_backpressured
                                .fetch_add(1, Ordering::Relaxed);
                            runtime_config
                                .telemetry
                                .ws_notifications_replaced_oldest
                                .fetch_add(1, Ordering::Relaxed);
                            runtime_config
                                .telemetry
                                .ws_notifications_enqueued
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            runtime_config
                                .telemetry
                                .ws_notifications_dropped
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(
                                policy = queue_overflow_policy.as_str(),
                                "notification queue closed; stopping ws reader"
                            );
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
    notification_queue: Arc<NotificationQueue>,
    out_tx: mpsc::Sender<FetchedObservation>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    fetch_to_output_depth: Arc<AtomicUsize>,
) {
    loop {
        let notification = notification_queue.pop().await;

        let Some(notification) = notification else {
            debug!(
                worker_id,
                "fetch worker exiting because notification queue is closed"
            );
            return;
        };
        decrement_atomic_usize(&ws_to_fetch_depth);

        if runtime_config
            .prefetch_stale_drop
            .is_some_and(|limit| notification.enqueued_at.elapsed() > limit)
        {
            runtime_config
                .telemetry
                .prefetch_stale_dropped
                .fetch_add(1, Ordering::Relaxed);
            runtime_config
                .telemetry
                .fetch_failed
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

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
                    .fetch_no_swap
                    .fetch_add(1, Ordering::Relaxed);
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
        http_endpoints = runtime_config.http_endpoints.len(),
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
        let endpoint = runtime_config.next_http_endpoint();
        let started = Instant::now();
        runtime_config
            .telemetry
            .fetch_inflight
            .fetch_add(1, Ordering::Relaxed);
        let result = fetch_swap_from_signature(
            runtime_config,
            endpoint.as_ref(),
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
            Ok(None) => return Ok(None),
            Err(fetch_error) => {
                let can_retry = fetch_error.retryable && attempt < runtime_config.tx_fetch_retries;
                let error = fetch_error.error;
                if !can_retry {
                    if fetch_error.retryable {
                        runtime_config
                            .telemetry
                            .fetch_retry_exhausted
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        runtime_config
                            .telemetry
                            .fetch_retry_terminal
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    warn!(
                        error = %error,
                        signature = %notification.signature,
                        attempt,
                        retryable = fetch_error.retryable,
                        "tx fetch attempt failed"
                    );
                    return Err(error);
                }

                runtime_config
                    .telemetry
                    .fetch_retry_attempts
                    .fetch_add(1, Ordering::Relaxed);
                let wait = compute_retry_delay(
                    runtime_config.tx_fetch_retry_base_ms,
                    runtime_config.tx_fetch_retry_max_ms,
                    runtime_config.tx_fetch_retry_jitter_ms,
                    attempt,
                    &notification.signature,
                    fetch_error.retry_after,
                );
                debug!(
                    error = %error,
                    signature = %notification.signature,
                    attempt,
                    wait_ms = wait.as_millis() as u64,
                    "retrying tx fetch attempt after backoff"
                );
                time::sleep(wait).await;
            }
        }
    }

    Ok(None)
}

#[derive(Debug)]
struct FetchAttemptError {
    error: anyhow::Error,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl FetchAttemptError {
    fn retryable(error: anyhow::Error, retry_after: Option<Duration>) -> Self {
        Self {
            error,
            retryable: true,
            retry_after,
        }
    }

    fn terminal(error: anyhow::Error) -> Self {
        Self {
            error,
            retryable: false,
            retry_after: None,
        }
    }
}

async fn fetch_swap_from_signature(
    runtime_config: &HeliusRuntimeConfig,
    endpoint: &HeliusEndpoint,
    signature: &str,
    slot_hint: u64,
    logs_hint: &[String],
) -> std::result::Result<Option<RawSwapObservation>, FetchAttemptError> {
    if let Some(global_limiter) = runtime_config.global_http_limiter.as_ref() {
        global_limiter.acquire().await;
    }
    if let Some(endpoint_limiter) = endpoint.limiter.as_ref() {
        endpoint_limiter.acquire().await;
    }

    let http_url = endpoint.url.as_str();
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
        .post(http_url)
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            FetchAttemptError::retryable(
                anyhow!("failed getTransaction POST for {signature} via {http_url}: {error}"),
                None,
            )
        })?;

    let status = response.status();
    let retry_after = parse_retry_after(&response);
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
    if status.as_u16() == 429 || status.is_server_error() {
        return Err(FetchAttemptError::retryable(
            anyhow!("retryable getTransaction status {status} for {signature} via {http_url}"),
            retry_after,
        ));
    }
    if !status.is_success() {
        return Err(FetchAttemptError::terminal(anyhow!(
            "non-success getTransaction status {status} for {signature} via {http_url}"
        )));
    }

    let response: Value = response.json().await.map_err(|error| {
        FetchAttemptError::terminal(anyhow!(
            "failed parsing getTransaction json for {signature} via {http_url}: {error}"
        ))
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
        .ok_or_else(|| {
            FetchAttemptError::terminal(anyhow!("missing signer in parsed account keys"))
        })?;

    let program_ids = match normalize_program_ids_or_fallback(
        HeliusWsSource::extract_program_ids(result, meta, logs_hint),
        &runtime_config.interested_program_ids,
        runtime_config.telemetry.as_ref(),
        "missing program ids in helius transaction update",
    )
    .map_err(FetchAttemptError::terminal)?
    {
        Some(value) => value,
        None => return Ok(None),
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
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

    fn test_runtime_config(telemetry: Arc<IngestionTelemetry>) -> Arc<HeliusRuntimeConfig> {
        Arc::new(HeliusRuntimeConfig {
            ws_url: "wss://example".to_string(),
            http_endpoints: vec![Arc::new(HeliusEndpoint {
                url: "https://example".to_string(),
                limiter: None,
            })],
            http_endpoint_rr: AtomicUsize::new(0),
            global_http_limiter: None,
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            tx_fetch_retries: 1,
            tx_fetch_retry_base_ms: 50,
            tx_fetch_retry_max_ms: 500,
            tx_fetch_retry_jitter_ms: 20,
            seen_signatures_limit: 100,
            seen_signatures_ttl: Duration::from_secs(120),
            prefetch_stale_drop: Some(Duration::from_secs(30)),
            interested_program_ids: HashSet::new(),
            raydium_program_ids: HashSet::new(),
            pumpswap_program_ids: HashSet::new(),
            http_client: Client::new(),
            telemetry,
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
    fn classify_parse_reject_reason_maps_known_patterns() {
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("missing slot in yellowstone update")),
            "missing_slot"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("missing status in yellowstone update")),
            "missing_status"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("missing signer in yellowstone update")),
            "missing_signer"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("missing program ids in yellowstone update")),
            "missing_program_ids"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("missing transaction signature in update")),
            "missing_signature"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("invalid timestamp nanos in payload")),
            "invalid_timestamp"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("failed balance inference for signer")),
            "invalid_balance_inference"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("account key index out of bounds")),
            "invalid_account_keys"
        );
        assert_eq!(
            classify_parse_reject_reason(&anyhow!("unexpected parser failure")),
            "other"
        );
    }

    #[test]
    fn note_parse_rejected_tracks_reason_breakdown() {
        let telemetry = IngestionTelemetry::default();
        telemetry.note_parse_rejected(&anyhow!("missing signer in yellowstone update"));
        telemetry.note_parse_rejected(&anyhow!("missing transaction signature in update"));
        telemetry.note_parse_rejected(&anyhow!("unclassified parser issue"));

        assert_eq!(telemetry.parse_rejected_total.load(Ordering::Relaxed), 3);
        let reasons = telemetry
            .parse_rejected_by_reason
            .lock()
            .expect("parse_rejected_by_reason mutex should be available");
        assert_eq!(reasons.get("missing_signer"), Some(&1));
        assert_eq!(reasons.get("missing_signature"), Some(&1));
        assert_eq!(reasons.get("other"), Some(&1));
    }

    #[test]
    fn note_parse_fallback_tracks_reason_breakdown() {
        let telemetry = IngestionTelemetry::default();
        telemetry.note_parse_fallback("missing_program_ids_fallback");
        telemetry.note_parse_fallback("missing_program_ids_fallback");

        let reasons = telemetry
            .parse_fallback_by_reason
            .lock()
            .expect("parse_fallback_by_reason mutex should be available");
        assert_eq!(reasons.get("missing_program_ids_fallback"), Some(&2));
    }

    #[test]
    fn normalize_program_ids_or_fallback_tracks_missing_program_ids_fallback() -> Result<()> {
        let telemetry = IngestionTelemetry::default();
        let interested = HashSet::from([String::from("prog-1")]);
        let normalized = normalize_program_ids_or_fallback(
            HashSet::new(),
            &interested,
            &telemetry,
            "missing program ids in test",
        )?;
        let normalized = normalized.expect("missing program ids should fallback to interested set");
        assert!(normalized.contains("prog-1"));
        let reasons = telemetry
            .parse_fallback_by_reason
            .lock()
            .expect("parse_fallback_by_reason mutex should be available");
        assert_eq!(reasons.get("missing_program_ids_fallback"), Some(&1));
        Ok(())
    }

    #[test]
    fn normalize_program_ids_or_fallback_drops_non_interested_programs() -> Result<()> {
        let telemetry = IngestionTelemetry::default();
        let interested = HashSet::from([String::from("prog-1")]);
        let extracted = HashSet::from([String::from("prog-2")]);
        let normalized = normalize_program_ids_or_fallback(
            extracted,
            &interested,
            &telemetry,
            "missing program ids in test",
        )?;
        assert!(
            normalized.is_none(),
            "non-interested program ids should be dropped"
        );
        let reasons = telemetry
            .parse_fallback_by_reason
            .lock()
            .expect("parse_fallback_by_reason mutex should be available");
        assert_eq!(
            reasons.get("missing_program_ids_fallback"),
            None,
            "drop path should not increment fallback counters"
        );
        Ok(())
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
        let runtime_config = test_runtime_config(telemetry);

        let mut source = HeliusWsSource {
            runtime_config,
            fetch_concurrency: 1,
            ws_queue_capacity: 16,
            queue_overflow_policy: QueueOverflowPolicy::Block,
            output_queue_capacity: 16,
            reorder: ReorderBuffer::new(1, 8),
            telemetry_report_seconds: 30,
            pipeline: None,
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
        source.reorder.set_max_buffer(1);
        let first = source
            .pop_ready_observation()
            .expect("first observation should be released");
        assert_eq!(first.slot, 10);
        assert_eq!(first.signature, "a");
    }

    #[test]
    fn reorder_uses_arrival_sequence_within_same_slot() {
        let telemetry = Arc::new(IngestionTelemetry::default());
        let runtime_config = test_runtime_config(telemetry);

        let mut source = HeliusWsSource {
            runtime_config,
            fetch_concurrency: 1,
            ws_queue_capacity: 16,
            queue_overflow_policy: QueueOverflowPolicy::Block,
            output_queue_capacity: 16,
            reorder: ReorderBuffer::new(1, 8),
            telemetry_report_seconds: 30,
            pipeline: None,
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

        source.reorder.set_max_buffer(1);
        let first = source.pop_ready_observation().expect("first observation");
        assert_eq!(first.signature, "Z-signature");
    }

    #[tokio::test]
    async fn notification_queue_drop_oldest_keeps_freshest_items() {
        let queue = NotificationQueue::new(2);
        let build = |signature: &str| LogsNotification {
            signature: signature.to_string(),
            slot: 1,
            arrival_seq: 0,
            logs: vec![],
            is_failed: false,
            enqueued_at: Instant::now(),
        };

        assert!(matches!(
            queue
                .push(build("sig-1"), QueueOverflowPolicy::Block)
                .await
                .expect("queue open"),
            QueuePushResult::Enqueued { .. }
        ));
        assert!(matches!(
            queue
                .push(build("sig-2"), QueueOverflowPolicy::Block)
                .await
                .expect("queue open"),
            QueuePushResult::Enqueued { .. }
        ));
        assert!(matches!(
            queue
                .push(build("sig-3"), QueueOverflowPolicy::DropOldest)
                .await
                .expect("queue open"),
            QueuePushResult::ReplacedOldest
        ));

        let first = queue.pop().await.expect("first item");
        let second = queue.pop().await.expect("second item");
        assert_eq!(first.signature, "sig-2");
        assert_eq!(second.signature, "sig-3");
    }

    #[test]
    fn dedupe_ttl_prunes_expired_signatures() {
        let mut seen_signatures_map: HashMap<String, Instant> = HashMap::new();
        let mut seen_signatures_queue: VecDeque<SeenSignatureEntry> = VecDeque::new();
        let ttl = Duration::from_millis(100);
        let now = Instant::now();
        mark_seen_signature(
            &mut seen_signatures_map,
            &mut seen_signatures_queue,
            16,
            ttl,
            "sig-1".to_string(),
            now,
        );
        assert!(is_seen_signature(
            &seen_signatures_map,
            "sig-1",
            ttl,
            now + Duration::from_millis(50)
        ));

        prune_seen_signatures(
            &mut seen_signatures_map,
            &mut seen_signatures_queue,
            16,
            ttl,
            now + Duration::from_millis(150),
        );
        assert!(!is_seen_signature(
            &seen_signatures_map,
            "sig-1",
            ttl,
            now + Duration::from_millis(150)
        ));
    }

    #[test]
    fn retry_delay_respects_retry_after_and_cap() {
        let delay =
            compute_retry_delay(100, 500, 50, 1, "signature-a", Some(Duration::from_secs(2)));
        assert!(delay >= Duration::from_secs(2));
        assert!(delay <= Duration::from_millis(2_050));
    }

    #[test]
    fn effective_per_endpoint_limit_avoids_single_endpoint_self_throttle() {
        assert_eq!(effective_per_endpoint_rps_limit(16, 45, 1), 45);
        assert_eq!(effective_per_endpoint_rps_limit(0, 45, 1), 45);
        assert_eq!(effective_per_endpoint_rps_limit(16, 45, 3), 16);
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

    #[test]
    fn yellowstone_subscribe_request_uses_confirmed_commitment_and_program_filters() {
        let mut interested = HashSet::new();
        interested.insert("Program1111111111111111111111111111111111".to_string());
        let runtime_config = YellowstoneRuntimeConfig {
            grpc_url: "https://example.quicknode.com:10000".to_string(),
            x_token: "token".to_string(),
            connect_timeout_ms: 5_000,
            subscribe_timeout_ms: 15_000,
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            stream_buffer_capacity: 512,
            seen_signatures_limit: 5_000,
            seen_signatures_ttl: Duration::from_secs(60),
            interested_program_ids: interested,
            raydium_program_ids: HashSet::new(),
            pumpswap_program_ids: HashSet::new(),
            telemetry: Arc::new(IngestionTelemetry::default()),
        };

        let request = build_yellowstone_subscribe_request(&runtime_config);
        assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
        let tx_filter = request
            .transactions
            .get("copybot-swaps")
            .expect("transaction filter should be present");
        assert_eq!(tx_filter.vote, Some(false));
        assert_eq!(tx_filter.failed, Some(false));
        assert_eq!(tx_filter.account_include.len(), 1);
    }

    #[test]
    fn infer_swap_from_proto_prefers_sol_leg_with_lamport_delta() {
        let signer = "Leader111111111111111111111111111111111";
        let pre_token = yellowstone_grpc_proto::prelude::TokenBalance {
            account_index: 0,
            mint: "TokenMintA".to_string(),
            ui_token_amount: Some(UiTokenAmount {
                ui_amount: 0.0,
                decimals: 6,
                amount: "0".to_string(),
                ui_amount_string: "0".to_string(),
            }),
            owner: signer.to_string(),
            program_id: String::new(),
        };
        let post_token = yellowstone_grpc_proto::prelude::TokenBalance {
            account_index: 0,
            mint: "TokenMintA".to_string(),
            ui_token_amount: Some(UiTokenAmount {
                ui_amount: 100.0,
                decimals: 6,
                amount: "100000000".to_string(),
                ui_amount_string: "100".to_string(),
            }),
            owner: signer.to_string(),
            program_id: String::new(),
        };
        let meta = TransactionStatusMeta {
            pre_balances: vec![2_000_000_000],
            post_balances: vec![1_000_000_000],
            pre_token_balances: vec![pre_token],
            post_token_balances: vec![post_token],
            ..Default::default()
        };

        let inferred = infer_swap_from_proto_balances(&meta, 0, signer)
            .expect("expected SOL->token inference");
        assert_eq!(inferred.0, SOL_MINT);
        assert_eq!(inferred.2, "TokenMintA");
        assert!((inferred.1 - 1.0).abs() < 1e-9);
        assert!((inferred.3 - 100.0).abs() < 1e-9);
    }
}
