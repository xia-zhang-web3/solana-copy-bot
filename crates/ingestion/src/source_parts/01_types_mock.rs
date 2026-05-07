use super::*;

#[derive(Debug, Clone)]
pub struct RawSwapObservation {
    pub signature: String,
    pub slot: u64,
    pub signer: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub exact_amounts: Option<ExactSwapAmounts>,
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
    pub yellowstone_output_queue_depth: u64,
    pub yellowstone_output_queue_capacity: u64,
    pub yellowstone_output_queue_fill_ratio: f64,
    pub yellowstone_output_oldest_age_ms: u64,
}

pub enum IngestionSource {
    Mock(MockSource),
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
            "helius" | "helius_ws" => Err(anyhow!(
                "ingestion.source=helius_ws is no longer supported; use yellowstone_grpc"
            )),
            "yellowstone" | "yellowstone_grpc" => {
                Ok(Self::YellowstoneGrpc(YellowstoneGrpcSource::new(config)?))
            }
            other => Err(anyhow!("unknown ingestion.source: {other}")),
        }
    }

    pub async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        match self {
            Self::Mock(source) => source.next_observation().await,
            Self::YellowstoneGrpc(source) => source.next_observation().await,
        }
    }

    pub fn runtime_snapshot(&self) -> Option<IngestionRuntimeSnapshot> {
        match self {
            Self::Mock(_) => None,
            Self::YellowstoneGrpc(source) => Some(source.runtime_snapshot()),
        }
    }
}

pub(in crate::source) fn redacted_url_for_log(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        match Url::parse(trimmed) {
            Ok(mut parsed) => {
                let had_query = parsed.query().is_some();
                let had_password = parsed.password().is_some();
                let had_username = !parsed.username().is_empty();
                if had_username {
                    let _ = parsed.set_username("");
                }
                if had_password {
                    let _ = parsed.set_password(None);
                }
                if had_query {
                    parsed.set_query(None);
                }

                let sanitized = parsed.to_string();
                if had_query {
                    if let Some((base, fragment)) = sanitized.split_once('#') {
                        format!("{base}?<redacted>#{fragment}")
                    } else {
                        format!("{sanitized}?<redacted>")
                    }
                } else {
                    sanitized
                }
            }
            Err(_) => {
                if let Some((prefix, _)) = trimmed.split_once('?') {
                    format!("{prefix}?<redacted>")
                } else {
                    trimmed.to_string()
                }
            }
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
            exact_amounts: None,
            program_ids: vec![self.raydium_program_id.clone()],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        }))
    }
}

#[derive(Debug, Clone)]
pub(in crate::source) struct LogsNotification {
    pub(in crate::source) signature: String,
    pub(in crate::source) slot: u64,
    pub(in crate::source) arrival_seq: u64,
    pub(in crate::source) logs: Vec<String>,
    pub(in crate::source) is_failed: bool,
    pub(in crate::source) enqueued_at: Instant,
}

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::source) struct FetchedObservation {
    pub(in crate::source) raw: RawSwapObservation,
    pub(in crate::source) arrival_seq: u64,
    pub(in crate::source) fetch_latency_ms: u64,
    pub(in crate::source) enqueued_at: Instant,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::source) type HeliusWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(in crate::source) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(in crate::source) const WS_IDLE_TIMEOUT_SECS: u64 = 45;
pub(in crate::source) const TELEMETRY_SAMPLE_CAPACITY: usize = 4096;

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::source) type NotificationQueue = OverflowQueue<LogsNotification>;
pub(in crate::source) type RawObservationQueue = OverflowQueue<FetchedObservation>;

#[derive(Debug, Clone)]
pub(in crate::source) struct SeenSignatureEntry {
    pub(in crate::source) signature: String,
    pub(in crate::source) seen_at: Instant,
}

#[allow(dead_code)]
pub(in crate::source) struct HeliusPipeline {
    pub(in crate::source) output_rx: mpsc::Receiver<FetchedObservation>,
    pub(in crate::source) ws_to_fetch_depth: Arc<AtomicUsize>,
    pub(in crate::source) fetch_to_output_depth: Arc<AtomicUsize>,
    pub(in crate::source) ws_reader_task: JoinHandle<()>,
    pub(in crate::source) fetcher_tasks: Vec<JoinHandle<()>>,
}

impl Drop for HeliusPipeline {
    fn drop(&mut self) {
        self.ws_reader_task.abort();
        for task in &self.fetcher_tasks {
            task.abort();
        }
    }
}

#[allow(dead_code)]
pub(in crate::source) struct HeliusRuntimeConfig {
    pub(in crate::source) ws_url: String,
    pub(in crate::source) http_endpoints: Vec<Arc<HeliusEndpoint>>,
    pub(in crate::source) http_endpoint_rr: AtomicUsize,
    pub(in crate::source) global_http_limiter: Option<Arc<TokenBucketLimiter>>,
    pub(in crate::source) reconnect_initial_ms: u64,
    pub(in crate::source) reconnect_max_ms: u64,
    pub(in crate::source) tx_fetch_retries: u32,
    pub(in crate::source) tx_fetch_retry_base_ms: u64,
    pub(in crate::source) tx_fetch_retry_max_ms: u64,
    pub(in crate::source) tx_fetch_retry_jitter_ms: u64,
    pub(in crate::source) seen_signatures_limit: usize,
    pub(in crate::source) seen_signatures_ttl: Duration,
    pub(in crate::source) prefetch_stale_drop: Option<Duration>,
    pub(in crate::source) interested_program_ids: HashSet<String>,
    pub(in crate::source) raydium_program_ids: HashSet<String>,
    pub(in crate::source) pumpswap_program_ids: HashSet<String>,
    pub(in crate::source) http_client: Client,
    pub(in crate::source) telemetry: Arc<IngestionTelemetry>,
}

#[allow(dead_code)]
impl HeliusRuntimeConfig {
    pub(in crate::source) fn next_http_endpoint(&self) -> Arc<HeliusEndpoint> {
        let len = self.http_endpoints.len();
        let index = self.http_endpoint_rr.fetch_add(1, Ordering::Relaxed) % len;
        Arc::clone(&self.http_endpoints[index])
    }
}

#[allow(dead_code)]
pub struct HeliusWsSource {
    pub(in crate::source) runtime_config: Arc<HeliusRuntimeConfig>,
    pub(in crate::source) fetch_concurrency: usize,
    pub(in crate::source) ws_queue_capacity: usize,
    pub(in crate::source) queue_overflow_policy: QueueOverflowPolicy,
    pub(in crate::source) output_queue_capacity: usize,
    pub(in crate::source) reorder: ReorderBuffer,
    pub(in crate::source) telemetry_report_seconds: u64,
    pub(in crate::source) pipeline: Option<HeliusPipeline>,
}

#[allow(dead_code)]
pub(in crate::source) enum OutputRecvOutcome {
    Item(FetchedObservation),
    ChannelClosed,
    TimedOut,
}
