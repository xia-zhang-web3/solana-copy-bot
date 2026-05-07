use super::*;

pub(in crate::source) struct YellowstonePipeline {
    pub(in crate::source) output_queue: Arc<RawObservationQueue>,
    pub(in crate::source) output_queue_depth: Arc<AtomicUsize>,
    pub(in crate::source) stream_task: JoinHandle<()>,
}

impl Drop for YellowstonePipeline {
    fn drop(&mut self) {
        self.stream_task.abort();
    }
}

pub(in crate::source) struct YellowstoneRuntimeConfig {
    pub(in crate::source) grpc_url: String,
    pub(in crate::source) x_token: String,
    pub(in crate::source) connect_timeout_ms: u64,
    pub(in crate::source) subscribe_timeout_ms: u64,
    pub(in crate::source) reconnect_initial_ms: u64,
    pub(in crate::source) reconnect_max_ms: u64,
    pub(in crate::source) stream_buffer_capacity: usize,
    pub(in crate::source) seen_signatures_limit: usize,
    pub(in crate::source) seen_signatures_ttl: Duration,
    pub(in crate::source) interested_program_ids: HashSet<String>,
    pub(in crate::source) raydium_program_ids: HashSet<String>,
    pub(in crate::source) pumpswap_program_ids: HashSet<String>,
    pub(in crate::source) telemetry: Arc<IngestionTelemetry>,
}

pub struct YellowstoneGrpcSource {
    pub(in crate::source) runtime_config: Arc<YellowstoneRuntimeConfig>,
    pub(in crate::source) queue_overflow_policy: QueueOverflowPolicy,
    pub(in crate::source) reorder: ReorderBuffer,
    pub(in crate::source) telemetry_report_seconds: u64,
    pub(in crate::source) pipeline: Option<YellowstonePipeline>,
}

pub(in crate::source) enum YellowstoneRecvOutcome {
    Item(FetchedObservation),
    QueueClosed,
    TimedOut,
}

pub(in crate::source) enum YellowstoneParsedUpdate {
    Observation(RawSwapObservation),
    Ping,
}
