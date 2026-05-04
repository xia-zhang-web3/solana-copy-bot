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
