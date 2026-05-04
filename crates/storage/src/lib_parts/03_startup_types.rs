#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteStartupLargeWalCheckpointSummary {
    pub threshold_bytes: u64,
    pub before_wal_bytes: u64,
    pub after_wal_bytes: u64,
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

fn sqlite_startup_large_wal_checkpoint_detail(
    summary: SqliteStartupLargeWalCheckpointSummary,
) -> String {
    format!(
        "threshold_bytes={} before_wal_bytes={} after_wal_bytes={} busy={} log_frames={} checkpointed_frames={}",
        summary.threshold_bytes,
        summary.before_wal_bytes,
        summary.after_wal_bytes,
        summary.busy,
        summary.log_frames,
        summary.checkpointed_frames
    )
}

fn sqlite_startup_large_wal_checkpoint_skip_detail(
    reason: &str,
    threshold_bytes: u64,
    before_wal_bytes: Option<u64>,
) -> String {
    format!(
        "reason={} threshold_bytes={} before_wal_bytes={}",
        reason,
        threshold_bytes,
        before_wal_bytes.unwrap_or(0)
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupStepOutcome {
    Started,
    Waiting,
    Completed,
    Failed,
    TimedOut,
    Skipped,
}

impl StartupStepOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupStepProgress {
    pub stage: &'static str,
    pub outcome: StartupStepOutcome,
    pub elapsed_ms: u64,
    pub budget_ms: Option<u64>,
    pub detail: Option<String>,
}

pub type StartupStepProgressReporter = Arc<dyn Fn(StartupStepProgress) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy)]
pub struct StartupStepRuntimePolicy {
    pub wait_log_interval: StdDuration,
    pub timeout: Option<StdDuration>,
    pub timeout_behavior: StartupStepTimeoutBehavior,
}

impl StartupStepRuntimePolicy {
    pub const fn new(wait_log_interval: StdDuration, timeout: Option<StdDuration>) -> Self {
        Self {
            wait_log_interval,
            timeout,
            timeout_behavior: StartupStepTimeoutBehavior::ReturnError,
        }
    }

    pub const fn with_timeout_behavior(
        mut self,
        timeout_behavior: StartupStepTimeoutBehavior,
    ) -> Self {
        self.timeout_behavior = timeout_behavior;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupStepTimeoutBehavior {
    ReturnError,
    Panic,
    AbortProcess,
}

impl StartupStepTimeoutBehavior {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReturnError => "return_error",
            Self::Panic => "panic",
            Self::AbortProcess => "abort_process",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupStepTimeout {
    pub stage: &'static str,
    pub elapsed_ms: u64,
    pub budget_ms: u64,
}

impl fmt::Display for StartupStepTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "startup step {} timed out after {}ms (budget={}ms)",
            self.stage, self.elapsed_ms, self.budget_ms
        )
    }
}

impl std::error::Error for StartupStepTimeout {}

#[derive(Debug, Clone, Copy, Default)]
pub struct SqliteContentionSnapshot {
    pub write_retry_total: u64,
    pub busy_error_total: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummary {
    pub deleted_rows: usize,
    pub batches: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummaryWithCompletion {
    pub deleted_rows: usize,
    pub batches: usize,
    pub completed_full_sweep: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SqliteStartupPolicy {
    pub open_step: StartupStepRuntimePolicy,
    pub pragma_step: StartupStepRuntimePolicy,
    pub large_wal_checkpoint_step: StartupStepRuntimePolicy,
    pub schema_bootstrap_step: StartupStepRuntimePolicy,
    pub migrations_scan_step: StartupStepRuntimePolicy,
    pub migrations_apply_step: StartupStepRuntimePolicy,
    pub large_wal_checkpoint_threshold_bytes: u64,
}

impl Default for SqliteStartupPolicy {
    fn default() -> Self {
        Self {
            open_step: StartupStepRuntimePolicy::new(
                StdDuration::from_secs(5),
                Some(StdDuration::from_secs(120)),
            ),
            pragma_step: StartupStepRuntimePolicy::new(
                StdDuration::from_secs(5),
                Some(StdDuration::from_secs(30)),
            ),
            large_wal_checkpoint_step: StartupStepRuntimePolicy::new(
                StdDuration::from_secs(5),
                Some(StdDuration::from_secs(15 * 60)),
            ),
            schema_bootstrap_step: StartupStepRuntimePolicy::new(
                StdDuration::from_secs(5),
                Some(StdDuration::from_secs(30)),
            ),
            migrations_scan_step: StartupStepRuntimePolicy::new(
                StdDuration::from_secs(5),
                Some(StdDuration::from_secs(30)),
            ),
            migrations_apply_step: StartupStepRuntimePolicy::new(
                StdDuration::from_secs(5),
                Some(StdDuration::from_secs(120)),
            ),
            large_wal_checkpoint_threshold_bytes:
                SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
        }
    }
}

pub struct SqliteStartupBootstrapResult {
    pub store: SqliteStore,
    pub applied_migrations: usize,
    pub deferred_migrations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryScoringBoundarySeedLot {
    pub buy_signature: String,
    pub wallet_id: String,
    pub token: String,
    pub qty: f64,
    pub cost_sol: f64,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryScoringBoundarySeedSnapshot {
    pub boundary_start_ts: DateTime<Utc>,
    pub boundary_cursor: DiscoveryRuntimeCursor,
    pub open_lots: Vec<DiscoveryScoringBoundarySeedLot>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryScoringSeedBoundaryInstallMarker {
    pub boundary_start_ts: DateTime<Utc>,
    pub boundary_cursor: DiscoveryRuntimeCursor,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiscoveryScoringBatchStageTimings {
    pub prepare_ms: u64,
    pub apply_ms: u64,
    pub rug_finalize_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiscoveryScoringCheckpointedBatchTimings {
    pub prepare_ms: u64,
    pub apply_ms: u64,
    pub progress_update_ms: u64,
}
