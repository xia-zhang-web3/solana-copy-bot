#[derive(Debug, Clone, Copy, Default)]
pub struct FollowlistUpdateResult {
    pub activated: usize,
    pub deactivated: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedSwapCursorPage {
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteSnapshotRetryReason {
    Busy,
    Locked,
    BusyAndLocked,
}

impl SqliteSnapshotRetryReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::Locked => "locked",
            Self::BusyAndLocked => "busy_and_locked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteSnapshotDeferredReason {
    AttemptDurationBudgetExceeded,
}

impl SqliteSnapshotDeferredReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AttemptDurationBudgetExceeded => "attempt_duration_budget_exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteSnapshotSummary {
    pub duration_ms: u64,
    pub backup_step_count: usize,
    pub backup_retry_count: usize,
    pub busy_retry_count: usize,
    pub locked_retry_count: usize,
    pub retry_exhausted_reason: Option<SqliteSnapshotRetryReason>,
    pub deferred_reason: Option<SqliteSnapshotDeferredReason>,
    pub total_page_count: usize,
    pub remaining_page_count: usize,
    pub copied_page_count: usize,
}

impl Default for SqliteSnapshotSummary {
    fn default() -> Self {
        Self {
            duration_ms: 0,
            backup_step_count: 0,
            backup_retry_count: 0,
            busy_retry_count: 0,
            locked_retry_count: 0,
            retry_exhausted_reason: None,
            deferred_reason: None,
            total_page_count: 0,
            remaining_page_count: 0,
            copied_page_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqliteSnapshotOutcome {
    Written(SqliteSnapshotSummary),
    RetryableBusy(SqliteSnapshotSummary),
    Deferred(SqliteSnapshotSummary),
}

#[derive(Debug, Clone)]
pub struct SqliteSnapshotPolicy {
    pub busy_timeout: StdDuration,
    pub pages_per_step: i32,
    pub pause_between_steps: StdDuration,
    pub retry_backoff_ms: Vec<u64>,
    pub max_attempt_duration: Option<StdDuration>,
    pub pin_source_snapshot: bool,
}

impl Default for SqliteSnapshotPolicy {
    fn default() -> Self {
        Self {
            busy_timeout: StdDuration::from_millis(250),
            pages_per_step: 16,
            pause_between_steps: StdDuration::from_millis(25),
            retry_backoff_ms: vec![100, 300, 700],
            max_attempt_duration: Some(StdDuration::from_millis(90_000)),
            pin_source_snapshot: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteSnapshotSourceMetrics {
    pub page_size_bytes: usize,
    pub page_count: usize,
}
