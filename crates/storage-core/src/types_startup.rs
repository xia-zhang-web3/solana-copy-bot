use std::fmt;
use std::sync::Arc;
use std::time::Duration as StdDuration;

pub const SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES: i64 = 1_000;
pub const SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE: &str =
    "sqlite_startup_large_wal_checkpoint_truncate";
pub const SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES: u64 = 1024 * 1024 * 1024;

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
