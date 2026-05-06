use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::backup::{Backup, StepResult};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

pub use copybot_core_types::{
    CopySignalRow, ExactSwapAmounts, Lamports, SignedLamports, TokenQualityCacheRow,
    TokenQualityRpcRow, TokenQuantity, WalletMetricRow, WalletUpsertRow,
    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
pub use copybot_storage_core::{
    report_startup_step_progress, run_observed_startup_step,
    run_observed_startup_step_with_completion_detail, SqliteStartupPolicy, StartupStepOutcome,
    StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy, StartupStepTimeout,
    StartupStepTimeoutBehavior, SHADOW_CLOSE_CONTEXT_MARKET,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, SHADOW_RISK_CONTEXT_MARKET,
    SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];
const SQLITE_SNAPSHOT_PAGES_PER_STEP: i32 = 16;
const SQLITE_SNAPSHOT_PAUSE_BETWEEN_STEPS_MS: u64 = 25;
const SQLITE_SNAPSHOT_BUSY_TIMEOUT_MS: u64 = 250;
const SQLITE_SNAPSHOT_DEFAULT_MAX_ATTEMPT_DURATION_MS: u64 = 90_000;
const DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS: i64 = 3;
pub(crate) const POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER: &str = "legacy_pre_cutover";
pub(crate) const POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER: &str = "exact_post_cutover";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
static SQLITE_WRITE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static SQLITE_BUSY_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

mod discovery;
mod discovery_scoring;
mod discovery_scoring_builder;
mod execution_orders;
mod history_retention;
mod market_data;
mod migrations;
mod pricing;
mod risk_metrics;
mod shadow;
mod sqlite_retry;
mod system_events;

pub use discovery_scoring_builder::{
    DiscoveryScoringBoundaryLotBuilder, DiscoveryScoringReplayBuilder,
};
pub use history_retention::{HistoryRetentionCutoffs, HistoryRetentionSummary};
pub use market_data::{
    ObservedSolLegCursorAccessPath, ObservedWalletActivityDayCountSource,
    ObservedWalletActivityPage, ObservedWalletActivityRow,
};
pub use sqlite_retry::{is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error};
pub use system_events::RiskEventRow;

pub struct SqliteStore {
    conn: Connection,
}

include!("lib_parts/01_snapshot_types.rs");
include!("lib_parts/02_snapshot_helpers.rs");
include!("lib_parts/03_startup_types.rs");
include!("lib_parts/04_startup_runtime.rs");
include!("lib_parts/05_discovery_publication_types.rs");
include!("lib_parts/06_discovery_runtime_scoring_types.rs");
include!("lib_parts/10_store_connection_snapshot_impl.rs");
include!("lib_parts/11_store_snapshot_probe_impl.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use copybot_core_types::SwapEvent;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    include!("lib_tests/00_storage_tests.rs");
    include!("lib_tests/01_storage_tests.rs");
    include!("lib_tests/02_storage_tests.rs");
    include!("lib_tests/03_storage_tests.rs");
    include!("lib_tests/04_storage_tests.rs");
    include!("lib_tests/05_storage_tests.rs");
    include!("lib_tests/06_storage_tests.rs");
    include!("lib_tests/07_storage_tests.rs");
    include!("lib_tests/08_storage_tests.rs");
    include!("lib_tests/09_storage_tests.rs");
    include!("lib_tests/10_storage_tests.rs");
    include!("lib_tests/11_storage_tests.rs");
    include!("lib_tests/12_storage_tests.rs");
    include!("lib_tests/13_storage_tests.rs");
}

include!("lib_parts/90_money_helpers.rs");

#[cfg(test)]
mod runtime_artifact_tests {
    include!("lib_tests/90_runtime_artifact_tests.rs");
}
