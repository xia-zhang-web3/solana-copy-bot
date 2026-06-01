use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
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
    run_observed_startup_step_with_completion_detail, RiskEventRow, SqliteStartupPolicy,
    StartupStepOutcome, StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy,
    StartupStepTimeout, StartupStepTimeoutBehavior, SHADOW_CLOSE_CONTEXT_MARKET,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE, SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
    SHADOW_RISK_CONTEXT_MARKET, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
    STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL, STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
pub(crate) use copybot_storage_core::{
    POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER,
};

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];
const DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS: i64 = 3;
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
static SQLITE_WRITE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static SQLITE_BUSY_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

mod discovery;
mod discovery_quality_types;
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

pub struct SqliteStore {
    conn: Connection,
}

#[path = "lib_parts/05_discovery_publication_types.rs"]
mod discovery_publication_types;
#[path = "lib_parts/06_discovery_runtime_scoring_types.rs"]
mod discovery_runtime_scoring_types;
#[path = "lib_parts/90_money_helpers.rs"]
mod money_helpers;
#[path = "lib_parts/02_snapshot_helpers.rs"]
mod snapshot_helpers;
#[path = "lib_parts/01_snapshot_types.rs"]
mod snapshot_types;
#[path = "lib_parts/04_startup_runtime.rs"]
mod startup_runtime;
#[path = "lib_parts/03_startup_types.rs"]
mod startup_types;
#[path = "lib_parts/10_store_connection_snapshot_impl.rs"]
mod store_connection_snapshot_impl;
#[path = "lib_parts/11_store_snapshot_probe_impl.rs"]
mod store_snapshot_probe_impl;

pub use self::discovery_publication_types::*;
pub use self::discovery_runtime_scoring_types::*;
pub(crate) use self::money_helpers::*;
pub use self::snapshot_helpers::SqliteSnapshotSourceMetrics;
use self::snapshot_helpers::*;
pub use self::snapshot_types::*;
use self::startup_runtime::checkpoint_large_startup_wal_if_needed;
pub use self::startup_runtime::*;
pub use self::startup_types::*;
use self::startup_types::{
    sqlite_startup_large_wal_checkpoint_detail, sqlite_startup_large_wal_checkpoint_skip_detail,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "lib_tests/90_runtime_artifact_tests.rs"]
mod runtime_artifact_tests;
