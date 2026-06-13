use std::time::Duration as StdDuration;

use tokio::time::Duration;

use crate::observed_swap_writer::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY;
#[cfg(test)]
use std::sync::Mutex;

pub(crate) const DEFAULT_CONFIG_PATH: &str = "configs/dev.toml";
pub(crate) const SHADOW_WORKER_POOL_SIZE: usize = 4;
pub(crate) const SHADOW_INLINE_WORKER_OVERFLOW: usize = 1;
pub(crate) const SHADOW_MAX_CONCURRENT_WORKERS: usize =
    SHADOW_WORKER_POOL_SIZE + SHADOW_INLINE_WORKER_OVERFLOW;
pub(crate) const SHADOW_PENDING_TASK_CAPACITY: usize = 256;
pub(crate) const INGESTION_ERROR_BACKOFF_MS: [u64; 6] = [100, 250, 500, 1_000, 2_000, 5_000];
pub(crate) const RISK_DB_REFRESH_MIN_SECONDS: i64 = 5;
pub(crate) const RISK_INFRA_SAMPLE_MIN_SECONDS: i64 = 10;
pub(crate) const RISK_FAIL_CLOSED_LOG_THROTTLE_SECONDS: i64 = 60;
pub(crate) const RISK_INFRA_EVENT_THROTTLE_SECONDS: i64 = 300;
pub(crate) const RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES: u64 = 5;
pub(crate) const RISK_INFRA_CLEAR_HEALTHY_SAMPLES: u64 = 5;
pub(crate) const RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY: usize = 32_768;
pub(crate) const APP_CONSUMER_LOOP_LATENCY_SAMPLE_CAPACITY: usize = 512;
pub(crate) const DISCOVERY_CRITICAL_TARGET_BUY_MINTS_BACKPRESSURE_REFRESH_INTERVAL: StdDuration =
    StdDuration::from_secs(1);
pub(crate) const DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY: usize =
    OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY;
pub(crate) const NONCRITICAL_IRRELEVANT_OUTPUT_PRESSURE_DROP_MIN_FILL_RATIO: f64 = 0.5;
pub(crate) const ZERO_UNIVERSE_EMPTY_TARGET_NONCRITICAL_BEST_EFFORT_REFILL_COOLDOWN: StdDuration =
    StdDuration::from_secs(5);
pub(crate) const STALE_LOT_CLEANUP_BATCH_LIMIT: u32 = 300;
pub(crate) const HARD_STOP_CLEAR_HEALTHY_REFRESHES: u64 = 6;
pub(crate) const SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO: f64 = 0.25;
pub(crate) const SQLITE_MAINTENANCE_PARTIAL_RETRY_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_INGESTION_OVERRIDE_PATH: &str = "state/ingestion_source_override.env";
pub(crate) const DEFAULT_OPERATOR_EMERGENCY_STOP_PATH: &str = "state/operator_emergency_stop.flag";
pub(crate) const DEFAULT_OPERATOR_EMERGENCY_STOP_POLL_MS: u64 = 500;
pub(crate) const APP_LOG_FILTER_ENV: &str = "COPYBOT_APP_LOG_FILTER";
pub(crate) const LEGACY_RUST_LOG_ENV: &str = "RUST_LOG";
pub(crate) const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
pub(crate) const OBSERVED_SWAP_WRITER_BACKPRESSURE_RETRY_INTERVAL: Duration =
    Duration::from_millis(50);
pub(crate) const OBSERVED_SWAP_WRITER_BACKPRESSURE_LOG_THROTTLE: StdDuration =
    StdDuration::from_secs(5);
#[cfg(test)]
pub(crate) static APP_ENV_LOCK: Mutex<()> = Mutex::new(());
