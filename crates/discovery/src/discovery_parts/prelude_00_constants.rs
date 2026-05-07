use super::*;

pub(crate) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(crate) const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
pub(crate) const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
pub(crate) const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
pub(crate) const QUALITY_MAX_FETCH_PER_CYCLE: usize = 20;
pub(crate) const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
pub(crate) const CAP_TRUNCATION_FOLLOWLIST_DEACTIVATION_GUARD_CYCLES: u32 = 2;
pub(crate) const RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON: &str =
    "raw_window_zero_publishable_universe";
pub(crate) const STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS: usize = 2_048;
pub(crate) const COLLECT_BUY_MINTS_FRESH_SCAN_BATCH_CAP: usize = 512;
pub(crate) const STALE_RECONCILE_TOKEN_BATCH_CAP: usize = 256;
pub(crate) const STALE_RECONCILE_EXACT_COUNT_BATCH_CAP: usize = 32;
pub(crate) const COLLECT_BUY_MINTS_CATCH_UP_PAGE_LIMIT_MULTIPLIER: usize = 2;
pub(crate) const COLLECT_BUY_MINTS_QUALITY_WARMUP_RESERVE_MS: u64 = 1_500;
pub(crate) const REPLAY_WALLET_STATS_WALLET_BATCH_CAP: usize = 900;
pub(crate) const REPLAY_WALLET_STATS_CATCH_UP_PAGE_LIMIT_MULTIPLIER: usize = 2;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEFAULT_TIME_BUDGET_MS: u64 = 10_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_RUNTIME_WINDOW_REFRESH_MIN_TIME_BUDGET_MS: u64 =
    60_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MIN_TIME_BUDGET_MS:
    u64 = 180_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MAX_TIME_BUDGET_MS:
    u64 =
    2_700_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_TARGET_MS_PER_PAGE:
    u64 = 2_250;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MAX_OBSERVED_MS_PER_PAGE:
    u64 =
    5_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MIN_TIME_BUDGET_MS: u64 =
    180_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MAX_TIME_BUDGET_MS: u64 =
    900_000;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MAX_OBSERVED_MS_PER_PAGE:
    u64 = 10_000;
pub(crate) const RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME: &str =
    ".discovery_recent_raw_staged.sqlite.archive-staged";
pub(crate) const RECENT_RAW_STAGED_METADATA_FILE_NAME: &str =
    ".discovery_recent_raw_staged.sqlite.archive-staged.json";
pub(crate) const RECENT_RAW_ATTEMPT_TELEMETRY_SCAN_FILE_LIMIT: usize = 256;
pub(crate) const RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME: &str =
    "discovery_recent_raw_snapshot_attempt_latest.json";
pub(crate) const RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_EXPLICIT_PATHS: &str =
    "bounded_explicit_paths";
pub(crate) const RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_DEEP_SCAN: &str = "deep_directory_scan";
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_PAGE_HEADROOM_NUMERATOR:
    usize = 3;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_PAGE_HEADROOM_DENOMINATOR:
    usize = 2;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_NUMERATOR:
    usize = 19;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_DENOMINATOR:
    usize = 20;
pub(crate) const DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_PUBLISHABLE_HORIZON_PROGRESS_MULTIPLIER:
    usize = 4;
pub(crate) const ACTIONABLE_OPEN_POSITION_HOLD_MULTIPLIER: i64 = 4;
pub(crate) const ACTIONABLE_OPEN_POSITION_MIN_HOLD_SAMPLES: usize = 3;
pub(crate) const OBSERVED_SWAP_WINDOW_PAGED_READ_LIMIT: usize = 256;
pub(crate) static DISCOVERY_PUBLICATION_TRUTH_REPAIR_TRACE_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
pub(crate) static REPLAY_RESUME_EXACT_TARGET_SURFACE_TEST_WALLET_PAGE_LIMIT: AtomicUsize =
    AtomicUsize::new(0);
#[cfg(test)]
pub(crate) static REPLAY_RESUME_EXACT_TARGET_SURFACE_TEST_WALLET_PAGE_LIMIT_LOCK: Mutex<()> =
    Mutex::new(());
#[cfg(test)]
thread_local! {
    static TEST_FORCE_COLLECT_BUY_MINTS_FRESH_SCAN_ROW_LIMIT: Cell<usize> = const { Cell::new(0) };
    static TEST_FORCE_RECONCILE_NEW_TAIL_ZERO_ROW_TIMEOUT: Cell<bool> = const { Cell::new(false) };
    static TEST_FORCE_RECONCILE_EXPIRED_HEAD_EXACT_BATCH_ROW_LIMIT: Cell<usize> = const { Cell::new(0) };
    static TEST_FORCE_RECONCILE_NEW_TAIL_EXACT_BATCH_ROW_LIMIT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn arm_test_force_collect_buy_mints_fresh_scan_row_limit(limit: usize) {
    TEST_FORCE_COLLECT_BUY_MINTS_FRESH_SCAN_ROW_LIMIT.with(|value| value.set(limit));
}

#[cfg(test)]
pub(crate) fn take_test_force_collect_buy_mints_fresh_scan_row_limit() -> Option<usize> {
    TEST_FORCE_COLLECT_BUY_MINTS_FRESH_SCAN_ROW_LIMIT.with(|value| {
        let limit = value.replace(0);
        (limit > 0).then_some(limit)
    })
}

#[cfg(test)]
pub(crate) fn arm_test_force_reconcile_new_tail_zero_row_timeout() {
    TEST_FORCE_RECONCILE_NEW_TAIL_ZERO_ROW_TIMEOUT.with(|flag| flag.set(true));
}

#[cfg(test)]
pub(crate) fn take_test_force_reconcile_new_tail_zero_row_timeout() -> bool {
    TEST_FORCE_RECONCILE_NEW_TAIL_ZERO_ROW_TIMEOUT.with(|flag| flag.replace(false))
}

#[cfg(test)]
pub(crate) fn arm_test_force_reconcile_expired_head_exact_batch_row_limit(limit: usize) {
    TEST_FORCE_RECONCILE_EXPIRED_HEAD_EXACT_BATCH_ROW_LIMIT.with(|value| value.set(limit));
}

#[cfg(test)]
pub(crate) fn take_test_force_reconcile_expired_head_exact_batch_row_limit() -> Option<usize> {
    TEST_FORCE_RECONCILE_EXPIRED_HEAD_EXACT_BATCH_ROW_LIMIT.with(|value| {
        let limit = value.replace(0);
        (limit > 0).then_some(limit)
    })
}

#[cfg(test)]
pub(crate) fn arm_test_force_reconcile_new_tail_exact_batch_row_limit(limit: usize) {
    TEST_FORCE_RECONCILE_NEW_TAIL_EXACT_BATCH_ROW_LIMIT.with(|value| value.set(limit));
}

#[cfg(test)]
pub(crate) fn take_test_force_reconcile_new_tail_exact_batch_row_limit() -> Option<usize> {
    TEST_FORCE_RECONCILE_NEW_TAIL_EXACT_BATCH_ROW_LIMIT.with(|value| {
        let limit = value.replace(0);
        (limit > 0).then_some(limit)
    })
}

pub(crate) fn discovery_runtime_cursor_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(crate) fn discovery_runtime_cursor_load_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(crate) fn discovery_recent_window_load_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(crate) fn discovery_wallet_activity_day_count_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}
