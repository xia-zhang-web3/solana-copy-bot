use super::*;

pub(super) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(super) const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
pub(super) const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
pub(super) const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
pub(super) const QUALITY_MAX_FETCH_PER_BATCH: usize = 20;
pub(super) const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
pub(super) const DISCOVERY_SCORING_PREPARE_PROGRESS_OPS: i32 = 10_000;
pub(super) const DISCOVERY_SCORING_PREPARE_RUNTIME_BUDGET_EXHAUSTED_REASON: &str =
    "discovery_scoring_prepare_runtime_budget_exhausted";
pub(super) const DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS: &str =
    "discovery_aggregate_repair_lock_first_budget_exhausted_without_progress";
pub(super) const DISCOVERY_SCORING_LOCK_FIRST_REPAIR_QUERY_PAGE_ROWS: usize = 512;
pub(super) const RUG_LOOKAHEAD_STATS_QUERY: &str = "SELECT
                COALESCE(SUM(sol_notional), 0.0) AS volume_sol,
                COUNT(DISTINCT wallet_id) AS unique_traders
             FROM (
                SELECT wallet_id, qty_out AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, qty_in AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_out_in_ts
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )";

#[derive(Debug, Clone)]
pub(super) struct OpenLotRow {
    pub(super) buy_signature: String,
    pub(super) qty: f64,
    pub(super) cost_sol: f64,
    pub(super) opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct QualitySnapshot {
    pub(super) source: WalletScoringQualitySource,
    pub(super) token_age_seconds: Option<u64>,
    pub(super) holders: Option<u64>,
    pub(super) liquidity_sol: Option<f64>,
}

#[derive(Debug, Clone)]
pub(super) struct QualityCacheRowLocal {
    pub(super) holders: Option<u64>,
    pub(super) liquidity_sol: Option<f64>,
    pub(super) token_age_seconds: Option<u64>,
    pub(super) fetched_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub(super) struct QualityFetchBudget {
    pub(super) rpc_attempted: usize,
    pub(super) started_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub(super) struct QualityCacheUpsert {
    pub(super) mint: String,
    pub(super) holders: Option<u64>,
    pub(super) liquidity_sol: Option<f64>,
    pub(super) token_age_seconds: Option<u64>,
    pub(super) fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedBuyFact {
    pub(super) market_stats: TokenMarketStats,
    pub(super) quality: QualitySnapshot,
    pub(super) quality_cache_upsert: Option<QualityCacheUpsert>,
    pub(super) rug_check_after_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedScoringSwap {
    pub(super) swap: SwapEvent,
    pub(super) buy_fact: Option<PreparedBuyFact>,
}

#[derive(Debug, Clone)]
pub(super) struct CarryoverLotRow {
    pub(super) qty: f64,
    pub(super) cost_sol: f64,
    pub(super) oldest_opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RugLookaheadFinalizeOutcome {
    pub(super) deferred_due_to_budget_hotspot: bool,
    pub(super) batch_prefetch_used: bool,
    pub(super) exact_count: usize,
    pub(super) deferred_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct RepairRugFact {
    pub(super) buy_signature: String,
    pub(super) token: String,
    pub(super) buy_ts: DateTime<Utc>,
    pub(super) check_after_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct RepairRugLookaheadEvent {
    pub(super) wallet_id: String,
    pub(super) ts: DateTime<Utc>,
    pub(super) sol_notional: f64,
}

#[cfg(debug_assertions)]
thread_local! {
    pub(super) static DISCOVERY_SCORING_FAIL_AFTER_MATERIALIZATION_BEFORE_CHECKPOINT: Cell<bool> =
        const { Cell::new(false) };
    pub(super) static DISCOVERY_SCORING_FORCE_PREPARE_RUNTIME_BUDGET_EXHAUSTED: Cell<bool> =
        const { Cell::new(false) };
    pub(super) static DISCOVERY_SCORING_LOCK_FIRST_REPAIR_BUDGET_AFTER_ROWS: Cell<Option<usize>> =
        const { Cell::new(None) };
    pub(super) static DISCOVERY_SCORING_RUG_LOOKAHEAD_BUDGET_FAIL_ABOVE_ROWS: Cell<Option<usize>> =
        const { Cell::new(None) };
    pub(super) static DISCOVERY_SCORING_RUG_LOOKAHEAD_BATCH_BUDGET_FAIL_ABOVE_ROWS: Cell<Option<usize>> =
        const { Cell::new(None) };
    pub(super) static DISCOVERY_SCORING_RUG_LOOKAHEAD_UNKNOWN_FAILPOINT: Cell<bool> =
        const { Cell::new(false) };
    pub(super) static DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS: Cell<usize> =
        const { Cell::new(0) };
    pub(super) static DISCOVERY_SCORING_RUG_LOOKAHEAD_STATS_CALL_COUNT: Cell<usize> =
        const { Cell::new(0) };
}

#[cfg(debug_assertions)]
pub(super) fn set_discovery_scoring_atomic_checkpoint_failpoint(enabled: bool) {
    DISCOVERY_SCORING_FAIL_AFTER_MATERIALIZATION_BEFORE_CHECKPOINT
        .with(|failpoint| failpoint.set(enabled));
}

#[cfg(debug_assertions)]
pub(crate) fn maybe_fail_after_materialization_before_checkpoint() -> Result<()> {
    let fired = DISCOVERY_SCORING_FAIL_AFTER_MATERIALIZATION_BEFORE_CHECKPOINT.with(|failpoint| {
        let fired = failpoint.get();
        failpoint.set(false);
        fired
    });
    if fired {
        anyhow::bail!(
            "test failpoint: discovery scoring crash after materialization before checkpoint"
        );
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
pub(crate) fn maybe_fail_after_materialization_before_checkpoint() -> Result<()> {
    Ok(())
}

#[cfg(debug_assertions)]
impl SqliteStore {
    #[doc(hidden)]
    pub fn set_discovery_scoring_atomic_checkpoint_failpoint_for_tests(enabled: bool) {
        set_discovery_scoring_atomic_checkpoint_failpoint(enabled);
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_prepare_runtime_budget_failpoint_for_tests(enabled: bool) {
        DISCOVERY_SCORING_FORCE_PREPARE_RUNTIME_BUDGET_EXHAUSTED
            .with(|failpoint| failpoint.set(enabled));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_lock_first_repair_budget_after_rows_for_tests(
        rows: Option<usize>,
    ) {
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_BUDGET_AFTER_ROWS.with(|failpoint| failpoint.set(rows));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_rug_lookahead_budget_fail_above_rows_for_tests(
        rows: Option<usize>,
    ) {
        DISCOVERY_SCORING_RUG_LOOKAHEAD_BUDGET_FAIL_ABOVE_ROWS
            .with(|failpoint| failpoint.set(rows));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_rug_lookahead_batch_budget_fail_above_rows_for_tests(
        rows: Option<usize>,
    ) {
        DISCOVERY_SCORING_RUG_LOOKAHEAD_BATCH_BUDGET_FAIL_ABOVE_ROWS
            .with(|failpoint| failpoint.set(rows));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_rug_lookahead_unknown_failpoint_for_tests(enabled: bool) {
        DISCOVERY_SCORING_RUG_LOOKAHEAD_UNKNOWN_FAILPOINT.with(|failpoint| failpoint.set(enabled));
    }

    #[doc(hidden)]
    pub fn take_discovery_scoring_rug_lookahead_stats_call_count_for_tests() -> usize {
        DISCOVERY_SCORING_RUG_LOOKAHEAD_STATS_CALL_COUNT.with(|counter| {
            let count = counter.get();
            counter.set(0);
            count
        })
    }
}

pub(super) struct DiscoveryScoringPrepareProgressGuard<'a> {
    pub(super) conn: &'a Connection,
}

impl<'a> DiscoveryScoringPrepareProgressGuard<'a> {
    pub(super) fn install(conn: &'a Connection, deadline: Instant) -> Self {
        conn.progress_handler(
            DISCOVERY_SCORING_PREPARE_PROGRESS_OPS,
            Some(move || Instant::now() >= deadline),
        );
        Self { conn }
    }
}

impl Drop for DiscoveryScoringPrepareProgressGuard<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}
