use super::{FailedExpenseTask, FailedTransactionFacts};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FailedExpenseReport {
    pub since: String,
    pub as_of: String,
    pub window_basis: String,
    pub source_basis: String,
    pub history_coverage: String,
    pub coverage: String,
    pub total_orders: u64,
    pub known_orders: u64,
    pub unknown_orders: u64,
    pub unresolved_orders: u64,
    pub legacy_uncovered_orders: u64,
    pub known_wallet_fee_lamports: Option<String>,
    pub cohort_wallet_fee_lamports: Option<String>,
    pub known_native_delta_lamports: Option<String>,
    pub known_unexplained_delta_lamports: Option<String>,
    pub economic_pnl_lamports: Option<String>,
    pub risk_policy: String,
    pub rows_truncated: bool,
    pub rows: Vec<FailedExpenseReportRow>,
}
impl Default for FailedExpenseReport {
    fn default() -> Self {
        Self {
            since: String::new(),
            as_of: String::new(),
            window_basis: "original_order_submit_ts_[since,as_of)".into(),
            source_basis: "failed_getTransaction_confirmed_or_finalized".into(),
            history_coverage: "unverified_prior_history_no_backfill".into(),
            coverage: "empty_unknown".into(),
            total_orders: 0,
            known_orders: 0,
            unknown_orders: 0,
            unresolved_orders: 0,
            legacy_uncovered_orders: 0,
            known_wallet_fee_lamports: None,
            cohort_wallet_fee_lamports: None,
            known_native_delta_lamports: None,
            known_unexplained_delta_lamports: None,
            economic_pnl_lamports: None,
            risk_policy: "R06_known_canary_wallet_fee_subtotal_in_UTC_daily_BUY_cap_unknown_below_cap_retains_runtime_policy_readiness_unchanged_no_SOL_reserve"
                .into(),
            rows_truncated: false,
            rows: Vec::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FailedExpenseReportRow {
    pub order_id: String,
    pub operation_at: String,
    pub reason: String,
    pub task: Option<FailedExpenseTask>,
    pub facts: Option<FailedTransactionFacts>,
    /// Immutable ledger observation remains visible even when identity later conflicts.
    pub recorded_wallet_fee_lamports: Option<String>,
    pub wallet_fee_lamports: Option<String>,
    pub native_delta_lamports: Option<String>,
    pub unexplained_delta_lamports: Option<String>,
}
