use serde::Serialize;

/// Complete SELL cohort counts/sums, with bounded row samples. Exact amounts are
/// decimal strings so JSON clients cannot round them through binary64.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCashSettlementReport {
    pub basis: String,
    pub decomposition: String,
    pub total_sell_orders: u64,
    pub settled_orders: u64,
    pub legacy_fill_orders: u64,
    pub unsettled_orders: u64,
    pub known_wallet_native_delta_lamports: Option<String>,
    pub known_cash_result_delta_lamports: Option<String>,
    pub cohort_cash_result_delta_lamports: Option<String>,
    pub economic_pnl_sol: Option<f64>,
    pub rows_truncated: bool,
    pub rows: Vec<ExecutionCashSettlementReportRow>,
}

impl Default for ExecutionCashSettlementReport {
    fn default() -> Self {
        Self {
            basis: "wallet_native_cash_minus_allocated_entry_basis".into(),
            decomposition: "unresolved".into(),
            total_sell_orders: 0,
            settled_orders: 0,
            legacy_fill_orders: 0,
            unsettled_orders: 0,
            known_wallet_native_delta_lamports: None,
            known_cash_result_delta_lamports: None,
            cohort_cash_result_delta_lamports: None,
            economic_pnl_sol: None,
            rows_truncated: false,
            rows: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCashSettlementReportRow {
    pub order_id: String,
    pub token: String,
    pub status: String,
    pub accounting_basis: String,
    pub position_id: Option<String>,
    pub sold_raw: Option<String>,
    pub decimals: Option<u8>,
    pub wallet_native_cash_delta_lamports: Option<String>,
    pub allocated_entry_basis_lamports: Option<String>,
    pub cash_result_delta_lamports: Option<String>,
    pub swap_price_sol: Option<f64>,
    pub economic_pnl_sol: Option<f64>,
    pub transaction_fee_lamports: Option<String>,
    pub fee_coverage: String,
    pub fee_payer: Option<String>,
    pub decomposition: String,
}
