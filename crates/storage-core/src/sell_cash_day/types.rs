use serde::Serialize;

/// Exact arithmetic for validated, dated SELL events only; never portfolio/economic PnL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecognizedSellCashEvents {
    pub signed_net_cash_result_lamports: String,
    pub gross_negative_cash_result_lamports: String,
    pub events: u64,
    pub negative_events: u64,
    pub zero_events: u64,
    pub positive_events: u64,
    /// Historical remaining quantity stored in each fill, not current position state.
    pub partial_events: u64,
    pub full_events: u64,
}

/// Undated obligations in the current read snapshot, NOT historical state at as_of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UndatedSellCashObligations {
    pub scope: String,
    pub legacy_fill_orders: u64,
    pub submitted_without_fill: u64,
    pub confirmed_unreconciled_without_fill: u64,
    pub confirmed_without_fill: u64,
    /// Subset of no-fill obligations; missing signature does not erase the obligation.
    pub without_signature: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionCanarySellCashDay {
    pub since: String,
    pub as_of: String,
    pub scope: String,
    pub window_basis: String,
    pub coverage: String,
    pub known_events: RecognizedSellCashEvents,
    pub undated_obligations: UndatedSellCashObligations,
    /// Always None: an event subtotal plus current obligations cannot prove past coverage.
    pub full_day_cash_result_lamports: Option<String>,
    /// Native cash minus allocated entry basis is not a decomposition into economic PnL.
    pub economic_pnl_lamports: Option<String>,
    pub decomposition: String,
}
