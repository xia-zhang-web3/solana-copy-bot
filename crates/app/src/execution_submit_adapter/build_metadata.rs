//! Daemon build metadata and private runtime-only proof carriers.
use chrono::{DateTime, Utc};
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ExecutionBuildPlanMetadata {
    #[serde(skip)]
    pub(crate) transaction_owned_wsol: Option<Box<crate::execution_wsol_lifecycle::Authority>>,
    #[serde(skip)]
    pub(crate) controlled_exit_wire: Option<Box<crate::execution_controlled_receipt_lot::ExitWire>>,
    #[serde(skip)]
    pub(crate) protected_capital:
        Option<Box<crate::execution_native_floor_policy::protected::ContextProof>>,
    #[serde(default)]
    pub(crate) owned_sell_amount: Option<crate::execution_source_sell_guard::amount::Proof>,
    #[serde(skip)]
    pub(crate) rpc_owned_live: Option<crate::execution_owned_sell_prepare::submit::guard::Live>,
    #[serde(skip)]
    pub(crate) rpc_owned_sell:
        Option<Box<copybot_storage_core::rpc_owned_sell_handoff::dispatch::Prepared>>,
    pub(crate) http_request_started_ts: Option<DateTime<Utc>>,
    pub(crate) quote_response_available_ts: Option<DateTime<Utc>>,
    pub(crate) quote_source: Option<String>,
    pub(crate) quote_event_id: Option<String>,
    pub(crate) quote_request_ts: Option<DateTime<Utc>>,
    pub(crate) quote_status: Option<String>,
    pub(crate) quote_in_amount_raw: Option<String>,
    pub(crate) quote_out_amount_raw: Option<String>,
    pub(crate) quote_response_json: Option<String>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) price_impact_pct: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) priority_fee_source: Option<String>,
    pub(crate) priority_fee_status: Option<String>,
    pub(crate) priority_fee_lamports: Option<u64>,
    pub(crate) priority_fee_json: Option<String>,
    pub(crate) slippage_bps: Option<f64>,
    pub(crate) decision_status: Option<String>,
    pub(crate) decision_reason: Option<String>,
}

impl Default for ExecutionBuildPlanMetadata {
    fn default() -> Self {
        Self {
            controlled_exit_wire: None,
            transaction_owned_wsol: None,
            rpc_owned_live: None,
            rpc_owned_sell: None,
            owned_sell_amount: None,
            protected_capital: None,
            quote_source: Some("not_available".to_string()),
            quote_event_id: None,
            quote_request_ts: None,
            http_request_started_ts: None,
            quote_response_available_ts: None,
            quote_status: None,
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_response_json: None,
            quote_price_sol: None,
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_source: Some("not_available".to_string()),
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            slippage_bps: None,
            decision_status: None,
            decision_reason: None,
        }
    }
}
