use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct SimulateRequest {
    pub(crate) action: Option<String>,
    pub(crate) contract_version: Option<String>,
    pub(crate) request_id: String,
    pub(crate) signal_id: String,
    pub(crate) side: String,
    pub(crate) token: String,
    pub(crate) notional_sol: f64,
    pub(crate) signal_ts: String,
    pub(crate) route: String,
    pub(crate) dry_run: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ComputeBudgetRequest {
    pub(crate) cu_limit: u32,
    pub(crate) cu_price_micro_lamports: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubmitRequest {
    pub(crate) contract_version: Option<String>,
    pub(crate) signal_id: String,
    pub(crate) client_order_id: String,
    pub(crate) request_id: String,
    pub(crate) side: String,
    pub(crate) token: String,
    pub(crate) notional_sol: f64,
    pub(crate) signal_ts: String,
    pub(crate) route: String,
    pub(crate) slippage_bps: f64,
    pub(crate) route_slippage_cap_bps: f64,
    pub(crate) tip_lamports: u64,
    pub(crate) compute_budget: ComputeBudgetRequest,
}

#[cfg(test)]
mod tests {
    use super::{SimulateRequest, SubmitRequest};
    use serde_json::json;

    #[test]
    fn request_types_simulate_requires_request_id_field() {
        let payload = json!({
            "action": "simulate",
            "contract_version": "v1",
            "signal_id": "signal-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-24T00:00:00Z",
            "route": "rpc",
            "dry_run": true
        });
        let error = serde_json::from_value::<SimulateRequest>(payload)
            .expect_err("request_id is required");
        assert!(error.to_string().contains("request_id"));
    }

    #[test]
    fn request_types_submit_requires_compute_budget_field() {
        let payload = json!({
            "contract_version": "v1",
            "signal_id": "signal-1",
            "client_order_id": "client-1",
            "request_id": "request-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-24T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0
        });
        let error = serde_json::from_value::<SubmitRequest>(payload)
            .expect_err("compute_budget is required");
        assert!(error.to_string().contains("compute_budget"));
    }
}
