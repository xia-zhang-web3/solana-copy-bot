use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ExecutionExitPolicySimConfig {
    pub since: DateTime<Utc>,
    pub limit: u32,
    pub sample_window_minutes: i64,
    pub min_sol_notional: f64,
    pub min_samples: usize,
    pub max_samples_per_checkpoint: usize,
    pub checkpoint_step_minutes: i64,
    pub horizon_minutes: i64,
    pub backstop_minutes: Vec<i64>,
    pub price_collapse_ratios: Vec<f64>,
    pub activity_idle_minutes: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionExitPolicySimReport {
    pub since: DateTime<Utc>,
    pub limit: u32,
    pub params: ExecutionExitPolicySimParams,
    pub baseline: ExecutionExitPolicyBaseline,
    pub policies: Vec<ExecutionExitPolicySummary>,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionExitPolicySimParams {
    pub sample_window_minutes: i64,
    pub min_sol_notional: f64,
    pub min_samples: usize,
    pub max_samples_per_checkpoint: usize,
    pub checkpoint_step_minutes: i64,
    pub horizon_minutes: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionExitPolicyBaseline {
    pub trades: u64,
    pub entry_cost_sol: f64,
    pub exit_value_sol: f64,
    pub pnl_sol: f64,
    pub market_trades: u64,
    pub market_pnl_sol: f64,
    pub non_market_trades: u64,
    pub non_market_pnl_sol: f64,
    pub stale_rug_like_trades: u64,
    pub stale_rug_like_pnl_sol: f64,
    pub contexts: Vec<ExecutionExitPolicyContextSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionExitPolicyContextSummary {
    pub close_context: String,
    pub trades: u64,
    pub entry_cost_sol: f64,
    pub pnl_sol: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionExitPolicySummary {
    pub policy: String,
    pub triggered_trades: u64,
    pub priced_triggers: u64,
    pub unpriced_triggers: u64,
    pub original_pnl_sol: f64,
    pub simulated_pnl_sol: f64,
    pub net_delta_sol: f64,
    pub loss_saved_sol: f64,
    pub winners_cut: u64,
    pub winner_upside_lost_sol: f64,
    pub stale_rug_like_triggered: u64,
    pub stale_rug_like_net_delta_sol: f64,
    pub market_triggered: u64,
    pub market_net_delta_sol: f64,
}
