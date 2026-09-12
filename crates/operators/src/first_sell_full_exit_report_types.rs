use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FirstSellAuditSummary {
    pub metric_basis: String,
    pub entry_population: String,
    pub minimum_eligible_entry_followup_seconds: Option<i64>,
    pub verdict: String,
    pub caveats: Vec<String>,
    pub coverage: AuditCoverage,
    pub fee_model: FeeModelSummary,
    pub entry_gate_model: EntryGateModelSummary,
    pub observed_entry_notionals: Vec<ObservedNotional>,
    pub notional_frontier_status: String,
    pub policies: Vec<PolicySummary>,
    pub prospective_requirements: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct AuditCoverage {
    pub actual_http_entry_events: u64,
    pub unknown_http_entry_events: u64,
    pub actual_entry_ready_events: u64,
    pub loaded_entry_events: u64,
    pub eligible_entry_events: u64,
    pub skipped_entry_events: u64,
    pub shadow_dropped_entry_events: u64,
    pub shadow_pending_entry_events: u64,
    pub invalid_entry_amount_events: u64,
    pub entry_gate_rejected_events: u64,
    pub entry_timing_unknown_events: u64,
    pub loaded_sell_signals: u64,
    pub loaded_sell_quote_events: u64,
    pub loaded_close_outcomes: u64,
    pub entry_limit_hit: bool,
    pub sell_limit_hit: bool,
    pub quote_limit_hit: bool,
    pub close_limit_hit: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeeModelSummary {
    pub priority_fee_cap_lamports_per_leg: u64,
    pub base_fee_lamports_per_leg: u64,
    pub new_ata_cash_lamports_per_entry: u64,
    pub legs_assumed_per_round_trip: u64,
    pub missing_priority_sample_policy: String,
    pub actual_network_fees_available: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EntryGateModelSummary {
    pub expected_in_lamports: u64,
    pub max_slippage_bps: u64,
    pub earliest_open_basis: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ObservedNotional {
    pub entry_in_lamports: u64,
    pub entry_notional_sol: f64,
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolicySummary {
    pub policy: String,
    pub pnl_scope: String,
    pub entries: u64,
    pub triggers: u64,
    pub valued_exit_events: u64,
    pub quote_backed_exit_events: u64,
    pub quote_backed_exit_coverage_pct: Option<f64>,
    pub complete_quote_backed_exit_coverage: bool,
    pub cross_wallet_triggers: u64,
    pub no_trigger_events: u64,
    pub exact_amount_events: u64,
    pub scaled_up_events: u64,
    pub scaled_down_events: u64,
    pub terminal_zero_events: u64,
    pub unknown_exit_events: u64,
    pub missing_entry_priority_samples: u64,
    pub missing_exit_priority_samples: u64,
    pub exact_amount: PnlStats,
    pub strict_unscaled: PnlStats,
    pub amount_scaled_estimate: PnlStats,
    pub by_source_cohort: Vec<CohortSummary>,
    pub by_rank_cohort: Vec<CohortSummary>,
    pub no_data_reasons: Vec<ReasonCount>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PnlStats {
    pub gross_events: u64,
    pub planned_net_events: u64,
    pub wins: u64,
    pub losses_or_zero: u64,
    pub gross_pnl_sol: Option<f64>,
    pub planned_net_existing_ata_sol: Option<f64>,
    pub planned_net_new_ata_cash_sol: Option<f64>,
    pub planned_net_median_sol: Option<f64>,
    pub planned_net_ex_top3_sol: Option<f64>,
    pub planned_net_ex_top_wallet_sol: Option<f64>,
    pub planned_net_max_drawdown_sol: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CohortSummary {
    pub cohort: String,
    pub entries: u64,
    pub exact_amount_events: u64,
    pub amount_scaled_estimate: PnlStats,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReasonCount {
    pub reason: String,
    pub events: u64,
}
