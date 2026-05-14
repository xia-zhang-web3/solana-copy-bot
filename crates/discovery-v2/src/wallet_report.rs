use crate::{DiscoveryV2LivePortfolioStatus, DiscoveryV2Status, DiscoveryV2WalletMetric};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::SqliteDiscoveryStore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct DiscoveryV2WalletReportOptions {
    pub now: DateTime<Utc>,
    pub limit: usize,
    pub include_rejected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletReport {
    pub source: String,
    pub generated_at: DateTime<Utc>,
    pub status_now: DateTime<Utc>,
    pub status_age_seconds: i64,
    pub window_start: DateTime<Utc>,
    pub window_minutes: u64,
    pub production_green: bool,
    pub blockers: Vec<String>,
    pub policy_fingerprint: String,
    pub thresholds: DiscoveryV2WalletReportThresholds,
    pub live_portfolio: Option<DiscoveryV2LivePortfolioStatus>,
    pub wallet_metrics_total: usize,
    pub wallet_metrics_returned: usize,
    pub wallet_metrics_truncated: bool,
    pub candidate_wallet_count: usize,
    pub active_follow_wallet_count: usize,
    pub wallets: Vec<DiscoveryV2WalletReportRow>,
    pub top_rejected_wallets: Vec<DiscoveryV2WalletReportRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletReportThresholds {
    pub follow_top_n: u32,
    pub min_score: f64,
    pub min_trades: u32,
    pub min_active_days: u32,
    pub min_buy_count: u32,
    pub min_leader_notional_sol: f64,
    pub min_tradable_ratio: f64,
    pub max_rug_ratio: f64,
    pub require_open_positions_for_publication: bool,
    pub live_portfolio_gate_enabled: bool,
    pub min_live_sol_balance: f64,
    pub min_live_portfolio_value_sol: f64,
    pub quality_gates_enabled: bool,
    pub min_token_age_seconds: u64,
    pub min_holders: u64,
    pub min_liquidity_sol: f64,
    pub min_volume_5m_sol: f64,
    pub min_unique_traders_5m: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletReportRow {
    pub rank: usize,
    pub wallet_id: String,
    pub candidate: bool,
    pub active_follow: bool,
    pub eligible: bool,
    pub score: f64,
    pub trades: u32,
    pub active_days: u32,
    pub buys: u32,
    pub sells: u32,
    pub max_buy_notional_sol: f64,
    pub pnl_sol: f64,
    pub win_rate: f64,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub tradable_ratio: f64,
    pub missing_quality_evidence_buys: u32,
    pub rug_ratio: f64,
    pub rug_lookahead_evaluated: u32,
    pub rug_lookahead_unevaluated: u32,
    pub live_sol_balance: Option<f64>,
    pub live_token_value_sol: Option<f64>,
    pub live_token_positions: Option<u32>,
    pub live_tradable_token_positions: Option<u32>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub filters: DiscoveryV2WalletFilterEvidence,
    pub reject_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletFilterEvidence {
    pub passed_all: bool,
    pub score_pass: bool,
    pub trades_pass: bool,
    pub active_days_pass: bool,
    pub buy_count_pass: bool,
    pub notional_pass: bool,
    pub tradable_ratio_pass: bool,
    pub token_quality_evidence_pass: bool,
    pub rug_evidence_pass: bool,
    pub open_position_pass: bool,
    pub live_portfolio_pass: bool,
}

pub fn build_discovery_v2_wallet_report(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    status: DiscoveryV2Status,
    options: DiscoveryV2WalletReportOptions,
) -> Result<DiscoveryV2WalletReport> {
    let limit = options.limit.max(1);
    let active_follow_wallets = store.list_active_follow_wallets()?;
    let metric_by_wallet = status
        .wallet_metrics
        .iter()
        .map(|metric| (metric.wallet_id.as_str(), metric))
        .collect::<HashMap<_, _>>();
    let candidate_wallets = status
        .candidate_wallets
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut wallets = Vec::new();
    for (index, wallet_id) in status.candidate_wallets.iter().take(limit).enumerate() {
        let Some(metric) = metric_by_wallet.get(wallet_id.as_str()) else {
            bail!("candidate wallet is missing from V2 wallet metrics: {wallet_id}");
        };
        wallets.push(report_row(
            index + 1,
            metric,
            true,
            active_follow_wallets.contains(wallet_id),
            discovery,
        ));
    }
    let top_rejected_wallets = if options.include_rejected {
        status
            .wallet_metrics
            .iter()
            .filter(|metric| !candidate_wallets.contains(metric.wallet_id.as_str()))
            .take(limit)
            .enumerate()
            .map(|(index, metric)| {
                report_row(
                    index + 1,
                    metric,
                    false,
                    active_follow_wallets.contains(&metric.wallet_id),
                    discovery,
                )
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(DiscoveryV2WalletReport {
        source: status.source,
        generated_at: options.now,
        status_now: status.now,
        status_age_seconds: options.now.signed_duration_since(status.now).num_seconds(),
        window_start: status.window_start,
        window_minutes: status.window_minutes,
        production_green: status.production_green,
        blockers: status.blockers,
        policy_fingerprint: status.policy_fingerprint,
        thresholds: thresholds(discovery, shadow),
        live_portfolio: status.live_portfolio,
        wallet_metrics_total: status.wallet_metrics_total,
        wallet_metrics_returned: status.wallet_metrics_returned,
        wallet_metrics_truncated: status.wallet_metrics_truncated,
        candidate_wallet_count: status.candidate_wallets.len(),
        active_follow_wallet_count: active_follow_wallets.len(),
        wallets,
        top_rejected_wallets,
    })
}

fn report_row(
    rank: usize,
    metric: &DiscoveryV2WalletMetric,
    candidate: bool,
    active_follow: bool,
    discovery: &DiscoveryConfig,
) -> DiscoveryV2WalletReportRow {
    DiscoveryV2WalletReportRow {
        rank,
        wallet_id: metric.wallet_id.clone(),
        candidate,
        active_follow,
        eligible: metric.eligible,
        score: metric.score,
        trades: metric.trades,
        active_days: metric.active_days,
        buys: metric.buys,
        sells: metric.sells,
        max_buy_notional_sol: metric.max_buy_notional_sol,
        pnl_sol: metric.pnl_sol,
        win_rate: metric.win_rate,
        closed_trades: metric.closed_trades,
        hold_median_seconds: metric.hold_median_seconds,
        tradable_ratio: metric.tradable_ratio,
        missing_quality_evidence_buys: metric.missing_quality_evidence_buys,
        rug_ratio: metric.rug_ratio,
        rug_lookahead_evaluated: metric.rug_lookahead_evaluated,
        rug_lookahead_unevaluated: metric.rug_lookahead_unevaluated,
        live_sol_balance: metric.live_sol_balance,
        live_token_value_sol: metric.live_token_value_sol,
        live_token_positions: metric.live_token_positions,
        live_tradable_token_positions: metric.live_tradable_token_positions,
        first_seen: metric.first_seen,
        last_seen: metric.last_seen,
        filters: filter_evidence(metric, discovery),
        reject_reasons: metric.reject_reasons.clone(),
    }
}

fn filter_evidence(
    metric: &DiscoveryV2WalletMetric,
    discovery: &DiscoveryConfig,
) -> DiscoveryV2WalletFilterEvidence {
    let score_pass = metric.score >= discovery.min_score && !has_reason(metric, "below_min_score");
    let trades_pass =
        metric.trades >= discovery.min_trades && !has_reason(metric, "insufficient_trades");
    let active_days_pass = metric.active_days >= discovery.min_active_days
        && !has_reason(metric, "insufficient_active_days");
    let buy_count_pass = metric.buy_total >= discovery.min_buy_count
        && !has_reason(metric, "insufficient_buy_count");
    let notional_pass = metric.max_buy_notional_sol >= discovery.min_leader_notional_sol
        && !has_reason(metric, "low_notional");
    let tradable_ratio_pass = metric.tradable_ratio >= discovery.min_tradable_ratio
        && !has_reason(metric, "low_tradable_ratio");
    let token_quality_evidence_pass = metric.missing_quality_evidence_buys == 0
        && !has_reason(metric, "token_quality_evidence_missing");
    let rug_evidence_pass =
        !has_reason(metric, "rug_gate") && !has_reason(metric, "rug_lookahead_unevaluated");
    let open_position_pass = !discovery.require_open_positions_for_publication
        || !has_reason(metric, "open_position_required_missing");
    let live_portfolio_pass = !discovery.live_portfolio_gate_enabled
        || (live_portfolio_value_satisfies(metric, discovery)
            && !has_any_reason(
                metric,
                &[
                    "live_portfolio_rpc_missing",
                    "live_portfolio_rpc_unavailable",
                    "capital_drained_after_window",
                    "only_dust_positions",
                    "only_illiquid_positions",
                ],
            ));
    let passed_all = metric.eligible
        && score_pass
        && trades_pass
        && active_days_pass
        && buy_count_pass
        && notional_pass
        && tradable_ratio_pass
        && token_quality_evidence_pass
        && rug_evidence_pass
        && open_position_pass
        && live_portfolio_pass;
    DiscoveryV2WalletFilterEvidence {
        passed_all,
        score_pass,
        trades_pass,
        active_days_pass,
        buy_count_pass,
        notional_pass,
        tradable_ratio_pass,
        token_quality_evidence_pass,
        rug_evidence_pass,
        open_position_pass,
        live_portfolio_pass,
    }
}

fn thresholds(
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
) -> DiscoveryV2WalletReportThresholds {
    DiscoveryV2WalletReportThresholds {
        follow_top_n: discovery.follow_top_n,
        min_score: discovery.min_score,
        min_trades: discovery.min_trades,
        min_active_days: discovery.min_active_days,
        min_buy_count: discovery.min_buy_count,
        min_leader_notional_sol: discovery.min_leader_notional_sol,
        min_tradable_ratio: discovery.min_tradable_ratio,
        max_rug_ratio: discovery.max_rug_ratio,
        require_open_positions_for_publication: discovery.require_open_positions_for_publication,
        live_portfolio_gate_enabled: discovery.live_portfolio_gate_enabled,
        min_live_sol_balance: discovery.min_live_sol_balance,
        min_live_portfolio_value_sol: discovery.min_live_portfolio_value_sol,
        quality_gates_enabled: shadow.quality_gates_enabled,
        min_token_age_seconds: shadow.min_token_age_seconds,
        min_holders: shadow.min_holders,
        min_liquidity_sol: shadow.min_liquidity_sol,
        min_volume_5m_sol: shadow.min_volume_5m_sol,
        min_unique_traders_5m: shadow.min_unique_traders_5m,
    }
}

fn has_reason(metric: &DiscoveryV2WalletMetric, reason: &str) -> bool {
    metric
        .reject_reasons
        .iter()
        .any(|candidate| candidate == reason)
}

fn has_any_reason(metric: &DiscoveryV2WalletMetric, reasons: &[&str]) -> bool {
    reasons.iter().any(|reason| has_reason(metric, reason))
}

fn live_portfolio_value_satisfies(
    metric: &DiscoveryV2WalletMetric,
    discovery: &DiscoveryConfig,
) -> bool {
    metric.live_sol_balance.unwrap_or(0.0) >= discovery.min_live_sol_balance
        || metric.live_token_value_sol.unwrap_or(0.0) >= discovery.min_live_portfolio_value_sol
}
