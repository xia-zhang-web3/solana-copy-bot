use crate::filters::{build_budget_exhausted_filter_status, DiscoveryV2FilterStatusBuilder};
use crate::live_portfolio::apply_live_portfolio_gate;
use crate::maturity::{
    apply_maturity_ranking, configured_status as configured_maturity_status,
    observe_selected_maturity_tiers,
};
use crate::metric::wallet_metric_from_accumulator;
use crate::policy::{discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions};
use crate::shadow_feedback::{apply_shadow_feedback, load_shadow_wallet_feedback};
use crate::status::status_blockers::blockers;
use crate::status::status_load::{load_coverage_sample, load_tail_status, scan_window_metrics};
use crate::status::status_rank::{retain_top_wallet_metric, scan_status, sort_wallet_metrics};
use anyhow::{Context, Result};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::{ShadowSignalSummary, SqliteDiscoveryStore};
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

pub use crate::filters::DiscoveryV2FilterStatus;
pub use crate::live_portfolio::DiscoveryV2LivePortfolioStatus;
pub use status_types::{
    DiscoveryV2BuildTiming, DiscoveryV2CoverageSample, DiscoveryV2MaturityStatus,
    DiscoveryV2ScanStatus, DiscoveryV2ShadowSignalStatus, DiscoveryV2Status, DiscoveryV2TailStatus,
    OPERATOR_WALLET_METRIC_LIMIT,
};

#[path = "status_blockers.rs"]
mod status_blockers;
#[path = "status_load.rs"]
mod status_load;
#[path = "status_rank.rs"]
mod status_rank;
#[path = "status_types.rs"]
mod status_types;

pub use crate::policy::TOKEN_QUALITY_TTL_SECONDS;
pub use copybot_config::DISCOVERY_V2_SCORING_SOURCE;

pub fn build_discovery_v2_status(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: DiscoveryV2BuildOptions,
) -> Result<DiscoveryV2Status> {
    let build_started = Instant::now();
    let window_start = options.window_start();
    let scan_deadline = Instant::now() + StdDuration::from_millis(options.time_budget_ms);
    let tail_started = Instant::now();
    let tail = load_tail_status(store, options.now, options.max_tail_lag_seconds)?;
    let tail_elapsed_ms = elapsed_ms_ceil(tail_started.elapsed());
    let scoring_data_now = tail
        .as_ref()
        .map(|status| status.cursor.ts_utc.min(options.now))
        .unwrap_or(options.now);
    let coverage_started = Instant::now();
    let coverage_sample = load_coverage_sample(store, window_start)?;
    let coverage_elapsed_ms = elapsed_ms_ceil(coverage_started.elapsed());
    let scan_started = Instant::now();
    let window_scan = scan_window_metrics(
        store,
        discovery,
        shadow,
        window_start,
        options.now,
        scoring_data_now,
        options.max_rows,
        scan_deadline,
    )?;
    let scan_elapsed_ms = elapsed_ms_ceil(scan_started.elapsed());
    let rows_seen = window_scan.rows_seen;
    let unique_wallets_seen = window_scan.unique_wallets_seen;
    let scan = scan_status(
        options.max_rows,
        options.time_budget_ms,
        rows_seen,
        window_scan.time_budget_exhausted,
        unique_wallets_seen,
    );
    if scan.budget_exhausted {
        return Ok(budget_exhausted_status(
            discovery,
            shadow,
            window_start,
            tail,
            coverage_sample,
            scan,
            unique_wallets_seen,
            &options,
            DiscoveryV2BuildTiming {
                tail_elapsed_ms,
                coverage_elapsed_ms,
                scan_elapsed_ms,
                total_elapsed_ms: elapsed_ms_ceil(build_started.elapsed()),
                ..DiscoveryV2BuildTiming::default()
            },
        ));
    }
    let metric_started = Instant::now();
    let retained_metric_limit = retained_wallet_metric_limit(discovery);
    let mut wallet_metrics =
        Vec::with_capacity(retained_metric_limit.min(window_scan.wallets.len()));
    let mut filters = DiscoveryV2FilterStatusBuilder::default();
    let shadow_feedback = load_shadow_wallet_feedback(store, options.now)?;
    let mut wallet_metrics_total = 0usize;
    let mut metric_time_budget_exhausted = false;
    for (wallet_id, acc) in window_scan.wallets {
        if Instant::now() >= scan_deadline {
            metric_time_budget_exhausted = true;
            break;
        }
        let mut metric =
            wallet_metric_from_accumulator(wallet_id, acc, discovery, scoring_data_now);
        let feedback = shadow_feedback.get(&metric.wallet_id);
        apply_shadow_feedback(&mut metric, feedback);
        wallet_metrics_total = wallet_metrics_total.saturating_add(1);
        filters.observe_metric(&metric);
        retain_top_wallet_metric(&mut wallet_metrics, metric, retained_metric_limit);
    }
    if !metric_time_budget_exhausted {
        let terminal_total = window_scan.terminal_rejected.total();
        let terminal_reject_counts = window_scan.terminal_rejected.reject_counts().clone();
        let mut sampled_terminal_reject_counts = HashMap::<String, u64>::new();
        wallet_metrics_total = wallet_metrics_total.saturating_add(terminal_total);
        for (wallet_id, acc) in window_scan.terminal_rejected.samples() {
            if Instant::now() >= scan_deadline {
                metric_time_budget_exhausted = true;
                break;
            }
            *sampled_terminal_reject_counts
                .entry(acc.terminal_reject_reason().to_string())
                .or_insert(0) += 1;
            let metric =
                wallet_metric_from_accumulator(wallet_id, acc, discovery, scoring_data_now);
            filters.observe_metric(&metric);
            retain_top_wallet_metric(&mut wallet_metrics, metric, retained_metric_limit);
        }
        for (reason, count) in terminal_reject_counts {
            let sampled = sampled_terminal_reject_counts
                .get(&reason)
                .copied()
                .unwrap_or(0);
            filters.observe_rejected_wallets(&reason, count.saturating_sub(sampled));
        }
    }
    let metric_elapsed_ms = elapsed_ms_ceil(metric_started.elapsed());
    if metric_time_budget_exhausted {
        let scan = scan_status(
            options.max_rows,
            options.time_budget_ms,
            rows_seen,
            true,
            unique_wallets_seen,
        );
        return Ok(budget_exhausted_status(
            discovery,
            shadow,
            window_start,
            tail,
            coverage_sample,
            scan,
            unique_wallets_seen,
            &options,
            DiscoveryV2BuildTiming {
                tail_elapsed_ms,
                coverage_elapsed_ms,
                scan_elapsed_ms,
                metric_elapsed_ms,
                total_elapsed_ms: elapsed_ms_ceil(build_started.elapsed()),
                ..DiscoveryV2BuildTiming::default()
            },
        ));
    }
    let maturity_started = Instant::now();
    let maturity = apply_maturity_ranking(
        store,
        discovery,
        options.now,
        scan_deadline,
        &mut wallet_metrics,
    )?;
    let maturity_elapsed_ms = elapsed_ms_ceil(maturity_started.elapsed());
    sort_wallet_metrics(&mut wallet_metrics, discovery);
    let live_portfolio_started = Instant::now();
    let live_gate =
        apply_live_portfolio_gate(store, discovery, shadow, &options, &mut wallet_metrics)?;
    let live_portfolio_elapsed_ms = elapsed_ms_ceil(live_portfolio_started.elapsed());
    for reason in &live_gate.live_reject_reasons {
        filters.observe_live_rejection(reason);
    }
    let filters = filters.finish();
    let candidate_wallets = live_gate.candidate_wallets;
    let mut maturity = maturity;
    observe_selected_maturity_tiers(&mut maturity, &wallet_metrics, &candidate_wallets);
    let live_portfolio = live_gate.status;
    let shadow_signals_24h = Some(load_discovery_v2_shadow_signal_status(store, options.now)?);
    let mut blockers = blockers(
        discovery,
        shadow,
        &tail,
        coverage_sample.as_ref(),
        &scan,
        &candidate_wallets,
        options.execution_enabled,
        options.window_minutes,
    );
    if live_portfolio
        .as_ref()
        .is_some_and(|status| status.rpc_missing)
    {
        blockers.push("discovery_v2_live_portfolio_rpc_missing".to_string());
    }
    if live_portfolio.as_ref().is_some_and(|status| {
        status.checked_wallets >= status.max_wallets
            && candidate_wallets.len() < discovery.follow_top_n.max(1) as usize
    }) {
        blockers.push("discovery_v2_live_portfolio_candidate_budget_exhausted".to_string());
    }
    if maturity.time_budget_exhausted {
        blockers.push("discovery_v2_maturity_budget_exhausted".to_string());
    }
    let production_green = blockers.is_empty();
    let total_elapsed_ms = elapsed_ms_ceil(build_started.elapsed());
    let build_timing = DiscoveryV2BuildTiming {
        tail_elapsed_ms,
        coverage_elapsed_ms,
        scan_elapsed_ms,
        metric_elapsed_ms,
        maturity_elapsed_ms,
        live_portfolio_elapsed_ms,
        total_elapsed_ms,
    };
    Ok(DiscoveryV2Status {
        source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        now: options.now,
        build_elapsed_ms: total_elapsed_ms,
        build_timing,
        window_start,
        window_minutes: options.window_minutes,
        max_tail_lag_seconds: options.max_tail_lag_seconds,
        tail,
        coverage_sample,
        scan,
        maturity,
        live_portfolio,
        shadow_signals_24h,
        filters,
        wallet_metrics_total,
        wallet_metrics_returned: wallet_metrics.len(),
        wallet_metrics_truncated: wallet_metrics.len() < wallet_metrics_total,
        wallet_metrics,
        candidate_wallets,
        execution_enabled: options.execution_enabled,
        execution_disabled: !options.execution_enabled,
        blockers,
        production_green,
        policy_fingerprint: discovery_v2_policy_fingerprint(discovery, shadow, &options),
    })
}

fn retained_wallet_metric_limit(discovery: &DiscoveryConfig) -> usize {
    OPERATOR_WALLET_METRIC_LIMIT
        .max(discovery.follow_top_n.max(1) as usize)
        .max(discovery.live_portfolio_max_wallets)
}

fn budget_exhausted_status(
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    window_start: chrono::DateTime<chrono::Utc>,
    tail: Option<DiscoveryV2TailStatus>,
    coverage_sample: Option<DiscoveryV2CoverageSample>,
    scan: DiscoveryV2ScanStatus,
    unique_wallets_seen: usize,
    options: &DiscoveryV2BuildOptions,
    build_timing: DiscoveryV2BuildTiming,
) -> DiscoveryV2Status {
    let candidate_wallets = Vec::new();
    let filters = build_budget_exhausted_filter_status(unique_wallets_seen);
    let blockers = blockers(
        discovery,
        shadow,
        &tail,
        coverage_sample.as_ref(),
        &scan,
        &candidate_wallets,
        options.execution_enabled,
        options.window_minutes,
    );
    DiscoveryV2Status {
        source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        now: options.now,
        build_elapsed_ms: build_timing.total_elapsed_ms,
        build_timing,
        window_start,
        window_minutes: options.window_minutes,
        max_tail_lag_seconds: options.max_tail_lag_seconds,
        tail,
        coverage_sample,
        scan,
        maturity: configured_maturity_status(discovery),
        live_portfolio: None,
        shadow_signals_24h: None,
        filters,
        wallet_metrics_total: unique_wallets_seen,
        wallet_metrics_returned: 0,
        wallet_metrics_truncated: unique_wallets_seen > 0,
        wallet_metrics: Vec::new(),
        candidate_wallets,
        execution_enabled: options.execution_enabled,
        execution_disabled: !options.execution_enabled,
        blockers,
        production_green: false,
        policy_fingerprint: discovery_v2_policy_fingerprint(discovery, shadow, options),
    }
}

pub fn load_discovery_v2_shadow_signal_status(
    store: &SqliteDiscoveryStore,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<DiscoveryV2ShadowSignalStatus> {
    let since = now - chrono::Duration::hours(24);
    let summary = store
        .shadow_signal_summary_since(since)
        .context("failed loading discovery v2 shadow signal status")?;
    Ok(discovery_v2_shadow_signal_status(since, summary))
}

fn discovery_v2_shadow_signal_status(
    since: chrono::DateTime<chrono::Utc>,
    summary: ShadowSignalSummary,
) -> DiscoveryV2ShadowSignalStatus {
    DiscoveryV2ShadowSignalStatus {
        since,
        buy_signals: summary.buy_signals,
        sell_signals_total: summary.sell_signals_total,
        sell_signals_matched: summary.sell_signals_matched,
        sell_signals_no_position: summary.sell_signals_no_position,
        closed_trades: summary.closed_trades,
        wins: summary.wins,
        losses: summary.losses,
        pnl_sol: summary.pnl_sol,
        entry_cost_sol: summary.entry_cost_sol,
        roi: summary.roi(),
        avg_hold_seconds: summary.avg_hold_seconds,
        open_lots: summary.open_lots,
        open_notional_sol: summary.open_notional_sol,
    }
}

fn elapsed_ms_ceil(duration: StdDuration) -> u64 {
    duration.as_millis().max(1).min(u64::MAX as u128) as u64
}
