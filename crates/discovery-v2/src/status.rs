use crate::filters::{build_budget_exhausted_filter_status, DiscoveryV2FilterStatusBuilder};
use crate::live_portfolio::apply_live_portfolio_gate;
use crate::metric::wallet_metric_from_accumulator;
use crate::policy::{discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions};
use crate::status::status_blockers::blockers;
use crate::status::status_load::{load_coverage_sample, load_tail_status, scan_window_metrics};
use crate::status::status_rank::{retain_top_wallet_metric, scan_status, sort_wallet_metrics};
use anyhow::Result;
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::SqliteDiscoveryStore;
use std::time::{Duration as StdDuration, Instant};

pub use crate::filters::DiscoveryV2FilterStatus;
pub use crate::live_portfolio::DiscoveryV2LivePortfolioStatus;
pub use status_types::{
    DiscoveryV2CoverageSample, DiscoveryV2ScanStatus, DiscoveryV2Status, DiscoveryV2TailStatus,
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
    let window_start = options.window_start();
    let scan_deadline = Instant::now() + StdDuration::from_millis(options.time_budget_ms);
    let tail = load_tail_status(store, options.now, options.max_tail_lag_seconds)?;
    let scoring_data_now = tail
        .as_ref()
        .map(|status| status.cursor.ts_utc.min(options.now))
        .unwrap_or(options.now);
    let coverage_sample = load_coverage_sample(store, window_start)?;
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
        ));
    }
    let retained_metric_limit = retained_wallet_metric_limit(discovery);
    let mut wallet_metrics =
        Vec::with_capacity(retained_metric_limit.min(window_scan.wallets.len()));
    let mut filters = DiscoveryV2FilterStatusBuilder::default();
    let mut wallet_metrics_total = 0usize;
    let mut metric_time_budget_exhausted = false;
    for (wallet_id, acc) in window_scan.wallets {
        if Instant::now() >= scan_deadline {
            metric_time_budget_exhausted = true;
            break;
        }
        let metric = wallet_metric_from_accumulator(wallet_id, acc, discovery, scoring_data_now);
        wallet_metrics_total = wallet_metrics_total.saturating_add(1);
        filters.observe_metric(&metric);
        retain_top_wallet_metric(&mut wallet_metrics, metric, retained_metric_limit);
    }
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
        ));
    }
    sort_wallet_metrics(&mut wallet_metrics);
    let live_gate =
        apply_live_portfolio_gate(store, discovery, shadow, &options, &mut wallet_metrics)?;
    for reason in &live_gate.live_reject_reasons {
        filters.observe_live_rejection(reason);
    }
    let filters = filters.finish();
    let candidate_wallets = live_gate.candidate_wallets;
    let live_portfolio = live_gate.status;
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
    let production_green = blockers.is_empty();
    Ok(DiscoveryV2Status {
        source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        now: options.now,
        window_start,
        window_minutes: options.window_minutes,
        max_tail_lag_seconds: options.max_tail_lag_seconds,
        tail,
        coverage_sample,
        scan,
        live_portfolio,
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
        window_start,
        window_minutes: options.window_minutes,
        max_tail_lag_seconds: options.max_tail_lag_seconds,
        tail,
        coverage_sample,
        scan,
        live_portfolio: None,
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
