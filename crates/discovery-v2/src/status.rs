use crate::filters::{build_budget_exhausted_filter_status, build_filter_status};
use crate::metric::wallet_metric_from_accumulator;
use crate::policy::{discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions};
use crate::status::status_blockers::blockers;
use crate::status::status_load::{load_coverage_sample, load_tail_status, scan_window_metrics};
use crate::status::status_rank::{candidate_wallets, scan_status, sort_wallet_metrics};
use anyhow::Result;
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::SqliteDiscoveryStore;
use std::time::{Duration as StdDuration, Instant};

pub use crate::filters::DiscoveryV2FilterStatus;
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
    let coverage_sample = load_coverage_sample(store, window_start)?;
    let window_scan = scan_window_metrics(
        store,
        discovery,
        shadow,
        window_start,
        options.now,
        options.max_rows,
        scan_deadline,
    )?;
    let rows_seen = window_scan.rows_seen;
    let unique_wallets_seen = window_scan.unique_wallets_seen;
    let scoring_data_now = tail
        .as_ref()
        .map(|status| status.cursor.ts_utc.min(options.now))
        .unwrap_or(options.now);
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
    let token_sol_history = window_scan.token_sol_history;
    let mut wallet_metrics = Vec::with_capacity(window_scan.wallets.len());
    let mut metric_time_budget_exhausted = false;
    for (wallet_id, acc) in window_scan.wallets {
        if Instant::now() >= scan_deadline {
            metric_time_budget_exhausted = true;
            break;
        }
        wallet_metrics.push(wallet_metric_from_accumulator(
            wallet_id,
            acc,
            discovery,
            &token_sol_history,
            scoring_data_now,
        ));
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
    let candidate_wallets = candidate_wallets(discovery, &wallet_metrics);
    let filters = build_filter_status(&wallet_metrics);
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
        filters,
        wallet_metrics_total: wallet_metrics.len(),
        wallet_metrics_returned: wallet_metrics.len(),
        wallet_metrics_truncated: false,
        wallet_metrics,
        candidate_wallets,
        execution_enabled: options.execution_enabled,
        execution_disabled: !options.execution_enabled,
        blockers,
        production_green,
        policy_fingerprint: discovery_v2_policy_fingerprint(discovery, shadow, &options),
    })
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
