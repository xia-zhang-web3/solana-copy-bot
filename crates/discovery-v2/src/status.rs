use crate::filters::build_filter_status;
use crate::metric::wallet_metric_from_accumulator;
use crate::policy::{discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions};
use crate::status::status_blockers::blockers;
use crate::status::status_load::{
    load_coverage_sample, load_tail_status, load_token_quality_cache_for_swaps,
    load_window_tail_swaps,
};
use crate::status::status_rank::{candidate_wallets, scan_status, sort_wallet_metrics};
use crate::tradability::{build_token_sol_history, build_wallet_accumulators};
use anyhow::Result;
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::SqliteDiscoveryStore;
use std::time::{Duration as StdDuration, Instant};

pub use crate::filters::DiscoveryV2FilterStatus;
pub use status_types::{
    DiscoveryV2CoverageSample, DiscoveryV2ScanStatus, DiscoveryV2Status, DiscoveryV2TailStatus,
};

#[path = "status_blockers.rs"]
mod status_blockers;
#[path = "status_load.rs"]
mod status_load;
#[path = "status_rank.rs"]
mod status_rank;
#[path = "status_types.rs"]
mod status_types;

pub const DISCOVERY_V2_SCORING_SOURCE: &str = "discovery_v2_operational_window";
pub use crate::policy::TOKEN_QUALITY_TTL_SECONDS;

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
    let (swaps, window_truncated, time_budget_exhausted) = load_window_tail_swaps(
        store,
        window_start,
        options.now,
        options.max_rows,
        scan_deadline,
    )?;
    let token_quality_cache = load_token_quality_cache_for_swaps(store, &swaps, options.now)?;
    let accumulators = build_wallet_accumulators(&swaps, discovery, shadow, &token_quality_cache);
    let token_sol_history = build_token_sol_history(&swaps);
    let scoring_data_now = tail
        .as_ref()
        .map(|status| status.cursor.ts_utc.min(options.now))
        .unwrap_or(options.now);
    let mut wallet_metrics = accumulators
        .into_iter()
        .map(|(wallet_id, acc)| {
            wallet_metric_from_accumulator(
                wallet_id,
                acc,
                discovery,
                &token_sol_history,
                scoring_data_now,
            )
        })
        .collect::<Vec<_>>();
    sort_wallet_metrics(&mut wallet_metrics);
    let candidate_wallets = candidate_wallets(discovery, &wallet_metrics);
    let scan = scan_status(
        options.max_rows,
        options.time_budget_ms,
        swaps.len().saturating_add(usize::from(window_truncated)),
        time_budget_exhausted,
        &wallet_metrics,
    );
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
        wallet_metrics,
        candidate_wallets,
        execution_enabled: options.execution_enabled,
        execution_disabled: !options.execution_enabled,
        blockers,
        production_green,
        policy_fingerprint: discovery_v2_policy_fingerprint(discovery, shadow, &options),
    })
}
