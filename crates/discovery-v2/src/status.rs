use crate::filters::build_filter_status;
use crate::metric::{wallet_metric_from_accumulator, DiscoveryV2WalletMetric};
use crate::policy::{discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions};
use crate::tradability::{build_token_sol_history, build_wallet_accumulators};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{SwapEvent, TokenQualityCacheRow};
use copybot_storage_core::{DiscoveryRuntimeCursor, SqliteDiscoveryStore};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration as StdDuration, Instant};

pub use crate::filters::DiscoveryV2FilterStatus;

pub const DISCOVERY_V2_SCORING_SOURCE: &str = "discovery_v2_operational_window";
const TOKEN_QUALITY_TTL_SECONDS: i64 = 10 * 60;

include!("status_types.rs");

pub fn build_discovery_v2_status(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: DiscoveryV2BuildOptions,
) -> Result<DiscoveryV2Status> {
    let window_start = options.window_start();
    let tail = load_tail_status(store, options.now, options.max_tail_lag_seconds)?;
    let coverage_sample = load_coverage_sample(store, window_start)?;
    let deadline = Instant::now() + StdDuration::from_millis(options.time_budget_ms);
    let mut swaps = Vec::new();
    let mut accepted_rows = 0usize;
    let page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
        window_start,
        options.now,
        None,
        options.max_rows.saturating_add(1),
        deadline,
        |swap| {
            if accepted_rows < options.max_rows {
                swaps.push(swap);
            }
            accepted_rows = accepted_rows.saturating_add(1);
            Ok(())
        },
    )?;
    let token_quality_cache = load_token_quality_cache_for_swaps(store, &swaps, options.now)?;
    let accumulators = build_wallet_accumulators(&swaps, discovery, shadow, &token_quality_cache);
    let token_sol_history = build_token_sol_history(&swaps);
    let mut wallet_metrics = accumulators
        .into_iter()
        .map(|(wallet_id, acc)| {
            wallet_metric_from_accumulator(
                wallet_id,
                acc,
                discovery,
                &token_sol_history,
                options.now,
            )
        })
        .collect::<Vec<_>>();
    sort_wallet_metrics(&mut wallet_metrics);
    let candidate_wallets = candidate_wallets(discovery, &wallet_metrics);
    let scan = scan_status(
        options.max_rows,
        options.time_budget_ms,
        accepted_rows,
        page.time_budget_exhausted,
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
        policy_fingerprint: discovery_v2_policy_fingerprint(discovery, &options),
    })
}

include!("status_load.rs");
include!("status_rank.rs");
include!("status_blockers.rs");
