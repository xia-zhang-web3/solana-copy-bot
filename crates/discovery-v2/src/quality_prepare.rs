use crate::policy::TOKEN_QUALITY_TTL_SECONDS;
use crate::token_market::{is_sol_buy, sol_leg_token_and_notional};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::DiscoveryConfig;
use copybot_config::ShadowConfig;
use copybot_core_types::SwapEvent;
use copybot_storage_core::SqliteDiscoveryStore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration as StdDuration, Instant};

#[derive(Debug, Clone)]
pub struct DiscoveryV2PrepareQualityOptions {
    pub now: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_rows: usize,
    pub time_budget_ms: u64,
    pub max_mints: usize,
    pub commit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2PrepareQualityReport {
    pub dry_run: bool,
    pub committed: bool,
    pub quality_source: String,
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_rows: usize,
    pub max_mints: usize,
    pub rows_scanned: usize,
    pub unique_buy_mints: usize,
    pub mints_considered: usize,
    pub skipped_fresh_complete: usize,
    pub upserted: usize,
    pub incomplete_after_prepare: usize,
    pub max_rows_exhausted: bool,
    pub time_budget_exhausted: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone)]
struct ObservedQualityEvidence {
    mint: String,
    first_seen: DateTime<Utc>,
    max_sol_notional: f64,
    buy_count: u64,
    sol_trade_count: u64,
    wallets: HashSet<String>,
}

impl DiscoveryV2PrepareQualityOptions {
    pub fn from_config(
        discovery: &DiscoveryConfig,
        now: DateTime<Utc>,
        max_mints: usize,
        commit: bool,
    ) -> Self {
        Self {
            now,
            window_minutes: u64::from(discovery.scoring_window_days.max(1)) * 24 * 60,
            max_rows: discovery.max_window_swaps_in_memory.max(1),
            time_budget_ms: discovery.fetch_time_budget_ms.max(1),
            max_mints: max_mints.max(1),
            commit,
        }
    }

    fn window_start(&self) -> DateTime<Utc> {
        self.now - Duration::minutes(self.window_minutes.min(i64::MAX as u64) as i64)
    }
}

pub fn prepare_discovery_v2_quality(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: DiscoveryV2PrepareQualityOptions,
) -> Result<DiscoveryV2PrepareQualityReport> {
    let window_start = options.window_start();
    let deadline = Instant::now() + StdDuration::from_millis(options.time_budget_ms);
    let mut rows_seen = 0usize;
    let mut evidence = HashMap::<String, ObservedQualityEvidence>::new();
    let wallet_evidence_cap = wallet_evidence_cap(shadow);
    let page = store
        .for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
            window_start,
            options.now,
            None,
            options.max_rows.saturating_add(1),
            deadline,
            |swap| {
                rows_seen = rows_seen.saturating_add(1);
                if rows_seen > options.max_rows {
                    return Ok(());
                }
                observe_quality_evidence(&mut evidence, &swap, wallet_evidence_cap);
                Ok(())
            },
        )
        .context("failed streaming discovery v2 quality prepare window")?;
    evidence.retain(|_, value| value.buy_count > 0);
    let window_truncated = rows_seen > options.max_rows;
    let time_budget_exhausted = page.time_budget_exhausted;
    let unique_buy_mints = evidence.len();
    let mut ranked = evidence.into_values().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .max_sol_notional
            .partial_cmp(&left.max_sol_notional)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.buy_count.cmp(&left.buy_count))
            .then_with(|| right.wallets.len().cmp(&left.wallets.len()))
            .then_with(|| left.first_seen.cmp(&right.first_seen))
            .then_with(|| left.mint.cmp(&right.mint))
    });
    let considered = ranked
        .into_iter()
        .take(options.max_mints)
        .collect::<Vec<_>>();

    let ttl = Duration::seconds(TOKEN_QUALITY_TTL_SECONDS);
    let mut skipped_fresh_complete = 0usize;
    let mut upsert_candidates = Vec::new();
    let mut incomplete_after_prepare = 0usize;
    for evidence in &considered {
        let existing = store
            .get_token_quality_cache(&evidence.mint)
            .with_context(|| format!("failed loading token quality cache for {}", evidence.mint))?;
        if existing.as_ref().is_some_and(|row| {
            row.fetched_at <= options.now
                && options.now - row.fetched_at <= ttl
                && quality_row_has_required_fields(row, shadow)
        }) {
            skipped_fresh_complete = skipped_fresh_complete.saturating_add(1);
            continue;
        }
        let holders = Some(evidence.wallets.len().max(1) as u64);
        let token_age_seconds = Some(
            options
                .now
                .signed_duration_since(evidence.first_seen)
                .num_seconds()
                .max(0) as u64,
        );
        let liquidity_sol = Some(evidence.max_sol_notional.max(0.0));
        if !observed_evidence_satisfies_required_fields(
            holders,
            liquidity_sol,
            token_age_seconds,
            shadow,
        ) {
            incomplete_after_prepare = incomplete_after_prepare.saturating_add(1);
            continue;
        }
        upsert_candidates.push((
            evidence.mint.clone(),
            holders,
            liquidity_sol,
            token_age_seconds,
        ));
    }

    let mut blockers = Vec::new();
    if rows_seen == 0 {
        blockers.push("discovery_v2_quality_prepare_observed_window_empty".to_string());
    }
    if window_truncated {
        blockers.push("discovery_v2_quality_prepare_max_rows_exhausted".to_string());
    }
    if time_budget_exhausted {
        blockers.push("discovery_v2_quality_prepare_time_budget_exhausted".to_string());
    }
    if unique_buy_mints > options.max_mints {
        blockers.push("discovery_v2_quality_prepare_mint_budget_exhausted".to_string());
    }
    if incomplete_after_prepare > 0 {
        blockers.push("discovery_v2_quality_prepare_incomplete_evidence".to_string());
    }
    if discovery.max_window_swaps_in_memory == 0 {
        blockers.push("discovery_v2_quality_prepare_invalid_max_rows".to_string());
    }

    let commit_allowed = options.commit && blockers.is_empty();
    let mut upserted = if options.commit {
        0
    } else {
        upsert_candidates.len()
    };
    if commit_allowed {
        for (mint, holders, liquidity_sol, token_age_seconds) in &upsert_candidates {
            store
                .upsert_token_quality_cache(
                    mint,
                    *holders,
                    *liquidity_sol,
                    *token_age_seconds,
                    options.now,
                )
                .with_context(|| {
                    format!("failed upserting discovery v2 quality evidence for {mint}")
                })?;
            upserted = upserted.saturating_add(1);
        }
    }

    Ok(DiscoveryV2PrepareQualityReport {
        dry_run: !options.commit,
        committed: commit_allowed,
        quality_source: "observed_window_proxy".to_string(),
        now: options.now,
        window_start,
        window_minutes: options.window_minutes,
        max_rows: options.max_rows,
        max_mints: options.max_mints,
        rows_scanned: rows_seen,
        unique_buy_mints,
        mints_considered: considered.len(),
        skipped_fresh_complete,
        upserted,
        incomplete_after_prepare,
        max_rows_exhausted: window_truncated,
        time_budget_exhausted,
        blockers,
    })
}

fn observe_quality_evidence(
    evidence: &mut HashMap<String, ObservedQualityEvidence>,
    swap: &SwapEvent,
    wallet_evidence_cap: usize,
) {
    let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) else {
        return;
    };
    if !sol_notional.is_finite() || sol_notional <= 0.0 {
        return;
    }
    let entry = evidence
        .entry(token.to_string())
        .or_insert_with(|| ObservedQualityEvidence {
            mint: token.to_string(),
            first_seen: swap.ts_utc,
            max_sol_notional: 0.0,
            buy_count: 0,
            sol_trade_count: 0,
            wallets: HashSet::new(),
        });
    entry.first_seen = entry.first_seen.min(swap.ts_utc);
    entry.max_sol_notional = entry.max_sol_notional.max(sol_notional);
    entry.sol_trade_count = entry.sol_trade_count.saturating_add(1);
    if entry.wallets.len() < wallet_evidence_cap || entry.wallets.contains(&swap.wallet) {
        entry.wallets.insert(swap.wallet.clone());
    }
    if is_sol_buy(swap) {
        entry.buy_count = entry.buy_count.saturating_add(1);
    }
}

fn wallet_evidence_cap(shadow: &ShadowConfig) -> usize {
    shadow
        .min_holders
        .max(shadow.min_unique_traders_5m)
        .max(1)
        .min(usize::MAX as u64) as usize
}

fn quality_row_has_required_fields(
    row: &copybot_core_types::TokenQualityCacheRow,
    shadow: &ShadowConfig,
) -> bool {
    observed_evidence_satisfies_required_fields(
        row.holders,
        row.liquidity_sol,
        row.token_age_seconds,
        shadow,
    )
}

fn observed_evidence_satisfies_required_fields(
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    shadow: &ShadowConfig,
) -> bool {
    (shadow.min_holders == 0 || holders.is_some())
        && (shadow.min_token_age_seconds == 0 || token_age_seconds.is_some())
        && (shadow.min_liquidity_sol <= 0.0
            || liquidity_sol.is_some_and(|value| value.is_finite() && value >= 0.0))
}
