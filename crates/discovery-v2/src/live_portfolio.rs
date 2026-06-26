use crate::candidate_selection::{candidate_wallets_without_live_gate, stable_candidate_wallets};
use crate::live_portfolio_selection::process_live_portfolio_candidate_rows;
use crate::metric::{reject_wallet_metric, DiscoveryV2WalletMetric};
use crate::slow_hold::{SOURCE_BASELINE, SOURCE_SLOW_HOLD};
use crate::status::DiscoveryV2CandidateWalletSource;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig, DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS};
use copybot_core_types::TokenQualityCacheRow;
use copybot_storage_core::SqliteDiscoveryStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) const LIVE_PORTFOLIO_RPC_BATCH_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct LiveTokenPosition {
    pub mint: String,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct LivePortfolioSnapshot {
    pub sol_balance: f64,
    pub token_positions: Vec<LiveTokenPosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2LivePortfolioStatus {
    pub enabled: bool,
    pub checked_wallets: usize,
    pub accepted_wallets: usize,
    pub rejected_wallets: usize,
    pub rpc_failures: usize,
    pub max_wallets: usize,
    pub rpc_missing: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LivePortfolioEvaluation {
    pub accepted: bool,
    pub reject_reason: Option<&'static str>,
    pub sol_balance: f64,
    pub token_value_sol: f64,
    pub token_positions: u32,
    pub tradable_token_positions: u32,
}

pub(super) struct LivePortfolioGateResult {
    pub(super) candidate_wallets: Vec<String>,
    pub(super) candidate_wallet_sources: Vec<DiscoveryV2CandidateWalletSource>,
    pub(super) status: Option<DiscoveryV2LivePortfolioStatus>,
    pub(super) live_reject_reasons: Vec<String>,
}

pub(super) fn apply_live_portfolio_gate(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: &crate::policy::DiscoveryV2BuildOptions,
    metrics: &mut [DiscoveryV2WalletMetric],
) -> Result<LivePortfolioGateResult> {
    let active_follow_wallets = store.list_active_follow_wallets()?;
    if !discovery.live_portfolio_gate_enabled {
        let selection = candidate_wallets_without_live_gate(
            discovery,
            metrics,
            &active_follow_wallets,
            options.now,
        );
        return Ok(LivePortfolioGateResult {
            candidate_wallets: selection.wallets,
            candidate_wallet_sources: selection.sources,
            status: None,
            live_reject_reasons: Vec::new(),
        });
    }
    let mut status = DiscoveryV2LivePortfolioStatus {
        enabled: true,
        checked_wallets: 0,
        accepted_wallets: 0,
        rejected_wallets: 0,
        rpc_failures: 0,
        max_wallets: discovery.live_portfolio_max_wallets,
        rpc_missing: options.live_portfolio_rpc_url.is_none(),
    };
    let mut candidates = Vec::new();
    let mut live_reject_reasons = Vec::new();
    let client = options
        .live_portfolio_rpc_url
        .as_deref()
        .map(|url| {
            crate::live_portfolio_rpc::LivePortfolioRpcClient::new(
                url,
                discovery.live_portfolio_request_timeout_ms,
                discovery.live_portfolio_max_token_accounts,
            )
        })
        .transpose()?;

    let Some(client) = client.as_ref() else {
        for metric in metrics.iter_mut() {
            if status.checked_wallets >= status.max_wallets {
                break;
            }
            if !metric.eligible || metric.score < discovery.min_score {
                continue;
            }
            status.checked_wallets += 1;
            let reason = "live_portfolio_rpc_missing";
            reject_metric(metric, reason);
            live_reject_reasons.push(reason.to_string());
            status.rejected_wallets += 1;
        }
        return Ok(LivePortfolioGateResult {
            candidate_wallets: candidates,
            candidate_wallet_sources: Vec::new(),
            status: Some(status),
            live_reject_reasons,
        });
    };

    let eligible_wallets = stable_candidate_wallets(
        discovery,
        metrics,
        &active_follow_wallets,
        status.max_wallets,
        live_gate_candidate_pool_limit(discovery, status.max_wallets),
        options.now,
    );
    let mut candidate_sources = Vec::new();
    let baseline_rows = eligible_wallets
        .iter()
        .filter(|(_, _, source)| *source == SOURCE_BASELINE)
        .cloned()
        .collect::<Vec<_>>();
    process_live_portfolio_candidate_rows(
        store,
        client,
        discovery,
        shadow,
        options,
        metrics,
        &baseline_rows,
        discovery.follow_top_n.max(1) as usize,
        SOURCE_BASELINE,
        &mut status,
        &mut candidates,
        &mut candidate_sources,
        &mut live_reject_reasons,
    )?;
    if discovery.slow_hold_wallets_enabled {
        let slow_rows = eligible_wallets
            .iter()
            .filter(|(_, _, source)| *source == SOURCE_SLOW_HOLD)
            .cloned()
            .collect::<Vec<_>>();
        process_live_portfolio_candidate_rows(
            store,
            client,
            discovery,
            shadow,
            options,
            metrics,
            &slow_rows,
            discovery.slow_hold_top_m.max(1) as usize,
            SOURCE_SLOW_HOLD,
            &mut status,
            &mut candidates,
            &mut candidate_sources,
            &mut live_reject_reasons,
        )?;
    }
    Ok(LivePortfolioGateResult {
        candidate_wallets: candidates,
        candidate_wallet_sources: candidate_sources,
        status: Some(status),
        live_reject_reasons,
    })
}

fn live_gate_candidate_pool_limit(discovery: &DiscoveryConfig, max_wallets: usize) -> usize {
    if discovery.slow_hold_wallets_enabled {
        max_wallets.saturating_add(discovery.slow_hold_top_m.max(1) as usize)
    } else {
        max_wallets
    }
}

pub(crate) fn evaluate_live_portfolio_snapshot(
    snapshot: &LivePortfolioSnapshot,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    token_prices_sol: &HashMap<String, f64>,
    quality_cache: &HashMap<String, TokenQualityCacheRow>,
    now: DateTime<Utc>,
) -> LivePortfolioEvaluation {
    if snapshot.sol_balance >= discovery.min_live_sol_balance {
        return evaluation(true, None, snapshot.sol_balance, 0.0, snapshot, 0);
    }
    let mut token_value_sol = 0.0;
    let mut tradable_positions = 0u32;
    for position in snapshot
        .token_positions
        .iter()
        .filter(|position| position.amount > 0.0 && position.amount.is_finite())
    {
        let Some(price) = token_prices_sol.get(&position.mint).copied() else {
            continue;
        };
        if !quality_satisfies_shadow_gate(quality_cache.get(&position.mint), shadow, now) {
            continue;
        }
        let value = position.amount * price;
        if value.is_finite() && value > 0.0 {
            token_value_sol += value;
            tradable_positions = tradable_positions.saturating_add(1);
        }
    }
    let accepted = token_value_sol >= discovery.min_live_portfolio_value_sol;
    let reject_reason = if accepted {
        None
    } else if snapshot.token_positions.is_empty() {
        Some("capital_drained_after_window")
    } else if token_value_sol > 0.0 {
        Some("only_dust_positions")
    } else {
        Some("only_illiquid_positions")
    };
    evaluation(
        accepted,
        reject_reason,
        snapshot.sol_balance,
        token_value_sol,
        snapshot,
        tradable_positions,
    )
}

pub(super) fn load_live_token_quality(
    store: &SqliteDiscoveryStore,
    snapshot: &LivePortfolioSnapshot,
) -> Result<HashMap<String, TokenQualityCacheRow>> {
    let mut rows = HashMap::new();
    for position in &snapshot.token_positions {
        if rows.contains_key(&position.mint) || position.amount <= 0.0 {
            continue;
        }
        if let Some(row) = store.get_token_quality_cache(&position.mint)? {
            rows.insert(position.mint.clone(), row);
        }
    }
    Ok(rows)
}

pub(super) fn load_live_token_prices(
    store: &SqliteDiscoveryStore,
    snapshot: &LivePortfolioSnapshot,
    now: DateTime<Utc>,
) -> Result<HashMap<String, f64>> {
    let mut rows = HashMap::new();
    for position in &snapshot.token_positions {
        if rows.contains_key(&position.mint) || position.amount <= 0.0 {
            continue;
        }
        if let Some(price) = store.latest_token_sol_price(&position.mint, now)? {
            rows.insert(position.mint.clone(), price);
        }
    }
    Ok(rows)
}

fn quality_satisfies_shadow_gate(
    quality: Option<&TokenQualityCacheRow>,
    shadow: &ShadowConfig,
    now: DateTime<Utc>,
) -> bool {
    if !shadow.quality_gates_enabled {
        return true;
    }
    let Some(quality) = quality else {
        return false;
    };
    if quality.fetched_at > now
        || now - quality.fetched_at > Duration::seconds(DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS)
    {
        return false;
    }
    if shadow.min_token_age_seconds > 0
        && !quality
            .token_age_seconds
            .is_some_and(|age| age >= shadow.min_token_age_seconds)
    {
        return false;
    }
    if shadow.min_holders > 0
        && !quality
            .holders
            .is_some_and(|holders| holders >= shadow.min_holders)
    {
        return false;
    }
    if shadow.min_liquidity_sol > 0.0
        && !quality
            .liquidity_sol
            .is_some_and(|liquidity| liquidity + 1e-12 >= shadow.min_liquidity_sol)
    {
        return false;
    }
    true
}

pub(super) fn reject_metric(metric: &mut DiscoveryV2WalletMetric, reason: &str) {
    reject_wallet_metric(metric, reason);
}

fn evaluation(
    accepted: bool,
    reject_reason: Option<&'static str>,
    sol_balance: f64,
    token_value_sol: f64,
    snapshot: &LivePortfolioSnapshot,
    tradable_token_positions: u32,
) -> LivePortfolioEvaluation {
    LivePortfolioEvaluation {
        accepted,
        reject_reason,
        sol_balance,
        token_value_sol,
        token_positions: snapshot.token_positions.len() as u32,
        tradable_token_positions,
    }
}
