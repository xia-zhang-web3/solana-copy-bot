use crate::metric::DiscoveryV2WalletMetric;
use crate::token_market::SolLegTrade;
use crate::tradability::TOKEN_ROLLING_MARKET_WINDOW_SECONDS;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig, DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS};
use copybot_core_types::TokenQualityCacheRow;
use copybot_storage_core::SqliteDiscoveryStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

pub(super) fn apply_live_portfolio_gate(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: &crate::policy::DiscoveryV2BuildOptions,
    token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    metrics: &mut [DiscoveryV2WalletMetric],
) -> Result<(Vec<String>, Option<DiscoveryV2LivePortfolioStatus>)> {
    if !discovery.live_portfolio_gate_enabled {
        return Ok((
            candidate_wallets_without_live_gate(discovery, metrics),
            None,
        ));
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

    for metric in metrics.iter_mut() {
        if candidates.len() >= discovery.follow_top_n.max(1) as usize {
            break;
        }
        if status.checked_wallets >= status.max_wallets {
            break;
        }
        if !metric.eligible || metric.score < discovery.min_score {
            continue;
        }
        status.checked_wallets += 1;
        let Some(client) = client.as_ref() else {
            reject_metric(metric, "live_portfolio_rpc_missing");
            status.rejected_wallets += 1;
            continue;
        };
        let snapshot = match client.fetch_snapshot(&metric.wallet_id) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                reject_metric(metric, "live_portfolio_rpc_unavailable");
                status.rpc_failures += 1;
                status.rejected_wallets += 1;
                continue;
            }
        };
        let quality_cache = load_live_token_quality(store, &snapshot)?;
        let evaluation = evaluate_live_portfolio_snapshot(
            &snapshot,
            discovery,
            shadow,
            token_sol_history,
            &quality_cache,
            options.now,
        );
        metric.live_sol_balance = Some(evaluation.sol_balance);
        metric.live_token_value_sol = Some(evaluation.token_value_sol);
        metric.live_token_positions = Some(evaluation.token_positions);
        metric.live_tradable_token_positions = Some(evaluation.tradable_token_positions);
        if evaluation.accepted {
            status.accepted_wallets += 1;
            candidates.push(metric.wallet_id.clone());
        } else {
            reject_metric(
                metric,
                evaluation
                    .reject_reason
                    .unwrap_or("live_portfolio_rejected"),
            );
            status.rejected_wallets += 1;
        }
    }
    Ok((candidates, Some(status)))
}

pub(crate) fn evaluate_live_portfolio_snapshot(
    snapshot: &LivePortfolioSnapshot,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
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
        let Some(price) = recent_price_sol(&position.mint, token_sol_history, now) else {
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

fn load_live_token_quality(
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

fn recent_price_sol(
    mint: &str,
    token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    now: DateTime<Utc>,
) -> Option<f64> {
    let cutoff = now - Duration::seconds(TOKEN_ROLLING_MARKET_WINDOW_SECONDS);
    token_sol_history.get(mint)?.iter().rev().find_map(|trade| {
        if trade.ts < cutoff
            || trade.ts > now
            || trade.sol_notional <= 0.0
            || trade.token_qty <= 0.0
        {
            return None;
        }
        Some(trade.sol_notional / trade.token_qty)
    })
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

fn reject_metric(metric: &mut DiscoveryV2WalletMetric, reason: &str) {
    metric.eligible = false;
    metric.score = 0.0;
    if !metric
        .reject_reasons
        .iter()
        .any(|existing| existing == reason)
    {
        metric.reject_reasons.push(reason.to_string());
    }
}

fn candidate_wallets_without_live_gate(
    discovery: &DiscoveryConfig,
    metrics: &[DiscoveryV2WalletMetric],
) -> Vec<String> {
    metrics
        .iter()
        .filter(|metric| metric.eligible && metric.score >= discovery.min_score)
        .take(discovery.follow_top_n.max(1) as usize)
        .map(|metric| metric.wallet_id.clone())
        .collect()
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
