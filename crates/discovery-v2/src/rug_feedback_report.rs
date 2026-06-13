use crate::status::DISCOVERY_V2_SCORING_SOURCE;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{RugWalletFeedback, SqliteDiscoveryStore};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct DiscoveryV2RugFeedbackDistributionOptions {
    pub generated_at: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub min_closed_trades: u32,
    pub max_stale_terminal_rate: f64,
    pub max_stale_terminal_pnl_sol: f64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2RugFeedbackDistributionReport {
    pub source: String,
    pub generated_at: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub min_closed_trades: u32,
    pub max_stale_terminal_rate: f64,
    pub max_stale_terminal_pnl_sol: f64,
    pub wallet_feedback_count: usize,
    pub eligible_wallet_count: usize,
    pub active_follow_wallet_count: usize,
    pub active_follow_eligible_count: usize,
    pub threshold_rejected_wallet_count: usize,
    pub rate_rejected_wallet_count: usize,
    pub pnl_rejected_wallet_count: usize,
    pub active_follow_rejected_wallet_count: usize,
    pub totals: DiscoveryV2RugFeedbackTotals,
    pub stale_terminal_rate_percentiles: DiscoveryV2Percentiles,
    pub stale_terminal_pnl_percentiles: DiscoveryV2Percentiles,
    pub stale_terminal_rate_buckets: Vec<DiscoveryV2Bucket>,
    pub stale_terminal_pnl_buckets: Vec<DiscoveryV2Bucket>,
    pub worst_wallets: Vec<DiscoveryV2RugFeedbackWalletRow>,
    pub active_follow_rejected_wallets: Vec<DiscoveryV2RugFeedbackWalletRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2RugFeedbackTotals {
    pub closed_trades: u64,
    pub stale_terminal_closes: u64,
    pub stale_terminal_pnl_sol: f64,
    pub stale_terminal_entry_cost_sol: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2Percentiles {
    pub p10: Option<f64>,
    pub p50: Option<f64>,
    pub p90: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2Bucket {
    pub label: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2RugFeedbackWalletRow {
    pub wallet_id: String,
    pub active_follow: bool,
    pub closed_trades: u64,
    pub stale_terminal_closes: u64,
    pub stale_terminal_rate: Option<f64>,
    pub stale_terminal_pnl_sol: f64,
    pub stale_terminal_entry_cost_sol: f64,
    pub rate_rejected: bool,
    pub pnl_rejected: bool,
    pub rejected: bool,
}

pub fn build_discovery_v2_rug_feedback_distribution_report(
    store: &SqliteDiscoveryStore,
    options: DiscoveryV2RugFeedbackDistributionOptions,
) -> Result<DiscoveryV2RugFeedbackDistributionReport> {
    let feedback = store.rug_wallet_feedback_between(options.since, options.until)?;
    let active_follow_wallets = store.list_active_follow_wallets()?;
    let active_follow = active_follow_wallets
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let min_closed = u64::from(options.min_closed_trades.max(1));
    let mut rows = feedback
        .iter()
        .filter(|(_, feedback)| feedback.closed_trades >= min_closed)
        .map(|(wallet_id, feedback)| {
            wallet_row(
                wallet_id,
                *feedback,
                active_follow.contains(wallet_id.as_str()),
                &options,
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(worst_wallet_order);
    let mut rate_values = Vec::with_capacity(rows.len());
    let mut pnl_values = Vec::with_capacity(rows.len());
    let mut totals = DiscoveryV2RugFeedbackTotals {
        closed_trades: 0,
        stale_terminal_closes: 0,
        stale_terminal_pnl_sol: 0.0,
        stale_terminal_entry_cost_sol: 0.0,
    };
    let mut rate_rejected = 0usize;
    let mut pnl_rejected = 0usize;
    let mut active_follow_eligible = 0usize;
    let mut active_follow_rejected = 0usize;
    for row in &rows {
        totals.closed_trades = totals.closed_trades.saturating_add(row.closed_trades);
        totals.stale_terminal_closes = totals
            .stale_terminal_closes
            .saturating_add(row.stale_terminal_closes);
        totals.stale_terminal_pnl_sol += row.stale_terminal_pnl_sol;
        totals.stale_terminal_entry_cost_sol += row.stale_terminal_entry_cost_sol;
        if let Some(rate) = row.stale_terminal_rate {
            rate_values.push(rate);
        }
        pnl_values.push(row.stale_terminal_pnl_sol);
        rate_rejected += usize::from(row.rate_rejected);
        pnl_rejected += usize::from(row.pnl_rejected);
        active_follow_eligible += usize::from(row.active_follow);
        active_follow_rejected += usize::from(row.active_follow && row.rejected);
    }
    let limit = options.limit.max(1);
    let active_follow_rejected_wallets = rows
        .iter()
        .filter(|row| row.active_follow && row.rejected)
        .take(limit)
        .cloned()
        .collect();
    Ok(DiscoveryV2RugFeedbackDistributionReport {
        source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        generated_at: options.generated_at,
        since: options.since,
        until: options.until,
        min_closed_trades: options.min_closed_trades.max(1),
        max_stale_terminal_rate: options.max_stale_terminal_rate,
        max_stale_terminal_pnl_sol: options.max_stale_terminal_pnl_sol,
        wallet_feedback_count: feedback.len(),
        eligible_wallet_count: rows.len(),
        active_follow_wallet_count: active_follow_wallets.len(),
        active_follow_eligible_count: active_follow_eligible,
        threshold_rejected_wallet_count: rows.iter().filter(|row| row.rejected).count(),
        rate_rejected_wallet_count: rate_rejected,
        pnl_rejected_wallet_count: pnl_rejected,
        active_follow_rejected_wallet_count: active_follow_rejected,
        totals,
        stale_terminal_rate_percentiles: percentiles(rate_values),
        stale_terminal_pnl_percentiles: percentiles(pnl_values),
        stale_terminal_rate_buckets: bucketize(&rows, rate_bucket_label),
        stale_terminal_pnl_buckets: bucketize(&rows, pnl_bucket_label),
        worst_wallets: rows.into_iter().take(limit).collect(),
        active_follow_rejected_wallets,
    })
}

fn wallet_row(
    wallet_id: &str,
    feedback: RugWalletFeedback,
    active_follow: bool,
    options: &DiscoveryV2RugFeedbackDistributionOptions,
) -> DiscoveryV2RugFeedbackWalletRow {
    let stale_terminal_rate = feedback.stale_terminal_rate();
    let rate_rejected =
        stale_terminal_rate.is_some_and(|rate| rate > options.max_stale_terminal_rate);
    let pnl_rejected = feedback.stale_terminal_pnl_sol < options.max_stale_terminal_pnl_sol;
    DiscoveryV2RugFeedbackWalletRow {
        wallet_id: wallet_id.to_string(),
        active_follow,
        closed_trades: feedback.closed_trades,
        stale_terminal_closes: feedback.stale_terminal_closes,
        stale_terminal_rate,
        stale_terminal_pnl_sol: feedback.stale_terminal_pnl_sol,
        stale_terminal_entry_cost_sol: feedback.stale_terminal_entry_cost_sol,
        rate_rejected,
        pnl_rejected,
        rejected: rate_rejected || pnl_rejected,
    }
}

fn worst_wallet_order(
    left: &DiscoveryV2RugFeedbackWalletRow,
    right: &DiscoveryV2RugFeedbackWalletRow,
) -> std::cmp::Ordering {
    left.stale_terminal_pnl_sol
        .total_cmp(&right.stale_terminal_pnl_sol)
        .then_with(|| {
            right
                .stale_terminal_rate
                .unwrap_or(0.0)
                .total_cmp(&left.stale_terminal_rate.unwrap_or(0.0))
        })
        .then_with(|| right.closed_trades.cmp(&left.closed_trades))
}

fn percentiles(mut values: Vec<f64>) -> DiscoveryV2Percentiles {
    values.retain(|value| value.is_finite());
    values.sort_by(f64::total_cmp);
    DiscoveryV2Percentiles {
        p10: percentile(&values, 0.10),
        p50: percentile(&values, 0.50),
        p90: percentile(&values, 0.90),
    }
}

fn percentile(values: &[f64], p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() - 1) as f64 * p).round() as usize;
    values.get(index).copied()
}

fn bucketize(
    rows: &[DiscoveryV2RugFeedbackWalletRow],
    label: fn(&DiscoveryV2RugFeedbackWalletRow) -> &'static str,
) -> Vec<DiscoveryV2Bucket> {
    let mut counts = BTreeMap::<String, usize>::new();
    for row in rows {
        *counts.entry(label(row).to_string()).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .map(|(label, count)| DiscoveryV2Bucket { label, count })
        .collect()
}

fn rate_bucket_label(row: &DiscoveryV2RugFeedbackWalletRow) -> &'static str {
    let rate = row.stale_terminal_rate.unwrap_or(0.0);
    if rate == 0.0 {
        "rate=0"
    } else if rate <= 0.05 {
        "0<rate<=0.05"
    } else if rate <= 0.10 {
        "0.05<rate<=0.10"
    } else if rate <= 0.20 {
        "0.10<rate<=0.20"
    } else if rate <= 0.40 {
        "0.20<rate<=0.40"
    } else {
        "rate>0.40"
    }
}

fn pnl_bucket_label(row: &DiscoveryV2RugFeedbackWalletRow) -> &'static str {
    let pnl = row.stale_terminal_pnl_sol;
    if pnl >= 0.0 {
        "pnl>=0"
    } else if pnl >= -0.01 {
        "-0.01<=pnl<0"
    } else if pnl >= -0.05 {
        "-0.05<=pnl<-0.01"
    } else if pnl >= -0.10 {
        "-0.10<=pnl<-0.05"
    } else if pnl >= -0.20 {
        "-0.20<=pnl<-0.10"
    } else {
        "pnl<-0.20"
    }
}
