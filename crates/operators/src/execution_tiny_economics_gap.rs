use copybot_storage_core::ExecutionCanaryQuotePnlTrade;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FollowerGapReport {
    pub samples: u64,
    pub avg_exit_quote_to_shadow_price: f64,
    pub p10_exit_quote_to_shadow_price: f64,
    pub p50_exit_quote_to_shadow_price: f64,
    pub p90_exit_quote_to_shadow_price: f64,
    pub min_exit_quote_to_shadow_price: f64,
    pub shadow_positive_quote_after_fee_negative: u64,
    pub quote_after_fee_negative: u64,
}

pub fn follower_gap_from_trades(
    trades: &[ExecutionCanaryQuotePnlTrade],
) -> Option<FollowerGapReport> {
    let mut ratios = Vec::new();
    let mut shadow_positive_quote_negative = 0_u64;
    let mut quote_after_fee_negative = 0_u64;
    for trade in trades {
        if trade.shadow_pnl_sol > 0.0
            && trade
                .quote_adjusted_pnl_after_priority_fee_sol
                .unwrap_or(0.0)
                < 0.0
        {
            shadow_positive_quote_negative += 1;
        }
        if trade
            .quote_adjusted_pnl_after_priority_fee_sol
            .unwrap_or(0.0)
            < 0.0
        {
            quote_after_fee_negative += 1;
        }
        let Some(shadow_price) = trade.exit_shadow_price_sol.filter(|price| *price > 0.0) else {
            continue;
        };
        let Some(quote_price) = trade.exit_quote_price_sol.filter(|price| *price > 0.0) else {
            continue;
        };
        ratios.push(quote_price / shadow_price);
    }
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(f64::total_cmp);
    let avg = ratios.iter().sum::<f64>() / ratios.len() as f64;
    Some(FollowerGapReport {
        samples: ratios.len() as u64,
        avg_exit_quote_to_shadow_price: avg,
        p10_exit_quote_to_shadow_price: percentile(&ratios, 0.10),
        p50_exit_quote_to_shadow_price: percentile(&ratios, 0.50),
        p90_exit_quote_to_shadow_price: percentile(&ratios, 0.90),
        min_exit_quote_to_shadow_price: ratios[0],
        shadow_positive_quote_after_fee_negative: shadow_positive_quote_negative,
        quote_after_fee_negative,
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let max_index = sorted.len().saturating_sub(1);
    let index = ((max_index as f64) * p).round() as usize;
    sorted[index.min(max_index)]
}
