use crate::accumulator::BuyObservation;
use crate::token_market::SolLegTrade;
use chrono::{DateTime, Duration, Utc};
use copybot_config::DiscoveryConfig;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
pub(crate) struct RugEvaluation {
    pub ratio: f64,
    pub evaluated: u32,
    pub unevaluated: u32,
}

pub(crate) fn compute_rug_evaluation(
    buys: &[BuyObservation],
    token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> RugEvaluation {
    if buys.is_empty() {
        return RugEvaluation {
            ratio: 0.0,
            evaluated: 0,
            unevaluated: 0,
        };
    }
    let lookahead = Duration::seconds(discovery.rug_lookahead_seconds.max(1) as i64);
    let mut evaluated = 0u32;
    let mut rugged = 0u32;
    let mut unevaluated = 0u32;
    for buy in buys {
        let window_end = buy.ts + lookahead;
        if window_end > now {
            unevaluated = unevaluated.saturating_add(1);
            continue;
        }
        evaluated = evaluated.saturating_add(1);
        let Some(trades) = token_sol_history.get(&buy.token) else {
            rugged = rugged.saturating_add(1);
            continue;
        };
        let mut volume_sol = 0.0;
        let mut unique_traders = HashSet::new();
        let start = trades.partition_point(|trade| trade.ts < buy.ts);
        let end = trades.partition_point(|trade| trade.ts <= window_end);
        for trade in &trades[start..end] {
            volume_sol += trade.sol_notional;
            unique_traders.insert(trade.trader_id);
        }
        let thin_volume = volume_sol + 1e-12 < discovery.thin_market_min_volume_sol;
        let thin_traders = unique_traders.len() < discovery.thin_market_min_unique_traders as usize;
        if thin_volume || thin_traders {
            rugged = rugged.saturating_add(1);
        }
    }
    let risky = rugged.saturating_add(unevaluated);
    let total = evaluated.saturating_add(unevaluated);
    RugEvaluation {
        ratio: if total == 0 {
            0.0
        } else {
            risky as f64 / total as f64
        },
        evaluated,
        unevaluated,
    }
}
