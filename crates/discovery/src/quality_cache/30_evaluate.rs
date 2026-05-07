use super::*;

impl DiscoveryService {
    pub(crate) fn evaluate_buy_tradability(
        &self,
        state: &TokenRollingState,
        quality: Option<&TokenQualityResolution>,
        signal_ts: DateTime<Utc>,
    ) -> BuyTradability {
        let liquidity_proxy = state
            .sol_trades_5m
            .iter()
            .map(|trade| trade.sol_notional)
            .fold(0.0, f64::max);
        if self.shadow_quality.min_volume_5m_sol > 0.0
            && state.sol_volume_5m + 1e-12 < self.shadow_quality.min_volume_5m_sol
        {
            return BuyTradability::Rejected;
        }
        if self.shadow_quality.min_unique_traders_5m > 0
            && state.sol_traders_5m.len() < self.shadow_quality.min_unique_traders_5m as usize
        {
            return BuyTradability::Rejected;
        }

        let mut deferred = false;
        if self.shadow_quality.min_token_age_seconds > 0 {
            let token_age_seconds = match quality {
                Some(TokenQualityResolution::Fresh(row)) => row.token_age_seconds,
                Some(TokenQualityResolution::Stale(row)) => row.token_age_seconds.map(|age| {
                    age.saturating_add(
                        signal_ts
                            .signed_duration_since(row.fetched_at)
                            .num_seconds()
                            .max(0) as u64,
                    )
                }),
                Some(TokenQualityResolution::Deferred) => {
                    deferred = true;
                    None
                }
                Some(TokenQualityResolution::Missing) | None => None,
            };
            let Some(token_age_seconds) = token_age_seconds else {
                return if deferred {
                    BuyTradability::Deferred
                } else {
                    BuyTradability::Rejected
                };
            };
            if token_age_seconds < self.shadow_quality.min_token_age_seconds {
                return BuyTradability::Rejected;
            }
        }
        if self.shadow_quality.min_holders > 0 {
            match quality {
                Some(TokenQualityResolution::Fresh(row)) => {
                    let Some(holders) = row.holders else {
                        return BuyTradability::Rejected;
                    };
                    if holders < self.shadow_quality.min_holders {
                        return BuyTradability::Rejected;
                    }
                }
                Some(TokenQualityResolution::Stale(row)) => {
                    if let Some(holders) = row.holders {
                        if holders < self.shadow_quality.min_holders {
                            return BuyTradability::Rejected;
                        }
                    }
                    deferred = true;
                }
                Some(TokenQualityResolution::Deferred) => deferred = true,
                Some(TokenQualityResolution::Missing) | None => {
                    return BuyTradability::Rejected;
                }
            }
        }
        if self.shadow_quality.min_liquidity_sol > 0.0 {
            let liquidity_sol = match quality {
                Some(TokenQualityResolution::Fresh(row)) => {
                    row.liquidity_sol.unwrap_or(liquidity_proxy)
                }
                _ => liquidity_proxy,
            };
            if liquidity_sol + 1e-12 < self.shadow_quality.min_liquidity_sol {
                return BuyTradability::Rejected;
            }
        }
        if deferred {
            BuyTradability::Deferred
        } else {
            BuyTradability::Tradable
        }
    }
}
