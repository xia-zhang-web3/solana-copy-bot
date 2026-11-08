use chrono::{DateTime, Duration, Utc};
use copybot_discovery_v2::{
    DiscoveryV2BuildOptions, DiscoveryV2LiveValuationEvidence, DiscoveryV2PriceContribution,
    DiscoveryV2ValuationDecision,
};
use copybot_storage_core::ObservedTokenSolPrice;

pub fn options(now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
    DiscoveryV2BuildOptions {
        now,
        window_minutes: 1440,
        max_tail_lag_seconds: 1200,
        max_rows: 100,
        time_budget_ms: 5000,
        execution_enabled: false,
        live_portfolio_rpc_url: None,
    }
}

pub fn valuation(now: DateTime<Utc>) -> DiscoveryV2LiveValuationEvidence {
    DiscoveryV2LiveValuationEvidence {
        contract_version: 2,
        observed_as_of: now,
        observed_window_start: now - Duration::hours(24),
        sol_balance_bits: 0.08f64.to_bits(),
        token_2022_positive_positions: 0,
        positive_positions: 2,
        token_2022_positions: Vec::new(),
        classic_positions: [('D', 0.1f64), ('E', 0.25f64)]
            .into_iter()
            .map(|(key, price_sol)| DiscoveryV2PriceContribution {
                token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
                token_2022_profile: None,
                token_2022_collection_failure: None,
                mint: key.to_string().repeat(43),
                token_amount: 1.0,
                token_amount_bits: 1.0f64.to_bits(),
                price_sol_bits: Some(price_sol.to_bits()),
                observation: Some(ObservedTokenSolPrice {
                    price_sol,
                    observed_at: now - Duration::minutes(5),
                    signature: format!("report-{key}"),
                    slot: 10,
                }),
                quality_eligible: true,
                quality_gate_enabled: true,
                quality_fetched_at: Some(now),
                initial_unknown_reason: None,
            })
            .collect(),
        decision: DiscoveryV2ValuationDecision {
            as_of: now,
            window_start: now - Duration::hours(24),
            known_classic_value_sol: 0.35,
            known_token_2022_value_sol: 0.0,
            known_supported_token_value_sol: 0.35,
            valued_classic_positions: 2,
            valued_token_2022_positions: 0,
            valued_supported_positions: 2,
            unvalued_positions: 0,
            unknown_reasons: Default::default(),
        },
    }
}
