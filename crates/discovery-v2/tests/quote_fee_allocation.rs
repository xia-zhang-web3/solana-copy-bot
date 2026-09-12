#[path = "../../storage-core/tests/common/quote_allocation_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::ensure_discovery_v2_schema;
use fixture::Fixture;

#[test]
fn discovery_uses_conserving_feedback_with_existing_now_and_rejection_thresholds() -> Result<()> {
    let f = Fixture::new()?;
    ensure_discovery_v2_schema(&f.store)?;
    f.buy("100", Some(7))?;
    f.exit_with_output(1, "25", Some(2), "25000003")?;
    f.exit_with_output(2, "75", Some(2), "75000010")?;
    f.exit(20, "100", Some(2))?; // excluded by status-builder now; would exhaust budget again
    let now = f.opened + Duration::seconds(10);
    let mut swaps = Vec::new();
    for (wallet, slot, time) in [
        ("tail", 1, now - Duration::hours(25)),
        ("wallet", 2, now - Duration::minutes(10)),
        ("tail", 3, now - Duration::minutes(2)),
    ] {
        swaps.push(SwapEvent {
            wallet: wallet.into(),
            dex: "test".into(),
            token_in: "So11111111111111111111111111111111111111112".into(),
            token_out: "Token".into(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: format!("allocation-{slot}"),
            slot,
            ts_utc: time,
            exact_amounts: None,
        });
    }
    f.store.insert_observed_swaps_batch(&swaps)?;
    f.store
        .upsert_token_quality_cache("Token", Some(5), Some(1.0), Some(60), now)?;
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 1;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.follow_top_n = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.max_rug_ratio = 0.60;
    discovery.rug_lookahead_seconds = 60;
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 1;
    discovery.executable_wallet_filter_enabled = true;
    discovery.executable_wallet_filter_min_samples = 2;
    discovery.executable_wallet_filter_max_pnl_sol = 0.0;
    discovery.executable_wallet_filter_max_flip_rate = 1.0;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    let status = build_discovery_v2_status(
        &f.store,
        &discovery,
        &shadow,
        DiscoveryV2BuildOptions {
            now,
            window_minutes: 1440,
            max_tail_lag_seconds: 1200,
            max_rows: 100,
            time_budget_ms: 5000,
            execution_enabled: false,
            live_portfolio_rpc_url: None,
        },
    )?;
    let metric = status
        .wallet_metrics
        .iter()
        .find(|m| m.wallet_id == "wallet")
        .unwrap();
    let primary = f
        .store
        .execution_canary_quote_pnl_summary(now, f.opened, 100)?;
    assert_eq!(metric.executable_feedback_samples, Some(2));
    assert_eq!(metric.executable_feedback_unknown_samples, Some(0));
    assert_eq!(
        metric.executable_feedback_pnl_after_fee_sol,
        primary.quote_adjusted_pnl_after_priority_fee_sol
    );
    assert!((metric.executable_feedback_pnl_after_fee_sol.unwrap() - 2e-9).abs() < 1e-12);
    assert_eq!(metric.executable_feedback_flip_rate, Some(0.5));
    assert!(!metric
        .reject_reasons
        .contains(&"executable_feedback_negative".into()));
    Ok(())
}
