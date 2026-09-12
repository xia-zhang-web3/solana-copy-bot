use super::{fixture, rpc};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_discovery_v2::{
    build_discovery_v2_status, prepare_discovery_v2_quality, DiscoveryV2PrepareQualityMode,
    DiscoveryV2PrepareQualityOptions,
};
use serde_json::json;

#[test]
fn b35_causal_stale_price_full_scan() -> Result<()> {
    stale_price_after_prepare(DiscoveryV2PrepareQualityMode::FullScan)
}

#[test]
fn b35_causal_stale_price_incremental() -> Result<()> {
    stale_price_after_prepare(DiscoveryV2PrepareQualityMode::Incremental)
}

fn stale_price_after_prepare(mode: DiscoveryV2PrepareQualityMode) -> Result<()> {
    let (_dir, store) = super::test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-09-07T12:00:00Z")?.with_timezone(&Utc);
    let held = rpc::key('E');
    let active = rpc::key('D');
    for i in 0..5 {
        store.insert_observed_swap(&super::swap_with_token(
            &format!("old-wallet-{i}"),
            &held,
            &format!("old-price-{i}"),
            i + 1,
            now - Duration::minutes(190 - i as i64),
        ))?;
    }
    let (mut discovery, shadow) = super::strict_policy();
    discovery.status_scan_window_minutes = 120;
    discovery.max_window_swaps_in_memory = 100;
    discovery.fetch_time_budget_ms = 5_000;
    discovery.live_portfolio_gate_enabled = true;
    discovery.live_portfolio_max_wallets = 1;
    discovery.min_live_sol_balance = 0.25;
    discovery.min_live_portfolio_value_sol = 0.25;
    let quality_at = now - Duration::minutes(110);
    let prepared = prepare_discovery_v2_quality(
        &store,
        &discovery,
        &shadow,
        DiscoveryV2PrepareQualityOptions::from_config(&discovery, quality_at, 10, true)
            .with_mode(mode),
    )?;
    assert!(prepared.committed);
    assert_eq!(
        store.get_token_quality_cache(&held)?.unwrap().fetched_at,
        quality_at
    );
    store.insert_observed_swaps_batch(&[
        super::swap_with_token(
            &rpc::key('A'),
            &active,
            "leader-buy",
            10,
            now - Duration::minutes(4),
        ),
        super::tail_coverage_swap("tail", 11, now - Duration::minutes(1)),
    ])?;
    super::insert_quality_for_token(&store, &active, now, Some(1.0))?;
    let server = fixture::responses(
        0,
        json!([rpc::account(
            &rpc::key('A'),
            &held,
            &rpc::key('J'),
            rpc::CLASSIC,
            "10000000",
            6,
        )]),
        json!([]),
    );
    let mut options = super::options(now);
    options.window_minutes = 120;
    options.live_portfolio_rpc_url = Some(server.url.clone());
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options)?;
    let calls = server.finish();
    rpc::assert_requests(&calls, &rpc::key('A'), 3);
    let metric = fixture::metric(&status);
    println!(
        "B35 causal mode={mode:?} quality_age_minutes=110 price_age_minutes=186 status={}",
        serde_json::to_string(&status)?
    );
    assert!(
        status.candidate_wallets.is_empty(),
        "stale price must not fund a candidate: {metric}"
    );
    assert_eq!(metric["live_token_value_sol"], json!(0.0));
    assert!(metric["reject_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("live_portfolio_price_valuation_unknown")));
    assert!(!status.production_green);
    Ok(())
}
