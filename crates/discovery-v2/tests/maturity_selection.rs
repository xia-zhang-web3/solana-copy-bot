use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn maturity_selection_fills_three_day_then_two_day_then_one_day() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let mature_token = "MatureToken111111111111111111111111111111";
    let two_day_token = "TwoDayToken11111111111111111111111111111";
    let fresh_token = "FreshToken1111111111111111111111111111111";

    store.insert_observed_swaps_batch(&[
        buy(
            "tail_wallet",
            "TailToken111111111111111111111111111111",
            "sig-coverage",
            9,
            now - Duration::hours(73),
        ),
        buy(
            "zzz_mature_wallet",
            mature_token,
            "sig-mature-day-1",
            10,
            now - Duration::hours(49),
        ),
        buy(
            "zzz_mature_wallet",
            mature_token,
            "sig-mature-day-2",
            11,
            now - Duration::hours(25),
        ),
        buy(
            "yyy_two_day_wallet",
            two_day_token,
            "sig-two-day-prior",
            12,
            now - Duration::hours(25),
        ),
        buy(
            "aaa_fresh_wallet",
            fresh_token,
            "sig-fresh-current",
            13,
            now - Duration::minutes(12),
        ),
        buy(
            "yyy_two_day_wallet",
            two_day_token,
            "sig-two-day-current",
            14,
            now - Duration::minutes(11),
        ),
        buy(
            "zzz_mature_wallet",
            mature_token,
            "sig-mature-current",
            15,
            now - Duration::minutes(10),
        ),
        buy(
            "tail_wallet",
            "TailToken111111111111111111111111111111",
            "sig-tail",
            16,
            now - Duration::minutes(2),
        ),
    ])?;
    for token in [mature_token, two_day_token, fresh_token] {
        store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    }
    let (mut discovery, shadow) = policy();
    discovery.follow_top_n = 3;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(
        status.candidate_wallets,
        vec![
            "zzz_mature_wallet".to_string(),
            "yyy_two_day_wallet".to_string(),
            "aaa_fresh_wallet".to_string()
        ]
    );
    assert_eq!(status.maturity.selected_primary_wallets, 1);
    assert_eq!(status.maturity.selected_secondary_wallets, 1);
    assert_eq!(status.maturity.selected_emergency_wallets, 1);
    assert_eq!(metric(&status, "zzz_mature_wallet").maturity_active_days, 3);
    assert_eq!(
        metric(&status, "yyy_two_day_wallet").maturity_active_days,
        2
    );
    assert_eq!(metric(&status, "aaa_fresh_wallet").maturity_active_days, 1);
    Ok(())
}

fn buy(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: token_mint.to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 1;
    discovery.min_active_days = 1;
    discovery.maturity_window_days = 3;
    discovery.maturity_min_active_days = 3;
    discovery.maturity_score_bonus = 0.08;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.rug_lookahead_seconds = 60;
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 1;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    (discovery, shadow)
}

fn options(now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
    DiscoveryV2BuildOptions {
        now,
        window_minutes: 24 * 60,
        max_tail_lag_seconds: 1_200,
        max_rows: 100,
        time_budget_ms: 5_000,
        execution_enabled: false,
        live_portfolio_rpc_url: None,
    }
}

fn metric<'a>(
    status: &'a copybot_discovery_v2::DiscoveryV2Status,
    wallet_id: &str,
) -> &'a copybot_discovery_v2::DiscoveryV2WalletMetric {
    status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == wallet_id)
        .expect("wallet metric")
}
