use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn single_bad_shadow_loss_rejects_wallet_before_waiting_for_three_trades() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let bad_wallet = "wallet_shadow_single_bad_loss";
    let good_wallet = "wallet_shadow_single_bad_loss_good";
    let replacement_wallet = "wallet_shadow_single_bad_loss_replacement";
    let token_a = "ShadowSingleBadLossToken111111111111111111";
    let token_b = "ShadowSingleBadLossToken222222222222222222";
    let token_c = "ShadowSingleBadLossToken333333333333333333";

    store.insert_observed_swaps_batch(&[
        swap(
            "tail_wallet",
            token_a,
            "sig-single-loss-coverage",
            1,
            now - Duration::hours(25),
        ),
        swap(
            bad_wallet,
            token_a,
            "sig-single-loss-bad",
            10,
            now - Duration::minutes(8),
        ),
        swap(
            good_wallet,
            token_b,
            "sig-single-loss-good",
            20,
            now - Duration::minutes(7),
        ),
        swap(
            replacement_wallet,
            token_c,
            "sig-single-loss-replacement",
            30,
            now - Duration::minutes(6),
        ),
        swap(
            "tail_wallet",
            token_a,
            "sig-single-loss-tail",
            99,
            now - Duration::minutes(2),
        ),
    ])?;
    for token in [token_a, token_b, token_c] {
        store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    }
    store.insert_shadow_closed_trade(
        "shadow-single-bad-loss",
        bad_wallet,
        token_a,
        1.0,
        0.20,
        0.12,
        -0.08,
        now - Duration::hours(1),
        now - Duration::minutes(30),
    )?;

    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(status.candidate_wallets.len(), 2);
    assert!(!status.candidate_wallets.contains(&bad_wallet.to_string()));
    assert!(status.candidate_wallets.contains(&good_wallet.to_string()));
    assert!(status
        .candidate_wallets
        .contains(&replacement_wallet.to_string()));
    assert_eq!(
        status
            .filters
            .reject_breakdown
            .get("shadow_feedback_single_bad_loss"),
        Some(&1)
    );
    let bad_metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == bad_wallet)
        .expect("bad wallet metric retained");
    assert!(!bad_metric.eligible);
    assert!(bad_metric
        .reject_reasons
        .contains(&"shadow_feedback_single_bad_loss".to_string()));
    assert_eq!(bad_metric.shadow_closed_trades_24h, Some(1));
    assert!(bad_metric
        .shadow_worst_trade_roi_24h
        .is_some_and(|roi| roi <= -0.39 && roi >= -0.41));
    Ok(())
}

fn swap(
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

fn strict_policy() -> (DiscoveryConfig, ShadowConfig) {
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
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    (discovery, shadow)
}
