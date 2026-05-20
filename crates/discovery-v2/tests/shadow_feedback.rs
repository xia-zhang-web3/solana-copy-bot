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

#[test]
fn fast_shadow_loss_rejects_wallet_before_aggregate_sample() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let bad_wallet = "wallet_shadow_fast_loss";
    let good_wallet = "wallet_shadow_fast_loss_good";
    let replacement_wallet = "wallet_shadow_fast_loss_replacement";
    let token_a = "ShadowFastLossToken1111111111111111111111";
    let token_b = "ShadowFastLossToken2222222222222222222222";
    let token_c = "ShadowFastLossToken3333333333333333333333";

    seed_candidate_swaps(
        &store,
        now,
        [
            (bad_wallet, token_a, "sig-fast-loss-bad", 10),
            (good_wallet, token_b, "sig-fast-loss-good", 20),
            (replacement_wallet, token_c, "sig-fast-loss-replacement", 30),
        ],
    )?;
    for token in [token_a, token_b, token_c] {
        store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    }
    let opened = now - Duration::minutes(20);
    store.insert_shadow_closed_trade(
        "shadow-fast-loss",
        bad_wallet,
        token_a,
        1.0,
        0.20,
        0.18,
        -0.02,
        opened,
        opened + Duration::seconds(30),
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
            .get("shadow_feedback_fast_loss"),
        Some(&1)
    );
    let bad_metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == bad_wallet)
        .expect("bad wallet metric retained");
    assert!(bad_metric
        .reject_reasons
        .contains(&"shadow_feedback_fast_loss".to_string()));
    assert!(bad_metric
        .shadow_fast_loss_roi_24h
        .is_some_and(|roi| roi <= -0.099 && roi >= -0.101));
    Ok(())
}

#[test]
fn stale_priced_shadow_loss_rejects_uncopyable_wallet() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let bad_wallet = "wallet_shadow_stale_copy_loss";
    let good_wallet = "wallet_shadow_stale_copy_loss_good";
    let replacement_wallet = "wallet_shadow_stale_copy_loss_replacement";
    let token_a = "ShadowStaleCopyLossToken1111111111111111";
    let token_b = "ShadowStaleCopyLossToken2222222222222222";
    let token_c = "ShadowStaleCopyLossToken3333333333333333";

    seed_candidate_swaps(
        &store,
        now,
        [
            (bad_wallet, token_a, "sig-stale-copy-loss-bad", 10),
            (good_wallet, token_b, "sig-stale-copy-loss-good", 20),
            (
                replacement_wallet,
                token_c,
                "sig-stale-copy-loss-replacement",
                30,
            ),
        ],
    )?;
    for token in [token_a, token_b, token_c] {
        store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    }
    store.insert_shadow_closed_trade(
        "stale-close-1000-1770000000",
        bad_wallet,
        token_a,
        1.0,
        0.20,
        0.176,
        -0.024,
        now - Duration::hours(2),
        now - Duration::hours(1),
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
            .get("shadow_feedback_stale_copy_loss"),
        Some(&1)
    );
    let bad_metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == bad_wallet)
        .expect("bad wallet metric retained");
    assert!(bad_metric
        .reject_reasons
        .contains(&"shadow_feedback_stale_copy_loss".to_string()));
    assert!(bad_metric
        .shadow_stale_copy_loss_roi_24h
        .is_some_and(|roi| roi <= -0.119 && roi >= -0.121));
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

fn seed_candidate_swaps<const N: usize>(
    store: &SqliteDiscoveryStore,
    now: DateTime<Utc>,
    candidates: [(&str, &str, &str, u64); N],
) -> Result<()> {
    let mut swaps = vec![swap(
        "tail_wallet",
        candidates[0].1,
        "sig-shadow-feedback-coverage",
        1,
        now - Duration::hours(25),
    )];
    for (wallet, token, signature, slot) in candidates {
        swaps.push(swap(
            wallet,
            token,
            signature,
            slot,
            now - Duration::minutes(10) + Duration::seconds(slot as i64),
        ));
    }
    swaps.push(swap(
        "tail_wallet",
        candidates[0].1,
        "sig-shadow-feedback-tail",
        99,
        now - Duration::minutes(2),
    ));
    store.insert_observed_swaps_batch(&swaps)?;
    Ok(())
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
