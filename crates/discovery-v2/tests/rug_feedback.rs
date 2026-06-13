use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{
    build_discovery_v2_rug_feedback_distribution_report, build_discovery_v2_status,
    DiscoveryV2BuildOptions, DiscoveryV2RugFeedbackDistributionOptions,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn rug_feedback_rejects_wallet_with_stale_terminal_tail() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let bad_wallet = "wallet_rug_feedback_bad";
    let good_wallet = "wallet_rug_feedback_good";
    let replacement_wallet = "wallet_rug_feedback_replacement";
    let token_a = "RugFeedbackBadToken111111111111111111";
    let token_b = "RugFeedbackGoodToken2222222222222222";
    let token_c = "RugFeedbackReplacementToken3333333";

    seed_candidate_swaps(
        &store,
        now,
        [
            (bad_wallet, token_a, "sig-rug-feedback-bad", 10),
            (good_wallet, token_b, "sig-rug-feedback-good", 20),
            (
                replacement_wallet,
                token_c,
                "sig-rug-feedback-replacement",
                30,
            ),
        ],
    )?;
    for token in [token_a, token_b, token_c] {
        store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    }
    for index in 0..9 {
        let opened = now - Duration::hours(1) + Duration::seconds(index);
        store.insert_shadow_closed_trade(
            &format!("rug-feedback-market-close-{index}"),
            bad_wallet,
            token_a,
            1000.0,
            0.20,
            0.21,
            0.01,
            opened,
            opened + Duration::seconds(30),
        )?;
    }
    let opened = now - Duration::minutes(30);
    store.insert_shadow_closed_trade_exact_with_context(
        "stale-close-rug-feedback",
        bad_wallet,
        token_a,
        1000.0,
        None,
        0.20,
        0.01,
        -0.19,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        opened,
        opened + Duration::minutes(30),
    )?;

    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    discovery.rug_wallet_filter_enabled = true;
    discovery.rug_wallet_filter_min_closed_trades = 10;
    discovery.rug_wallet_filter_max_stale_terminal_rate = 0.0;
    discovery.rug_wallet_filter_max_stale_terminal_pnl_sol = 0.0;
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
            .get("rug_feedback_stale_terminal"),
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
        .contains(&"rug_feedback_stale_terminal".to_string()));
    assert_eq!(bad_metric.rug_feedback_closed_trades, Some(10));
    assert_eq!(bad_metric.rug_feedback_stale_terminal_closes, Some(1));
    assert_eq!(bad_metric.rug_feedback_stale_terminal_rate, Some(0.1));
    assert!(bad_metric
        .rug_feedback_stale_terminal_pnl_sol
        .is_some_and(|pnl| pnl <= -0.189 && pnl >= -0.191));
    Ok(())
}

#[test]
fn rug_feedback_distribution_counts_full_feedback_window() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    for index in 0..9 {
        let opened = now - Duration::hours(2) + Duration::seconds(index);
        store.insert_shadow_closed_trade(
            &format!("rug-dist-bad-market-{index}"),
            "bad-wallet",
            "RugDistBadToken",
            1000.0,
            0.20,
            0.21,
            0.01,
            opened,
            opened + Duration::seconds(30),
        )?;
    }
    let opened = now - Duration::minutes(40);
    store.insert_shadow_closed_trade_exact_with_context(
        "rug-dist-bad-stale",
        "bad-wallet",
        "RugDistDeadToken",
        1000.0,
        None,
        0.20,
        0.01,
        -0.19,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        opened,
        opened + Duration::minutes(30),
    )?;
    for index in 0..10 {
        let opened = now - Duration::hours(1) + Duration::seconds(index);
        store.insert_shadow_closed_trade(
            &format!("rug-dist-good-market-{index}"),
            "good-wallet",
            "RugDistGoodToken",
            1000.0,
            0.20,
            0.21,
            0.01,
            opened,
            opened + Duration::seconds(30),
        )?;
    }

    let report = build_discovery_v2_rug_feedback_distribution_report(
        &store,
        DiscoveryV2RugFeedbackDistributionOptions {
            generated_at: now,
            since: now - Duration::hours(48),
            until: now,
            min_closed_trades: 10,
            max_stale_terminal_rate: 0.05,
            max_stale_terminal_pnl_sol: -0.05,
            limit: 10,
        },
    )?;

    assert_eq!(report.wallet_feedback_count, 2);
    assert_eq!(report.eligible_wallet_count, 2);
    assert_eq!(report.threshold_rejected_wallet_count, 1);
    assert_eq!(report.rate_rejected_wallet_count, 1);
    assert_eq!(report.pnl_rejected_wallet_count, 1);
    assert_eq!(report.worst_wallets[0].wallet_id, "bad-wallet");
    assert!(report.worst_wallets[0].rejected);
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
        "sig-rug-feedback-coverage",
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
        "sig-rug-feedback-tail",
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
