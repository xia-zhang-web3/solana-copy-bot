use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

#[test]
fn status_scan_does_not_materialize_sell_only_wallet_metrics() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = ts("2026-05-16T10:00:00Z")?;
    let token = "StatusScanToken11111111111111111111111111";
    let sell_only_token = "SellOnlyToken111111111111111111111111111";
    let tail_token = "TailToken1111111111111111111111111111111";
    store.insert_observed_swaps_batch(&[
        buy(
            "coverage_wallet",
            tail_token,
            "sig-coverage",
            1,
            now - Duration::hours(25),
        ),
        sell(
            "sell_only_wallet",
            sell_only_token,
            "sig-sell-only",
            10,
            now - Duration::minutes(20),
        ),
        buy(
            "candidate_wallet",
            token,
            "sig-candidate-buy",
            11,
            now - Duration::minutes(10),
        ),
        sell(
            "candidate_wallet",
            token,
            "sig-candidate-sell",
            12,
            now - Duration::minutes(9),
        ),
        buy(
            "candidate_wallet",
            token,
            "sig-candidate-open-buy",
            13,
            now - Duration::minutes(8),
        ),
        sell(
            "sell_only_wallet",
            token,
            "sig-sell-only-noise",
            14,
            now - Duration::minutes(7),
        ),
        buy(
            "tail_wallet",
            tail_token,
            "sig-tail",
            15,
            now - Duration::minutes(1),
        ),
    ])?;
    store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(tail_token, Some(5), Some(1.0), Some(60), now)?;
    let (discovery, shadow) = policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(
        status.candidate_wallets,
        vec!["candidate_wallet".to_string()]
    );
    assert_eq!(status.scan.rows_scanned, 6);
    assert_eq!(status.scan.unique_wallets, 2);
    assert!(status
        .wallet_metrics
        .iter()
        .all(|metric| metric.wallet_id != "sell_only_wallet"));
    Ok(())
}

#[test]
fn status_scan_keeps_missing_quality_wallet_rejected_after_prior_profit() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = ts("2026-05-16T11:00:00Z")?;
    let good_token = "GoodToken111111111111111111111111111111";
    let missing_quality_token = "MissingQualityToken111111111111111111111";
    let tail_token = "TailToken2222222222222222222222222222222";
    store.insert_observed_swaps_batch(&[
        buy(
            "coverage_wallet",
            tail_token,
            "sig-missing-coverage",
            1,
            now - Duration::hours(25),
        ),
        buy(
            "rejected_wallet",
            good_token,
            "sig-rejected-good-buy",
            10,
            now - Duration::minutes(15),
        ),
        sell(
            "rejected_wallet",
            good_token,
            "sig-rejected-good-sell",
            11,
            now - Duration::minutes(14),
        ),
        buy(
            "rejected_wallet",
            missing_quality_token,
            "sig-rejected-missing-quality",
            12,
            now - Duration::minutes(13),
        ),
        buy(
            "candidate_wallet",
            good_token,
            "sig-candidate-good-buy",
            13,
            now - Duration::minutes(12),
        ),
        sell(
            "candidate_wallet",
            good_token,
            "sig-candidate-good-sell",
            14,
            now - Duration::minutes(11),
        ),
        buy(
            "candidate_wallet",
            good_token,
            "sig-candidate-good-open",
            15,
            now - Duration::minutes(10),
        ),
        buy(
            "tail_wallet",
            tail_token,
            "sig-missing-tail",
            16,
            now - Duration::minutes(1),
        ),
    ])?;
    store.upsert_token_quality_cache(good_token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(tail_token, Some(5), Some(1.0), Some(60), now)?;
    let (discovery, shadow) = policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(
        status.candidate_wallets,
        vec!["candidate_wallet".to_string()]
    );
    let rejected = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "rejected_wallet")
        .expect("rejected wallet metric retained for operator evidence");
    assert!(!rejected.eligible);
    assert!(rejected
        .reject_reasons
        .iter()
        .any(|reason| reason == "token_quality_evidence_missing"));
    Ok(())
}

#[test]
fn status_scan_keeps_suspicious_wallet_fail_closed_after_prior_profit() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = ts("2026-05-16T12:00:00Z")?;
    let good_token = "GoodToken333333333333333333333333333333";
    let suspicious_token = "SuspiciousToken111111111111111111111";
    let tail_token = "TailToken3333333333333333333333333333333";
    store.insert_observed_swaps_batch(&[
        buy(
            "coverage_wallet",
            tail_token,
            "sig-suspicious-coverage",
            1,
            now - Duration::hours(25),
        ),
        buy(
            "suspicious_wallet",
            suspicious_token,
            "sig-suspicious-buy-1",
            10,
            now - Duration::minutes(20),
        ),
        sell(
            "suspicious_wallet",
            suspicious_token,
            "sig-suspicious-sell-1",
            11,
            now - Duration::minutes(20),
        ),
        buy(
            "suspicious_wallet",
            suspicious_token,
            "sig-suspicious-open-buy",
            12,
            now - Duration::minutes(20),
        ),
        buy(
            "candidate_wallet",
            good_token,
            "sig-safe-buy",
            13,
            now - Duration::minutes(12),
        ),
        sell(
            "candidate_wallet",
            good_token,
            "sig-safe-sell",
            14,
            now - Duration::minutes(11),
        ),
        buy(
            "candidate_wallet",
            good_token,
            "sig-safe-open-buy",
            15,
            now - Duration::minutes(10),
        ),
        buy(
            "tail_wallet",
            tail_token,
            "sig-suspicious-tail",
            16,
            now - Duration::minutes(1),
        ),
    ])?;
    store.upsert_token_quality_cache(good_token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(suspicious_token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(tail_token, Some(5), Some(1.0), Some(60), now)?;
    let (mut discovery, shadow) = policy();
    discovery.max_tx_per_minute = 1;
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(
        status.candidate_wallets,
        vec!["candidate_wallet".to_string()]
    );
    let rejected = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "suspicious_wallet")
        .expect("suspicious wallet metric retained for operator evidence");
    assert!(!rejected.eligible);
    assert!(rejected
        .reject_reasons
        .iter()
        .any(|reason| reason == "suspicious_activity"));
    assert!(rejected
        .reject_reasons
        .iter()
        .any(|reason| reason == "open_position_required_missing"));
    Ok(())
}

#[test]
fn status_scan_prefers_still_eligible_active_wallet_to_reduce_churn() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = ts("2026-05-16T13:00:00Z")?;
    let active_token = "ActiveToken11111111111111111111111111111";
    let challenger_token = "ChallengerToken111111111111111111111";
    let tail_token = "TailToken4444444444444444444444444444444";
    store.activate_follow_wallet(
        "active_wallet",
        now - Duration::hours(2),
        "test-active-wallet",
    )?;
    store.insert_observed_swaps_batch(&[
        buy(
            "coverage_wallet",
            tail_token,
            "sig-sticky-coverage",
            1,
            now - Duration::hours(25),
        ),
        buy(
            "active_wallet",
            active_token,
            "sig-active-buy",
            10,
            now - Duration::minutes(15),
        ),
        swap(
            "active_wallet",
            active_token,
            SOL_MINT,
            10.0,
            1.05,
            "sig-active-sell",
            11,
            now - Duration::minutes(14),
        ),
        buy(
            "active_wallet",
            active_token,
            "sig-active-open",
            12,
            now - Duration::minutes(13),
        ),
        buy(
            "challenger_wallet",
            challenger_token,
            "sig-challenger-buy",
            13,
            now - Duration::minutes(12),
        ),
        swap(
            "challenger_wallet",
            challenger_token,
            SOL_MINT,
            10.0,
            2.0,
            "sig-challenger-sell",
            14,
            now - Duration::minutes(11),
        ),
        buy(
            "challenger_wallet",
            challenger_token,
            "sig-challenger-open",
            15,
            now - Duration::minutes(10),
        ),
        buy(
            "tail_wallet",
            tail_token,
            "sig-sticky-tail",
            16,
            now - Duration::minutes(1),
        ),
    ])?;
    store.upsert_token_quality_cache(active_token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(challenger_token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(tail_token, Some(5), Some(1.0), Some(60), now)?;
    let (mut discovery, shadow) = policy();
    discovery.follow_top_n = 1;
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    let challenger = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "challenger_wallet")
        .expect("challenger metric");
    let active = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "active_wallet")
        .expect("active metric");
    assert!(
        challenger.selection_score > active.selection_score,
        "test setup should make the inactive challenger rank higher"
    );
    assert_eq!(status.candidate_wallets, vec!["active_wallet".to_string()]);
    Ok(())
}

fn policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 3;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.follow_top_n = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.max_rug_ratio = 0.60;
    discovery.rug_lookahead_seconds = 10;
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

fn buy(wallet: &str, token: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap(wallet, SOL_MINT, token, 1.0, 10.0, signature, slot, ts_utc)
}

fn sell(wallet: &str, token: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap(wallet, token, SOL_MINT, 10.0, 1.2, signature, slot, ts_utc)
}

fn swap(
    wallet: &str,
    token_in: &str,
    token_out: &str,
    amount_in: f64,
    amount_out: f64,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in,
        amount_out,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}
