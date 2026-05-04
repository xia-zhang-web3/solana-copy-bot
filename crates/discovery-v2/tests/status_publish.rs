use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{
    build_discovery_v2_status, publish_discovery_v2_status, DiscoveryV2BuildOptions,
    DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, DiscoveryRuntimeMode, SqliteDiscoveryStore,
};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_MINT: &str = "TokenMint111111111111111111111111111111111";

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn swap(wallet: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: TOKEN_MINT.to_string(),
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
        window_minutes: 60,
        max_tail_lag_seconds: 1_200,
        max_rows: 100,
        time_budget_ms: 5_000,
        execution_enabled: false,
    }
}

fn strict_policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 1;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
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

fn insert_quality(
    store: &SqliteDiscoveryStore,
    now: DateTime<Utc>,
    liquidity_sol: Option<f64>,
) -> Result<()> {
    store.upsert_token_quality_cache(TOKEN_MINT, Some(5), liquidity_sol, Some(60), now)
}

#[test]
fn status_ready_when_tail_sample_scan_and_candidates_are_valid() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        swap("wallet_b", "sig-b", 11, now - Duration::minutes(5)),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green);
    assert!(status.blockers.is_empty());
    assert_eq!(status.scan.rows_scanned, 2);
    assert_eq!(status.candidate_wallets.len(), 2);
    assert_eq!(status.source, DISCOVERY_V2_SCORING_SOURCE);
    Ok(())
}

#[test]
fn status_blocks_when_liquidity_quality_evidence_is_missing() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)))?;
    insert_quality(&store, now, None)?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert!(status.candidate_wallets.is_empty());
    assert_eq!(status.wallet_metrics[0].tradable_ratio, 0.0);
    assert!(status.wallet_metrics[0]
        .reject_reasons
        .contains(&"low_tradable_ratio".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_rug_lookahead_is_unevaluated() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(1)))?;
    insert_quality(&store, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.rug_lookahead_seconds = 900;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert_eq!(status.wallet_metrics[0].rug_lookahead_evaluated, 0);
    assert_eq!(status.wallet_metrics[0].rug_lookahead_unevaluated, 1);
    assert!(status.wallet_metrics[0]
        .reject_reasons
        .contains(&"rug_lookahead_unevaluated".to_string()));
    Ok(())
}

#[test]
fn publish_commit_writes_followlist_and_publication_state_when_green() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)))?;
    insert_quality(&store, now, Some(1.0))?;
    let (discovery, shadow) = strict_policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let report = publish_discovery_v2_status(&store, status, true)?;

    assert!(report.committed);
    assert_eq!(report.published_wallet_count, 1);
    assert!(store.list_active_follow_wallets()?.contains("wallet_a"));
    let state = store
        .discovery_publication_state_read_only()?
        .expect("publication state");
    assert_eq!(state.runtime_mode, DiscoveryRuntimeMode::Healthy);
    assert_eq!(
        state.published_scoring_source.as_deref(),
        Some(DISCOVERY_V2_SCORING_SOURCE)
    );
    Ok(())
}

#[test]
fn publish_commit_refuses_to_mutate_when_blocked() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let (discovery, shadow) = strict_policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let err = publish_discovery_v2_status(&store, status, true).expect_err("blocked publish");

    assert!(err
        .to_string()
        .contains("refusing to mutate publication state"));
    assert!(store.discovery_publication_state_read_only()?.is_none());
    Ok(())
}
