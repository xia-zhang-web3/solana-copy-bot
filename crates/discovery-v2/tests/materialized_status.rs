use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{
    load_materialized_discovery_v2_status_for_publish, materialize_discovery_v2_status,
    publish_discovery_v2_status, reusable_materialized_discovery_v2_status_for_prepare,
    DiscoveryV2BuildOptions,
};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_MINT: &str = "MaterializedToken111111111111111111111111";

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn buy(wallet: &str, token: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: token.to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn tail_swap(signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    buy(
        "tail-wallet",
        "MaterializedTailToken111111111111111111111",
        signature,
        slot,
        ts_utc,
    )
}

fn policy() -> (DiscoveryConfig, ShadowConfig) {
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
    discovery.rug_lookahead_seconds = 10;
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 1;
    discovery.metric_snapshot_interval_seconds = 60;
    discovery.refresh_seconds = 60;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 1;
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

fn seed_green_status(store: &SqliteDiscoveryStore, now: DateTime<Utc>) -> Result<()> {
    store.insert_observed_swaps_batch(&[
        tail_swap("sig-coverage-floor", 9, now - Duration::hours(25)),
        buy(
            "wallet-a",
            TOKEN_MINT,
            "sig-wallet-a",
            10,
            now - Duration::seconds(30),
        ),
        tail_swap("sig-tail", 11, now - Duration::seconds(5)),
    ])?;
    store.upsert_token_quality_cache(TOKEN_MINT, Some(1), Some(1.0), Some(60), now)?;
    Ok(())
}

#[test]
fn materialized_status_round_trip_can_feed_publish() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    seed_green_status(&store, now)?;
    let (discovery, shadow) = policy();

    let (_status, materialized) =
        materialize_discovery_v2_status(&store, &discovery, &shadow, options(now))?;
    assert!(materialized.committed);
    assert!(materialized.production_green);

    let publish_options = options(now + Duration::seconds(30));
    let (status, loaded) = load_materialized_discovery_v2_status_for_publish(
        &store,
        &discovery,
        &shadow,
        &publish_options,
    )?;
    let report = publish_discovery_v2_status(&store, status, true)?;

    assert_eq!(loaded.status_age_seconds, 30);
    assert!(report.committed);
    assert_eq!(report.published_wallet_count, 1);
    assert!(store.list_active_follow_wallets()?.contains("wallet-a"));
    Ok(())
}

#[test]
fn materialized_status_rejects_policy_mismatch() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    seed_green_status(&store, now)?;
    let (mut discovery, shadow) = policy();
    materialize_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    discovery.min_score += 0.01;
    let err = load_materialized_discovery_v2_status_for_publish(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(30)),
    )
    .expect_err("policy mismatch must fail closed");
    assert!(err.to_string().contains("policy mismatch"));
    Ok(())
}

#[test]
fn materialized_status_rejects_stale_snapshot() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    seed_green_status(&store, now)?;
    let (discovery, shadow) = policy();
    materialize_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let err = load_materialized_discovery_v2_status_for_publish(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::minutes(4)),
    )
    .expect_err("stale materialized status must fail closed");
    assert!(err.to_string().contains("stale"));
    Ok(())
}

#[test]
fn prepare_can_reuse_green_materialized_status_before_refresh_age() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    seed_green_status(&store, now)?;
    let (discovery, shadow) = policy();
    materialize_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let reused = reusable_materialized_discovery_v2_status_for_prepare(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(30)),
    )?
    .expect("fresh green snapshot should be reusable");

    assert!(!reused.committed);
    assert!(reused.reused_existing_snapshot);
    assert_eq!(reused.status_age_seconds, 30);
    assert_eq!(reused.max_status_age_seconds, 180);
    assert_eq!(reused.refresh_after_age_seconds, 60);
    assert_eq!(reused.reuse_before_age_seconds, 120);
    assert_eq!(reused.rebuild_after_age_seconds, 180);
    assert!(reused.production_green);
    Ok(())
}

#[test]
fn prepare_reuse_window_keeps_refresh_margin_before_stale() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    seed_green_status(&store, now)?;
    let (mut discovery, shadow) = policy();
    discovery.metric_snapshot_interval_seconds = 1_800;
    discovery.refresh_seconds = 600;
    materialize_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let reused = reusable_materialized_discovery_v2_status_for_prepare(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(4_799)),
    )?
    .expect("snapshot should be reusable before the refresh margin");

    assert_eq!(reused.refresh_after_age_seconds, 1_800);
    assert_eq!(reused.reuse_before_age_seconds, 4_800);
    assert_eq!(reused.rebuild_after_age_seconds, 5_400);
    assert!(reused.reused_existing_snapshot);

    let expired_after_reuse_window = reusable_materialized_discovery_v2_status_for_prepare(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(4_801)),
    )?;
    assert!(expired_after_reuse_window.is_none());

    let expired = reusable_materialized_discovery_v2_status_for_prepare(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(5_401)),
    )?;
    assert!(expired.is_none());
    Ok(())
}

#[test]
fn prepare_rebuilds_materialized_status_at_reuse_threshold() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    seed_green_status(&store, now)?;
    let (discovery, shadow) = policy();
    materialize_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let reused = reusable_materialized_discovery_v2_status_for_prepare(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(119)),
    )?;

    assert!(reused.is_some());
    let reuse_expired = reusable_materialized_discovery_v2_status_for_prepare(
        &store,
        &discovery,
        &shadow,
        &options(now + Duration::seconds(120)),
    )?;
    assert!(reuse_expired.is_none());
    Ok(())
}
