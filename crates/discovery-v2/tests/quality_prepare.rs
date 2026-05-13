use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{prepare_discovery_v2_quality, DiscoveryV2PrepareQualityOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_MINT: &str = "QualityToken111111111111111111111111111111";

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn buy(wallet: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: TOKEN_MINT.to_string(),
        amount_in: 1.25,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.scoring_window_days = 1;
    discovery.max_window_swaps_in_memory = 100;
    discovery.fetch_time_budget_ms = 5_000;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 2;
    shadow.min_liquidity_sol = 1.0;
    (discovery, shadow)
}

fn options(
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
    commit: bool,
) -> DiscoveryV2PrepareQualityOptions {
    DiscoveryV2PrepareQualityOptions::from_config(discovery, now, 10, commit)
}

#[test]
fn quality_prepare_dry_run_does_not_write_cache() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-11T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        buy("wallet-a", "sig-a", 10, now - Duration::minutes(10)),
        buy("wallet-b", "sig-b", 11, now - Duration::minutes(8)),
    ])?;
    let (discovery, shadow) = policy();

    let report =
        prepare_discovery_v2_quality(&store, &discovery, &shadow, options(&discovery, now, false))?;

    assert!(report.dry_run);
    assert!(!report.committed);
    assert_eq!(report.unique_buy_mints, 1);
    assert_eq!(report.upserted, 1);
    assert!(store.get_token_quality_cache(TOKEN_MINT)?.is_none());
    Ok(())
}

#[test]
fn quality_prepare_commit_writes_observed_proxy_evidence() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-11T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        buy("wallet-a", "sig-a", 10, now - Duration::minutes(10)),
        buy("wallet-b", "sig-b", 11, now - Duration::minutes(8)),
    ])?;
    let (discovery, shadow) = policy();

    let report =
        prepare_discovery_v2_quality(&store, &discovery, &shadow, options(&discovery, now, true))?;
    let row = store
        .get_token_quality_cache(TOKEN_MINT)?
        .expect("quality prepare should materialize a cache row");

    assert!(report.committed);
    assert_eq!(report.upserted, 1);
    assert_eq!(row.holders, Some(2));
    assert_eq!(row.liquidity_sol, Some(1.25));
    assert_eq!(row.token_age_seconds, Some(600));
    assert_eq!(row.fetched_at, now);
    Ok(())
}

#[test]
fn quality_prepare_scans_only_fresh_quality_window() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-11T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        buy("wallet-a", "sig-old-a", 10, now - Duration::hours(3)),
        buy(
            "wallet-b",
            "sig-old-b",
            11,
            now - Duration::hours(3) + Duration::minutes(1),
        ),
    ])?;
    let (discovery, shadow) = policy();

    let report =
        prepare_discovery_v2_quality(&store, &discovery, &shadow, options(&discovery, now, true))?;

    assert!(!report.committed);
    assert_eq!(report.scoring_window_minutes, 24 * 60);
    assert_eq!(report.window_minutes, 120);
    assert_eq!(report.rows_scanned, 0);
    assert!(report
        .blockers
        .contains(&"discovery_v2_quality_prepare_observed_window_empty".to_string()));
    assert!(store.get_token_quality_cache(TOKEN_MINT)?.is_none());
    Ok(())
}

#[test]
fn quality_prepare_caps_wallet_evidence_at_required_threshold() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-11T10:00:00Z")?.with_timezone(&Utc);
    let swaps = (0..20)
        .map(|index| {
            buy(
                &format!("wallet-{index:02}"),
                &format!("sig-{index:02}"),
                10 + index,
                now - Duration::minutes(10),
            )
        })
        .collect::<Vec<_>>();
    store.insert_observed_swaps_batch(&swaps)?;
    let (discovery, mut shadow) = policy();
    shadow.min_holders = 5;

    let report =
        prepare_discovery_v2_quality(&store, &discovery, &shadow, options(&discovery, now, true))?;
    let row = store
        .get_token_quality_cache(TOKEN_MINT)?
        .expect("quality prepare should materialize capped holder evidence");

    assert!(report.committed);
    assert_eq!(row.holders, Some(5));
    Ok(())
}

#[test]
fn quality_prepare_commit_does_not_write_partial_window() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-11T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        buy("wallet-a", "sig-a", 10, now - Duration::minutes(10)),
        buy("wallet-b", "sig-b", 11, now - Duration::minutes(8)),
    ])?;
    let (mut discovery, mut shadow) = policy();
    discovery.max_window_swaps_in_memory = 1;
    shadow.min_holders = 1;

    let report =
        prepare_discovery_v2_quality(&store, &discovery, &shadow, options(&discovery, now, true))?;

    assert!(!report.committed);
    assert_eq!(report.upserted, 0);
    assert!(report.max_rows_exhausted);
    assert!(report
        .blockers
        .contains(&"discovery_v2_quality_prepare_max_rows_exhausted".to_string()));
    assert!(store.get_token_quality_cache(TOKEN_MINT)?.is_none());
    Ok(())
}

#[test]
fn quality_prepare_skips_fresh_complete_cache() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-11T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        buy("wallet-a", "sig-a", 10, now - Duration::minutes(10)),
        buy("wallet-b", "sig-b", 11, now - Duration::minutes(8)),
    ])?;
    store.upsert_token_quality_cache(TOKEN_MINT, Some(10), Some(3.0), Some(120), now)?;
    let (discovery, shadow) = policy();

    let report =
        prepare_discovery_v2_quality(&store, &discovery, &shadow, options(&discovery, now, true))?;
    let row = store
        .get_token_quality_cache(TOKEN_MINT)?
        .expect("existing row should remain");

    assert_eq!(report.skipped_fresh_complete, 1);
    assert_eq!(report.upserted, 0);
    assert_eq!(row.holders, Some(10));
    assert_eq!(row.liquidity_sol, Some(3.0));
    assert_eq!(row.token_age_seconds, Some(120));
    Ok(())
}
