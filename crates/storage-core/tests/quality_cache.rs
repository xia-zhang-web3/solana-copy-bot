use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{
    ensure_discovery_v2_schema, DiscoveryV2QualityPrepareUpsert, SqliteDiscoveryStore,
};
use rusqlite::Connection;
use tempfile::tempdir;

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

#[test]
fn fresh_token_quality_cache_filters_stale_and_future_rows() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-13T12:00:00+00:00")?;
    store.upsert_token_quality_cache("fresh-token", Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(
        "stale-token",
        Some(5),
        Some(1.0),
        Some(60),
        now - Duration::hours(3),
    )?;
    store.upsert_token_quality_cache(
        "future-token",
        Some(5),
        Some(1.0),
        Some(60),
        now + Duration::minutes(5),
    )?;

    let rows = store.fresh_token_quality_cache_read_only(now, Duration::hours(2))?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].mint, "fresh-token");
    Ok(())
}

#[test]
fn fresh_token_quality_cache_rejects_malformed_fetched_at() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    {
        let store = SqliteDiscoveryStore::open(&db_path)?;
        ensure_discovery_v2_schema(&store)?;
    }
    Connection::open(&db_path)?.execute(
        "INSERT INTO token_quality_cache(
            mint, holders, liquidity_sol, token_age_seconds, fetched_at
         ) VALUES ('bad-token', 5, 1.0, 60, '2026-05-13T99:00:00+00:00')",
        [],
    )?;
    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;

    let err = store
        .fresh_token_quality_cache_read_only(ts("2026-05-13T12:00:00+00:00")?, Duration::hours(2))
        .expect_err("malformed fresh cache evidence must fail closed");

    assert!(err
        .to_string()
        .contains("invalid token_quality_cache.fetched_at"));
    Ok(())
}

#[test]
fn quality_evidence_prune_deletes_old_rows_and_keeps_fresh_rows() -> Result<()> {
    let (_dir, store) = test_store()?;
    let cutoff = ts("2026-05-13T12:00:00+00:00")?;
    for index in 0..5 {
        let ts_utc = if index < 3 {
            cutoff - Duration::minutes(1 + index)
        } else {
            cutoff + Duration::minutes(index)
        };
        store.insert_discovery_v2_quality_observed_evidence(&DiscoveryV2QualityPrepareUpsert {
            signature: format!("sig-{index}"),
            mint: "token-a".to_string(),
            wallet_id: format!("wallet-{index}"),
            ts_utc,
            slot: 100 + index as u64,
            sol_notional: 1.0,
            is_buy: true,
        })?;
    }

    let deleted = store.prune_discovery_v2_quality_observed_evidence(cutoff)?;
    let remaining =
        store.discovery_v2_quality_observed_evidence_count(cutoff, cutoff + Duration::hours(1))?;

    assert_eq!(deleted, 3);
    assert_eq!(remaining, 2);
    Ok(())
}
