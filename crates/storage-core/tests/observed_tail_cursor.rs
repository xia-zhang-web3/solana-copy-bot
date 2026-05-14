use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn swap(signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        signature: signature.to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "Token111111111111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 2.0,
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

#[test]
fn tail_cursor_at_or_before_ignores_future_rows() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-14T15:00:00+00:00")?;
    store.insert_observed_swaps_batch(&[
        swap("sig-ready", 10, now - Duration::seconds(5)),
        swap("sig-future", 11, now + Duration::seconds(15)),
    ])?;

    let tail = store
        .observed_swaps_tail_cursor_at_or_before_read_only(now)?
        .expect("non-future tail");

    assert_eq!(tail.signature, "sig-ready");
    assert_eq!(tail.ts_utc, now - Duration::seconds(5));
    Ok(())
}

#[test]
fn tail_cursor_at_or_before_returns_none_when_only_future_rows_exist() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = ts("2026-05-14T15:00:00+00:00")?;
    store.insert_observed_swap(&swap("sig-future", 11, now + Duration::seconds(15)))?;

    let tail = store.observed_swaps_tail_cursor_at_or_before_read_only(now)?;

    assert!(tail.is_none());
    Ok(())
}
