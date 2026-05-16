use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use std::time::{Duration, Instant};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

#[test]
fn sol_leg_window_scan_omits_unused_exact_amount_payload() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;

    let swap = SwapEvent {
        signature: "sig-sol-leg".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TOKEN".to_string(),
        amount_in: 1.5,
        amount_out: 200.0,
        slot: 42,
        ts_utc: ts("2026-05-05T10:00:00Z")?,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "1500000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "200000000".to_string(),
            amount_out_decimals: 6,
        }),
    };
    store.insert_observed_swap(&swap)?;

    let mut scanned = Vec::new();
    let page = store.for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
        ts("2026-05-05T09:00:00Z")?,
        ts("2026-05-05T11:00:00Z")?,
        None,
        10,
        Instant::now() + Duration::from_secs(5),
        |swap| {
            scanned.push(swap);
            Ok(())
        },
    )?;

    assert!(!page.time_budget_exhausted);
    assert_eq!(page.rows_seen, 1);
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].wallet, "wallet-a");
    assert_eq!(scanned[0].token_in, SOL_MINT);
    assert_eq!(scanned[0].amount_in, 1.5);
    assert!(scanned[0].exact_amounts.is_none());
    Ok(())
}
