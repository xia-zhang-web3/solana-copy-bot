use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use rusqlite::Connection;
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

#[test]
fn sol_leg_projection_scan_reconstructs_buys_sells_and_respects_cursor() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;

    let buy = SwapEvent {
        signature: "sig-buy".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TOKEN-A".to_string(),
        amount_in: 1.25,
        amount_out: 125.0,
        slot: 100,
        ts_utc: ts("2026-05-05T10:00:00Z")?,
        exact_amounts: None,
    };
    let sell = SwapEvent {
        signature: "sig-sell".to_string(),
        wallet: "wallet-b".to_string(),
        dex: "orca".to_string(),
        token_in: "TOKEN-B".to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 50.0,
        amount_out: 0.75,
        slot: 101,
        ts_utc: ts("2026-05-05T10:01:00Z")?,
        exact_amounts: None,
    };
    let non_sol = SwapEvent {
        signature: "sig-non-sol".to_string(),
        wallet: "wallet-c".to_string(),
        dex: "orca".to_string(),
        token_in: "TOKEN-C".to_string(),
        token_out: "TOKEN-D".to_string(),
        amount_in: 2.0,
        amount_out: 3.0,
        slot: 102,
        ts_utc: ts("2026-05-05T10:02:00Z")?,
        exact_amounts: None,
    };
    store.insert_observed_swaps_batch(&[buy, sell, non_sol])?;
    let window_start = ts("2026-05-05T09:00:00Z")?;
    let window_end = ts("2026-05-05T11:00:00Z")?;
    let rebuilt = store.rebuild_observed_sol_leg_projection_window(window_start, window_end)?;
    assert_eq!(rebuilt, 2);

    let mut scanned = Vec::new();
    let first_page = store.for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
        window_start,
        window_end,
        None,
        1,
        Instant::now() + Duration::from_secs(5),
        |swap| {
            scanned.push(swap);
            Ok(())
        },
    )?;
    assert!(!first_page.time_budget_exhausted);
    assert_eq!(first_page.rows_seen, 1);
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].signature, "sig-buy");
    assert_eq!(scanned[0].dex, "");
    assert_eq!(scanned[0].token_in, SOL_MINT);
    assert_eq!(scanned[0].token_out, "TOKEN-A");
    assert_eq!(scanned[0].amount_in, 1.25);
    assert_eq!(scanned[0].amount_out, 125.0);

    let cursor = copybot_storage_core::DiscoveryRuntimeCursor {
        ts_utc: scanned[0].ts_utc,
        slot: scanned[0].slot,
        signature: scanned[0].signature.clone(),
    };
    let mut after_cursor = Vec::new();
    let second_page = store.for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
        window_start,
        window_end,
        Some(&cursor),
        10,
        Instant::now() + Duration::from_secs(5),
        |swap| {
            after_cursor.push(swap);
            Ok(())
        },
    )?;
    assert!(!second_page.time_budget_exhausted);
    assert_eq!(second_page.rows_seen, 1);
    assert_eq!(after_cursor.len(), 1);
    assert_eq!(after_cursor[0].signature, "sig-sell");
    assert_eq!(after_cursor[0].token_in, "TOKEN-B");
    assert_eq!(after_cursor[0].token_out, SOL_MINT);
    assert_eq!(after_cursor[0].amount_in, 50.0);
    assert_eq!(after_cursor[0].amount_out, 0.75);

    let later_buy = SwapEvent {
        signature: "sig-later-buy".to_string(),
        wallet: "wallet-d".to_string(),
        dex: "raydium".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TOKEN-LATER".to_string(),
        amount_in: 0.4,
        amount_out: 40.0,
        slot: 103,
        ts_utc: ts("2026-05-05T11:30:00Z")?,
        exact_amounts: None,
    };
    store.insert_observed_swap(&later_buy)?;
    let later_non_sol = SwapEvent {
        signature: "sig-later-non-sol".to_string(),
        wallet: "wallet-e".to_string(),
        dex: "orca".to_string(),
        token_in: "TOKEN-X".to_string(),
        token_out: "TOKEN-Y".to_string(),
        amount_in: 10.0,
        amount_out: 11.0,
        slot: 104,
        ts_utc: ts("2026-05-05T11:45:00Z")?,
        exact_amounts: None,
    };
    store.insert_observed_swap(&later_non_sol)?;
    Connection::open(&db_path)?.execute(
        "DROP INDEX idx_observed_swaps_sol_leg_ts_slot_signature",
        [],
    )?;

    let mut extended = Vec::new();
    let extended_page = store.for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
        window_start,
        ts("2026-05-05T12:00:00Z")?,
        None,
        10,
        Instant::now() + Duration::from_secs(5),
        |swap| {
            extended.push(swap);
            Ok(())
        },
    )?;
    assert!(!extended_page.time_budget_exhausted);
    assert_eq!(extended_page.rows_seen, 3);
    assert_eq!(
        extended
            .iter()
            .map(|swap| swap.signature.as_str())
            .collect::<Vec<_>>(),
        vec!["sig-buy", "sig-sell", "sig-later-buy"]
    );
    Ok(())
}
