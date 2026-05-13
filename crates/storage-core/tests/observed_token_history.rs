use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn swap(
    signature: &str,
    wallet: &str,
    token_in: &str,
    token_out: &str,
    ts_utc: DateTime<Utc>,
    slot: u64,
) -> SwapEvent {
    SwapEvent {
        signature: signature.to_string(),
        wallet: wallet.to_string(),
        dex: "raydium".to_string(),
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in: 1.0,
        amount_out: 2.0,
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

#[test]
fn token_history_stream_reads_only_requested_sol_leg_tokens() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;

    let start = ts("2026-05-13T10:00:00Z")?;
    store.insert_observed_swaps_batch(&[
        swap("sig-a-buy", "wallet-a", SOL_MINT, "TOKEN_A", start, 10),
        swap("sig-b-buy", "wallet-b", SOL_MINT, "TOKEN_B", start, 11),
        swap("sig-a-sell", "wallet-c", "TOKEN_A", SOL_MINT, start, 12),
        swap("sig-non-sol", "wallet-d", "TOKEN_A", "TOKEN_C", start, 13),
    ])?;

    let mut signatures = Vec::new();
    let page = store.for_each_sol_leg_observed_swap_for_tokens_in_window_with_budget(
        &["TOKEN_A".to_string()],
        start - chrono::Duration::minutes(1),
        start + chrono::Duration::minutes(1),
        10,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
        |swap| {
            signatures.push(swap.signature);
            Ok(())
        },
    )?;

    assert!(!page.time_budget_exhausted);
    assert_eq!(page.rows_seen, 2);
    assert_eq!(signatures, vec!["sig-a-buy", "sig-a-sell"]);
    Ok(())
}

#[test]
fn token_history_stream_respects_global_row_limit_across_tokens() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;

    let start = ts("2026-05-13T10:00:00Z")?;
    store.insert_observed_swaps_batch(&[
        swap("sig-a", "wallet-a", SOL_MINT, "TOKEN_A", start, 10),
        swap("sig-b", "wallet-b", SOL_MINT, "TOKEN_B", start, 11),
        swap("sig-c", "wallet-c", SOL_MINT, "TOKEN_C", start, 12),
    ])?;

    let mut signatures = Vec::new();
    let page = store.for_each_sol_leg_observed_swap_for_tokens_in_window_with_budget(
        &[
            "TOKEN_A".to_string(),
            "TOKEN_B".to_string(),
            "TOKEN_C".to_string(),
        ],
        start - chrono::Duration::minutes(1),
        start + chrono::Duration::minutes(1),
        2,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
        |swap| {
            signatures.push(swap.signature);
            Ok(())
        },
    )?;

    assert_eq!(page.rows_seen, 2);
    assert_eq!(signatures, vec!["sig-a", "sig-b"]);
    Ok(())
}
