use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{SqliteStore, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN: &str = "DecayTokenMint111111111111111111111111111111";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

#[test]
fn stale_quote_decay_reports_checkpoint_value_ratios() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.keep().join("decay.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let opened = ts("2026-06-10T00:00:00Z")?;
    let closed = opened + Duration::hours(7);
    store.insert_shadow_closed_trade_exact_with_context(
        "stale-close-1",
        "wallet-a",
        TOKEN,
        100.0,
        None,
        1.0,
        0.01,
        -0.99,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        opened,
        closed,
    )?;
    store.insert_observed_swaps_batch_with_activity_days(&[
        token_to_sol_swap("sig-30m", opened + Duration::minutes(30), 100.0, 0.5),
        sol_to_token_swap("sig-60m", opened + Duration::minutes(60), 0.25, 100.0),
    ])?;

    let report = store.execution_stale_quote_decay_report(
        opened - Duration::minutes(1),
        10,
        20,
        0.0,
        1,
        10,
    )?;

    assert_eq!(report.trades, 1);
    let row = &report.rows[0];
    assert_eq!(row.shadow_closed_trade_id, 1);
    assert_ratio(row, 30, Some(0.5));
    assert_ratio(row, 60, Some(0.25));
    assert_ratio(row, 120, None);
    let thirty = report
        .summaries
        .iter()
        .find(|summary| summary.offset_minutes == 30)
        .unwrap();
    assert_eq!(thirty.eligible_trades, 1);
    assert_eq!(thirty.priced_trades, 1);
    assert_eq!(thirty.p50_value_to_entry_ratio, Some(0.5));
    Ok(())
}

fn token_to_sol_swap(signature: &str, ts_utc: DateTime<Utc>, token: f64, sol: f64) -> SwapEvent {
    SwapEvent {
        signature: signature.to_string(),
        wallet: "wallet-a".to_string(),
        dex: "pumpswap".to_string(),
        token_in: TOKEN.to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: token,
        amount_out: sol,
        slot: 42,
        ts_utc,
        exact_amounts: None,
    }
}

fn sol_to_token_swap(signature: &str, ts_utc: DateTime<Utc>, sol: f64, token: f64) -> SwapEvent {
    SwapEvent {
        signature: signature.to_string(),
        wallet: "wallet-a".to_string(),
        dex: "pumpswap".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: TOKEN.to_string(),
        amount_in: sol,
        amount_out: token,
        slot: 43,
        ts_utc,
        exact_amounts: None,
    }
}

fn assert_ratio(
    row: &copybot_storage_core::ExecutionStaleDecayTrade,
    offset: i64,
    expected: Option<f64>,
) {
    let point = row
        .points
        .iter()
        .find(|point| point.offset_minutes == offset)
        .unwrap();
    assert_eq!(point.value_to_entry_ratio, expected);
}
