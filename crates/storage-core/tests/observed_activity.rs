use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

#[test]
fn wallet_sol_leg_activity_window_counts_maturity_days() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;
    let now = ts("2026-05-05T12:00:00Z")?;
    let token = "TokenMaturity111111111111111111111111111111";
    let mut swaps = Vec::new();
    for (idx, hours_ago) in [(1u64, 49), (2, 25), (3, 1)] {
        swaps.push(SwapEvent {
            signature: format!("sig-maturity-{idx}"),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: token.to_string(),
            amount_in: 1.0,
            amount_out: 2.0,
            slot: 40 + idx,
            ts_utc: now - Duration::hours(hours_ago),
            exact_amounts: None,
        });
    }
    swaps.push(SwapEvent {
        signature: "sig-non-sol-leg".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: "TokenA1111111111111111111111111111111111".to_string(),
        token_out: token.to_string(),
        amount_in: 1.0,
        amount_out: 2.0,
        slot: 50,
        ts_utc: now - Duration::minutes(30),
        exact_amounts: None,
    });
    store.insert_observed_swaps_batch(&swaps)?;

    let activity = store.wallet_sol_leg_activity_in_window_read_only(
        "wallet-a",
        now - Duration::days(3),
        now,
        std::time::Instant::now() + std::time::Duration::from_secs(60),
    )?;

    assert_eq!(activity.trades, 3);
    assert_eq!(activity.active_days, 3);
    assert_eq!(activity.first_seen, Some(now - Duration::hours(49)));
    assert_eq!(activity.last_seen, Some(now - Duration::hours(1)));
    assert!(!activity.time_budget_exhausted);
    Ok(())
}
