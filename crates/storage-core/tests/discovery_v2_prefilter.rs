use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::SqliteDiscoveryStore;
use std::time::{Duration as StdDuration, Instant};

const SOL: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn insert_sol_buy(
    store: &SqliteDiscoveryStore,
    signature: &str,
    wallet: &str,
    token: &str,
    slot: i64,
    ts_raw: &str,
    sol: f64,
) -> Result<()> {
    store.insert_observed_swap(&copybot_core_types::SwapEvent {
        signature: signature.to_string(),
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL.to_string(),
        token_out: token.to_string(),
        amount_in: sol,
        amount_out: 100.0,
        slot: slot as u64,
        ts_utc: ts(ts_raw)?,
        exact_amounts: None,
    })?;
    Ok(())
}

#[test]
fn discovery_v2_prefilter_keeps_only_wallets_that_can_pass_base_gates() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.ensure_observed_swap_writer_tables()?;
    for idx in 0..10 {
        insert_sol_buy(
            &store,
            &format!("good-{idx}"),
            "wallet-good",
            "token-good",
            100 + idx,
            "2026-05-13T10:00:00+00:00",
            0.50,
        )?;
    }
    for idx in 0..9 {
        insert_sol_buy(
            &store,
            &format!("few-{idx}"),
            "wallet-few-buys",
            "token-few",
            200 + idx,
            "2026-05-13T10:01:00+00:00",
            0.50,
        )?;
    }
    for idx in 0..10 {
        insert_sol_buy(
            &store,
            &format!("small-{idx}"),
            "wallet-small",
            "token-small",
            300 + idx,
            "2026-05-13T10:02:00+00:00",
            0.10,
        )?;
    }

    let read_only = SqliteDiscoveryStore::open_read_only(dir.path().join("runtime.db"))?;
    let prefilter = read_only.discovery_v2_wallet_prefilter_read_only(
        ts("2026-05-13T09:00:00+00:00")?,
        ts("2026-05-13T11:00:00+00:00")?,
        100,
        10,
        10,
        1,
        0.50,
        Instant::now() + StdDuration::from_secs(5),
    )?;

    assert_eq!(prefilter.rows_seen, 29);
    assert_eq!(prefilter.unique_wallets, 3);
    assert_eq!(prefilter.wallet_ids, vec!["wallet-good".to_string()]);
    assert!(!prefilter.time_budget_exhausted);
    Ok(())
}
