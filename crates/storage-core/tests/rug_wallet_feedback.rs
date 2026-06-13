use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{
    SqliteStore, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

#[test]
fn rug_wallet_feedback_counts_stale_terminal_tail() -> Result<()> {
    let store = open_migrated_store("rug-wallet-feedback")?;
    let now = ts("2026-06-13T10:00:00Z");
    for index in 0..8 {
        let opened = now - Duration::hours(1) + Duration::seconds(index);
        store.insert_shadow_closed_trade(
            &format!("market-close-{index}"),
            "rug-wallet",
            &format!("MarketToken{index}"),
            1000.0,
            0.20,
            0.21,
            0.01,
            opened,
            opened + Duration::seconds(30),
        )?;
    }
    let opened = now - Duration::minutes(30);
    store.insert_shadow_closed_trade_exact_with_context(
        "stale-close-1",
        "rug-wallet",
        "DeadToken",
        1000.0,
        None,
        0.20,
        0.01,
        -0.19,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        opened,
        opened + Duration::minutes(30),
    )?;
    let opened = now - Duration::minutes(20);
    store.insert_shadow_closed_trade_exact_with_context(
        "recovery-terminal-1",
        "rug-wallet",
        "RecoveryDeadToken",
        1000.0,
        None,
        0.20,
        0.0,
        -0.20,
        SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
        opened,
        opened + Duration::minutes(20),
    )?;

    let feedback = store.rug_wallet_feedback_since(now - Duration::hours(48))?;
    let rug = feedback.get("rug-wallet").expect("rug wallet feedback");
    assert_eq!(rug.closed_trades, 10);
    assert_eq!(rug.stale_terminal_closes, 2);
    assert_close(rug.stale_terminal_rate().expect("rate"), 0.2);
    assert_close(rug.stale_terminal_pnl_sol, -0.39);
    assert_close(rug.stale_terminal_entry_cost_sol, 0.40);
    Ok(())
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
}
