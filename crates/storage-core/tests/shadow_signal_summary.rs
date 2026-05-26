use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::SqliteStore;
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn signal(
    signal_id: &str,
    wallet: &str,
    side: &str,
    token: &str,
    ts: DateTime<Utc>,
) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: wallet.to_string(),
        side: side.to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

#[test]
fn shadow_signal_summary_splits_matched_and_no_position_sells() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let now = ts("2026-05-26T08:00:00Z");
    let since = now - Duration::hours(24);
    store.insert_copy_signal(&signal(
        "buy-1",
        "wallet-a",
        "buy",
        "token-a",
        now - Duration::hours(3),
    ))?;
    store.insert_copy_signal(&signal(
        "sell-matched",
        "wallet-a",
        "sell",
        "token-a",
        now - Duration::hours(2),
    ))?;
    store.insert_copy_signal(&signal(
        "sell-no-position",
        "wallet-b",
        "sell",
        "token-b",
        now - Duration::hours(1),
    ))?;
    store.insert_copy_signal(&signal(
        "sell-old",
        "wallet-c",
        "sell",
        "token-c",
        since - Duration::seconds(1),
    ))?;
    store.insert_shadow_closed_trade(
        "sell-matched",
        "wallet-a",
        "token-a",
        1.0,
        0.20,
        0.12,
        -0.08,
        now - Duration::hours(3),
        now - Duration::hours(2),
    )?;

    let summary = store.shadow_signal_summary_since(since)?;

    assert_eq!(summary.buy_signals, 1);
    assert_eq!(summary.sell_signals_total, 2);
    assert_eq!(summary.sell_signals_matched, 1);
    assert_eq!(summary.sell_signals_no_position, 1);
    assert_eq!(summary.closed_trades, 1);
    assert_eq!(summary.wins, 0);
    assert_eq!(summary.losses, 1);
    assert!((summary.pnl_sol + 0.08).abs() < 1e-9);
    assert!((summary.entry_cost_sol - 0.20).abs() < 1e-9);
    assert!(summary.roi().is_some_and(|roi| (roi + 0.40).abs() < 1e-9));
    Ok(())
}
