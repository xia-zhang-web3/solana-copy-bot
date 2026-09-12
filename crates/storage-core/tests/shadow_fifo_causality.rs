use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{SqliteStore, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE};
use tempfile::{tempdir, TempDir};

fn setup() -> Result<(TempDir, SqliteStore, DateTime<Utc>)> {
    let dir = tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("fifo.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok((dir, store, "2026-09-05T12:00:00Z".parse()?))
}

#[test]
fn shadow_fifo_sell_cannot_close_a_future_lot() -> Result<()> {
    for exact in [None, Some(TokenQuantity::new(10000, 3))] {
        let (_dir, store, ts) = setup()?;
        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            10.0,
            exact,
            0.1,
            ts + Duration::seconds(20),
        )?;
        let before = store.list_shadow_lots("wallet", "token")?;
        let out = store.close_shadow_lots_fifo_atomic_exact(
            "sell",
            "wallet",
            "token",
            10.0,
            exact,
            0.02,
            ts + Duration::seconds(10),
        )?;
        assert_eq!(out.closed_qty, 0.0);
        assert!(out.has_open_lots_after);
        let after = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].qty, before[0].qty);
        assert_eq!(after[0].cost_lamports, before[0].cost_lamports);
        assert_eq!(after[0].qty_exact, exact);
        assert!(store
            .list_execution_quote_canary_close_candidates_for_signal("sell", 10)?
            .is_empty());
    }
    Ok(())
}

#[test]
fn shadow_fifo_mixed_lots_keeps_future_inventory_and_eligible_fifo() -> Result<()> {
    let (_dir, store, ts) = setup()?;
    let first = store.insert_shadow_lot_exact(
        "wallet",
        "token",
        3.0,
        Some(TokenQuantity::new(3000, 3)),
        0.03,
        ts,
    )?;
    let future = store.insert_shadow_lot_exact(
        "wallet",
        "token",
        10.0,
        Some(TokenQuantity::new(10000, 3)),
        0.2,
        ts + Duration::seconds(20),
    )?;
    let second = store.insert_shadow_lot_exact(
        "wallet",
        "token",
        4.0,
        Some(TokenQuantity::new(4000, 3)),
        0.08,
        ts + Duration::seconds(5),
    )?;
    let out = store.close_shadow_lots_fifo_atomic_exact(
        "sell",
        "wallet",
        "token",
        5.0,
        Some(TokenQuantity::new(5000, 3)),
        0.03,
        ts + Duration::seconds(10),
    )?;
    assert_eq!(out.closed_qty, 5.0);
    let lots = store.list_shadow_lots("wallet", "token")?;
    assert!(!lots.iter().any(|lot| lot.id == first));
    let untouched = lots
        .iter()
        .find(|lot| lot.id == future)
        .expect("future lot");
    assert_eq!(untouched.qty_exact, Some(TokenQuantity::new(10000, 3)));
    assert_eq!(untouched.cost_lamports.unwrap().as_u64(), 200_000_000);
    let partial = lots
        .iter()
        .find(|lot| lot.id == second)
        .expect("eligible partial lot");
    assert_eq!(partial.qty_exact, Some(TokenQuantity::new(2000, 3)));
    assert_eq!(partial.cost_lamports.unwrap().as_u64(), 40_000_000);
    let closed = store.list_execution_quote_canary_close_candidates_for_signal("sell", 10)?;
    assert_eq!(closed.len(), 2);
    assert!((out.realized_pnl_sol - 0.08).abs() < 1e-12);
    let excess = store.close_shadow_lots_fifo_atomic_exact(
        "next-sell",
        "wallet",
        "token",
        100.0,
        Some(TokenQuantity::new(100000, 3)),
        0.03,
        ts + Duration::seconds(10),
    )?;
    assert_eq!(excess.closed_qty, 2.0);
    let remaining = store.list_shadow_lots("wallet", "token")?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, future);
    assert_eq!(remaining[0].qty_exact.unwrap().raw(), 10000);
    assert_eq!(remaining[0].cost_lamports.unwrap().as_u64(), 200_000_000);
    assert!(closed
        .iter()
        .all(|row| row.closed_ts == ts + Duration::seconds(10)));
    Ok(())
}

#[test]
fn shadow_fifo_timestamp_boundary_preserves_subsecond_precision() -> Result<()> {
    let (_dir, store, ts) = setup()?;
    let sell_ts = ts + Duration::nanoseconds(1);
    store.insert_shadow_lot("wallet", "token", 1.0, 0.01, sell_ts)?;
    store.insert_shadow_lot(
        "wallet",
        "token",
        1.0,
        0.02,
        sell_ts + Duration::nanoseconds(1),
    )?;
    let out = store
        .close_shadow_lots_fifo_atomic_exact("sell", "wallet", "token", 2.0, None, 0.03, sell_ts)?;
    assert_eq!(out.closed_qty, 1.0);
    assert_eq!(
        store.list_shadow_lots("wallet", "token")?[0].opened_ts,
        sell_ts + Duration::nanoseconds(1)
    );
    Ok(())
}

#[test]
fn shadow_fifo_partial_duplicate_after_reopen_has_one_economic_effect() -> Result<()> {
    let (dir, store, ts) = setup()?;
    store.insert_shadow_lot_exact(
        "wallet",
        "token",
        10.0,
        Some(TokenQuantity::new(10000, 3)),
        0.1,
        ts,
    )?;
    let close = |store: &SqliteStore| {
        store.close_shadow_lots_fifo_atomic_exact(
            "sell",
            "wallet",
            "token",
            2.0,
            Some(TokenQuantity::new(2000, 3)),
            0.02,
            ts + Duration::seconds(1),
        )
    };
    assert_eq!(close(&store)?.closed_qty, 2.0);
    drop(store);
    let store = SqliteStore::open(dir.path().join("fifo.db"))?;
    assert_eq!(close(&store)?.closed_qty, 0.0);
    assert_eq!(
        store.list_shadow_lots("wallet", "token")?[0].qty_exact,
        Some(TokenQuantity::new(8000, 3))
    );
    assert_eq!(
        store
            .list_execution_quote_canary_close_candidates_for_signal("sell", 10)?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn shadow_fifo_recovery_context_has_the_same_causal_boundary() -> Result<()> {
    let (_dir, store, ts) = setup()?;
    store.insert_shadow_lot("wallet", "token", 1.0, 0.01, ts)?;
    store.insert_shadow_lot("wallet", "token", 2.0, 0.04, ts + Duration::seconds(20))?;
    let out = store.close_shadow_lots_fifo_atomic_exact_with_context(
        "recovery",
        "wallet",
        "token",
        3.0,
        None,
        0.0,
        SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
        ts + Duration::seconds(10),
    )?;
    assert_eq!(out.closed_qty, 1.0);
    assert!((out.realized_pnl_sol + 0.01).abs() < 1e-12);
    assert_eq!(store.list_shadow_lots("wallet", "token")?[0].qty, 2.0);
    assert_eq!(
        store
            .shadow_closed_trade_close_context("recovery")?
            .as_deref(),
        Some(SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE)
    );
    Ok(())
}

#[test]
fn shadow_fifo_failed_insert_rolls_back_lots_and_allows_retry() -> Result<()> {
    let (dir, store, ts) = setup()?;
    store.insert_shadow_lot("wallet", "token", 1.0, 0.01, ts)?;
    store.insert_shadow_lot("wallet", "token", 1.0, 0.02, ts)?;
    let conn = rusqlite::Connection::open(dir.path().join("fifo.db"))?;
    conn.execute_batch(
        "CREATE TRIGGER reject_close BEFORE INSERT ON shadow_closed_trades
        WHEN (SELECT COUNT(*) FROM shadow_closed_trades) > 0
        BEGIN SELECT RAISE(ABORT, 'synthetic second close failure'); END;",
    )?;
    let close = || {
        store.close_shadow_lots_fifo_atomic_exact("sell", "wallet", "token", 2.0, None, 0.02, ts)
    };
    assert!(close().is_err());
    let lots = store.list_shadow_lots("wallet", "token")?;
    assert_eq!(lots.len(), 2);
    assert_eq!(lots.iter().map(|lot| lot.qty).sum::<f64>(), 2.0);
    assert_eq!(
        lots.iter()
            .map(|lot| lot.cost_lamports.unwrap().as_u64())
            .sum::<u64>(),
        30_000_000
    );
    assert!(store
        .list_execution_quote_canary_close_candidates_for_signal("sell", 10)?
        .is_empty());
    conn.execute_batch("DROP TRIGGER reject_close")?;
    assert_eq!(close()?.closed_qty, 2.0);
    assert_eq!(close()?.closed_qty, 0.0);
    Ok(())
}

#[test]
fn shadow_fifo_replay_lookup_uses_persistent_signal_index() -> Result<()> {
    let (dir, mut store, ts) = setup()?;
    store.insert_shadow_lot("wallet", "token", 2.0, 0.02, ts)?;
    let conn = rusqlite::Connection::open(dir.path().join("fifo.db"))?;
    let plan: String = conn.query_row(
        "EXPLAIN QUERY PLAN SELECT 1 FROM shadow_closed_trades WHERE signal_id = ?1 AND wallet_id = ?2 AND token = ?3",
        ["sell", "wallet", "token"], |r| r.get(3))?;
    assert!(
        plan.contains("USING COVERING INDEX idx_shadow_closed_trades_signal_wallet_token"),
        "{plan}"
    );
    assert_eq!(
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?,
        0
    );
    assert_eq!(store.list_shadow_lots("wallet", "token")?[0].qty, 2.0);
    Ok(())
}
