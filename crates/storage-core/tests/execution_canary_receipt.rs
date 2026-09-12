use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage_core::*;
use rusqlite::Connection;
use std::path::Path;
use tempfile::tempdir;

fn submitted(store: &SqliteStore, name: &str, side: &str) -> Result<String> {
    let now = Utc::now();
    store.insert_copy_signal(&CopySignalRow {
        signal_id: name.into(),
        wallet_id: "leader".into(),
        side: side.into(),
        token: "mint".into(),
        notional_sol: 1.0,
        notional_lamports: Some(Lamports::new(1_000_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    })?;
    let o = store
        .reserve_execution_canary_order(name, "tiny", now)?
        .order;
    store.mark_execution_canary_built(&o.order_id, now)?;
    store.mark_execution_canary_simulated(
        &o.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(&o.order_id, now, "signature")?;
    Ok(o.order_id)
}
fn proof(side: &str) -> ExecutionCanaryReceiptProof {
    ExecutionCanaryReceiptProof {
        tx_signature: "signature".into(),
        wallet_pubkey: "wallet".into(),
        token: "mint".into(),
        side: side.into(),
        confirmation_status: "finalized".into(),
        slot: Some(42),
        confirmed_at: Utc::now(),
        reason: "receipt_not_available".into(),
    }
}
fn migrations() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"))
}
fn snapshot(conn: &Connection) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for table in ["orders", "positions", "fills"] {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let columns = stmt.column_count();
        rows.extend(
            stmt.query_map([], |r| {
                (0..columns)
                    .map(|c| {
                        r.get::<_, rusqlite::types::Value>(c)
                            .map(|v| format!("{v:?}"))
                    })
                    .collect()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    Ok(rows)
}

#[test]
fn receipt_migration_opens_existing_database_without_rewriting_accounted_history() -> Result<()> {
    let dir = tempdir()?;
    let old_migrations = dir.path().join("old-migrations");
    std::fs::create_dir(&old_migrations)?;
    for entry in std::fs::read_dir(migrations())? {
        let entry = entry?;
        if entry.file_name() != "0051_execution_canary_receipt_proofs.sql"
            && entry.file_name() != "0061_buy_receipt_ownership_indexes.sql"
        {
            std::fs::copy(entry.path(), old_migrations.join(entry.file_name()))?;
        }
    }
    let path = dir.path().join("synthetic-old.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old_migrations)?;
    let accounted = submitted(&store, "accounted", "buy")?;
    store.confirm_execution_canary_buy_fill(
        &accounted,
        "mint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        1.0,
        Utc::now(),
        Utc::now(),
        None,
    )?;
    let missing = submitted(&store, "legacy-missing", "sell")?;
    store.mark_execution_canary_confirmed(&missing, Utc::now())?;
    drop(store);
    let before = snapshot(&Connection::open(&path)?)?;
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(migrations())?, 2); // 0051 proof table + 0061 indexes
    assert_eq!(store.run_migrations(migrations())?, 0);
    assert_eq!(snapshot(&Connection::open(&path)?)?, before);
    assert!(store
        .load_execution_canary_receipt_proof(&accounted)?
        .is_none());
    assert!(store
        .load_execution_canary_receipt_proof(&missing)?
        .is_none());
    let pending = store.list_reconcilable_execution_canary_orders_for_route("tiny", "retry", 10)?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].order_id, missing);
    assert!(store.execution_canary_accounting_pending()?);
    // Marking already accounted history is a no-op, even if a caller has stale input.
    store.mark_execution_canary_confirmed_unreconciled(&accounted, &proof("buy"), Utc::now())?;
    assert_eq!(snapshot(&Connection::open(&path)?)?, before);
    Ok(())
}

#[test]
fn receipt_pending_proof_identity_survives_restart_and_cannot_expire_or_retry() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("pending.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(migrations())?;
    let id = submitted(&store, "pending", "sell")?;
    let original = proof("sell");
    store.mark_execution_canary_confirmed_unreconciled(&id, &original, Utc::now())?;
    drop(store);
    let store = SqliteStore::open(&path)?;
    assert_eq!(
        store.load_execution_canary_receipt_proof(&id)?.unwrap(),
        original
    );
    let now = original.confirmed_at + Duration::days(1);
    assert_eq!(
        store
            .execution_canary_confirm_timeout_decision(&id, now, Duration::seconds(1))?
            .decision_status,
        EXECUTION_CANARY_CONFIRM_DECISION_WAIT
    );
    assert!(store
        .mark_execution_canary_expired(&id, now, "expired")
        .is_err());
    assert!(store
        .mark_execution_canary_retry_after_submit_timeout(&id, now, Duration::seconds(1), "retry")
        .is_err());
    assert!(store
        .mark_execution_canary_submitted_unknown(&id, now, "unknown")
        .is_err());
    let mut changed = original.clone();
    changed.wallet_pubkey = "another-wallet".into();
    assert!(store
        .mark_execution_canary_confirmed_unreconciled(&id, &changed, Utc::now())
        .is_err());
    assert_eq!(
        store.load_execution_canary_receipt_proof(&id)?.unwrap(),
        original
    );
    assert_eq!(
        store
            .load_execution_canary_order(&id)?
            .unwrap()
            .tx_signature
            .as_deref(),
        Some("signature")
    );
    Ok(())
}

#[test]
fn receipt_completion_transaction_rolls_back_fill_position_status_and_proof() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("rollback.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(migrations())?;
    let id = submitted(&store, "pending", "buy")?;
    store.mark_execution_canary_confirmed_unreconciled(&id, &proof("buy"), Utc::now())?;
    let conn = Connection::open(&path)?;
    conn.execute_batch(
        "CREATE TRIGGER fail_proof_completion BEFORE UPDATE ON execution_canary_receipt_proofs
        WHEN NEW.reason = 'accounting_complete' BEGIN SELECT RAISE(ABORT,'injected'); END;",
    )?;
    let before = snapshot(&conn)?;
    assert!(store
        .confirm_execution_canary_buy_fill(
            &id,
            "mint",
            7.0,
            Some(TokenQuantity::new(7000, 3)),
            0.9,
            Utc::now(),
            Utc::now(),
            Some(Lamports::new(900_000_000))
        )
        .is_err());
    assert_eq!(snapshot(&conn)?, before);
    assert_eq!(
        store
            .load_execution_canary_receipt_proof(&id)?
            .unwrap()
            .reason,
        "receipt_not_available"
    );
    conn.execute_batch("DROP TRIGGER fail_proof_completion")?;
    store.confirm_execution_canary_buy_fill(
        &id,
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.9,
        Utc::now(),
        Utc::now(),
        Some(Lamports::new(900_000_000)),
    )?;
    let after = snapshot(&conn)?;
    store.confirm_execution_canary_buy_fill(
        &id,
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.9,
        Utc::now(),
        Utc::now(),
        Some(Lamports::new(900_000_000)),
    )?;
    assert_eq!(snapshot(&conn)?, after);
    assert!(!store.execution_canary_accounting_pending()?);
    assert!(store
        .list_reconcilable_execution_canary_orders_for_route("tiny", "retry", 10)?
        .is_empty());
    Ok(())
}

#[test]
fn receipt_sweep_rotates_past_unavailable_receipts_with_batch_limit_one() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("fair-sweep.db"))?;
    store.run_migrations(migrations())?;
    let first = submitted(&store, "first", "buy")?;
    let second = submitted(&store, "second", "buy")?;
    let now = Utc::now();
    store.mark_execution_canary_confirmed_unreconciled(&first, &proof("buy"), now)?;
    store.mark_execution_canary_confirmed_unreconciled(
        &second,
        &proof("buy"),
        now + Duration::seconds(1),
    )?;
    let next = store.list_reconcilable_execution_canary_orders_for_route("tiny", "retry", 1)?;
    assert_eq!(next[0].order_id, first);
    store.mark_execution_canary_confirmed_unreconciled(
        &first,
        &proof("buy"),
        now + Duration::seconds(2),
    )?;
    let next = store.list_reconcilable_execution_canary_orders_for_route("tiny", "retry", 1)?;
    assert_eq!(next[0].order_id, second);
    let report = store.execution_canary_status_report(now)?;
    assert_eq!(report.confirmed, 0);
    assert_eq!(report.active_count(), 2);
    Ok(())
}
