use anyhow::Result;
use chrono::Utc;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::*;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
};

fn migrations() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
}
fn store(path: &Path) -> Result<SqliteStore> {
    let mut s = SqliteStore::open(path)?;
    s.run_migrations(&migrations())?;
    Ok(s)
}
fn candidate(
    s: &SqliteStore,
    id: &str,
) -> Result<(ExecutionCanaryOrder, CopySignalRow, ExecutionCanaryDispatch)> {
    let now = Utc::now();
    let signal = CopySignalRow {
        signal_id: id.into(),
        wallet_id: "leader".into(),
        side: "buy".into(),
        token: id.into(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    };
    s.insert_copy_signal(&signal)?;
    let o = s
        .reserve_execution_canary_order(id, "metis-canary", now)?
        .order;
    s.mark_execution_canary_built(&o.order_id, now)?;
    let order = s.mark_execution_canary_simulated(
        &o.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    let identity = ExecutionCanaryDispatch {
        order_id: order.order_id.clone(),
        signal_id: id.into(),
        client_order_id: order.client_order_id.clone(),
        route: order.route.clone(),
        attempt: order.attempt,
        wallet: "synthetic-wallet".into(),
        token: id.into(),
        side: "buy".into(),
        tx_signature: format!("storage-signature-{id}"),
        message_sha256: "a".repeat(64),
        transaction_sha256: "b".repeat(64),
    };
    Ok((order, signal, identity))
}

#[test]
fn dispatch_two_connections_barrier_only_one_new_buy_claim() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("race.db");
    let s = store(&path)?;
    let a = candidate(&s, "a")?;
    let b = candidate(&s, "b")?;
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for (order, signal, identity) in [a, b] {
        let (path, barrier) = (path.clone(), barrier.clone());
        workers.push(std::thread::spawn(move || {
            let s = SqliteStore::open(path).unwrap();
            barrier.wait();
            s.claim_execution_canary_dispatch(&order, &signal, &identity, Utc::now())
        }));
    }
    barrier.wait();
    let results = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(ExecutionDispatchClaim::New)))
            .count(),
        1
    );
    assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1);
    assert!(s.execution_canary_unresolved_buy()?);
    let count: i64 = rusqlite::Connection::open(&path)?.query_row(
        "SELECT COUNT(*) FROM execution_canary_dispatch",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(count, 1);
    println!("B53_CONCURRENT claims_new=1 rejected=1 durable_rows={count}");
    Ok(())
}

#[test]
fn dispatch_replay_and_identity_conflicts_never_grant_new_send() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("replay.db");
    let s = store(&path)?;
    let (o, signal, identity) = candidate(&s, "a")?;
    assert_eq!(
        s.claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())?,
        ExecutionDispatchClaim::New
    );
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert_eq!(
        s.claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())?,
        ExecutionDispatchClaim::Existing
    );
    for n in 0..4 {
        let mut other = identity.clone();
        match n {
            0 => other.tx_signature.push('x'),
            1 => other.wallet.push('x'),
            2 => other.message_sha256 = "c".repeat(64),
            _ => other.transaction_sha256 = "d".repeat(64),
        };
        assert!(s
            .claim_execution_canary_dispatch(&o, &signal, &other, Utc::now())
            .is_err());
    }
    assert_eq!(
        s.load_execution_canary_dispatch(&o.order_id)?,
        Some(identity)
    );
    Ok(())
}

#[test]
fn dispatch_atomic_write_rollback_and_exact_state_change() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollback.db");
    let s = store(&path)?;
    let (o, signal, identity) = candidate(&s, "a")?;
    let conn = rusqlite::Connection::open(&path)?;
    conn.execute_batch("CREATE TRIGGER reject BEFORE UPDATE OF tx_signature ON orders BEGIN SELECT RAISE(ABORT,'write failure'); END;")?;
    assert!(s
        .claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())
        .is_err());
    assert!(s.load_execution_canary_dispatch(&o.order_id)?.is_none());
    assert_eq!(s.load_execution_canary_order(&o.order_id)?, Some(o.clone()));
    conn.execute_batch("DROP TRIGGER reject; UPDATE copy_signals SET status='changed';")?;
    assert!(s
        .claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())
        .is_err());
    conn.execute("UPDATE copy_signals SET status=?1", [&signal.status])?;
    conn.execute_batch("UPDATE orders SET simulation_error='changed';")?;
    assert!(s
        .claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())
        .is_err());
    Ok(())
}

#[test]
fn dispatch_upgrade_reopen_is_additive_without_0063() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("upgrade.db");
    let old = dir.path().join("migrations");
    std::fs::create_dir(&old)?;
    for entry in std::fs::read_dir(migrations())? {
        let p = entry?.path();
        if p.extension().is_some_and(|x| x == "sql")
            && !p
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("0064_")
        {
            std::fs::copy(&p, old.join(p.file_name().unwrap()))?;
        }
    }
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&old)?;
    let (o, signal, identity) = candidate(&s, "a")?;
    assert!(s.execution_canary_unresolved_buy().is_err());
    assert_eq!(s.run_migrations(&migrations())?, 1);
    assert_eq!(s.run_migrations(&migrations())?, 0);
    assert_eq!(s.load_execution_canary_order(&o.order_id)?, Some(o.clone()));
    assert_eq!(
        s.claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())?,
        ExecutionDispatchClaim::New
    );
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert!(s.execution_canary_unresolved_buy()?);
    Ok(())
}

#[test]
fn dispatch_missing_or_corrupt_modern_schema_never_allows_buy() -> Result<()> {
    for damage in [
        "ALTER TABLE execution_canary_dispatch RENAME TO backup",
        "ALTER TABLE execution_canary_reconcile_attempts RENAME TO backup",
        "DROP VIEW execution_canary_unresolved_dispatch",
        "ALTER TABLE execution_canary_dispatch RENAME COLUMN wallet TO broken_wallet",
    ] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("schema.db");
        let s = store(&path)?;
        let (o, signal, identity) = candidate(&s, "a")?;
        rusqlite::Connection::open(&path)?.execute_batch(damage)?;
        assert!(s.execution_canary_unresolved_buy().is_err(), "{damage}");
        assert!(
            s.claim_execution_canary_dispatch(&o, &signal, &identity, Utc::now())
                .is_err(),
            "{damage}"
        );
        assert_eq!(s.load_execution_canary_order(&o.order_id)?, Some(o));
    }
    Ok(())
}

#[test]
fn dispatch_reader_returns_one_current_legacy_buy_witness_in_order() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("witness.db");
    let s = store(&path)?;
    assert_eq!(s.execution_canary_unresolved_buy_order_id()?, None);
    let (z, _, _) = candidate(&s, "z")?;
    let (a, _, _) = candidate(&s, "a")?;
    s.mark_execution_canary_submitted_unknown(&z.order_id, Utc::now(), "legacy_unsigned")?;
    s.mark_execution_canary_submitted(&a.order_id, Utc::now(), "legacy_signed")?;
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert_eq!(
        s.execution_canary_unresolved_buy_order_id()?,
        Some(a.order_id.clone())
    );
    // Exact old view predicate: failed status alone cannot release a signed A.
    s.mark_execution_canary_failed(&a.order_id, Utc::now(), "failed", "still_unknown")?;
    assert_eq!(
        s.execution_canary_unresolved_buy_order_id()?,
        Some(a.order_id)
    );
    assert!(s.execution_canary_unresolved_buy()?);
    rusqlite::Connection::open(&path)?
        .execute_batch("DROP VIEW execution_canary_unresolved_dispatch")?;
    assert!(s.execution_canary_unresolved_buy_order_id().is_err());
    assert!(s.execution_canary_unresolved_buy().is_err());
    Ok(())
}
