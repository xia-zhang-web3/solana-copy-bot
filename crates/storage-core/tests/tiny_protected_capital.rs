use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::*;
use std::{
    path::Path,
    sync::{Arc, Barrier},
};
#[path = "common/tiny_budget_fixture.rs"]
mod fixture;
fn fresh() -> Result<fixture::Db> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("capital.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(fixture::Db {
        dir,
        path,
        store,
        now: "2026-09-12T17:00:00Z".parse()?,
    })
}
fn activate(d: &fixture::Db, balance: u64) -> Result<ProtectedNativePolicy> {
    d.store.prepare_tiny_native_policy(
        "one",
        "wallet",
        balance,
        50_000_001,
        42,
        d.now,
        || Ok(d.now),
    )
}
#[test]
fn tiny_capital_once_concurrent_reopen_and_no_legacy_conversion() -> Result<()> {
    let d = fresh()?;
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for b in [102_324_740, 202_324_740] {
        let path = d.path.clone();
        let barrier = barrier.clone();
        let now = d.now;
        threads.push(std::thread::spawn(move || {
            let s = SqliteStore::open(path).unwrap();
            barrier.wait();
            s.prepare_tiny_native_policy("one", "wallet", b, 50_000_001, 42, now, || Ok(now))
        }));
    }
    barrier.wait();
    let a = threads.remove(0).join().unwrap()?;
    let b = threads.remove(0).join().unwrap()?;
    assert_eq!(a, b);
    assert_eq!(a.floor_lamports, a.initial_lamports - 15_000_000);
    let s = SqliteStore::open(&d.path)?;
    assert_eq!(
        s.prepare_tiny_native_policy(
            "one",
            "wallet",
            999999999,
            1,
            999,
            d.now + Duration::seconds(10),
            || Ok(d.now + Duration::seconds(10))
        )?,
        a
    );
    for (id, wallet) in [("two", "wallet"), ("one", "other")] {
        assert!(s
            .prepare_tiny_native_policy(
                id,
                wallet,
                102_324_740,
                50_000_001,
                42,
                d.now,
                || Ok(d.now)
            )
            .is_err());
    }
    assert!(s.activate_tiny_experiment("one", "wallet", d.now).is_err());
    assert!(s.tiny_native_policy("one", "wallet", a.deadline).is_err());
    assert!(s
        .prepare_tiny_native_policy("one", "wallet", 999999999, 1, 999, a.deadline, || Ok(
            a.deadline
        ))
        .is_err());
    let old = fixture::Db::new()?;
    assert!(activate(&old, 102_324_740).is_err());
    assert_eq!(
        old.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_tiny_native_policy",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    Ok(())
}
#[test]
fn tiny_capital_checked_anchor_errors_leave_no_partial_activation() -> Result<()> {
    for (balance, reserve, offset) in [
        (14_999_999, 1, 0),
        (50_000_001, 50_000_001, 0),
        (102_324_740, 0, 0),
        (102_324_740, 1, -1),
        (102_324_740, 1, 31),
    ] {
        let d = fresh()?;
        assert!(d
            .store
            .prepare_tiny_native_policy("one", "wallet", balance, reserve, 42, d.now, || Ok(
                d.now + Duration::seconds(offset)
            ))
            .is_err());
        assert!(d.store.load_tiny_experiment(d.now)?.is_none());
    }
    Ok(())
}
#[test]
fn tiny_capital_claim_is_distinct_bound_and_never_releases_buy_slot() -> Result<()> {
    for case in [
        "valid",
        "decoded",
        "both",
        "missing",
        "wrong-floor",
        "wrong-policy",
        "wrong-request",
        "wrong-signature",
        "plus-one",
    ] {
        let d = fresh()?;
        let policy = activate(&d, 102_324_740)?;
        let mut c = d.candidate("buy", "buy")?;
        c.3.buy_lamports = None;
        c.3.protected_capital = Some(ProtectedCapitalClaim {
            policy: policy.clone(),
            requested_lamports: 10_000_000,
            floor_lamports: 87_324_740,
            request_sha256: "d".repeat(64),
        });
        match case {
            "decoded" => {
                c.3.buy_lamports = Some(10_000_000);
                c.3.protected_capital = None;
            }
            "both" => c.3.buy_lamports = Some(10_000_000),
            "missing" => c.3.protected_capital = None,
            "wrong-floor" => c.3.protected_capital.as_mut().unwrap().floor_lamports -= 1,
            "wrong-policy" => {
                c.3.protected_capital
                    .as_mut()
                    .unwrap()
                    .policy
                    .initial_lamports += 1
            }
            "wrong-request" => {
                c.3.protected_capital.as_mut().unwrap().request_sha256 = "invalid".into()
            }
            "wrong-signature" => c.3.tx_signature = "foreign".into(),
            "plus-one" => c.3.protected_capital.as_mut().unwrap().requested_lamports += 1,
            _ => {}
        }
        let result = d.claim(&c, d.now);
        if case == "valid" {
            assert_eq!(result?, ExecutionDispatchClaim::New);
            assert_eq!(d.claim(&c, d.now)?, ExecutionDispatchClaim::Existing);
            assert_eq!(d.totals()?, (1, 100_000, 0));
            let mut next = d.candidate("second", "buy")?;
            next.3.protected_capital = c.3.protected_capital.clone();
            next.3.buy_lamports = None;
            assert!(d.claim(&next, d.now + Duration::seconds(1)).is_err());
            assert_eq!(d.store.tiny_native_policy("one", "wallet", d.now)?, policy);
            let row: Option<u64> = d.conn()?.query_row(
                "SELECT buy_lamports FROM execution_tiny_reservations",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(row, None);
        } else {
            assert!(result.is_err(), "{case}");
            assert_eq!(d.totals()?, (0, 0, 0));
            assert!(d
                .store
                .load_execution_canary_dispatch(&c.0.order_id)?
                .is_none());
        }
    }
    Ok(())
}
#[test]
fn tiny_capital_upgrade_preserves_0077_active_history() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let old = dir.path().join("migrations");
    std::fs::create_dir(&old)?;
    let migrations = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    for p in std::fs::read_dir(migrations)? {
        let p = p?.path();
        if p.extension().is_some_and(|e| e == "sql")
            && !p
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("0078_")
        {
            std::fs::copy(&p, old.join(p.file_name().unwrap()))?;
        }
    }
    let mut s = SqliteStore::open(dir.path().join("old.db"))?;
    s.run_migrations(&old)?;
    let now: DateTime<Utc> = "2026-09-12T17:00:00Z".parse()?;
    let path = dir.path().join("old.db");
    let mut d = fixture::Db {
        dir,
        path,
        store: s,
        now,
    };
    let c = d.candidate("old-buy", "buy")?;
    d.store
        .claim_execution_canary_dispatch(&c.0, &c.1, &c.2, now)?;
    let e = d.store.activate_tiny_experiment("one", "wallet", now)?;
    // A historical0077 reservation bound to a canonical dispatch, with fee still NULL.
    // The migration must copy its exact values without a backfill or activation.
    d.conn()?.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) VALUES(?1,'one',?2,'wallet','buy',?3,?4,10000000,100000,50000,'42',?5)",
        rusqlite::params![c.2.order_id,c.2.tx_signature,c.2.message_sha256,c.2.transaction_sha256,now.to_rfc3339()])?;
    let history = |conn: &rusqlite::Connection| -> Result<Vec<rusqlite::types::Value>> {
        let mut stmt = conn.prepare("SELECT * FROM execution_tiny_reservations")?;
        let count = stmt.column_count();
        Ok(stmt.query_row([], |row| {
            (0..count)
                .map(|i| row.get(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?)
    };
    let before = history(&d.conn()?)?;
    d.store.run_migrations(migrations)?;
    assert_eq!(history(&d.conn()?)?, before);
    assert_eq!(d.totals()?, (1, 100_000, 0));
    assert_eq!(d.store.load_tiny_experiment(now)?, Some(e));
    assert!(d
        .store
        .prepare_tiny_native_policy("one", "wallet", 102324740, 50000001, 42, now, || Ok(now))
        .is_err());
    Ok(())
}
