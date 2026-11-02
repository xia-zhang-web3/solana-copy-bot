#[path = "common/local_session_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;
#[cfg(feature = "local-bounded-session")]
#[test]
fn local_session_identity_reopen_never_converts_or_renews_once_anchor() -> Result<()> {
    let f = F::new()?;
    let anchor: String = f.conn()?.query_row(
        "SELECT activated_at||deadline||state FROM execution_tiny_experiment",
        [],
        |r| r.get(0),
    )?;
    let reopened = SqliteStore::open(&f.path)?;
    reopened.register_local_bounded_session(&f.authority)?;
    let mut other = f.authority.clone();
    other.cohort.deadline += chrono::Duration::seconds(1);
    assert!(reopened.register_local_bounded_session(&other).is_err());
    assert!(reopened
        .activate_tiny_experiment("model-session", BOT, f.now)
        .is_err());
    assert!(reopened
        .prepare_tiny_native_policy("model-session", BOT, 200000000, 0, 43, f.now, || Ok(f.now))
        .is_err());
    assert_eq!(
        f.conn()?.query_row(
            "SELECT activated_at||deadline||state FROM execution_tiny_experiment",
            [],
            |r| r.get::<_, String>(0)
        )?,
        anchor
    );
    let old = F::blank()?;
    old.store.activate_tiny_experiment("legacy", BOT, old.now)?;
    assert!(old
        .store
        .register_local_bounded_session(&old.authority)
        .is_err());
    assert_eq!(old.count("local_bounded_sessions")?, 0);
    Ok(())
}
#[cfg(feature = "local-bounded-session")]
#[test]
fn local_session_unknown_and_accounting_pending_block_next_cycle_and_receipt_is_idempotent(
) -> Result<()> {
    let f = F::new()?;
    let a = f.candidate(0)?;
    assert_eq!(f.claim(&a, f.now)?, ExecutionDispatchClaim::New);
    let b = f.candidate(1)?;
    assert!(f.claim(&b, f.now).is_err());
    let reopened = SqliteStore::open(&f.path)?;
    reopened.register_local_bounded_session(&f.authority)?;
    assert!(reopened
        .claim_tiny_experiment_dispatch(&b.0, &b.1, &b.2, &b.3, f.now)
        .is_err());
    assert_eq!(
        f.conn()?.query_row(
            "SELECT count(*) FROM execution_tiny_reservations WHERE actual_fee IS NULL",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        1
    );
    let facts = f.facts(&a, 5000)?;
    f.store
        .record_execution_canary_receipt_facts(&facts, f.now)?;
    f.store.refresh_local_bounded_session(f.now)?;
    assert_eq!(
        f.conn()?.query_row(
            "SELECT sum(actual_fee) FROM execution_tiny_reservations",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        5000
    );
    assert!(f.claim(&b, f.now).is_err());
    assert_eq!(f.count("local_bounded_orders")?, 1);
    assert_eq!(
        f.conn()?.query_row(
            "SELECT count(*) FROM local_bounded_cycles WHERE state='closed'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    Ok(())
}
#[cfg(feature = "local-bounded-session")]
#[test]
fn local_session_foreign_expired_amount_fee_and_aggregate_observation_refuse_before_claim(
) -> Result<()> {
    for mode in ["identity", "expiry", "amount", "fee", "priority"] {
        let f = F::new()?;
        let mut c = f.candidate(0)?;
        let mut at = f.now;
        match mode {
            "identity" => c.3.experiment_id = "foreign".into(),
            "expiry" => at = f.authority.cohort.deadline,
            "amount" => c.3.protected_capital.as_mut().unwrap().requested_lamports += 1,
            "fee" => c.3.total_fee = 100001,
            _ => c.3.priority_fee = 22001,
        }
        assert!(f.claim(&c, at).is_err(), "{mode}");
        assert_eq!(f.count("execution_canary_dispatch")?, 0);
        assert_eq!(f.count("local_bounded_orders")?, 0);
    }
    let f = F::new()?;
    let c = f.candidate(0)?;
    let next = f.candidate(1)?;
    f.claim(&c, f.now)?;
    f.facts(&c, 400001)?;
    assert_eq!(
        f.conn()?.query_row(
            "SELECT sum(actual_fee) FROM execution_tiny_reservations",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        400001
    );
    assert_eq!(f.count("local_bounded_stops")?, 1);
    assert!(f.claim(&next, f.now).is_err());
    Ok(())
}
#[cfg(feature = "local-bounded-session")]
#[test]
fn local_session_two_actual_claim_contenders_get_one_dispatch() -> Result<()> {
    let f = F::new()?;
    let c = f.candidate(0)?;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut threads = vec![];
    for i in 0..2 {
        let path = f.path.clone();
        let mut c = c.clone();
        let b = barrier.clone();
        let now = f.now;
        c.2.tx_signature = format!("contender-{i}");
        c.3.tx_signature = c.2.tx_signature.clone();
        threads.push(std::thread::spawn(move || {
            let s = SqliteStore::open(path).unwrap();
            b.wait();
            s.claim_tiny_experiment_dispatch(&c.0, &c.1, &c.2, &c.3, now)
        }));
    }
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(ExecutionDispatchClaim::New)))
            .count(),
        1
    );
    assert_eq!(f.count("execution_canary_dispatch")?, 1);
    assert_eq!(f.count("execution_tiny_reservations")?, 1);
    Ok(())
}
#[cfg(feature = "local-bounded-session")]
#[test]
fn local_session_closed_history_plus_buy_and_mandatory_exit_reserve_exact_cap() -> Result<()> {
    for (historical_fees, admitted) in [(200000_u64, true), (200001_u64, false)] {
        let f = F::new()?;
        let old = f.candidate(0)?;
        let mut next = f.candidate(1)?;
        f.claim(&old, f.now)?;
        // Isolated reserve-guard fixture: a closed historical expense aggregate.
        // No live state uses SQL setup; the native LOCAL run proves actual closure.
        let c = f.conn()?;
        c.execute("INSERT INTO fills(order_id,token,qty,avg_price,fee) VALUES(?1,?2,0,0,0)", rusqlite::params![old.0.order_id, OWNER_EXIT_MINT])?;
        c.execute("UPDATE execution_tiny_reservations SET actual_fee=?1,outcome='successful',reconciled_at=?3 WHERE order_id=?2", rusqlite::params![historical_fees,old.0.order_id,f.now.to_rfc3339()])?;
        c.execute("UPDATE local_bounded_cycles SET state='closed',closed_at=?1 WHERE cycle_no=1", [f.now.to_rfc3339()])?;
        next.3.total_fee = 100000;
        let before = f.count("execution_canary_dispatch")?;
        let result = f.claim(&next, f.now);
        assert_eq!(result.is_ok(), admitted);
        if !admitted {
            assert!(format!("{:#}", result.unwrap_err()).contains("local_session_buy_cap_or_exit_reserve"));
        }
        assert_eq!(f.count("execution_canary_dispatch")?, before + u64::from(admitted));
        assert_eq!(f.count("execution_tiny_reservations")?, 1 + u64::from(admitted));
        assert_eq!(c.query_row("SELECT actual_fee FROM execution_tiny_reservations WHERE order_id=?1", [&old.0.order_id], |r|r.get::<_,u64>(0))?,historical_fees);
        assert_eq!(f.count("local_bounded_stops")?, 0);
    }
    Ok(())
}
#[cfg(not(feature = "local-bounded-session"))]
#[test]
fn local_session_default_build_cannot_register_authority() -> Result<()> {
    let f = F::blank()?;
    assert!(f
        .store
        .register_local_bounded_session(&f.authority)
        .is_err());
    assert_eq!(f.count("local_bounded_sessions")?, 0);
    Ok(())
}
