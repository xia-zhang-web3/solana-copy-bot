#[path = "common/tiny_budget_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use fixture::*;
use std::sync::{Arc, Barrier};

#[test]
fn tiny_budget_activation_restart_day_config_and_exact_deadline() -> Result<()> {
    let d = Db::new()?;
    let initial = d.store.load_tiny_experiment(d.now)?.unwrap();
    let c = d.candidate("buy", "buy")?;
    d.claim(&c, d.now)?;
    let reopened = SqliteStore::open(&d.path)?;
    assert_eq!(
        reopened
            .activate_tiny_experiment("one", "wallet", d.now + Duration::minutes(20))?
            .activated_at,
        initial.activated_at
    );
    assert!(reopened
        .activate_tiny_experiment("two", "wallet", d.now)
        .is_err());
    assert!(reopened
        .activate_tiny_experiment("one", "other", d.now)
        .is_err());
    assert_eq!(d.totals()?, (1, 100_000, 0));
    assert_eq!(
        reopened
            .load_tiny_experiment(initial.deadline)?
            .unwrap()
            .state,
        "stopped"
    );
    assert_eq!(
        reopened
            .activate_tiny_experiment("one", "wallet", initial.deadline + Duration::days(1))?
            .deadline,
        initial.deadline
    );
    assert_eq!(
        d.claim(&c, initial.deadline)?,
        ExecutionDispatchClaim::Existing
    );
    let n = d.candidate("new", "buy")?;
    assert!(d.claim(&n, initial.deadline).is_err());
    assert!(d
        .store
        .claim_execution_canary_dispatch(&n.0, &n.1, &n.2, d.now)
        .is_err());
    Ok(())
}
#[test]
fn tiny_budget_exact_limits_and_plus_one_are_atomic() -> Result<()> {
    for case in [
        "amount",
        "fee",
        "priority",
        "binding",
        "wallet",
        "id",
        "at_deadline",
        "before_activation",
    ] {
        let d = Db::new()?;
        let mut c = d.candidate("a", "buy")?;
        let mut at = d.now;
        match case {
            "amount" => c.3.buy_lamports = Some(c.3.buy_lamports.unwrap() + 1),
            "fee" => c.3.total_fee += 1,
            "priority" => c.3.priority_fee += 1,
            "binding" => c.3.tx_signature = "other".into(),
            "wallet" => c.3.wallet = "other".into(),
            "id" => c.3.experiment_id = "other".into(),
            "at_deadline" => at += Duration::seconds(3600),
            _ => at -= Duration::nanoseconds(1),
        }
        assert!(d.claim(&c, at).is_err(), "{case}");
        assert_eq!(d.totals()?, (0, 0, 0));
        assert!(d
            .store
            .load_execution_canary_dispatch(&c.0.order_id)?
            .is_none());
    }
    let d = Db::new()?;
    let c = d.candidate("exact", "buy")?;
    assert_eq!(
        d.claim(
            &c,
            d.now + Duration::seconds(3600) - Duration::nanoseconds(1)
        )?,
        ExecutionDispatchClaim::New
    );
    assert_eq!(d.totals()?, (1, 100_000, 0));
    Ok(())
}
#[test]
fn tiny_budget_two_sells_after_buy_refusal_third_stops_open_position() -> Result<()> {
    let d = Db::new()?;
    let a = d.candidate("a", "buy")?;
    d.open_buy(&a, 100_000)?;
    let b = d.candidate("b", "buy")?;
    assert!(d.claim(&b, d.now).is_err());
    for id in ["sell1", "sell2"] {
        let c = d.candidate(id, "sell")?;
        d.claim(&c, d.now)?;
        d.failed(&c, 100_000, d.now)?;
    }
    assert_eq!(d.totals()?, (3, 0, 300_000));
    assert_eq!(
        d.store.load_tiny_experiment(d.now)?.unwrap().state,
        "stopped"
    );
    let c = d.candidate("sell3", "sell")?;
    assert!(d.claim(&c, d.now).is_err());
    assert!(d
        .store
        .load_execution_canary_open_position("mint")?
        .is_some());
    assert_eq!(
        d.conn()?
            .query_row("SELECT COUNT(*) FROM positions", [], |r| r.get::<_, u64>(0))?,
        1
    );
    Ok(())
}
#[test]
fn tiny_budget_competing_connections_cannot_consume_last_sell_slot() -> Result<()> {
    let d = Db::new()?;
    let a = d.candidate("a", "buy")?;
    d.open_buy(&a, 5000)?;
    let one = d.candidate("s1", "sell")?;
    d.claim(&one, d.now)?;
    d.failed(&one, 5000, d.now)?;
    let candidates = [d.candidate("s2", "sell")?, d.candidate("s3", "sell")?];
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for c in candidates {
        let path = d.path.clone();
        let b = barrier.clone();
        let now = d.now;
        threads.push(std::thread::spawn(move || {
            let s = SqliteStore::open(path).unwrap();
            b.wait();
            s.claim_tiny_experiment_dispatch(&c.0, &c.1, &c.2, &c.3, now)
        }));
    }
    barrier.wait();
    let results: Vec<_> = threads.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(ExecutionDispatchClaim::New)))
            .count(),
        1
    );
    assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1);
    assert_eq!(d.totals()?, (3, 100_000, 10_000));
    Ok(())
}
#[test]
fn tiny_budget_receipt_fee_once_after_stop_never_returns_slot_or_cash_proof() -> Result<()> {
    let d = Db::new()?;
    let a = d.candidate("a", "buy")?;
    d.open_buy(&a, 5000)?;
    let c = d.candidate("s1", "sell")?;
    d.claim(&c, d.now)?;
    let late = d.now + Duration::hours(2);
    d.store.load_tiny_experiment(late)?;
    let mut f = d.failure(&c, 7000, late)?;
    f.wallet_native_pre_lamports = None;
    f.wallet_native_post_lamports = None;
    f.native_coverage = FailedExpenseCoverage::Missing;
    for _ in 0..2 {
        SqliteStore::open(&d.path)?.apply_failed_expense(&c.0.order_id, &f, late)?;
    }
    assert_eq!(d.totals()?, (2, 0, 12000));
    assert_eq!(
        d.store
            .load_failed_expense_task(&c.0.order_id)?
            .unwrap()
            .status,
        "pending"
    );
    assert!(d
        .store
        .load_execution_canary_cash_settlement(&c.0.order_id)?
        .is_none());
    assert!(d
        .store
        .load_execution_canary_open_position("mint")?
        .is_some());
    assert!(d.claim(&d.candidate("s2", "sell")?, late).is_err());
    let d = Db::new()?;
    let a = d.candidate("a", "buy")?;
    d.claim(&a, d.now)?;
    let late = d.now + Duration::hours(2);
    d.store.load_tiny_experiment(late)?;
    let f = d.successful(&a, 7000, late)?;
    for _ in 0..2 {
        SqliteStore::open(&d.path)?.record_execution_canary_receipt_facts(&f, late)?;
    }
    assert_eq!(d.totals()?, (1, 0, 7000));
    assert_eq!(
        d.store.load_tiny_experiment(late)?.unwrap().state,
        "stopped"
    );
    Ok(())
}
#[test]
fn tiny_budget_missing_or_mismatched_receipts_hold_reserve() -> Result<()> {
    for case in ["missing_fee", "missing_payer", "payer", "signature"] {
        let d = Db::new()?;
        let c = d.candidate("a", "buy")?;
        d.claim(&c, d.now)?;
        let mut f = d.failure(&c, 5000, d.now)?;
        match case {
            "missing_fee" => {
                f.transaction_fee_lamports = None;
                f.fee_coverage = FailedExpenseCoverage::Missing;
            }
            "missing_payer" => {
                f.payer = None;
                f.payer_coverage = FailedExpenseCoverage::Missing;
            }
            "payer" => f.payer = Some("foreign".into()),
            _ => f.tx_signature = "foreign".into(),
        }
        let _ = d.store.apply_failed_expense(&c.0.order_id, &f, d.now);
        assert_eq!(d.totals()?, (1, 100_000, 0), "{case}");
    }
    Ok(())
}
#[test]
fn tiny_budget_failed_transaction_cannot_credit_deposit_or_overbound_fee() -> Result<()> {
    let d = Db::new()?;
    let c = d.candidate("a", "buy")?;
    d.claim(&c, d.now)?;
    let mut f = d.failure(&c, 100_001, d.now)?;
    f.wallet_native_post_lamports = Some("200000000".into());
    d.store.apply_failed_expense(&c.0.order_id, &f, d.now)?;
    assert_eq!(d.totals()?, (1, 0, 100_001));
    assert_eq!(
        d.store.load_tiny_experiment(d.now)?.unwrap().state,
        "stopped"
    );
    Ok(())
}

#[test]
fn tiny_budget_completed_position_never_rearms_buy() -> Result<()> {
    let d = Db::new()?;
    let a = d.candidate("a", "buy")?;
    d.open_buy(&a, 5000)?;
    d.store.close_execution_canary_open_position(
        "mint",
        7.0,
        Some(copybot_core_types::TokenQuantity::new(7000, 3)),
        0.002,
        1e-12,
        d.now,
    )?;
    assert_eq!(
        d.store.load_tiny_experiment(d.now)?.unwrap().state,
        "completed"
    );
    assert_eq!(
        d.store
            .activate_tiny_experiment("one", "wallet", d.now)?
            .state,
        "completed"
    );
    assert!(d.claim(&d.candidate("new", "buy")?, d.now).is_err());
    assert_eq!(d.totals()?, (1, 0, 5000));
    Ok(())
}

#[test]
fn tiny_budget_samples_deadline_with_write_lock_held() -> Result<()> {
    let d = Db::new()?;
    let c = d.candidate("deadline-lock", "buy")?;
    let competing = d.conn()?;
    competing.busy_timeout(std::time::Duration::ZERO)?;
    let result = d
        .store
        .claim_tiny_experiment_dispatch_with_clock(&c.0, &c.1, &c.2, &c.3, || {
            assert!(
                competing.execute_batch("BEGIN IMMEDIATE").is_err(),
                "decision clock must be sampled under the dispatch write lock"
            );
            Ok(d.now + Duration::seconds(3600))
        });
    assert!(result.is_err());
    assert_eq!(d.totals()?, (0, 0, 0));
    assert!(d
        .store
        .load_execution_canary_dispatch(&c.0.order_id)?
        .is_none());
    assert_eq!(
        d.store.load_execution_canary_order(&c.0.order_id)?.unwrap(),
        c.0
    );
    Ok(())
}
