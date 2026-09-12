#[path = "common/entry_cost_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::FailedExpenseCoverage as Coverage;
use fixture::*;

#[test]
fn entry_cost_exact_cap_and_neighbor_lamports_survive_repeat_and_reopen() -> Result<()> {
    for raw in [19_999_996, 19_999_997, 19_999_998] {
        let mut db = fixed()?;
        closed(&db, "position", Some(-raw), None, "closed", db.now)?;
        complete(&db, ORDER, 3)?;
        let before = snapshot(&db)?;
        for _ in 0..3 {
            db.reopen()?;
            let value = cost(&db)?;
            assert_eq!(
                value.known_total_lamports.as_deref().unwrap(),
                (raw + 3).to_string()
            );
            assert_eq!(value.closed_loss.loss_lamports, raw.to_string());
            assert!(value.closed_loss.exact);
            assert_eq!(
                value.failed_expenses.known_wallet_fee_lamports.as_deref(),
                Some("3")
            );
            assert_eq!(value.check_cap(0.02)?.exhausted, raw >= 19_999_997);
            assert_eq!(value.check_cap(0.02)?.cap_lamports_ceiling, "20000000");
            assert_eq!(snapshot(&db)?, before);
        }
    }
    Ok(())
}

#[test]
fn entry_cost_known_zero_foreign_fee_only_closed_only_and_partial_coverage() -> Result<()> {
    for case in [
        "zero",
        "foreign",
        "fee_only",
        "closed_only",
        "mixed",
        "unknown",
        "known_native_missing",
    ] {
        let db = fixed()?;
        if case == "closed_only" {
            closed(&db, "closed", Some(-20_000_000), None, "closed", db.now)?;
        } else {
            db.detect(ORDER, "signature_status")?;
            let mut facts = db.facts(ORDER, if case == "zero" { 0 } else { 20_000_000 })?;
            if case == "foreign" {
                facts.payer = Some("foreign-payer".into());
            }
            if case == "unknown" {
                facts.payer = None;
                facts.payer_coverage = Coverage::Missing;
            }
            if case == "known_native_missing" {
                facts.wallet_native_pre_lamports = None;
                facts.wallet_native_post_lamports = None;
                facts.native_coverage = Coverage::Missing;
            }
            db.store.apply_failed_expense(ORDER, &facts, db.now)?;
        }
        if case == "mixed" {
            db.add("exec-canary:unknown", "unknown-sig", "buy", db.now)?;
            db.detect("exec-canary:unknown", "signature_status")?;
        }
        let value = cost(&db)?;
        let charged = matches!(
            case,
            "fee_only" | "closed_only" | "mixed" | "known_native_missing"
        );
        assert_eq!(value.check_cap(0.02)?.exhausted, charged, "{case}");
        assert_eq!(
            value.known_total_lamports.as_deref().unwrap(),
            if charged { "20000000" } else { "0" }
        );
        if matches!(case, "mixed" | "unknown" | "known_native_missing") {
            assert!(value.failed_expenses.cohort_wallet_fee_lamports.is_none());
            assert!(!value.selected_cost_complete());
        }
        if matches!(case, "zero" | "foreign") {
            assert_eq!(
                value.failed_expenses.known_wallet_fee_lamports.as_deref(),
                Some("0")
            );
        }
        if case == "unknown" {
            assert_eq!(value.failed_expenses.unknown_orders, 1);
        }
        if case == "known_native_missing" {
            assert_eq!(
                db.store.load_failed_expense_task(ORDER)?.unwrap().status,
                "pending"
            );
        }
    }
    Ok(())
}

#[test]
fn entry_cost_large_u64_fees_u128_total_and_overflow_are_checked() -> Result<()> {
    let db = fixed()?;
    complete(&db, ORDER, u64::MAX)?;
    db.add("exec-canary:large", "large-sig", "sell", db.now)?;
    complete(&db, "exec-canary:large", u64::MAX)?;
    let mut value = cost(&db)?;
    assert_eq!(
        value.known_total_lamports.as_deref().unwrap(),
        (2 * u128::from(u64::MAX)).to_string()
    );
    assert!(value.check_cap(0.02)?.exhausted);
    assert!(!value.check_cap(1e12)?.exhausted);
    assert!(value.check_cap(f64::MAX).is_err());
    value.closed_loss.loss_lamports = u128::MAX.to_string();
    assert!(value.known_total().is_err());
    assert!(value.check_cap(0.02).is_err());
    value.closed_loss.loss_lamports = "00".into();
    assert!(value.known_total().is_err());
    Ok(())
}

#[test]
fn entry_cost_cap_conversion_is_decimal_ceiling_without_epsilon() -> Result<()> {
    let db = fixed()?;
    complete(&db, ORDER, 1)?;
    let value = cost(&db)?;
    for (cap, ceiling, blocked) in [
        (0.0, "0", true),
        (-0.0, "0", true),
        (1e-10, "1", true),
        (1e-9, "1", true),
        (1.000000001e-9, "2", false),
        (5e-324, "1", true),
        (0.02, "20000000", false),
    ] {
        let check = value.check_cap(cap)?;
        assert_eq!(check.cap_lamports_ceiling, ceiling);
        assert_eq!(check.exhausted, blocked);
    }
    for invalid in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(value.check_cap(invalid).is_err());
    }
    Ok(())
}

#[test]
fn entry_cost_legacy_rounding_null_and_exact_precedence_are_explicit() -> Result<()> {
    let db = fixed()?;
    closed(&db, "legacy", None, Some(-0.019999997), "closed", db.now)?;
    closed(&db, "null", None, None, "closed", db.now)?;
    closed(&db, "profit", None, Some(1e30), "closed", db.now)?;
    closed(
        &db,
        "exact-wins",
        Some(1),
        Some(f64::INFINITY),
        "closed",
        db.now,
    )?;
    complete(&db, ORDER, 3)?;
    let value = cost(&db)?;
    assert_eq!(value.known_total_lamports.as_deref().unwrap(), "20000000");
    assert_eq!(value.closed_loss.legacy_f64_positions, 2);
    assert_eq!(value.closed_loss.legacy_null_positions, 1);
    assert_eq!(value.closed_loss.lamport_backed_positions, 1);
    assert!(!value.closed_loss.exact);
    assert!(!value.selected_cost_complete());
    assert!(value.check_cap(0.02)?.exhausted);
    assert!(value.economic_pnl_lamports.is_none());
    Ok(())
}

#[test]
fn entry_cost_legacy_conversion_bound_and_i64_min_do_not_wrap() -> Result<()> {
    for legacy in [Some(-9_007_199.254_740_994), Some(f64::NEG_INFINITY)] {
        let db = fixed()?;
        closed(&db, "bad", None, legacy, "closed", db.now)?;
        assert!(cost(&db).is_err());
    }
    let db = fixed()?;
    closed(&db, "minimum", Some(i64::MIN), None, "closed", db.now)?;
    assert_eq!(
        cost(&db)?.known_total_lamports.as_deref().unwrap(),
        "9223372036854775808"
    );
    Ok(())
}

#[test]
fn entry_cost_concurrent_writer_cannot_mix_closed_loss_and_fee_snapshots() -> Result<()> {
    use std::sync::{Arc, Barrier};
    let db = fixed()?;
    closed(&db, "snapshot-loss", Some(-1), None, "closed", db.now)?;
    complete(&db, ORDER, 2)?;
    let first = serde_json::to_string(&db.facts(ORDER, 2)?)?;
    let second = serde_json::to_string(&db.facts(ORDER, 20)?)?;
    let path = db.path.clone();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = barrier.clone();
    let writer = std::thread::spawn(move || -> Result<()> {
        let mut conn = rusqlite::Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(2))?;
        writer_barrier.wait();
        for n in 0..128 {
            let (loss, fee, facts) = if n % 2 == 0 {
                (10, "20", &second)
            } else {
                (1, "2", &first)
            };
            let tx = conn.transaction()?;
            tx.execute("UPDATE positions SET pnl_lamports=?1", [-loss])?;
            tx.execute(
                "UPDATE execution_failed_expense_facts SET facts_json=?1",
                [facts],
            )?;
            tx.execute("UPDATE execution_failed_expense_ledger SET wallet_fee_lamports=?1,transaction_fee_lamports=?1", [fee])?;
            tx.commit()?;
            std::thread::yield_now();
        }
        Ok(())
    });
    let reader = copybot_storage_core::SqliteStore::open_read_only(&db.path)?;
    barrier.wait();
    let reads = (|| -> Result<()> {
        for _ in 0..256 {
            let value =
                reader.execution_canary_entry_cost(db.now + chrono::Duration::seconds(1))?;
            let pair = (
                value.closed_loss.loss_lamports.as_str(),
                value
                    .failed_expenses
                    .known_wallet_fee_lamports
                    .as_deref()
                    .unwrap(),
            );
            assert!(
                matches!(pair, ("1", "2") | ("10", "20")),
                "mixed snapshot: {pair:?}"
            );
            assert!(matches!(
                value.known_total_lamports.as_deref().unwrap(),
                "3" | "30"
            ));
        }
        Ok(())
    })();
    writer.join().expect("snapshot writer panic")?;
    reads?;
    Ok(())
}
