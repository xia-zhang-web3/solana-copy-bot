#[path = "common/failed_expense_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;
use serde_json::json;

#[test]
fn failed_expense_dedup_sources_reopen_exact_zero_and_u64_domain() -> Result<()> {
    for fee in [0, 5000, 9_007_199_254_740_993, u64::MAX] {
        let mut db = Db::new()?;
        let task = db.detect(ORDER, "signature_status")?;
        assert_eq!(
            task.last_attempt_at.as_deref(),
            Some(task.detected_at.as_str())
        );
        let facts = db.facts(ORDER, fee)?;
        for source in ["receipt_meta", "signature_status", "receipt_meta"] {
            db.reopen()?;
            db.detect(ORDER, source)?;
            db.store.apply_failed_expense(ORDER, &facts, db.now)?;
            assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
            assert_eq!(db.count("fills")?, 0);
            assert_eq!(db.count("positions")?, 0);
            assert_eq!(
                db.store.load_failed_transaction_facts(ORDER)?,
                Some(facts.clone())
            );
        }
        assert_eq!(
            db.report(1)?.cohort_wallet_fee_lamports,
            Some(fee.to_string())
        );
        let kind: String = db.conn()?.query_row(
            "SELECT typeof(wallet_fee_lamports) FROM execution_failed_expense_ledger",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(kind, "text");
        assert!(db
            .store
            .mark_execution_canary_retry_after_submit_timeout(
                ORDER,
                db.now,
                chrono::Duration::seconds(1),
                "retry"
            )
            .is_err());
    }
    Ok(())
}
#[test]
fn failed_expense_partial_enriches_once_foreign_payer_and_residual_stay_separate() -> Result<()> {
    let db = Db::new()?;
    db.detect(ORDER, "signature_status")?;
    let full = db.facts(ORDER, 5000)?;
    let mut partial = full.clone();
    partial.transaction_fee_lamports = None;
    partial.fee_coverage = FailedExpenseCoverage::Missing;
    db.store.apply_failed_expense(ORDER, &partial, db.now)?;
    assert_eq!(db.report(1)?.unknown_orders, 1);
    assert_eq!(db.count("execution_failed_expense_ledger")?, 0);
    db.store.apply_failed_expense(ORDER, &full, db.now)?;
    db.store.apply_failed_expense(ORDER, &partial, db.now)?;
    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
    assert_eq!(
        db.report(1)?.cohort_wallet_fee_lamports.as_deref(),
        Some("5000")
    );
    let db = Db::new()?;
    db.detect(ORDER, "receipt_meta")?;
    let mut f = db.facts(ORDER, 5000)?;
    f.payer = Some("foreign-payer".into());
    db.store.apply_failed_expense(ORDER, &f, db.now)?;
    let report = db.report(1)?;
    assert_eq!(report.known_wallet_fee_lamports.as_deref(), Some("0"));
    assert_eq!(report.known_native_delta_lamports.as_deref(), Some("-5000"));
    assert_eq!(
        report.known_unexplained_delta_lamports.as_deref(),
        Some("-5000")
    );
    assert_eq!(report.unresolved_orders, 1);
    assert!(report.cohort_wallet_fee_lamports.is_none());
    Ok(())
}
#[test]
fn failed_expense_identity_conflicts_preserve_known_facts_and_guards() -> Result<()> {
    for conflict in ["fee", "wallet", "signature", "slot", "attempt", "duplicate"] {
        let db = Db::new()?;
        db.detect(ORDER, "signature_status")?;
        let original = db.facts(ORDER, 5000)?;
        db.store.apply_failed_expense(ORDER, &original, db.now)?;
        let mut fresh = original.clone();
        match conflict {
            "fee" => fresh.transaction_fee_lamports = Some("5001".into()),
            "wallet" => fresh.wallet = "foreign".into(),
            "signature" => fresh.tx_signature = "foreign".into(),
            "slot" => fresh.slot = 43,
            "attempt" => {
                db.conn()?
                    .execute("UPDATE orders SET attempt=2 WHERE order_id=?1", [ORDER])?;
            }
            _ => {
                db.add("exec-canary:b", "failed-signature", "buy", db.now)?;
                db.detect("exec-canary:b", "receipt_meta")?;
            }
        }
        db.store.apply_failed_expense(ORDER, &fresh, db.now)?;
        assert_eq!(
            db.store.load_failed_expense_task(ORDER)?.unwrap().status,
            "conflict",
            "{conflict}"
        );
        assert_eq!(
            db.store.load_failed_transaction_facts(ORDER)?,
            Some(original)
        );
        assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
        assert_eq!(
            db.report(1)?.unknown_orders,
            if conflict == "duplicate" { 2 } else { 1 }
        );
        assert!(db.store.execution_canary_accounting_pending()?);
        assert!(db.store.execution_canary_token_accounting_pending("mint")?);
        assert!(!db
            .store
            .execution_canary_token_accounting_pending("other-mint")?);
    }
    Ok(())
}
#[test]
fn failed_expense_rejects_invalid_decimals_and_non_proofs_without_writes() -> Result<()> {
    let db = Db::new()?;
    for error in [
        json!("provider timed out"),
        json!({"message":"failed"}),
        json!({"InstructionError":[256,{"Custom":7}]}),
        json!(null),
    ] {
        assert!(db
            .store
            .detect_failed_expense(
                ORDER,
                WALLET,
                "signature_status",
                "confirmed",
                Some(42),
                &error,
                db.now
            )
            .is_err());
    }
    assert_eq!(db.count("execution_failed_expense_tasks")?, 0);
    db.detect(ORDER, "receipt_meta")?;
    for value in ["-1", "01", "1.0", "1e3", "18446744073709551616"] {
        let mut f = db.facts(ORDER, 0)?;
        f.transaction_fee_lamports = Some(value.into());
        assert!(
            db.store.apply_failed_expense(ORDER, &f, db.now).is_err(),
            "{value}"
        );
    }
    assert_eq!(db.count("execution_failed_expense_ledger")?, 0);
    Ok(())
}
#[test]
fn failed_expense_duplicate_signature_before_detection_never_debits() -> Result<()> {
    let db = Db::new()?;
    db.add("exec-canary:b", "failed-signature", "sell", db.now)?;
    assert_eq!(db.detect(ORDER, "signature_status")?.status, "conflict");
    db.store
        .apply_failed_expense(ORDER, &db.facts(ORDER, 5000)?, db.now)?;
    assert_eq!(db.count("execution_failed_expense_ledger")?, 0);
    Ok(())
}
