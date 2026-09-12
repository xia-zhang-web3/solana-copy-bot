#[path = "common/entry_cost_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::*;
use rusqlite::params;

#[test]
fn entry_cost_broken_identity_duplicate_and_success_never_count_fee() -> Result<()> {
    for case in [
        "duplicate",
        "task_wallet",
        "proof_signature",
        "success",
        "conflict",
    ] {
        let db = fixed()?;
        complete(&db, ORDER, 20_000_000)?;
        match case {
            "duplicate" => db.add("exec-canary:duplicate", "failed-signature", "buy", db.now)?,
            "task_wallet" => {
                db.conn()?.execute(
                    "UPDATE execution_failed_expense_tasks SET wallet='other'",
                    [],
                )?;
            }
            "proof_signature" => {
                db.conn()?.execute("INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,token,side,confirmation_status,slot,confirmed_at,last_attempt_at,reason) VALUES (?1,'wrong',?2,'mint','sell','confirmed','42',?3,?3,'receipt_transaction_failed')", params![ORDER,WALLET,db.now.to_rfc3339()])?;
            }
            "success" => {
                db.conn()?.execute(
                    "INSERT INTO fills(order_id,token,qty) VALUES (?1,'mint',1)",
                    [ORDER],
                )?;
            }
            _ => {
                db.store
                    .reject_failed_expense(ORDER, "independent_conflict")?;
            }
        }
        let before = snapshot(&db)?;
        let result = cost(&db);
        if case == "task_wallet" {
            assert!(result.is_err());
        } else {
            let value = result?;
            assert_eq!(value.failed_expenses.known_orders, 0, "{case}");
            assert!(value.failed_expenses.cohort_wallet_fee_lamports.is_none());
            if case == "conflict" {
                assert!(db.store.execution_canary_accounting_pending()?);
            }
        }
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn entry_cost_bad_facts_ledger_proof_and_order_timestamp_are_errors() -> Result<()> {
    for case in [
        "fee_overflow",
        "fee_noncanonical",
        "ledger",
        "ledger_signature",
        "commitment",
        "failure_proof",
        "timestamp",
    ] {
        let db = fixed()?;
        complete(&db, ORDER, 7)?;
        let mut facts = db.facts(ORDER, 7)?;
        match case {
            "fee_overflow" => facts.transaction_fee_lamports = Some("18446744073709551616".into()),
            "fee_noncanonical" => facts.transaction_fee_lamports = Some("07".into()),
            "ledger" => {
                db.conn()?.execute(
                    "UPDATE execution_failed_expense_ledger SET wallet_fee_lamports='8'",
                    [],
                )?;
            }
            "ledger_signature" => {
                db.conn()?.execute(
                    "UPDATE execution_failed_expense_ledger SET tx_signature='wrong'",
                    [],
                )?;
            }
            "commitment" => facts.commitment = "processed".into(),
            "failure_proof" => {
                facts.transaction_error = serde_json::json!({"InstructionError":[0,{"Custom":8}]})
            }
            _ => {
                db.conn()?
                    .execute("UPDATE orders SET submit_ts='not-a-timestamp'", [])?;
            }
        }
        db.conn()?.execute(
            "UPDATE execution_failed_expense_facts SET facts_json=?1",
            [serde_json::to_string(&facts)?],
        )?;
        let before = snapshot(&db)?;
        assert!(cost(&db).is_err(), "{case}");
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn entry_cost_missing_schema_and_legacy_failure_never_claim_known_zero_coverage() -> Result<()> {
    let db = fixed()?;
    db.conn()?
        .execute("UPDATE orders SET status='execution_canary_failed'", [])?;
    let value = cost(&db)?;
    assert_eq!(value.failed_expenses.legacy_uncovered_orders, 1);
    assert_eq!(value.known_total_lamports.as_deref().unwrap(), "0");
    assert!(!value.check_cap(0.02)?.exhausted);
    assert!(value.failed_expenses.known_wallet_fee_lamports.is_none());
    db.conn()?
        .execute_batch("DROP TABLE execution_failed_expense_tasks")?;
    let value = cost(&db)?;
    assert_eq!(value.failed_expenses.coverage, "schema_unavailable");
    assert!(value.check_cap(0.02).is_err());
    assert!(value.failed_expenses.cohort_wallet_fee_lamports.is_none());
    Ok(())
}

#[test]
fn entry_cost_partial_schema_errors_and_not_sent_never_make_phantom_expenses() -> Result<()> {
    let db = fixed()?;
    db.conn()?.execute("UPDATE orders SET status='execution_canary_simulated',tx_signature=NULL,simulation_error='retry_after_rpc_submit_not_sent:error'", [])?;
    let value = cost(&db)?;
    assert_eq!(value.failed_expenses.total_orders, 0);
    assert_eq!(value.failed_expenses.coverage, "empty_unknown");
    assert!(!value.check_cap(0.02)?.exhausted);
    db.conn()?.execute(
        "UPDATE orders SET status='execution_canary_submitted',tx_signature='failed-signature'",
        [],
    )?;
    complete(&db, ORDER, 1)?;
    db.conn()?
        .execute_batch("DROP TABLE execution_failed_expense_ledger")?;
    assert!(cost(&db).is_err());
    Ok(())
}

#[test]
fn entry_cost_finalized_status_with_confirmed_receipt_preserves_valid_fee() -> Result<()> {
    let db = fixed()?;
    db.store.detect_failed_expense(
        ORDER,
        WALLET,
        "signature_status",
        "finalized",
        Some(42),
        &serde_json::json!({"InstructionError":[0,{"Custom":7}]}),
        db.now,
    )?;
    // Existing RPC parser fetches at confirmed commitment even after finalized status.
    db.store
        .apply_failed_expense(ORDER, &db.facts(ORDER, 7)?, db.now)?;
    assert_eq!(cost(&db)?.known_total_lamports.as_deref().unwrap(), "7");
    assert_eq!(
        db.report(10)?.known_wallet_fee_lamports.as_deref(),
        Some("7")
    );
    Ok(())
}
