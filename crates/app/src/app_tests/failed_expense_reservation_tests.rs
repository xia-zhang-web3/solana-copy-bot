use super::failed_expense_runtime_tests::{failed_receipt, failure, ledger_count};
use super::receipt_cash_facts_fixture::{money_snapshot, transaction_calls};
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::{
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use serde_json::json;

fn sequence(f: &Fixture) -> Result<i64> {
    Ok(f.conn()?.query_row(
        "SELECT attempt_seq FROM execution_failed_expense_tasks WHERE order_id=?1",
        [&f.order_id],
        |r| r.get(0),
    )?)
}

#[tokio::test]
async fn failed_expense_runtime_reservation_failure_stops_recovery_and_reopens() -> Result<()> {
    for source in ["signature_status", "receipt_meta"] {
        for target in [
            "BEFORE UPDATE OF attempt_seq ON execution_failed_expense_tasks",
            "BEFORE INSERT ON execution_failed_expense_cursor",
        ] {
            for action in [
                "RAISE(ABORT,'synthetic reservation failure')",
                "RAISE(IGNORE)",
            ] {
                let mut f = Fixture::new("sell")?;
                let initial = money_snapshot(&f)?;
                let rpc = Rpc::new(failed_receipt("sell", 5000)).await?;
                rpc.context(format!("failed_expense_runtime_reservation_failure_stops_recovery_and_reopens source={source:?} target={target:?} action={action:?}"));
                if source == "signature_status" {
                    rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
                }
                f.conn()?.execute_batch(&format!(
                    "CREATE TRIGGER reject_reservation {target} BEGIN SELECT {action}; END;"
                ))?;
                assert!(f.reconcile(&rpc, 1).await.is_err());
                // Receipt-meta evidence needs the normal confirmation fetch first;
                // signature detection must never start receipt I/O on failed reserve.
                assert_eq!(
                    transaction_calls(&rpc),
                    usize::from(source == "receipt_meta")
                );
                f.reopen()?;
                assert!(f.store.load_failed_expense_task(&f.order_id)?.is_none());
                assert_eq!(ledger_count(&f)?, 0);
                assert_eq!(money_snapshot(&f)?, initial);
                assert_eq!(
                    f.store
                        .load_execution_canary_order(&f.order_id)?
                        .unwrap()
                        .status,
                    if source == "signature_status" {
                        EXECUTION_STATUS_CANARY_SUBMITTED
                    } else {
                        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
                    }
                );
                f.conn()?.execute_batch("DROP TRIGGER reject_reservation")?;
                assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_failed, 1);
                assert_eq!(sequence(&f)?, 1);
                assert_eq!(ledger_count(&f)?, 1);
                assert_eq!(money_snapshot(&f)?, initial);
                assert!(!rpc
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|m| m == "sendTransaction"));
                rpc.finish().await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_expense_runtime_sweep_reservation_failure_performs_no_receipt_io() -> Result<()> {
    for action in [
        "RAISE(ABORT,'synthetic cursor update failure')",
        "RAISE(IGNORE)",
    ] {
        let mut f = Fixture::new("sell")?;
        let rpc = Rpc::new(json!({"result":null})).await?;
        rpc.context(format!("failed_expense_runtime_sweep_reservation_failure_performs_no_receipt_io action={action:?}"));
        rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
        f.reconcile(&rpc, 1).await?;
        let before = sequence(&f)?;
        let initial = money_snapshot(&f)?;
        rpc.set(failed_receipt("sell", 5000));
        rpc.calls.lock().unwrap().clear();
        f.conn()?.execute_batch(&format!("CREATE TRIGGER reject_reservation BEFORE UPDATE ON execution_failed_expense_cursor BEGIN SELECT {action}; END;"))?;
        assert!(crate::execution_submit_adapter::recover_failed_expenses(
            &config(&rpc.url),
            &f.store,
            f.now
        )
        .await
        .is_err());
        assert!(rpc.calls.lock().unwrap().is_empty());
        f.reopen()?;
        assert_eq!(sequence(&f)?, before);
        assert_eq!(
            f.store
                .load_failed_expense_task(&f.order_id)?
                .unwrap()
                .status,
            "pending"
        );
        assert_eq!(ledger_count(&f)?, 0);
        assert_eq!(money_snapshot(&f)?, initial);
        f.conn()?.execute_batch("DROP TRIGGER reject_reservation")?;
        crate::execution_submit_adapter::recover_failed_expenses(
            &config(&rpc.url),
            &f.store,
            f.now,
        )
        .await?;
        assert_eq!(sequence(&f)?, before + 1);
        assert_eq!(ledger_count(&f)?, 1);
        assert_eq!(money_snapshot(&f)?, initial);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_expense_runtime_complete_and_conflict_do_not_restart_receipt_work() -> Result<()> {
    for conflict in [false, true] {
        let mut f = Fixture::new("sell")?;
        let rpc = Rpc::new(failed_receipt("sell", 5000)).await?;
        rpc.context(format!("failed_expense_runtime_complete_and_conflict_do_not_restart_receipt_work conflict={conflict:?}"));
        rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
        f.reconcile(&rpc, 1).await?;
        if conflict {
            f.store
                .reject_failed_expense(&f.order_id, "synthetic_known_fact_conflict")?;
        }
        let before = sequence(&f)?;
        let initial = money_snapshot(&f)?;
        for _ in 0..2 {
            f.reopen()?;
            rpc.calls.lock().unwrap().clear();
            f.reconcile(&rpc, 1).await?;
            crate::execution_submit_adapter::recover_failed_expenses(
                &config(&rpc.url),
                &f.store,
                f.now,
            )
            .await?;
            assert_eq!(transaction_calls(&rpc), 0);
            assert_eq!(sequence(&f)?, before);
            assert_eq!(ledger_count(&f)?, 1);
            assert_eq!(money_snapshot(&f)?, initial);
            assert!(!rpc
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|m| m == "sendTransaction"));
        }
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_expense_runtime_pending_enrichment_reserves_without_double_expense() -> Result<()> {
    for source in ["signature_status", "receipt_meta"] {
        let mut f = Fixture::new("sell")?;
        let initial = money_snapshot(&f)?;
        let mut partial = failed_receipt("sell", 5000);
        partial["result"]["meta"]
            .as_object_mut()
            .unwrap()
            .remove("preBalances");
        let rpc = Rpc::new(partial).await?;
        rpc.context(format!("failed_expense_runtime_pending_enrichment_reserves_without_double_expense source={source:?}"));
        if source == "signature_status" {
            rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
        }
        f.reconcile(&rpc, 1).await?;
        assert_eq!(sequence(&f)?, 1);
        assert_eq!(ledger_count(&f)?, 1);
        assert_eq!(
            f.store
                .load_failed_transaction_facts(&f.order_id)?
                .unwrap()
                .native_coverage,
            copybot_storage_core::FailedExpenseCoverage::Missing
        );
        f.reopen()?;
        // Terminal failed orders leave normal confirmation; pending enrichment
        // is resumed by the daemon's dedicated recovery sweep.
        crate::execution_submit_adapter::recover_failed_expenses(
            &config(&rpc.url),
            &f.store,
            f.now,
        )
        .await?;
        assert_eq!(sequence(&f)?, 2);
        assert_eq!(ledger_count(&f)?, 1);
        assert_eq!(
            f.store
                .load_failed_expense_task(&f.order_id)?
                .unwrap()
                .status,
            "pending"
        );
        rpc.set(failed_receipt("sell", 5000));
        f.reopen()?;
        crate::execution_submit_adapter::recover_failed_expenses(
            &config(&rpc.url),
            &f.store,
            f.now,
        )
        .await?;
        assert_eq!(sequence(&f)?, 3);
        assert_eq!(
            f.store
                .load_failed_expense_task(&f.order_id)?
                .unwrap()
                .status,
            "complete"
        );
        assert_eq!(ledger_count(&f)?, 1);
        assert_eq!(money_snapshot(&f)?, initial);
        assert!(!rpc
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|m| m == "sendTransaction"));
        rpc.finish().await?;
    }
    Ok(())
}
