use super::receipt_cash_facts_fixture::money_snapshot;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn failed_expense_both_runtime_sources_record_exact_fee_without_fill() -> Result<()> {
    let mut missing = Vec::new();
    for source in ["signature_status", "receipt_meta"] {
        let f = Fixture::new("sell")?;
        let initial = money_snapshot(&f)?;
        let mut value = receipt("sell", -5000);
        value["result"]["meta"]["err"] = json!({"InstructionError":[0,{"Custom":7}]});
        value["result"]["meta"]["fee"] = json!(5000);
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "failed_expense_both_runtime_sources_record_exact_fee_without_fill source={source:?}"
        ));
        if source == "signature_status" {
            rpc.status.lock().unwrap()["result"]["value"][0]["err"] =
                json!({"InstructionError":[0,{"Custom":7}]});
        }
        let out = f.reconcile(&rpc, 1).await?;
        rpc.finish().await?;
        assert_eq!(out.confirmation_failed, 1);
        assert_eq!(money_snapshot(&f)?, initial);
        let exists: bool = f.conn()?.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='execution_failed_expense_ledger')", [], |r|r.get(0))?;
        if !exists {
            missing.push(source);
            continue;
        }
        let fee: String = f.conn()?.query_row(
            "SELECT wallet_fee_lamports FROM execution_failed_expense_ledger WHERE tx_signature=?1",
            [SIGNATURE],
            |r| r.get(0),
        )?;
        assert_eq!(fee, "5000");
        assert_eq!(f.fills()?, 0);
    }
    assert!(
        missing.is_empty(),
        "failed expenses missing for runtime sources: {missing:?}"
    );
    Ok(())
}

#[tokio::test]
async fn failed_expense_terminal_order_late_receipt_recovers_after_restart_without_resubmit(
) -> Result<()> {
    let mut f = Fixture::new("buy")?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!(
        "failed_expense_terminal_order_late_receipt_recovers_after_restart_without_resubmit"
    ));
    rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_failed, 1);
    assert_eq!(
        f.store
            .load_failed_expense_task(&f.order_id)?
            .unwrap()
            .status,
        "pending"
    );
    let terminal = f.store.load_execution_canary_order(&f.order_id)?.unwrap();
    assert_eq!(
        terminal.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(terminal.tx_signature.as_deref(), Some(SIGNATURE));
    // 0064 retains signed unresolved FAILED until both task and ledger complete.
    assert_eq!(
        f.store
            .list_reconcilable_execution_canary_orders_for_route(ROUTE, "retry", 10,)?,
        vec![terminal.clone()]
    );
    let initial = money_snapshot(&f)?;
    for _ in 0..2 {
        f.reopen()?;
        assert_eq!(
            f.store
                .list_reconcilable_execution_canary_orders_for_route(ROUTE, "retry", 10,)?,
            vec![terminal.clone()]
        );
        assert_eq!(ledger_count(&f)?, 0);
        crate::execution_submit_adapter::recover_failed_expenses(
            &config(&rpc.url),
            &f.store,
            f.now,
        )
        .await?;
    }
    rpc.set(failed_receipt("buy", 5000));
    f.reopen()?;
    assert_eq!(
        f.store.load_execution_canary_order(&f.order_id)?.unwrap(),
        terminal
    );
    for _ in 0..3 {
        crate::execution_submit_adapter::recover_failed_expenses(
            &config(&rpc.url),
            &f.store,
            f.now,
        )
        .await?;
        f.reopen()?;
    }
    assert_eq!(
        f.store
            .load_failed_expense_task(&f.order_id)?
            .unwrap()
            .status,
        "complete"
    );
    assert_eq!(ledger_count(&f)?, 1);
    assert_eq!(
        f.store.load_execution_canary_order(&f.order_id)?.unwrap(),
        terminal
    );
    assert!(f
        .store
        .list_reconcilable_execution_canary_orders_for_route(ROUTE, "retry", 10,)?
        .is_empty());
    let ledger: (String, String, String) = f.conn()?.query_row(
        "SELECT order_id, tx_signature, wallet_fee_lamports FROM execution_failed_expense_ledger",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(
        ledger,
        (f.order_id.clone(), SIGNATURE.into(), "5000".into())
    );
    assert_eq!(money_snapshot(&f)?, initial);
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|v| v == "sendTransaction"));
    rpc.finish().await?;
    Ok(())
}
#[tokio::test]
async fn failed_expense_processed_provider_and_pre_submit_errors_are_not_network_costs(
) -> Result<()> {
    for kind in [
        "processed",
        "rpc_error",
        "arbitrary_status",
        "arbitrary_receipt",
        "presubmit",
    ] {
        let f = Fixture::new("buy")?;
        let rpc = Rpc::new(failed_receipt("buy", 5000)).await?;
        rpc.context(format!("failed_expense_processed_provider_and_pre_submit_errors_are_not_network_costs kind={kind:?}"));
        match kind {
            "processed" => {
                let mut s = rpc.status.lock().unwrap();
                s["result"]["value"][0]["err"] = failure();
                s["result"]["value"][0]["confirmationStatus"] = json!("processed");
            }
            "rpc_error" => {
                *rpc.status.lock().unwrap() =
                    json!({"error":{"message":"transaction_error provider down"}})
            }
            "arbitrary_status" => {
                rpc.status.lock().unwrap()["result"]["value"][0]["err"] =
                    json!("provider transaction_error")
            }
            "arbitrary_receipt" => {
                let mut v = failed_receipt("buy", 5000);
                v["result"]["meta"]["err"] = json!("provider error");
                rpc.set(v);
            }
            _ => {
                f.conn()?.execute(
                    "UPDATE orders SET status='execution_canary_simulated',tx_signature=NULL",
                    [],
                )?;
            }
        }
        let result = f.reconcile(&rpc, 1).await;
        if kind != "presubmit" {
            assert_eq!(result?.confirmation_pending, 1, "{kind}");
        }
        assert_eq!(ledger_count(&f)?, 0);
        assert!(f.store.load_failed_expense_task(&f.order_id)?.is_none());
        assert_eq!(f.fills()?, 0);
        rpc.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn failed_expense_success_is_not_debited_again_and_contradictions_remain_unresolved(
) -> Result<()> {
    let f = Fixture::new("buy")?;
    let mut success = receipt("buy", -800_000_000);
    success["result"]["meta"]["fee"] = json!(5000);
    let rpc = Rpc::new(success).await?;
    rpc.context(format!(
        "failed_expense_success_is_not_debited_again_and_contradictions_remain_unresolved"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
    let before = money_snapshot(&f)?;
    f.store.detect_failed_expense(
        &f.order_id,
        WALLET,
        "signature_status",
        "confirmed",
        Some(42),
        &failure(),
        f.now,
    )?;
    assert_eq!(
        f.store
            .load_failed_expense_task(&f.order_id)?
            .unwrap()
            .status,
        "conflict"
    );
    assert_eq!(ledger_count(&f)?, 0);
    assert_eq!(money_snapshot(&f)?, before);
    assert_eq!(f.fills()?, 1);
    let mut f = Fixture::new("sell")?;
    rpc.finish().await?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!(
        "failed_expense_success_is_not_debited_again_and_contradictions_remain_unresolved"
    ));
    rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
    f.reconcile(&rpc, 1).await?;
    rpc.set(receipt("sell", 10_000));
    f.reopen()?;
    crate::execution_submit_adapter::recover_failed_expenses(&config(&rpc.url), &f.store, f.now)
        .await?;
    assert_eq!(
        f.store
            .load_failed_expense_task(&f.order_id)?
            .unwrap()
            .status,
        "conflict"
    );
    assert!(f.store.execution_canary_accounting_pending()?);
    assert!(f.store.execution_canary_token_accounting_pending(TOKEN)?);
    assert_eq!(f.fills()?, 0);
    assert_eq!(ledger_count(&f)?, 0);
    rpc.finish().await?;
    Ok(())
}
pub(super) fn failure() -> serde_json::Value {
    json!({"InstructionError":[0,{"Custom":7}]})
}
pub(super) fn failed_receipt(side: &str, fee: u64) -> serde_json::Value {
    let mut v = receipt(side, 0);
    v["result"]["meta"]["err"] = failure();
    v["result"]["meta"]["fee"] = json!(fee);
    v["result"]["meta"]["preBalances"][0] = json!(fee);
    v["result"]["meta"]["postBalances"][0] = json!(0);
    v
}
pub(super) fn ledger_count(f: &Fixture) -> Result<u64> {
    Ok(f.conn()?.query_row(
        "SELECT COUNT(*) FROM execution_failed_expense_ledger",
        [],
        |r| r.get(0),
    )?)
}
