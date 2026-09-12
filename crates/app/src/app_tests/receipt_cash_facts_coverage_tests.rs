use super::receipt_cash_facts_fixture::*;
use super::receipt_lifecycle_fixture::{lifecycle_receipt, SPL_TOKEN};
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::{
    ReceiptDecomposition, ReceiptFeeCoverage, ReceiptTokenCoverage, ReceiptWsolCoverage,
};
use serde_json::json;

#[tokio::test]
async fn receipt_cash_facts_prefund_external_close_destination_and_wsol_remain_unresolved(
) -> Result<()> {
    for side in ["buy", "sell"] {
        let f = Fixture::new(side)?;
        let mut value = lifecycle_receipt(side, SPL_TOKEN, false, true);
        value["result"]["meta"]["fee"] = json!(5000);
        if side == "sell" {
            value["result"]["meta"]["postBalances"][0] =
                value["result"]["meta"]["preBalances"][0].clone();
            // The existing guard proves closure, not that the refund reached this wallet.
            value["result"]["transaction"]["message"]["instructions"][1]["parsed"]["info"]
                ["destination"] = json!("pool-owner");
        }
        let keys = value["result"]["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap();
        let index = keys.len();
        keys.push(json!({"pubkey":"wrapped-account","signer":false,"writable":true}));
        for name in ["preBalances", "postBalances"] {
            value["result"]["meta"][name]
                .as_array_mut()
                .unwrap()
                .push(json!(2_040_280));
        }
        for name in ["preTokenBalances", "postTokenBalances"] {
            value["result"]["meta"][name].as_array_mut().unwrap().push(json!({
                "accountIndex":index,"owner":WALLET,"mint":"So11111111111111111111111111111111111111112",
                "programId":SPL_TOKEN,"uiTokenAmount":{"amount":"1000","decimals":9}}));
        }
        let rpc = Rpc::new(value).await?;
        rpc.context(format!("receipt_cash_facts_prefund_external_close_destination_and_wsol_remain_unresolved side={side:?}"));
        let out = f.reconcile(&rpc, 1).await?;
        let observed = facts(&f)?;
        assert_eq!(
            observed.token_coverage,
            ReceiptTokenCoverage::ProvenLifecycle
        );
        assert_eq!(observed.wsol_coverage, ReceiptWsolCoverage::Observed);
        assert_eq!(observed.decomposition, ReceiptDecomposition::Unresolved);
        assert_eq!(
            observed.wallet_native_delta.as_i128(),
            if side == "sell" { 0 } else { -900_000_000 }
        );
        assert_eq!(
            observed.token_delta.unwrap().raw,
            if side == "sell" { -10_000 } else { 7000 }
        );
        assert_eq!(out.confirmation_confirmed, 1);
        if side == "sell" {
            let cash = f
                .store
                .load_execution_canary_cash_settlement(&f.order_id)?
                .unwrap();
            assert_eq!(cash.wallet_native_cash_delta.as_i128(), 0);
            assert_eq!(cash.cash_result_delta.as_i128(), -800_000_000);
            assert!(cash.swap_price.is_none());
            assert_eq!(cash.decomposition, ReceiptDecomposition::Unresolved);
        }
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_partial_token_and_fee_enrich_without_releasing_pending_early(
) -> Result<()> {
    for case in ["missing_array", "unproven_lifecycle"] {
        let mut f = Fixture::new("buy")?;
        let mut complete = lifecycle_receipt("buy", SPL_TOKEN, false, true);
        complete["result"]["meta"]["fee"] = json!(5000);
        let mut partial = complete.clone();
        partial["result"]["meta"]
            .as_object_mut()
            .unwrap()
            .remove("fee");
        if case == "missing_array" {
            partial["result"]["meta"]["preTokenBalances"] = json!(null);
        } else {
            // Missing evidence can enrich; an observed empty instruction list cannot
            // later become a different known transaction.
            partial["result"]["transaction"]["message"]["instructions"] = json!(null);
        }
        let rpc = Rpc::new(partial).await?;
        rpc.context(format!("receipt_cash_facts_partial_token_and_fee_enrich_without_releasing_pending_early case={case:?}"));
        assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_pending, 1);
        let first = facts(&f)?;
        assert_eq!(first.wallet_native_delta.as_i128(), -900_000_000);
        assert_eq!(first.token_delta, None);
        assert_eq!(first.token_coverage, ReceiptTokenCoverage::Unresolved);
        assert_eq!(
            first.token_coverage_reason.as_deref(),
            Some(if case == "missing_array" {
                "receipt_token_balances_missing"
            } else {
                "receipt_token_creation_unproven"
            })
        );
        assert_eq!(first.transaction_fee, None);
        assert_eq!(first.fee_coverage, ReceiptFeeCoverage::Missing);
        assert_pending(&f)?;
        f.reopen()?;
        assert_eq!(facts(&f)?, first);
        rpc.set(complete);
        assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_confirmed, 1);
        f.reopen()?;
        let enriched = facts(&f)?;
        assert_eq!(enriched.token_delta.unwrap().raw, 7000);
        assert_eq!(
            enriched.token_coverage,
            ReceiptTokenCoverage::ProvenLifecycle
        );
        assert_eq!(enriched.token_coverage_reason, None);
        assert_eq!(enriched.transaction_fee.unwrap().as_u64(), 5000);
        assert_eq!(enriched.wallet_native_pre, first.wallet_native_pre);
        assert_eq!(enriched.wallet_native_post, first.wallet_native_post);
        assert_eq!(enriched.decomposition, ReceiptDecomposition::Unresolved);
        f.reconcile(&rpc, 3).await?;
        assert_eq!(f.fills()?, 1);
        assert_eq!(transaction_calls(&rpc), 2);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_rejects_invalid_identity_native_and_execution_evidence() -> Result<()> {
    for case in [
        "signature",
        "slot",
        "wallet",
        "mint",
        "owner",
        "missing_err",
        "invalid_err",
        "invalid_native",
        "overflow_native",
        "failed",
    ] {
        let f = Fixture::new("sell")?;
        let initial = money_snapshot(&f)?;
        let mut value = receipt("sell", 1_000_000);
        match case {
            "signature" => {
                value["result"]["transaction"]["signatures"][0] = json!("other-signature")
            }
            "slot" => value["result"]["slot"] = json!(43),
            "wallet" => {
                value["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
                    json!("other-wallet")
            }
            "mint" | "owner" => {
                for name in ["preTokenBalances", "postTokenBalances"] {
                    value["result"]["meta"][name][0]
                        [if case == "mint" { "mint" } else { "owner" }] = json!("other");
                }
            }
            "missing_err" => {
                value["result"]["meta"]
                    .as_object_mut()
                    .unwrap()
                    .remove("err");
            }
            "invalid_err" => value["result"]["meta"]["err"] = json!({}),
            "invalid_native" => value["result"]["meta"]["preBalances"][0] = json!(1.5),
            "overflow_native" => {
                value["result"]["meta"]["preBalances"][0] =
                    serde_json::from_str("18446744073709551616")?
            }
            "failed" => {
                value["result"]["meta"]["err"] = json!({"InstructionError":[0,{"Custom":7}]})
            }
            _ => unreachable!(),
        }
        let rpc = Rpc::new(value).await?;
        rpc.context(format!("receipt_cash_facts_rejects_invalid_identity_native_and_execution_evidence case={case:?}"));
        let out = f.reconcile(&rpc, 1).await?;
        assert!(
            f.store
                .load_execution_canary_receipt_facts(&f.order_id)?
                .is_none(),
            "{case}"
        );
        assert_eq!(money_snapshot(&f)?, initial, "{case}");
        if case == "failed" {
            assert_eq!(out.confirmation_failed, 1);
        } else {
            assert_pending(&f)?;
        }
        rpc.finish().await?;
    }
    // The pre-existing signature-status failure path never produces successful facts.
    let f = Fixture::new("sell")?;
    let rpc = Rpc::new(receipt("sell", 1_000_000)).await?;
    rpc.context(format!(
        "receipt_cash_facts_rejects_invalid_identity_native_and_execution_evidence"
    ));
    *rpc.status.lock().unwrap() = json!({"result":{"value":[{"err":{"InstructionError":[0,{"Custom":7}]},"slot":42,"confirmationStatus":"confirmed"}]}});
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_failed, 1);
    assert_eq!(transaction_calls(&rpc), 1);
    // Contradictory success receipt is fetched, retained as conflict, never a second accounting path.
    assert_eq!(
        f.store
            .load_failed_expense_task(&f.order_id)?
            .unwrap()
            .status,
        "conflict"
    );
    assert!(f.store.execution_canary_accounting_pending()?);
    assert!(f
        .store
        .load_execution_canary_receipt_facts(&f.order_id)?
        .is_none());
    assert_eq!(f.fills()?, 0);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_later_failed_receipt_cannot_erase_known_success_or_release_risk(
) -> Result<()> {
    let mut f = Fixture::new("sell")?;
    let initial = money_snapshot(&f)?;
    let mut value = receipt("sell", 0);
    let rpc = Rpc::new(value.clone()).await?;
    rpc.context(format!(
        "receipt_cash_facts_later_failed_receipt_cannot_erase_known_success_or_release_risk"
    ));
    stop_accounting(&f)?;
    let out = f.reconcile(&rpc, 1).await?;
    assert_eq!(
        (out.confirmation_pending, out.confirmation_confirmed),
        (1, 0)
    );
    assert_eq!(
        out.error.as_deref(),
        Some("receipt_accounting_write_failed")
    );
    f.conn()?.execute_batch("DROP TRIGGER stop_accounting")?;
    let original = facts(&f)?;
    value["result"]["meta"]["err"] = json!({"InstructionError":[0,{"Custom":7}]});
    rpc.set(value);
    f.reopen()?;
    let out = f.reconcile(&rpc, 2).await?;
    assert_eq!(out.confirmation_pending, 1);
    assert_eq!(out.confirmation_failed, 0);
    assert_eq!(
        out.reason.as_deref(),
        Some("receipt_facts_execution_conflict")
    );
    assert_eq!(facts(&f)?, original);
    assert_pending(&f)?;
    assert_eq!(money_snapshot(&f)?, initial);
    rpc.finish().await?;
    Ok(())
}
