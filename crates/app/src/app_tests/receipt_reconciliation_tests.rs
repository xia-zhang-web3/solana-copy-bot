use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn receipt_missing_never_books_quote() -> Result<()> {
    let fixture = Fixture::new("buy")?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!("receipt_missing_never_books_quote"));
    fixture.reconcile(&rpc, 10).await?;
    assert_eq!(
        fixture.fills()?,
        0,
        "missing receipt must not book the quote"
    );
    assert_eq!(fixture.store.execution_canary_open_position_count()?, 0);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_negative_sell_never_books_quote_price() -> Result<()> {
    let fixture = Fixture::new("sell")?;
    let rpc = Rpc::new(receipt("sell", -400_000)).await?;
    rpc.context(format!("receipt_negative_sell_never_books_quote_price"));
    fixture.reconcile(&rpc, 10).await?;
    assert_eq!(fixture.fills()?, 1);
    let cash = fixture
        .store
        .load_execution_canary_cash_settlement(&fixture.order_id)?
        .unwrap();
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), -400_000);
    assert!(cash.swap_price.is_none());
    assert_eq!(cash.remaining_quantity.raw(), 3000);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_unavailable_reasons_are_durable_and_proof_survives_rpc_regression() -> Result<()> {
    let mut f = Fixture::new("buy")?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!(
        "receipt_unavailable_reasons_are_durable_and_proof_survives_rpc_regression"
    ));
    let cases = [
        (200, r#"{"result":null}"#, 0, "receipt_not_available"),
        (503, "unavailable", 0, "receipt_rpc_http_error"),
        (200, r#"{"error":{"code":-32000}}"#, 0, "receipt_rpc_error"),
        (200, "{", 0, "receipt_rpc_json_invalid"),
        (200, r#"{"result":{}}"#, 0, "receipt_signatures_missing"),
        (200, r#"{"result":null}"#, 200, "receipt_rpc_timeout"),
    ];
    for (code, body, delay, reason) in cases {
        *rpc.response.lock().unwrap() = (code, body.into(), delay);
        rpc.context(format!("unavailable case={reason} side=buy"));
        let out = if reason == "receipt_rpc_timeout" {
            rpc.expect_receipt_cancellation();
            f.reconcile_with_timeout(&rpc, 1_000, 80).await?
        } else {
            f.reconcile(&rpc, 1_000).await?
        };
        assert_eq!(out.confirmation_pending, 1);
        assert_eq!(out.confirmation_confirmed, 0);
        assert_eq!(out.reason.as_deref(), Some(reason));
        f.reopen()?;
        let proof = f
            .store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap();
        assert_eq!(proof.reason, reason);
        assert_eq!(proof.confirmation_status, "confirmed");
        assert_eq!(proof.slot, Some(42));
        assert_eq!(proof.tx_signature, SIGNATURE);
        assert_eq!(f.fills()?, 0);
        assert_eq!(f.store.execution_canary_open_position_count()?, 0);
        *rpc.status.lock().unwrap() = json!({"error":{"code":-32001}});
    }
    rpc.finish().await?;
    assert_eq!(rpc.cancellations(), 1);
    let calls = rpc.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|m| *m == "getSignatureStatuses")
            .count(),
        1
    );
    assert_eq!(
        calls.iter().filter(|m| *m == "getTransaction").count(),
        cases.len()
    );
    assert!(!calls.iter().any(|m| m == "sendTransaction"));
    Ok(())
}

#[tokio::test]
async fn receipt_invalid_identity_balances_and_precision_never_create_fill() -> Result<()> {
    let cases = [
        ("/result/transaction/signatures/0", json!("wrong-signature")),
        ("/result/slot", json!(99)),
        (
            "/result/transaction/message/accountKeys/0/pubkey",
            json!("wrong-wallet"),
        ),
        (
            "/result/transaction/message/accountKeys/0/signer",
            json!(false),
        ),
        ("/result/meta/preBalances", json!(null)),
        ("/result/meta/postBalances", json!([])),
        ("/result/meta/preBalances/0", json!(-1)),
        ("/result/meta/postBalances/0", json!(2_000_000_000_u64)),
        ("/result/meta/preTokenBalances", json!(null)),
        ("/result/meta/postTokenBalances", json!(null)),
        ("/result/meta/postTokenBalances/0/accountIndex", json!(99)),
        (
            "/result/meta/postTokenBalances/0/owner",
            json!("wrong-owner"),
        ),
        ("/result/meta/postTokenBalances/0/mint", json!("wrong-mint")),
        (
            "/result/meta/postTokenBalances/0/uiTokenAmount/amount",
            json!("-7"),
        ),
        (
            "/result/meta/postTokenBalances/0/uiTokenAmount/amount",
            json!("7.0"),
        ),
        (
            "/result/meta/postTokenBalances/0/uiTokenAmount/amount",
            json!("18446744073709551616"),
        ),
        (
            "/result/meta/postTokenBalances/0/uiTokenAmount/amount",
            json!(null),
        ),
        (
            "/result/meta/postTokenBalances/0/uiTokenAmount/decimals",
            json!(2),
        ),
        (
            "/result/meta/postTokenBalances/0/uiTokenAmount/decimals",
            json!(256),
        ),
    ];
    for (pointer, bad) in cases {
        let f = Fixture::new("buy")?;
        let mut value = receipt("buy", -900_000_000);
        *value.pointer_mut(pointer).unwrap() = bad;
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "receipt_invalid_identity_balances_and_precision_never_create_fill pointer={pointer:?}"
        ));
        let out = f.reconcile(&rpc, 10).await?;
        assert_eq!(out.confirmation_confirmed, 0, "{pointer}");
        assert_eq!(out.confirmation_pending, 1, "{pointer}");
        assert_eq!(f.fills()?, 0, "{pointer}");
        assert_eq!(f.store.execution_canary_open_position_count()?, 0);
        assert!(f.store.execution_canary_accounting_pending()?);
        rpc.finish().await?;
    }
    // Missing fields differ from explicit null/zero and must not imply successful execution.
    for field in [
        "err",
        "preTokenBalances",
        "postTokenBalances",
        "preBalances",
        "postBalances",
    ] {
        let f = Fixture::new("buy")?;
        let mut value = receipt("buy", -900_000_000);
        value["result"]["meta"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "receipt_invalid_identity_balances_and_precision_never_create_fill field={field:?}"
        ));
        assert_eq!(
            f.reconcile(&rpc, 10).await?.confirmation_pending,
            1,
            "{field}"
        );
        assert_eq!(f.fills()?, 0);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_unsupported_signs_and_duplicate_rows_remain_pending() -> Result<()> {
    for (side, net) in [("buy", 0), ("buy", 100)] {
        let f = Fixture::new(side)?;
        let rpc = Rpc::new(receipt(side, net)).await?;
        rpc.context(format!(
            "receipt_unsupported_signs_and_duplicate_rows_remain_pending side={side:?} net={net:?}"
        ));
        assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_pending, 1);
        assert_eq!(f.fills()?, 0);
        rpc.finish().await?;
    }
    for (side, value) in [("buy", receipt("sell", -10)), ("sell", receipt("buy", 10))] {
        let f = Fixture::new(side)?;
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "receipt_unsupported_signs_and_duplicate_rows_remain_pending side={side:?}"
        ));
        assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_pending, 1);
        assert_eq!(f.fills()?, 0);
        rpc.finish().await?;
    }
    let f = Fixture::new("buy")?;
    let mut value = receipt("buy", -900_000_000);
    let duplicate = value["result"]["meta"]["postTokenBalances"][0].clone();
    value["result"]["meta"]["postTokenBalances"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "receipt_unsupported_signs_and_duplicate_rows_remain_pending"
    ));
    assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_pending, 1);
    assert_eq!(f.fills()?, 0);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_creation_and_closure_allow_absent_row_in_present_arrays() -> Result<()> {
    for side in ["buy", "sell"] {
        let f = Fixture::new(side)?;
        let value = super::receipt_lifecycle_fixture::lifecycle_receipt(
            side,
            super::receipt_lifecycle_fixture::SPL_TOKEN,
            true,
            false,
        );
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "receipt_creation_and_closure_allow_absent_row_in_present_arrays side={side:?}"
        ));
        let out = f.reconcile(&rpc, 10).await?;
        assert_eq!(out.confirmation_confirmed, 1);
        assert_eq!(f.fills()?, 1);
        assert_eq!(out.buy_opened + out.sell_closed, 1);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_transaction_failure_is_distinct_from_missing_data() -> Result<()> {
    let f = Fixture::new("sell")?;
    let mut value = receipt("sell", 1_000_000);
    value["result"]["meta"]["err"] = json!({"InstructionError":[0,{"Custom":7}]});
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "receipt_transaction_failure_is_distinct_from_missing_data"
    ));
    let out = f.reconcile(&rpc, 10).await?;
    assert_eq!(out.confirmation_failed, 1);
    assert_eq!(out.reason.as_deref(), Some("receipt_transaction_failed"));
    assert_eq!(f.fills()?, 0);
    let order = f.store.load_execution_canary_order(&f.order_id)?.unwrap();
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(order.tx_signature.as_deref(), Some(SIGNATURE));
    assert_eq!(
        f.store
            .load_execution_canary_open_position(TOKEN)?
            .unwrap()
            .qty,
        10.0
    );
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_accounting_failure_rolls_back_and_retry_commits_once() -> Result<()> {
    for side in ["buy", "sell"] {
        let mut f = Fixture::new(side)?;
        let rpc = Rpc::new(receipt(
            side,
            if side == "buy" {
                -900_000_000
            } else {
                1_200_000_000
            },
        ))
        .await?;
        rpc.context(format!(
            "receipt_accounting_failure_rolls_back_and_retry_commits_once side={side:?}"
        ));
        // Fail at final status change, after position and fill writes inside the transaction.
        f.conn()?.execute_batch("CREATE TRIGGER fail_receipt_complete BEFORE UPDATE OF status ON orders
            WHEN NEW.status = 'execution_canary_confirmed' BEGIN SELECT RAISE(ABORT, 'injected'); END;")?;
        let out = f.reconcile(&rpc, 10).await;
        if side == "buy" {
            assert!(out.is_err(), "legacy BUY DB failure still propagates");
        } else {
            let out = out?;
            assert_eq!(
                (out.confirmation_pending, out.confirmation_confirmed),
                (1, 0)
            );
            assert_eq!(
                out.error.as_deref(),
                Some("receipt_accounting_write_failed")
            );
        }
        f.reopen()?;
        assert_eq!(f.fills()?, 0);
        assert_eq!(
            f.store
                .load_execution_canary_receipt_proof(&f.order_id)?
                .unwrap()
                .reason,
            "receipt_accounting_write_failed"
        );
        if side == "sell" {
            assert_eq!(
                f.store
                    .load_execution_canary_open_position(TOKEN)?
                    .unwrap()
                    .qty,
                10.0
            );
        } else {
            assert_eq!(f.store.execution_canary_open_position_count()?, 0);
        }
        f.conn()?
            .execute_batch("DROP TRIGGER fail_receipt_complete")?;
        assert_eq!(f.reconcile(&rpc, 400).await?.confirmation_confirmed, 1);
        assert_eq!(f.reconcile(&rpc, 800).await?.buy_opened, 0);
        assert_eq!(f.fills()?, 1);
        assert!(!f.store.execution_canary_accounting_pending()?);
        assert_eq!(
            *rpc.calls.lock().unwrap(),
            vec!["getSignatureStatuses", "getTransaction", "getTransaction"]
        );
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_exact_lamports_survive_price_round_trip_and_position_mismatch_stays_pending(
) -> Result<()> {
    for side in ["buy", "sell"] {
        let f = Fixture::new(side)?;
        let amount = 123_456_789;
        let rpc = Rpc::new(receipt(side, if side == "buy" { -amount } else { amount })).await?;
        rpc.context(format!("receipt_exact_lamports_survive_price_round_trip_and_position_mismatch_stays_pending side={side:?}"));
        assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_confirmed, 1);
        let stored: i64 = f.conn()?.query_row(
            if side == "buy" {
                "SELECT notional_lamports FROM fills WHERE order_id = ?1"
            } else {
                "SELECT wallet_native_delta_lamports FROM fills WHERE order_id = ?1"
            },
            [&f.order_id],
            |r| r.get(0),
        )?;
        assert_eq!(stored, amount);
        rpc.finish().await?;
    }
    let f = Fixture::new("sell")?;
    f.conn()?
        .execute("UPDATE positions SET qty_decimals = 6", [])?;
    let rpc = Rpc::new(receipt("sell", 1_000_000)).await?;
    rpc.context(format!(
        "receipt_exact_lamports_survive_price_round_trip_and_position_mismatch_stays_pending"
    ));
    let out = f.reconcile(&rpc, 10).await?;
    assert_eq!(out.confirmation_pending, 1);
    assert_eq!(
        out.reason.as_deref(),
        Some("receipt_sell_unsupported:DecimalsMismatch")
    );
    assert_eq!(f.fills()?, 0);
    assert_eq!(
        f.store
            .load_execution_canary_open_position(TOKEN)?
            .unwrap()
            .qty,
        10.0
    );
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_signature_rpc_error_text_is_not_execution_failure_proof() -> Result<()> {
    let f = Fixture::new("buy")?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!(
        "receipt_signature_rpc_error_text_is_not_execution_failure_proof"
    ));
    *rpc.status.lock().unwrap() = json!({"error":{"message":"confirmation RPC transaction_error"}});
    let out = f.reconcile(&rpc, 10).await?;
    assert_eq!(out.confirmation_failed, 0);
    assert_eq!(out.confirmation_pending, 1);
    assert_eq!(
        f.store
            .load_execution_canary_order(&f.order_id)?
            .unwrap()
            .status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(f.fills()?, 0);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_malformed_meta_error_does_not_release_risk() -> Result<()> {
    for err in [
        json!({}),
        json!(""),
        json!(false),
        json!([]),
        json!({"InstructionError":[]}),
    ] {
        let f = Fixture::new("buy")?;
        let mut value = receipt("buy", -900_000_000);
        value["result"]["meta"]["err"] = err;
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "receipt_malformed_meta_error_does_not_release_risk"
        ));
        let out = f.reconcile(&rpc, 10).await?;
        assert_eq!(out.confirmation_pending, 1);
        assert_eq!(out.confirmation_failed, 0);
        assert_eq!(f.fills()?, 0);
        assert!(f.store.execution_canary_accounting_pending()?);
        rpc.finish().await?;
    }
    Ok(())
}
