use super::receipt_cash_facts_fixture::*;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn receipt_cash_facts_insert_failure_and_crash_before_accounting_are_atomic() -> Result<()> {
    for side in ["buy", "sell"] {
        let mut f = Fixture::new(side)?;
        let initial = money_snapshot(&f)?;
        let rpc = Rpc::new(receipt(
            side,
            if side == "buy" {
                -900_000_000
            } else {
                1_000_000
            },
        ))
        .await?;
        rpc.context(format!("receipt_cash_facts_insert_failure_and_crash_before_accounting_are_atomic side={side:?}"));
        rpc.context(format!("cash atomicity side={side}"));
        f.conn()?.execute_batch(
            "CREATE TRIGGER fail_facts BEFORE INSERT ON execution_canary_receipt_facts
            BEGIN SELECT RAISE(ABORT, 'synthetic_facts_insert_failure'); END;",
        )?;
        let out = f.reconcile(&rpc, 1).await?;
        assert_eq!(out.confirmation_pending, 1);
        assert_eq!(out.reason.as_deref(), Some("receipt_facts_write_failed"));
        assert!(f
            .store
            .load_execution_canary_receipt_facts(&f.order_id)?
            .is_none());
        assert_pending(&f)?;
        assert_eq!(money_snapshot(&f)?, initial);
        f.conn()?.execute_batch("DROP TRIGGER fail_facts")?;
        stop_accounting(&f)?;
        let out = f.reconcile(&rpc, 2).await;
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
        let saved = facts(&f)?;
        assert_pending(&f)?;
        assert_eq!(money_snapshot(&f)?, initial);
        f.reopen()?;
        assert_eq!(facts(&f)?, saved, "crash after evidence before accounting");
        // Exact replay must not even attempt INSERT/UPDATE, before retrying accounting.
        f.conn()?.execute_batch(
            "CREATE TRIGGER reject_replay BEFORE INSERT ON execution_canary_receipt_facts
            BEGIN SELECT RAISE(ABORT, 'unexpected_rewrite'); END;",
        )?;
        f.conn()?.execute_batch("DROP TRIGGER stop_accounting")?;
        rpc.response.lock().unwrap().2 = 120;
        assert_eq!(f.reconcile(&rpc, 3).await?.confirmation_confirmed, 1);
        f.reopen()?;
        f.reconcile(&rpc, 4).await?;
        assert_eq!(f.fills()?, 1);
        assert_eq!(facts(&f)?, saved);
        assert_eq!(transaction_calls(&rpc), 3);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_enrichment_write_failure_never_reaches_fill() -> Result<()> {
    let mut f = Fixture::new("buy")?;
    let initial = money_snapshot(&f)?;
    let mut value = receipt("buy", -900_000_000);
    let rpc = Rpc::new(value.clone()).await?;
    rpc.context(format!(
        "receipt_cash_facts_enrichment_write_failure_never_reaches_fill"
    ));
    stop_accounting(&f)?;
    assert!(f.reconcile(&rpc, 1).await.is_err());
    let original = facts(&f)?;
    f.conn()?.execute_batch(
        "DROP TRIGGER stop_accounting;
        CREATE TRIGGER fail_enrichment BEFORE UPDATE ON execution_canary_receipt_facts
        BEGIN SELECT RAISE(ABORT, 'synthetic_enrichment_failure'); END;",
    )?;
    value["result"]["meta"]["fee"] = json!(5000);
    rpc.set(value);
    f.reopen()?;
    assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_pending, 1);
    assert_eq!(facts(&f)?, original);
    assert_eq!(money_snapshot(&f)?, initial);
    assert_pending(&f)?;
    f.conn()?.execute_batch("DROP TRIGGER fail_enrichment")?;
    assert_eq!(f.reconcile(&rpc, 3).await?.confirmation_confirmed, 1);
    assert_eq!(facts(&f)?.transaction_fee.unwrap().as_u64(), 5000);
    assert_eq!(f.fills()?, 1);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_known_conflicts_preserve_original_and_block_accounting() -> Result<()> {
    for field in ["native", "token", "fee", "payer", "block_time"] {
        let mut f = Fixture::new("sell")?;
        let initial = money_snapshot(&f)?;
        let mut value = sponsored(receipt("sell", 1_000_000));
        value["result"]["meta"]["fee"] = json!(5000);
        let rpc = Rpc::new(value.clone()).await?;
        rpc.context(format!("receipt_cash_facts_known_conflicts_preserve_original_and_block_accounting field={field:?}"));
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
        let original = facts(&f)?;
        f.conn()?.execute_batch("DROP TRIGGER stop_accounting")?;
        match field {
            "native" => value["result"]["meta"]["postBalances"][1] = json!(2_002_000_000),
            "token" => {
                value["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] =
                    json!("3001")
            }
            "fee" => value["result"]["meta"]["fee"] = json!(5001),
            "payer" => {
                value["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
                    json!("other-sponsor")
            }
            "block_time" => value["result"]["blockTime"] = json!(1_780_000_001),
            _ => unreachable!(),
        }
        rpc.set(value);
        f.reopen()?;
        assert_eq!(
            f.reconcile(&rpc, 2).await?.confirmation_pending,
            1,
            "{field}"
        );
        assert_eq!(facts(&f)?, original, "{field}");
        assert_eq!(money_snapshot(&f)?, initial);
        assert_pending(&f)?;
        assert_eq!(transaction_calls(&rpc), 2);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_failure_or_pending_receipt_does_not_starve_other_mint_exit(
) -> Result<()> {
    use super::owned_sell_queue_fixture::Queue;
    for case in ["signed_pending", "insert_failure", "conflict"] {
        let mut q = Queue::new(1, 2).await?;
        q.allow_blocker_receipt("6000");
        let response = q.intake.f.receipt.clone();
        {
            let mut receipt = response.lock().unwrap();
            receipt.as_mut().unwrap()["meta"]["postBalances"][0] = json!(2_000_000_000);
        }
        if case == "insert_failure" {
            q.intake.conn()?.execute_batch(&format!("CREATE TRIGGER fail_A_facts BEFORE INSERT ON execution_canary_receipt_facts
                WHEN NEW.order_id = '{}' BEGIN SELECT RAISE(ABORT, 'synthetic_A_facts_failure'); END;", q.blocker))?;
        }
        if case == "conflict" {
            // Persist A through the actual boundary before the tick processes B.
            crate::execution_submit_adapter::record_execution_rpc_confirmation_boundary(
                &q.intake.f.store,
                &reqwest::Client::new(),
                &q.intake.f.config.submit_adapter_http_url,
                &q.blocker,
                &q.intake.f.config.canary_wallet_pubkey,
                q.intake.f.now,
                500,
            )
            .await?;
            response.lock().unwrap().as_mut().unwrap()["meta"]["postBalances"][0] =
                json!(2_010_000_000);
        }
        for _ in 0..3 {
            q.intake.f.reopen()?;
            q.intake.tick().await?;
            q.assert_a_pending()?;
        }
        q.assert_b_submitted()?;
        let facts = q
            .intake
            .f
            .store
            .load_execution_canary_receipt_facts(&q.blocker)?;
        if case == "insert_failure" {
            assert!(facts.is_none());
        } else {
            assert_eq!(facts.unwrap().wallet_native_delta.as_i128(), 0);
        }
        q.finish().await?;
    }
    Ok(())
}
