use super::receipt_cash_facts_fixture::{money_snapshot, transaction_calls};
use super::receipt_lifecycle_fixture::*;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::ObservationCoverage as Cov;
use serde_json::json;

#[tokio::test]
async fn receipt_native_write_rejection_rolls_back_bundle_before_cash_then_reopens() -> Result<()> {
    for action in [
        "RAISE(ABORT,'synthetic native observations write')",
        "RAISE(IGNORE)",
    ] {
        for side in ["buy", "sell"] {
            let mut f = Fixture::new(side)?;
            let before = money_snapshot(&f)?;
            let rpc = Rpc::new(program_receipt(side, SPL_TOKEN)).await?;
            rpc.context(format!("receipt_native_write_rejection_rolls_back_bundle_before_cash_then_reopens action={action:?} side={side:?}"));
            f.conn()?.execute_batch(&format!("CREATE TRIGGER reject_observation BEFORE INSERT ON execution_receipt_native_observations BEGIN SELECT {action}; END;"))?;
            assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_pending, 1);
            f.reopen()?;
            assert_eq!(money_snapshot(&f)?, before);
            assert!(f
                .store
                .load_execution_canary_receipt_facts(&f.order_id)?
                .is_none());
            assert!(f
                .store
                .load_receipt_native_observations(&f.order_id)?
                .is_none());
            assert!(f.store.execution_canary_accounting_pending()?);
            assert!(f.store.execution_canary_token_accounting_pending(TOKEN)?);
            f.conn()?.execute_batch("DROP TRIGGER reject_observation")?;
            assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_confirmed, 1);
            assert_eq!(f.fills()?, 1);
            rpc.finish().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn receipt_native_unsupported_sell_retains_partial_known_partial_and_sticky_conflict(
) -> Result<()> {
    let mut f = Fixture::new("sell")?;
    // Existing planner rejects imported/unknown prior PnL. Receipt facts are still retained.
    f.conn()?
        .execute("UPDATE positions SET pnl_lamports=NULL", [])?;
    let before = money_snapshot(&f)?;
    let mut full = program_receipt("sell", SPL_TOKEN);
    let mut partial = full.clone();
    partial["result"]["meta"]["innerInstructions"] = json!(null);
    let rpc = Rpc::new(partial.clone()).await?;
    rpc.context(format!(
        "receipt_native_unsupported_sell_retains_partial_known_partial_and_sticky_conflict"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_pending, 1);
    f.reopen()?;
    assert!(f
        .store
        .load_receipt_native_observations(&f.order_id)?
        .is_some());
    rpc.set(full.clone());
    f.reconcile(&rpc, 2).await?;
    let known = f
        .store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap();
    assert_eq!(known.instructions_coverage, Cov::Known);
    rpc.set(partial);
    f.reopen()?;
    f.reconcile(&rpc, 3).await?;
    assert_eq!(
        f.store
            .load_receipt_native_observations(&f.order_id)?
            .unwrap(),
        known
    );
    // Account-level amount contradicts known observations while aggregate target
    // change stays the same; cash facts alone could not detect this conflict.
    for name in ["preTokenBalances", "postTokenBalances"] {
        full["result"]["meta"][name][1]["uiTokenAmount"]["amount"] = json!("1");
    }
    rpc.set(full);
    f.reconcile(&rpc, 4).await?;
    f.reopen()?;
    assert_eq!(
        f.store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap()
            .reason,
        "native_observation_conflict"
    );
    rpc.set(program_receipt("sell", SPL_TOKEN));
    f.reconcile(&rpc, 5).await?;
    assert_eq!(
        f.store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap()
            .reason,
        "native_observation_conflict"
    );
    assert_eq!(f.fills()?, 0);
    assert_eq!(money_snapshot(&f)?, before);
    rpc.finish().await?;
    Ok(())
}
#[tokio::test]
async fn receipt_native_local_a_failure_allows_b_but_schema_failure_stops_tick() -> Result<()> {
    use super::owned_sell_queue_fixture::Queue;
    for kind in ["abort", "ignore", "schema", "pending_ignore"] {
        let mut q = Queue::new(1, 2).await?;
        q.allow_blocker_receipt("6000");
        if kind == "schema" {
            q.intake.conn()?.execute_batch("ALTER TABLE execution_receipt_native_observations RENAME TO missing_native_observations")?;
        } else {
            let action = if kind == "ignore" {
                "RAISE(IGNORE)"
            } else {
                "RAISE(ABORT,'synthetic A observations failure')"
            };
            q.intake.conn()?.execute_batch(&format!("CREATE TRIGGER reject_a BEFORE INSERT ON execution_receipt_native_observations WHEN NEW.order_id='{}' BEGIN SELECT {action}; END;",q.blocker))?;
            if kind == "pending_ignore" {
                q.intake.conn()?.execute_batch(&format!("CREATE TRIGGER reject_pending BEFORE UPDATE ON execution_canary_receipt_proofs WHEN NEW.order_id='{}' AND NEW.reason='receipt_facts_write_failed' BEGIN SELECT RAISE(IGNORE); END;",q.blocker))?;
            }
        }
        for _ in 0..2 {
            q.intake.f.reopen()?;
            let result = q.intake.tick().await;
            if matches!(kind, "schema" | "pending_ignore") {
                assert!(result.is_err(), "{kind}");
                assert_eq!(q.intake.f.sends(), 0);
            } else {
                result?;
                q.assert_b_submitted()?;
            }
            q.assert_a_pending()?;
        }
        q.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn receipt_native_completed_partial_and_legacy_early_return_do_not_enrich() -> Result<()> {
    for legacy in [false, true] {
        let mut f = Fixture::new("buy")?;
        let rpc = Rpc::new(receipt("buy", -800_000_000)).await?;
        rpc.context(format!("receipt_native_completed_partial_and_legacy_early_return_do_not_enrich legacy={legacy:?}"));
        f.reconcile(&rpc, 1).await?;
        if legacy {
            f.conn()?
                .execute("DELETE FROM execution_receipt_native_observations", [])?;
        }
        let stored = f.store.load_receipt_native_observations(&f.order_id)?;
        let calls = transaction_calls(&rpc);
        let money = money_snapshot(&f)?;
        rpc.set(program_receipt("buy", SPL_TOKEN));
        f.reopen()?;
        f.reconcile(&rpc, 2).await?;
        assert_eq!(transaction_calls(&rpc), calls);
        assert_eq!(
            f.store.load_receipt_native_observations(&f.order_id)?,
            stored
        );
        assert_eq!(f.fills()?, 1);
        assert_eq!(money_snapshot(&f)?, money);
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_native_missing_instructions_can_enrich_new_linked_account_without_guessing_owner(
) -> Result<()> {
    let mut f = Fixture::new("sell")?;
    f.conn()?
        .execute("UPDATE positions SET pnl_lamports=NULL", [])?;
    let mut full = program_receipt("sell", SPL_TOKEN);
    full["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .push(json!({"pubkey":"linked-token-account","signer":false,"writable":true}));
    full["result"]["meta"]["preBalances"]
        .as_array_mut()
        .unwrap()
        .push(json!(9));
    full["result"]["meta"]["postBalances"]
        .as_array_mut()
        .unwrap()
        .push(json!(0));
    full["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap()
        .push(json!({"programId":SPL_TOKEN,"parsed":{"type":"closeAccount","info":{"account":"linked-token-account","destination":WALLET,"owner":"ForeignAuthority"}}}));
    let mut partial = full.clone();
    partial["result"]["transaction"]["message"]["instructions"] = json!(null);
    let rpc = Rpc::new(partial.clone()).await?;
    rpc.context(format!(
        "receipt_native_missing_instructions_can_enrich_new_linked_account_without_guessing_owner"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_pending, 1);
    let first = f
        .store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap();
    assert_eq!(first.accounts.len(), 2);
    assert_eq!(first.accounts_coverage, Cov::Missing);
    f.reopen()?;
    rpc.set(full);
    assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_pending, 1);
    let known = f
        .store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap();
    assert_eq!(known.accounts.len(), 3);
    let linked = known.accounts.last().unwrap();
    assert_eq!(linked.native_delta.value.as_deref(), Some("-9"));
    assert_eq!(linked.post_token.token_owner.coverage, Cov::Missing);
    assert_eq!(linked.post_token.raw.coverage, Cov::Missing);
    assert_eq!(linked.relevance, vec!["wallet_instruction_link"]);
    rpc.set(partial);
    f.reopen()?;
    f.reconcile(&rpc, 3).await?;
    assert_eq!(
        f.store.load_receipt_native_observations(&f.order_id)?,
        Some(known)
    );
    assert_ne!(
        f.store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap()
            .reason,
        "native_observation_conflict"
    );
    assert_eq!(f.fills()?, 0);
    rpc.finish().await?;
    Ok(())
}
