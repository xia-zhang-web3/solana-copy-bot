use super::{
    receipt_cash_facts_fixture::{assert_pending, money_snapshot},
    receipt_reconciliation_fixture::*,
};
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn receipt_fixture_explicit_timeout_keeps_proof_locks_and_reopen_retry() -> Result<()> {
    for side in ["buy", "sell"] {
        let mut f = Fixture::new(side)?;
        let before = money_snapshot(&f)?;
        let value = receipt(
            side,
            if side == "buy" {
                -900_000_000
            } else {
                1_000_000
            },
        );
        let rpc = Rpc::new(value.clone()).await?;
        rpc.context(format!("explicit short timeout side={side}"));
        rpc.expect_receipt_cancellation();
        let out = f.reconcile_with_timeout(&rpc, 1, 80).await?;
        rpc.finish().await?;
        assert_eq!(rpc.cancellations(), 1);
        assert_eq!(rpc.tasks().counts(), (0, 0, 2));
        assert_eq!(
            (out.confirmation_pending, out.confirmation_confirmed),
            (1, 0)
        );
        assert_eq!(out.reason.as_deref(), Some("receipt_rpc_timeout"));
        f.reopen()?;
        let proof = f
            .store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap();
        assert_eq!(proof.reason, "receipt_rpc_timeout");
        assert_eq!(proof.confirmation_status, "confirmed");
        assert_eq!(proof.slot, Some(42));
        assert_eq!(proof.tx_signature, SIGNATURE);
        assert_pending(&f)?;
        assert_eq!(money_snapshot(&f)?, before);
        assert_eq!(
            *rpc.calls.lock().unwrap(),
            ["getSignatureStatuses", "getTransaction"]
        );
        let retry = Rpc::new(value).await?;
        retry.response.lock().unwrap().2 = 120;
        *retry.status.lock().unwrap() = json!({"error":{"code":-32001}});
        let out = f.reconcile(&retry, 2).await?;
        f.reopen()?;
        let replay = f.reconcile(&retry, 3).await?;
        retry.finish().await?;
        assert_eq!(out.confirmation_confirmed, 1);
        assert_eq!(
            replay.buy_opened + replay.sell_closed + replay.sell_partial,
            0
        );
        assert_eq!(f.fills()?, 1);
        assert_eq!(*retry.calls.lock().unwrap(), ["getTransaction"]);
        assert!(!f.store.execution_canary_accounting_pending()?);
    }
    Ok(())
}

#[tokio::test]
async fn receipt_fixture_repeated_cancelled_instances_leave_no_owned_tasks() -> Result<()> {
    for instance in 0..4 {
        let f = Fixture::new("buy")?;
        let rpc = Rpc::new(receipt("buy", -900_000_000)).await?;
        rpc.context(format!("cancelled fixture instance={instance}"));
        let tasks = rpc.tasks();
        rpc.expect_receipt_cancellation();
        let out = f.reconcile_with_timeout(&rpc, 1, 80).await?;
        rpc.finish().await?;
        assert_eq!(out.reason.as_deref(), Some("receipt_rpc_timeout"));
        assert_eq!(rpc.cancellations(), 1);
        assert_eq!(tasks.counts(), (0, 0, 2));
        drop(rpc);
        tasks.quiescent().await?;
        assert_eq!(tasks.counts(), (0, 0, 2));
    }
    Ok(())
}
