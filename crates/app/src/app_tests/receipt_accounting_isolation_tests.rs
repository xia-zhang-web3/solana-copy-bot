use super::open_risk_sell_fixture::TOKEN;
use super::owned_sell_queue_fixture::Queue;
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

pub(super) async fn ready_queue(failure: &str) -> Result<Queue> {
    let q = Queue::new(1, 2).await?;
    // Synthetic known basis is necessary to exercise the writer, not Unsupported.
    q.intake.conn()?.execute(
        "UPDATE positions SET pnl_lamports=0 WHERE token=?1",
        [TOKEN],
    )?;
    q.allow_blocker_receipt("6000");
    q.intake.f.receipt.lock().unwrap().as_mut().unwrap()["meta"]["postBalances"][0] =
        json!(2_000_000_000);
    let (target, condition) = if failure == "fill" {
        ("BEFORE INSERT ON fills", "")
    } else {
        (
            "BEFORE UPDATE ON execution_canary_receipt_proofs",
            "AND NEW.reason='accounting_complete'",
        )
    };
    q.intake.conn()?.execute_batch(&format!(
        "CREATE TRIGGER fail_accounting_A {target} WHEN NEW.order_id='{}' {condition}
         BEGIN SELECT RAISE(ABORT, 'synthetic_local_A_accounting_failure'); END;",
        q.blocker
    ))?;
    Ok(q)
}

fn money(q: &Queue) -> Result<(i64, i64, i64)> {
    Ok(q.intake.conn()?.query_row(
        "SELECT qty_raw,cost_lamports,pnl_lamports FROM positions WHERE token=?1 AND state='open'",
        [TOKEN],
        |r| {
            Ok((
                r.get::<_, String>(0)?.parse().unwrap(),
                r.get(1)?,
                r.get(2)?,
            ))
        },
    )?)
}

#[tokio::test]
async fn receipt_accounting_isolation_local_writes_allow_b_and_replay_a_once() -> Result<()> {
    let mut observations = Vec::new();
    for failure in ["fill", "proof"] {
        let mut q = ready_queue(failure).await?;
        let mut errors = Vec::new();
        for _ in 0..3 {
            q.intake.f.reopen()?;
            errors.push(q.intake.tick().await.err().map(|e| format!("{e:#}")));
            q.assert_a_pending()?;
            assert_eq!(money(&q)?, (7000, 70_000_000, 0));
            assert!(matches!(
                q.intake
                    .f
                    .store
                    .plan_execution_canary_sell_settlement(&q.blocker)?,
                ExecutionCanarySellSettlement::Ready(_)
            ));
            assert_eq!(
                q.intake
                    .f
                    .store
                    .load_execution_canary_receipt_facts(&q.blocker)?
                    .unwrap()
                    .wallet_native_delta
                    .as_i128(),
                0
            );
            assert_eq!(
                q.intake
                    .f
                    .store
                    .load_execution_canary_receipt_proof(&q.blocker)?
                    .unwrap()
                    .reason,
                "receipt_accounting_write_failed"
            );
            assert!(q
                .intake
                .f
                .store
                .execution_canary_token_accounting_pending(TOKEN)?);
            assert!(q
                .intake
                .f
                .store
                .execution_canary_receipt_submit_block_reason("new-sell", TOKEN, "sell")?
                .is_some());
            assert!(q
                .intake
                .f
                .store
                .execution_canary_receipt_submit_block_reason("new-buy", &q.token_b, "buy")?
                .is_some());
            assert!(q
                .intake
                .f
                .store
                .execution_canary_receipt_submit_block_reason("new-sell", &q.token_b, "sell")?
                .is_none());
        }
        let submits_while_failed = q.intake.f.sends();
        if errors.iter().all(Option::is_none) {
            q.assert_b_submitted()?;
        }
        // Causal control: remove only the fault. A settles; its remaining intent may submit.
        q.intake
            .conn()?
            .execute_batch("DROP TRIGGER fail_accounting_A")?;
        q.intake.f.reopen()?;
        q.intake.tick().await?;
        let b = q
            .intake
            .f
            .store
            .load_execution_canary_order_by_signal(&q.b.signal_id)?
            .unwrap();
        assert_eq!(b.status, EXECUTION_STATUS_CANARY_SUBMITTED);
        let meta = q
            .intake
            .f
            .store
            .load_execution_canary_build_plan_metadata(&b.order_id)?
            .unwrap();
        assert_eq!(meta.signal_id, q.b.signal_id);
        assert_eq!(meta.quote_in_amount_raw.as_deref(), Some("7000"));
        let quote: serde_json::Value =
            serde_json::from_str(meta.quote_response_json.as_deref().unwrap())?;
        assert_eq!(quote["inputMint"], q.token_b);
        assert_eq!(money(&q)?, (6000, 60_000_000, -10_000_000));
        let settled = q
            .intake
            .f
            .store
            .load_execution_canary_cash_settlement(&q.blocker)?
            .unwrap();
        for _ in 0..3 {
            q.intake.f.reopen()?;
            q.intake.tick().await?;
            assert_eq!(money(&q)?, (6000, 60_000_000, -10_000_000));
            assert_eq!(
                q.intake
                    .f
                    .store
                    .load_execution_canary_cash_settlement(&q.blocker)?,
                Some(settled.clone())
            );
            assert_eq!(
                q.intake
                    .f
                    .store
                    .load_execution_canary_order(&q.blocker)?
                    .unwrap()
                    .status,
                EXECUTION_STATUS_CANARY_CONFIRMED
            );
            let fills: i64 = q.intake.conn()?.query_row(
                "SELECT COUNT(*) FROM fills WHERE order_id=?1",
                [&q.blocker],
                |r| r.get(0),
            )?;
            assert_eq!(fills, 1);
        }
        observations.push(
            json!({"failure":failure,"errors":errors,"submits_while_failed":submits_while_failed}),
        );
        q.finish().await?;
    }
    println!("{}", serde_json::to_string_pretty(&observations)?);
    assert!(observations.iter().all(|v| v["errors"]
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e.is_null())
        && v["submits_while_failed"] == 1));
    Ok(())
}

#[tokio::test]
async fn receipt_accounting_isolation_requires_durable_pending_write_and_readback() -> Result<()> {
    for failure in ["initial", "abort_recovery", "ignore_recovery"] {
        let mut q = ready_queue("fill").await?;
        let condition = if failure == "initial" {
            "1=1"
        } else {
            "NEW.reason='receipt_accounting_write_failed'"
        };
        let action = if failure == "ignore_recovery" {
            "RAISE(IGNORE)"
        } else {
            "RAISE(ABORT, 'synthetic_pending_write_failure')"
        };
        q.intake.conn()?.execute_batch(&format!(
            "CREATE TRIGGER fail_pending_A BEFORE UPDATE ON execution_canary_receipt_proofs
             WHEN NEW.order_id='{}' AND {condition} BEGIN SELECT {action}; END;",
            q.blocker
        ))?;
        for _ in 0..3 {
            q.intake.f.reopen()?;
            assert!(q.intake.tick().await.is_err(), "{failure}");
            assert_eq!(q.quote_count(), 0);
            assert_eq!(q.intake.f.sends(), 0);
            q.assert_a_pending()?;
            assert_eq!(money(&q)?, (7000, 70_000_000, 0));
        }
        q.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_accounting_isolation_does_not_swallow_schema_failure() -> Result<()> {
    let mut q = ready_queue("fill").await?;
    q.intake
        .conn()?
        .execute_batch("ALTER TABLE positions RENAME TO unavailable_positions")?;
    q.intake.f.reopen()?;
    assert!(q.intake.tick().await.is_err());
    assert_eq!(q.quote_count(), 0);
    assert_eq!(q.intake.f.sends(), 0);
    assert!(!q.intake.f.store.execution_canary_fill_exists(&q.blocker)?);
    q.finish().await?;
    Ok(())
}
