use super::{failed_expense_runtime_tests::failure, owned_sell_queue_fixture::Queue};
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn failed_expense_stalled_a_allows_real_sell_b_before_receipt_and_each_accounting_write(
) -> Result<()> {
    for fault in ["unavailable", "facts", "ledger", "completion"] {
        let mut q = Queue::new(0, 1).await?;
        let f = &q.intake.f;
        f.store.detect_failed_expense(
            &q.blocker,
            &f.config.canary_wallet_pubkey,
            "signature_status",
            "confirmed",
            Some(1),
            &failure(),
            f.now,
        )?;
        if fault != "unavailable" {
            q.allow_blocker_receipt("7000");
            let mut v = q.intake.f.receipt.lock().unwrap();
            let v = v.as_mut().unwrap();
            v["meta"]["err"] = failure();
            v["meta"]["postBalances"][0] = json!(1_999_995_000);
            let target = match fault {
                "facts" => "BEFORE INSERT ON execution_failed_expense_facts",
                "ledger" => "BEFORE INSERT ON execution_failed_expense_ledger",
                _ => "BEFORE UPDATE ON execution_failed_expense_tasks",
            };
            let condition = if fault == "completion" {
                "AND NEW.status='complete'"
            } else {
                ""
            };
            q.intake.conn()?.execute_batch(&format!("CREATE TRIGGER fail_expense_a {target} WHEN NEW.order_id='{}' {condition} BEGIN SELECT RAISE(ABORT,'synthetic local expense rejection'); END;",q.blocker))?;
        }
        for _ in 0..3 {
            q.intake.f.reopen()?;
            q.intake.tick().await?;
            assert_eq!(
                q.intake
                    .f
                    .store
                    .load_failed_expense_task(&q.blocker)?
                    .unwrap()
                    .status,
                "pending"
            );
            assert!(!q.intake.f.store.execution_canary_fill_exists(&q.blocker)?);
            assert_eq!(
                q.intake
                    .f
                    .store
                    .load_execution_canary_open_position(super::open_risk_sell_fixture::TOKEN)?
                    .unwrap()
                    .qty_exact
                    .unwrap()
                    .raw(),
                7000
            );
        }
        q.assert_b_submitted()?;
        if fault != "unavailable" {
            q.intake
                .conn()?
                .execute_batch("DROP TRIGGER fail_expense_a")?;
        }
        q.allow_blocker_receipt("7000");
        {
            let mut v = q.intake.f.receipt.lock().unwrap();
            let v = v.as_mut().unwrap();
            v["meta"]["err"] = failure();
            v["meta"]["postBalances"][0] = json!(1_999_995_000);
        }
        q.intake.f.reopen()?;
        q.intake.tick().await?;
        assert_eq!(
            q.intake
                .f
                .store
                .load_failed_expense_task(&q.blocker)?
                .unwrap()
                .status,
            "complete"
        );
        let count: u64 = q.intake.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 1);
        assert_eq!(q.intake.f.sends(), 1);
        q.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn failed_expense_storage_failure_or_undurable_pending_stops_tick() -> Result<()> {
    for fault in ["schema", "abort_pending", "ignore_pending"] {
        let mut q = Queue::new(0, 1).await?;
        q.intake.f.store.detect_failed_expense(
            &q.blocker,
            &q.intake.f.config.canary_wallet_pubkey,
            "signature_status",
            "confirmed",
            Some(1),
            &failure(),
            q.intake.f.now,
        )?;
        if fault == "schema" {
            q.intake.conn()?.execute_batch(
                "ALTER TABLE execution_failed_expense_facts RENAME TO unavailable_failed_facts",
            )?;
            q.allow_blocker_receipt("7000");
            q.intake.f.receipt.lock().unwrap().as_mut().unwrap()["meta"]["err"] = failure();
        } else {
            let action = if fault == "abort_pending" {
                "RAISE(ABORT,'synthetic pending failure')"
            } else {
                "RAISE(IGNORE)"
            };
            q.intake.conn()?.execute_batch(&format!("CREATE TRIGGER reject_pending BEFORE UPDATE ON execution_failed_expense_tasks WHEN NEW.reason='failed_receipt_unavailable' BEGIN SELECT {action}; END;"))?;
        }
        q.intake.f.reopen()?;
        assert!(q.intake.tick().await.is_err(), "{fault}");
        assert_eq!(q.intake.f.sends(), 0);
        assert_eq!(q.quote_count(), 0);
        q.finish().await?;
    }
    Ok(())
}
