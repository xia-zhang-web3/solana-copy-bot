use super::source_sell_producer_fixture::Fixture;
use crate::execution_source_sell_producer::produce;
use anyhow::Result;

#[tokio::test]
async fn source_sell_producer_route_and_kill_gates_do_not_advance_staging() -> Result<()> {
    for gate in ["disabled", "kill", "quote-disabled", "other-route"] {
        let mut f = Fixture::new().await?;
        f.config.canary_tiny_submit_enabled = false;
        let staged = f.stage("gated").await?;
        match gate {
            "disabled" => f.config.canary_enabled = false,
            "kill" => std::fs::write(&f.config.canary_kill_switch_path, "stop")?,
            "quote-disabled" => f.config.quote_canary_enabled = false,
            _ => f.config.canary_route = "dry-run".into(),
        }
        let outcome = f.tick().await;
        f.finish().await?;
        let out = outcome?;
        assert_eq!(out.source_sell_production.visits, 0, "{gate}: {out:?}");
        assert_eq!(
            f.f.conn()?.query_row(
                "SELECT count(*) FROM execution_source_sell_staging_cursor",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        assert!(f
            .f
            .store
            .load_copy_signal_by_signal_id("shadow:gated:source-a:sell:mint")?
            .is_none());
        assert_eq!(
            f.f.staged("gated")?.unwrap().position_id,
            staged.position_id
        );
        assert_eq!(f.rpc.count("sendTransaction"), 0);
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_stale_generation_and_lost_proofs_refuse_without_money_change(
) -> Result<()> {
    for fault in ["generation", "witness", "observed"] {
        let mut f = Fixture::new().await?;
        f.config.canary_tiny_submit_enabled = false;
        let staged = f.stage("refused-a").await?;
        let reason = match fault {
            "generation" => {
                f.f.store
                    .record_execution_canary_manual_terminal_write_off(
                        "mint",
                        "tiny",
                        "synthetic-close-a",
                        f.f.now,
                    )?;
                f.f.buy("generation-b", "source-a")?;
                "source_sell_generation_mismatch"
            }
            "witness" => {
                f.f.conn()?.execute(
                    "DELETE FROM fills WHERE order_id=?1",
                    [&staged.buy_witness.order_id],
                )?;
                "source_sell_witness_not_proven"
            }
            _ => {
                f.f.conn()?.execute(
                    "DELETE FROM observed_swaps WHERE signature=?1",
                    [&staged.event.signature],
                )?;
                "source_sell_observed_mismatch"
            }
        };
        let position = f.f.store.load_execution_canary_open_position("mint")?;
        let before = f.f.money()?;
        let out = produce(&f.f.store)?;
        f.finish().await?;
        assert_eq!(out.rejected, 1, "{fault}: {out:?}");
        assert_eq!(out.refusals.order_id(), staged.intent_id);
        assert_eq!(out.refusals.reason(), reason);
        assert_eq!(f.f.money()?, before);
        assert_eq!(
            f.f.store.load_execution_canary_open_position("mint")?,
            position
        );
        assert_eq!(f.rpc.count("sendTransaction"), 0);
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_moved_duplicate_and_malformed_binding_leave_b_runnable() -> Result<()>
{
    for fault in ["moved", "duplicate", "time"] {
        let mut f = Fixture::new().await?;
        f.config.canary_tiny_submit_enabled = false;
        let a = f.stage("binding-a").await?;
        assert_eq!(f.tick().await?.source_sell_production.inserted, 1);
        // Explicit later corruption after real runtime promotion, not manual signal setup.
        match fault {
            "moved" => {
                f.f.conn()?.execute_batch(
                    "UPDATE execution_source_sell_promotions SET signal_id='moved-away'",
                )?;
            }
            "duplicate" => {
                f.f.conn()?.execute_batch("ALTER TABLE execution_source_sell_promotions RENAME TO original_bindings;
                CREATE TABLE execution_source_sell_promotions(signal_id TEXT,intent_id TEXT,promoted_at TEXT);
                INSERT INTO execution_source_sell_promotions SELECT * FROM original_bindings;
                INSERT INTO execution_source_sell_promotions SELECT 'duplicate',intent_id,promoted_at FROM original_bindings;")?;
            }
            _ => {
                f.f.conn()?.execute_batch(
                    "UPDATE execution_source_sell_promotions SET promoted_at='bad'",
                )?;
            }
        }
        let b = f.stage("binding-b").await?;
        let position = f.f.store.load_execution_canary_open_position("mint")?;
        let before = f.f.money()?;
        let out = produce(&f.f.store)?;
        f.finish().await?;
        assert_eq!(out.inserted, 1, "{fault}: {out:?}");
        assert_eq!(out.rejected + out.malformed, 1, "{fault}: {out:?}");
        assert_eq!(out.refusals.order_id(), a.intent_id);
        assert!(out.refusals.reason().len() <= 64);
        let after = f.f.money()?;
        for table in [
            "orders",
            "fills",
            "positions",
            "execution_canary_receipt_facts",
            "execution_quote_canary_events",
        ] {
            assert_eq!(after[table], before[table], "{fault}/{table}");
        }
        assert_eq!(
            f.f.store.load_execution_canary_open_position("mint")?,
            position
        );
        assert!(f
            .f
            .store
            .load_copy_signal_by_signal_id("shadow:binding-b:source-a:sell:mint")?
            .is_some());
        assert_eq!(f.f.staged("binding-b")?.unwrap().position_id, b.position_id);
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_schema_checkpoint_and_promotion_sql_errors_surface() -> Result<()> {
    for fault in [
        "cursor",
        "staging",
        "binding",
        "checkpoint",
        "promotion",
        "writer-invariant",
    ] {
        let mut f = Fixture::new().await?;
        f.config.canary_tiny_submit_enabled = false;
        f.stage("database-a").await?;
        let sql = match fault {
            "cursor" => "DROP TABLE execution_source_sell_staging_cursor;",
            "staging" => "DROP TABLE execution_source_sell_intents;",
            "binding" => "DROP TABLE execution_source_sell_promotions;",
            "checkpoint" => "CREATE TRIGGER refuse_checkpoint BEFORE INSERT ON execution_source_sell_staging_cursor BEGIN SELECT RAISE(ABORT,'disk I/O error'); END;",
            "promotion" => "CREATE TRIGGER refuse_promotion BEFORE INSERT ON execution_source_sell_promotions BEGIN SELECT RAISE(ABORT,'malformed source SELL promotion identity'); END;",
            _ => "CREATE TRIGGER ignore_promotion BEFORE INSERT ON execution_source_sell_promotions BEGIN SELECT RAISE(IGNORE); END;",
        };
        f.f.conn()?.execute_batch(sql)?;
        let before = f.f.money()?;
        let result = f.tick().await;
        f.finish().await?;
        let error = result.expect_err(fault);
        assert_eq!(f.f.money()?, before, "{fault}: {error:#}");
        assert_eq!(f.rpc.count("sendTransaction"), 0);
        if fault == "promotion" {
            assert!(error.chain().any(|e| e.is::<rusqlite::Error>()));
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_bad_row_shapes_do_not_poison_the_page() -> Result<()> {
    for mutation in [
        "event_ts='bad'",
        "staged_at='bad'",
        "event_signature='changed'",
        "buy_order_id=' '",
        "source_wallet=x'ff'",
        "amount_in_raw='not-a-number'",
        "intent_id=x'ff'",
    ] {
        let mut f = Fixture::new().await?;
        f.config.canary_tiny_submit_enabled = false;
        f.stage("valid-b").await?;
        f.stage("malformed-a").await?;
        f.f.conn()?.execute_batch(&format!(
            "UPDATE execution_source_sell_intents SET {mutation} WHERE rowid=2"
        ))?;
        let before = f.f.store.load_execution_canary_open_position("mint")?;
        let out = produce(&f.f.store)?;
        f.finish().await?;
        assert_eq!(
            (out.visits, out.malformed, out.inserted),
            (2, 1, 1),
            "{mutation}: {out:?}"
        );
        assert!(f
            .f
            .store
            .load_copy_signal_by_signal_id("shadow:valid-b:source-a:sell:mint")?
            .is_some());
        assert_eq!(
            f.f.store.load_execution_canary_open_position("mint")?,
            before
        );
        assert_eq!(f.rpc.count("sendTransaction"), 0);
    }
    Ok(())
}
