#[path = "common/source_write_off_db.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use fixture::*;

#[test]
fn all_promoted_write_off_modes_preserve_close_math_and_replay_after_reopen() -> Result<()> {
    for kind in kinds() {
        let qty = TokenQuantity::new(
            if matches!(kind, Kind::DustNoRoute) {
                1
            } else {
                7000
            },
            3,
        );
        let mut db = Db::new(kind, qty)?;
        let untouched = snapshot(&db.conn()?, &["orders", "positions"])?;
        let Outcome::WrittenOff {
            position_id,
            close_result: closed,
            order,
        } = db.run(kind)?
        else {
            panic!("expected actual write-off");
        };
        assert_eq!(position_id, db.staged.position_id);
        assert_eq!(closed.position_id.as_deref(), Some(position_id.as_str()));
        assert_eq!(closed.closed_qty_exact, Some(qty));
        assert_eq!(closed.entry_cost_lamports, Some(Lamports::new(1000)));
        assert_eq!(closed.exit_value_lamports, Some(Lamports::ZERO));
        assert_eq!(closed.pnl_lamports, Some(SignedLamports::new(-1000)));
        assert_eq!(
            closed.close_status,
            if matches!(kind, Kind::DustNoRoute) {
                EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED
            } else {
                EXECUTION_CANARY_POSITION_CLOSE_CLOSED
            }
        );
        assert_eq!(order.order_id, db.order.order_id);
        assert_eq!(order.attempt, 1);
        assert!(order
            .simulation_error
            .as_deref()
            .unwrap()
            .starts_with(kind.reason()));
        assert_eq!(snapshot(&db.conn()?, &["orders", "positions"])?, untouched);
        let state: (String, String, i64, i64) = db.conn()?.query_row(
            "SELECT state,qty_raw,cost_lamports,pnl_lamports FROM positions WHERE position_id=?1",
            [&position_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        assert_eq!(state, ("closed".into(), "0".into(), 0, -1000));
        let after = snapshot(&db.conn()?, &[])?;
        db.reopen()?;
        assert_eq!(
            db.run(kind)?,
            Outcome::Refused("source_sell_write_off_already_terminal")
        );
        assert_eq!(snapshot(&db.conn()?, &[])?, after);
    }
    Ok(())
}

#[test]
fn eligibility_is_fresh_and_dust_is_exact_without_terminal_attempt_exhaustion() -> Result<()> {
    for (kind, qty, change, reason) in [
        (Kind::TerminalSimulation {max_attempts:2}, TokenQuantity::new(7000,3), "", "source_sell_write_off_attempts_remaining"),
        (Kind::TerminalNoRoute {max_attempts:2}, TokenQuantity::new(7000,3), "", "source_sell_write_off_attempts_remaining"),
        (Kind::TerminalNoRoute {max_attempts:1}, TokenQuantity::new(7000,3), "UPDATE orders SET simulation_error='HTTP 429' WHERE order_id LIKE 'exec-canary:shadow:%'", "source_sell_write_off_no_route_unproven"),
        (Kind::DustNoRoute, TokenQuantity::new(1,3), "UPDATE orders SET simulation_error='No routes found' WHERE order_id LIKE 'exec-canary:shadow:%'", "source_sell_write_off_dust_error_mismatch"),
        (Kind::DustNoRoute, TokenQuantity::new(2,3), "", "source_sell_write_off_not_exact_dust"),
        (Kind::DustNoRoute, TokenQuantity::new(1,0), "", "source_sell_write_off_not_exact_dust"),
    ] {
        let db = Db::new(kind, qty)?;
        if !change.is_empty() { db.conn()?.execute_batch(change)?; }
        let before = snapshot(&db.conn()?, &[])?;
        assert_eq!(db.run(kind)?, Outcome::Refused(reason));
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    for error in [
        "NO_ROUTES_FOUND",
        "No routes found",
        "TOKEN_NOT_TRADABLE",
        "not tradable",
        "Bonding curve for mint not found",
    ] {
        let kind = Kind::TerminalNoRoute { max_attempts: 1 };
        let db = Db::new(kind, TokenQuantity::new(7000, 3))?;
        db.conn()?.execute(
            "UPDATE orders SET simulation_error=?1 WHERE order_id=?2",
            rusqlite::params![error, db.order.order_id],
        )?;
        assert!(matches!(db.run(kind)?, Outcome::WrittenOff { .. }));
    }
    Ok(())
}

#[test]
fn generation_witness_reverse_association_and_pending_state_refuse_atomically() -> Result<()> {
    for case in ["generation", "witness", "moved", "processed", "ambiguous"] {
        let kind = Kind::TerminalNoRoute { max_attempts: 1 };
        let db = Db::new(kind, TokenQuantity::new(7000, 3))?;
        match case {
            "generation" => {
                db.store.record_execution_canary_manual_terminal_write_off(
                    "mint",
                    "tiny",
                    "test_replace",
                    db.now,
                )?;
                proven_buy(
                    &db.store,
                    "buy-b",
                    "source-a",
                    db.now,
                    TokenQuantity::new(7000, 3),
                )?;
            }
            "witness" => {
                db.conn()?.execute(
                    "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
                    [&db.staged.buy_witness.order_id],
                )?;
            }
            "moved" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_promotions SET signal_id='wrong-key'",
                    [],
                )?;
            }
            "processed" => {
                db.conn()?.execute(
                    "UPDATE copy_signals SET status='handled' WHERE signal_id=?1",
                    [&db.order.signal_id],
                )?;
            }
            _ => {
                // Deliberate corruption after the actual BUY proof, bypassing the normal guard.
                db.conn()?
                    .execute_batch("DROP INDEX idx_positions_one_open_token_bucket;")?;
                db.conn()?.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,cost_lamports,qty_raw,qty_decimals,accounting_bucket)
                SELECT 'second-open',token,qty,cost_sol,opened_ts,state,cost_lamports,qty_raw,qty_decimals,accounting_bucket FROM positions WHERE position_id=?1",[&db.staged.position_id])?;
            }
        }
        let before = snapshot(&db.conn()?, &[])?;
        assert!(matches!(db.run(kind)?, Outcome::Refused(_)), "{case}");
        assert_eq!(snapshot(&db.conn()?, &[])?, before, "{case}");
    }
    Ok(())
}

#[test]
fn submitted_confirmed_and_receipt_evidence_block_even_with_stale_failure_state() -> Result<()> {
    for phase in ["submitted", "confirmed", "known-signature", "receipt-only"] {
        let kind = Kind::TerminalNoRoute { max_attempts: 1 };
        let db = Db::new(kind, TokenQuantity::new(1, 3))?;
        let id = &db.order.order_id;
        db.store
            .mark_execution_canary_failed_build_retry_candidate(id, db.now, "retry")?;
        db.store.mark_execution_canary_built(id, db.now)?;
        db.store.mark_execution_canary_simulated(
            id,
            db.now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        db.store
            .mark_execution_canary_submitted(id, db.now, "actual-submitted-signature")?;
        if phase == "confirmed" || phase == "receipt-only" {
            db.store.mark_execution_canary_confirmed_unreconciled(
                id,
                &ExecutionCanaryReceiptProof {
                    tx_signature: "actual-submitted-signature".into(),
                    wallet_pubkey: "execution-wallet".into(),
                    token: "mint".into(),
                    side: "sell".into(),
                    confirmation_status: "confirmed".into(),
                    slot: Some(42),
                    confirmed_at: db.now,
                    reason: "receipt_not_fetched".into(),
                },
                db.now,
            )?;
        }
        if phase == "known-signature" {
            db.store.mark_execution_canary_failed(
                id,
                db.now,
                EXECUTION_ERROR_BUILD_FAILED,
                "NO_ROUTES_FOUND",
            )?;
        }
        if phase == "receipt-only" {
            db.conn()?.execute("UPDATE orders SET status=?1,err_code=?2,simulation_error='NO_ROUTES_FOUND',tx_signature=NULL WHERE order_id=?3",
            rusqlite::params![EXECUTION_STATUS_CANARY_FAILED,EXECUTION_ERROR_BUILD_FAILED,id])?;
        }
        let before = snapshot(&db.conn()?, &[])?;
        for mode in kinds() {
            assert!(matches!(db.run(mode)?, Outcome::Refused(_)), "{phase}");
        }
        assert_eq!(snapshot(&db.conn()?, &[])?, before, "{phase}");
    }
    Ok(())
}

#[test]
fn legacy_classification_does_not_mark_close_or_adopt_staging() -> Result<()> {
    let kind = Kind::TerminalNoRoute { max_attempts: 1 };
    let db = Db::new(kind, TokenQuantity::new(7000, 3))?;
    db.conn()?
        .execute("DELETE FROM execution_source_sell_promotions", [])?;
    let before = snapshot(&db.conn()?, &[])?;
    assert_eq!(db.run(kind)?, Outcome::NotPromoted);
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    Ok(())
}
