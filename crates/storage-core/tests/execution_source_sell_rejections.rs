#[path = "common/source_sell_fixture.rs"]
mod fixture;
#[path = "common/buy_receipt_history.rs"]
mod history;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::ExecutionSourceSellReject as Reject;
use fixture::*;

#[test]
fn unknown_no_order_null_and_incompatible_buy_chains_never_prove_source() -> Result<()> {
    for case in [
        "no_order",
        "null",
        "missing_order",
        "missing_facts",
        "wrong_signature",
        "wrong_operands",
        "wrong_source",
    ] {
        let mut db = Db::new()?;
        if case == "no_order" {
            db.store.record_execution_canary_open_position(
                "imported",
                "mint",
                7.0,
                Some(TokenQuantity::new(7000, 3)),
                0.000001,
                db.now,
            )?;
        } else {
            db.proven("a", "source-a")?;
            let sql = match case {
                "null" => "UPDATE fills SET position_id=NULL",
                "missing_order" => "DELETE FROM orders",
                "missing_facts" => "DELETE FROM execution_canary_receipt_facts",
                "wrong_signature" => {
                    "UPDATE execution_canary_receipt_proofs SET tx_signature='other'"
                }
                "wrong_operands" => "UPDATE fills SET qty_raw='7001'",
                _ => "UPDATE copy_signals SET wallet_id='source-b'",
            };
            db.conn()?
                .execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        }
        let event = db.observed("sell", "source-a")?;
        let position = db.position()?;
        db.reopen()?;
        db.rejected(&event, &position, Reject::SourceNotProven)?;
    }
    Ok(())
}

#[test]
fn receipt_collision_revokes_membership_and_replay_but_preserves_independent_source() -> Result<()>
{
    let mut db = Db::new()?;
    db.proven("dup-a", "source-a")?;
    let position = db.position()?;
    let event = db.observed("was-proven", "source-a")?;
    inserted(
        db.store
            .stage_execution_source_sell_intent(&event, &position)?,
    );
    let duplicate = db.seed("dup-b", "source-b", "buy")?;
    // Restore historical receipt collision after a valid public BUY with a unique key.
    history::buy(&db, &duplicate, "mint")?;
    let independent = db.proven("c", "source-c")?;
    db.reopen()?;
    db.rejected(&event, &position, Reject::WitnessNoLongerProven)?;
    for source in ["source-a", "source-b"] {
        let event = db.observed(source, source)?;
        db.rejected(&event, &position, Reject::SourceNotProven)?;
    }
    let event = db.observed("c", "source-c")?;
    assert_eq!(
        inserted(
            db.store
                .stage_execution_source_sell_intent(&event, &position)?
        )
        .buy_witness
        .order_id,
        independent
    );
    Ok(())
}

#[test]
fn full_observed_identity_and_exact_operands_are_checked_before_staging_and_replay() -> Result<()> {
    for replay in [false, true] {
        for case in [
            "missing",
            "wallet",
            "token",
            "signature",
            "dex",
            "slot",
            "timestamp",
            "amount_in",
            "amount_out",
            "raw_in",
            "raw_out",
            "decimals",
            "missing_exact",
        ] {
            let db = Db::new()?;
            db.proven("a", "source-a")?;
            let position = db.position()?;
            let mut event = db.observed("sell", "source-a")?;
            if replay {
                inserted(
                    db.store
                        .stage_execution_source_sell_intent(&event, &position)?,
                );
            }
            match case {
                "missing" => {
                    db.conn()?.execute("DELETE FROM observed_swaps", [])?;
                }
                "wallet" => event.wallet = "another-source".into(),
                "token" => event.token_in = "another-mint".into(),
                "signature" => event.signature = "another-event".into(),
                "dex" => event.dex = "other-dex".into(),
                "slot" => event.slot += 1,
                "timestamp" => event.ts_utc += Duration::nanoseconds(1),
                "amount_in" => event.amount_in += 0.1,
                "amount_out" => event.amount_out += 0.1,
                "raw_in" => event.exact_amounts.as_mut().unwrap().amount_in_raw = "4001".into(),
                "raw_out" => {
                    event.exact_amounts.as_mut().unwrap().amount_out_raw = "100000001".into()
                }
                "decimals" => event.exact_amounts.as_mut().unwrap().amount_in_decimals = 4,
                _ => event.exact_amounts = None,
            }
            db.rejected(&event, &position, Reject::ObservedEventMismatch)?;
        }
    }
    Ok(())
}

#[test]
fn invalid_sell_shape_is_rejected_without_relaxing_legacy_minima() -> Result<()> {
    for case in [
        "buy",
        "non_sol_out",
        "empty_wallet",
        "empty_signature",
        "zero",
        "nan",
        "infinity",
        "negative",
        "tiny",
        "zero_raw",
        "bad_raw",
        "wrong_sol_decimals",
    ] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        let position = db.position()?;
        let mut event = db.sell("sell", "source-a");
        match case {
            "buy" => std::mem::swap(&mut event.token_in, &mut event.token_out),
            "non_sol_out" => event.token_out = "another-mint".into(),
            "empty_wallet" => event.wallet.clear(),
            "empty_signature" => event.signature.clear(),
            "zero" => event.amount_in = 0.0,
            "nan" => event.amount_in = f64::NAN,
            "infinity" => event.amount_out = f64::INFINITY,
            "negative" => event.amount_out = -1.0,
            "tiny" => event.amount_in = 1e-12,
            "zero_raw" => event.exact_amounts.as_mut().unwrap().amount_in_raw = "0".into(),
            "bad_raw" => event.exact_amounts.as_mut().unwrap().amount_out_raw = "bad".into(),
            _ => event.exact_amounts.as_mut().unwrap().amount_out_decimals = 8,
        }
        db.rejected(&event, &position, Reject::InvalidSell)?;
    }
    Ok(())
}

#[test]
fn temporal_latest_buy_shadow_and_inventory_controls_remain_effective() -> Result<()> {
    for case in [
        "before_position",
        "latest_buy",
        "shadow",
        "future_shadow",
        "wrong_generation",
        "closed",
        "zero_qty",
        "zero_raw",
    ] {
        let mut db = Db::new()?;
        db.proven("a", "source-a")?;
        let mut position = db.position()?;
        let mut event = db.sell("sell", "source-a");
        let reason = match case {
            "before_position" => {
                event.ts_utc = db.now - Duration::nanoseconds(1);
                Reject::SellBeforePosition
            }
            "latest_buy" => {
                db.now = event.ts_utc + Duration::nanoseconds(1);
                db.proven("confirmed-new-buy", "source-b")?;
                Reject::SellBeforeLatestBuy
            }
            "shadow" | "future_shadow" => {
                db.store.insert_shadow_lot(
                    "source-a",
                    "mint",
                    1.0,
                    0.1,
                    event.ts_utc + Duration::nanoseconds(i64::from(case == "future_shadow")),
                )?;
                Reject::ShadowRiskPresent
            }
            "wrong_generation" => {
                position = "not-the-current-generation".into();
                Reject::GenerationMismatch
            }
            "closed" => {
                db.close()?;
                Reject::NoOwnedPosition
            }
            "zero_qty" => {
                db.conn()?.execute("UPDATE positions SET qty=0", [])?;
                Reject::NoOwnedPosition
            }
            _ => {
                db.conn()?.execute("UPDATE positions SET qty_raw='0'", [])?;
                Reject::NoOwnedPosition
            }
        };
        db.store.insert_observed_swap(&event)?;
        if case == "future_shadow" {
            inserted(
                db.store
                    .stage_execution_source_sell_intent(&event, &position)?,
            );
        } else {
            db.rejected(&event, &position, reason)?;
        }
    }
    Ok(())
}

#[test]
fn legacy_nonexact_observed_operands_remain_data_without_invented_sizing() -> Result<()> {
    let mut db = Db::new()?;
    db.proven("a", "source-a")?;
    let position = db.position()?;
    let mut event = db.sell("legacy-operands", "source-a");
    event.exact_amounts = None;
    event.amount_in = 999.5; // Original leader event can exceed follower inventory.
    db.store.insert_observed_swap(&event)?;
    let row = inserted(
        db.store
            .stage_execution_source_sell_intent(&event, &position)?,
    );
    assert_eq!(row.event.amount_in, 999.5);
    assert!(row.event.exact_amounts.is_none());
    db.reopen()?;
    assert_eq!(
        format!(
            "{:?}",
            db.store
                .load_execution_source_sell_intent(&row.intent_id)?
                .unwrap()
        ),
        format!("{row:?}")
    );
    Ok(())
}
