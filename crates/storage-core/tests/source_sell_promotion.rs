#[path = "common/source_sell_promotion_fixture.rs"]
mod fixture;
#[path = "common/buy_receipt_history.rs"]
mod history;
use anyhow::Result;
use copybot_core_types::{
    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage_core::ExecutionSourceSellReject as Validation;
use fixture::*;

#[test]
fn promotion_preserves_unfollowed_event_notional_witness_and_runnable_identity_after_reopen(
) -> Result<()> {
    for exact in [false, true] {
        let mut db = Db::new()?;
        db.proven("a", "source-a")?;
        db.proven("b", "source-b")?;
        db.store
            .activate_follow_wallet("source-a", db.now, "test")?;
        let mut event = db.sell("exit", "source-a");
        if !exact {
            event.exact_amounts = None;
        }
        db.store.insert_observed_swap(&event)?;
        let staged = inserted(
            db.store
                .stage_execution_source_sell_intent(&event, &db.position()?)?,
        );
        db.store
            .deactivate_follow_wallet("source-a", db.now, "unfollow")?;
        for status in ["shadow_recorded", "execution_sell_intent", "anything"] {
            assert!(db
                .store
                .list_execution_quote_canary_owned_sell_signal_candidate_ids(status, db.now, 10)?
                .is_empty());
        }
        let before = snapshot(&db.conn()?, &["copy_signals", MARKER])?;
        let signal_count: i64 =
            db.conn()?
                .query_row("SELECT count(*) FROM copy_signals", [], |r| r.get(0))?;
        let binding = promoted(
            db.store
                .promote_execution_source_sell_intent(&staged.intent_id)?,
        );
        assert_eq!(binding.signal_id, "shadow:exit:source-a:sell:mint");
        assert_eq!(binding.intent_id, staged.intent_id);
        let saved = signal(&db, &binding)?;
        assert_eq!(saved.ts, event.ts_utc);
        assert_eq!(saved.notional_sol, event.amount_out);
        assert_eq!(
            saved.notional_lamports.map(|x| x.as_u64()),
            exact.then_some(100_000_000)
        );
        assert_eq!(
            saved.notional_origin,
            if exact {
                COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
            } else {
                COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
            }
        );
        assert_eq!(saved.status, "execution_sell_intent");
        assert_eq!(
            db.store
                .execution_sell_intent_position_block_reason(&saved)?,
            None
        );
        assert_eq!(
            db.conn()?
                .query_row("SELECT count(*) FROM copy_signals", [], |r| r
                    .get::<_, i64>(0))?,
            signal_count + 1
        );
        assert_eq!(snapshot(&db.conn()?, &["copy_signals", MARKER])?, before);
        assert_eq!(
            db.store
                .list_execution_quote_canary_owned_sell_signal_candidate_ids(
                    "shadow_recorded",
                    db.now,
                    10
                )?,
            vec![binding.signal_id.clone()]
        );
        db.reopen()?;
        let all_before = snapshot(&db.conn()?, &[])?;
        assert!(
            matches!(db.store.promote_execution_source_sell_intent(&staged.intent_id)?, Outcome::Existing(b) if b == binding)
        );
        assert_eq!(snapshot(&db.conn()?, &[])?, all_before);
        assert_eq!(
            format!(
                "{:?}",
                db.store
                    .load_execution_source_sell_intent(&staged.intent_id)?
                    .unwrap()
            ),
            format!("{staged:?}")
        );
    }
    Ok(())
}

#[test]
fn equal_timestamp_a_to_b_is_rejected_before_promotion_and_by_guard_after_promotion() -> Result<()>
{
    for promote_first in [false, true] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        let a = prepare(&db, "exit-a", "source-a")?;
        let signal_a = if promote_first {
            Some(signal(
                &db,
                &promoted(
                    db.store
                        .promote_execution_source_sell_intent(&a.intent_id)?,
                ),
            )?)
        } else {
            None
        };
        db.close()?;
        db.proven("b", "source-a")?;
        assert_ne!(a.position_id, db.position()?);
        rejected(
            &db,
            &a.intent_id,
            Reject::Validation(Validation::GenerationMismatch),
        )?;
        if let Some(a) = signal_a {
            assert_eq!(
                db.store.execution_sell_intent_position_block_reason(&a)?,
                Some("source_sell_generation_mismatch")
            );
        }
        let b = prepare(&db, "exit-b", "source-a")?;
        let signal_b = signal(
            &db,
            &promoted(
                db.store
                    .promote_execution_source_sell_intent(&b.intent_id)?,
            ),
        )?;
        assert_eq!(
            db.store
                .execution_sell_intent_position_block_reason(&signal_b)?,
            None
        );
    }
    Ok(())
}

#[test]
fn receipt_collision_after_staging_cannot_replace_original_witness_with_an_independent_buy(
) -> Result<()> {
    for promote_first in [false, true] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        let a = prepare(&db, "exit", "source-a")?;
        let saved = if promote_first {
            Some(signal(
                &db,
                &promoted(
                    db.store
                        .promote_execution_source_sell_intent(&a.intent_id)?,
                ),
            )?)
        } else {
            None
        };
        db.proven("independent", "source-a")?;
        let duplicate = db.seed_claim(
            "duplicate",
            "source-c",
            "buy",
            "mint",
            &a.buy_execution_wallet,
            &a.buy_witness.tx_signature,
        )?;
        // Restore a historical collision that the current BUY writer no longer creates.
        history::buy(&db, &duplicate, "mint")?;
        rejected(
            &db,
            &a.intent_id,
            Reject::Validation(Validation::WitnessNoLongerProven),
        )?;
        if let Some(saved) = saved {
            assert_eq!(
                db.store
                    .execution_sell_intent_position_block_reason(&saved)?,
                Some("source_sell_witness_not_proven")
            );
        }
        let next = prepare(&db, "independent-exit", "source-a")?;
        assert_ne!(next.buy_witness, a.buy_witness);
        promoted(
            db.store
                .promote_execution_source_sell_intent(&next.intent_id)?,
        );
    }
    Ok(())
}

#[test]
fn current_observation_inventory_temporal_and_shadow_guards_are_revalidated() -> Result<()> {
    for case in [
        "retention",
        "observed",
        "shadow",
        "latest-buy",
        "closed",
        "wallet",
    ] {
        for promoted_first in [false, true] {
            let mut db = Db::new()?;
            db.proven("a", "source-a")?;
            let a = prepare(&db, "exit", "source-a")?;
            let saved = if promoted_first {
                Some(signal(
                    &db,
                    &promoted(
                        db.store
                            .promote_execution_source_sell_intent(&a.intent_id)?,
                    ),
                )?)
            } else {
                None
            };
            let reason = match case {
                "retention" => {
                    // Deliberate corruption: normal retention now pins this OPEN source.
                    db.conn()?.execute(
                        "DELETE FROM observed_swaps WHERE signature=?1",
                        [&a.event.signature],
                    )?;
                    Validation::ObservedEventMismatch
                }
                "observed" => {
                    db.conn()?
                        .execute("UPDATE observed_swaps SET qty_out_raw='100000001'", [])?;
                    Validation::ObservedEventMismatch
                }
                "shadow" => {
                    db.store
                        .insert_shadow_lot("source-a", "mint", 1.0, 0.1, db.now)?;
                    Validation::ShadowRiskPresent
                }
                "latest-buy" => {
                    db.now += chrono::Duration::seconds(2);
                    db.proven("later", "source-b")?;
                    Validation::SellBeforeLatestBuy
                }
                "closed" => {
                    db.close()?;
                    Validation::NoOwnedPosition
                }
                _ => {
                    db.conn()?.execute(
                        "UPDATE execution_source_sell_intents SET buy_execution_wallet='other'",
                        [],
                    )?;
                    Validation::WitnessNoLongerProven
                }
            };
            rejected(&db, &a.intent_id, Reject::Validation(reason))?;
            if let Some(saved) = saved {
                assert!(
                    db.store
                        .execution_sell_intent_position_block_reason(&saved)?
                        .is_some(),
                    "{case}"
                );
            }
        }
    }
    Ok(())
}
