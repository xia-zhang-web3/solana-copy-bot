use super::*;
use copybot_core_types::TokenQuantity;

#[test]
fn partial_and_pending_open_risk_keep_pin_without_granting_execution_authority() -> Result<()> {
    for prune in [false, true] {
        for mode in [
            "partial",
            "pending",
            "witness-conflict",
            "observed-corruption",
        ] {
            let mut d = Db::new()?;
            d.proven("buy", "source-a")?;
            let event = d.observed("old-a", "source-a")?;
            let staged = inserted(
                d.store
                    .stage_execution_source_sell_intent(&event, &d.position()?)?,
            );
            let binding = promoted(
                d.store
                    .promote_execution_source_sell_intent(&staged.intent_id)?,
            );
            match mode {
                "partial" => {
                    let closed = d.store.close_execution_canary_open_position(
                        "mint",
                        1.0,
                        Some(TokenQuantity::new(1000, 3)),
                        0.01,
                        0.0,
                        event.ts_utc,
                    )?;
                    assert!(closed.remaining_position.is_some());
                }
                "pending" => {
                    d.seed_claim(
                        "pending-sell",
                        "source-a",
                        "sell",
                        "mint",
                        "execution-wallet",
                        "pending-signature",
                    )?;
                    assert!(d.store.execution_canary_token_accounting_pending("mint")?);
                }
                "witness-conflict" => {
                    d.conn()?.execute(
                        "UPDATE execution_canary_receipt_facts SET tx_signature='damaged-witness'",
                        [],
                    )?;
                }
                _ => {
                    d.conn()?.execute(
                        "UPDATE observed_swaps SET qty_out=99 WHERE signature=?1",
                        [&event.signature],
                    )?;
                }
            }
            let money = snapshot(
                &d.conn()?,
                &[
                    "schema_migrations",
                    "observed_retention_boundary",
                    "recent_raw_journal_state",
                ],
            )?;
            let store = SqliteStore::open(&d.path)?;
            let cutoff = event.ts_utc + Duration::seconds(1);
            let count = if prune {
                store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff)?
            } else {
                store
                    .delete_observed_swaps_before_batched(cutoff, 1)?
                    .deleted_rows
            };
            assert_eq!(count, 0);
            drop(store);
            d.reopen()?;
            assert_eq!(
                snapshot(
                    &d.conn()?,
                    &[
                        "schema_migrations",
                        "observed_retention_boundary",
                        "recent_raw_journal_state"
                    ]
                )?,
                money
            );
            let saved = signal(&d, &binding)?;
            let reason = d
                .store
                .execution_sell_intent_position_block_reason(&saved)?;
            match mode {
                "pending" => assert_eq!(
                    d.store.execution_canary_receipt_submit_block_reason(
                        "another-attempt",
                        "mint",
                        "sell"
                    )?,
                    Some("sell_token_accounting_pending")
                ),
                "witness-conflict" => assert_eq!(reason, Some("source_sell_witness_not_proven")),
                "observed-corruption" => assert_eq!(reason, Some("source_sell_observed_mismatch")),
                _ => assert_eq!(reason, None),
            }
        }
    }
    Ok(())
}

#[test]
fn closing_original_a_releases_pin_despite_new_b_of_same_mint() -> Result<()> {
    for prune in [false, true] {
        let mut d = Db::new()?;
        d.proven("buy-a", "source-a")?;
        let event = d.observed("old-a", "source-a")?;
        let a = inserted(
            d.store
                .stage_execution_source_sell_intent(&event, &d.position()?)?,
        );
        let binding = promoted(d.store.promote_execution_source_sell_intent(&a.intent_id)?);
        d.close()?;
        d.proven("buy-b", "source-a")?;
        let position_b = d.position()?;
        assert_ne!(position_b, a.position_id);
        assert_eq!(
            d.store
                .execution_sell_intent_position_block_reason(&signal(&d, &binding)?)?,
            Some("source_sell_generation_mismatch")
        );
        let store = SqliteStore::open(&d.path)?;
        let cutoff = event.ts_utc + Duration::seconds(1);
        let count = if prune {
            store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff)?
        } else {
            store
                .delete_observed_swaps_before_batched(cutoff, 1)?
                .deleted_rows
        };
        assert_eq!(count, 1);
        drop(store);
        d.reopen()?;
        assert_eq!(d.position()?, position_b);
        assert_eq!(
            d.store
                .load_execution_source_sell_intent(&a.intent_id)?
                .unwrap()
                .position_id,
            a.position_id
        );
        assert!(d
            .store
            .execution_sell_intent_position_block_reason(&signal(&d, &binding)?)?
            .is_some());
        assert!(matches!(
            d.store.promote_execution_source_sell_intent(&a.intent_id)?,
            Outcome::Rejected(_)
        ));
    }
    Ok(())
}
