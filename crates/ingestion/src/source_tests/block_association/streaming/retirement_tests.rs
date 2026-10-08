//! Complete-recovery retirement leaves generic arrival-order/history semantics intact.
use super::*;
use std::collections::HashSet;

#[test]
fn retirement_refuses_busy_and_pending_then_releases_only_complete_blocks() -> Result<()> {
    let (tx, block) = pair(false, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    a.push(context(0), tx_input(&tx)).unwrap();
    assert_eq!(
        a.retire_complete_blocks_through(block.slot),
        Err(Rejection::Busy)
    );
    assert!(drain(&mut a).is_empty());
    let mut absent = block.clone();
    absent.transactions.clear();
    feed(&mut a, 1, Input::Block(&absent));
    assert_eq!(
        a.retire_complete_blocks_through(block.slot),
        Err(Rejection::BlockRetirementPending)
    );
    assert_eq!(a.block_cache_usage().0, 1);
    let terminal = feed(&mut a, 2, Input::Block(&block));
    assert_eq!(terminal.len(), 1);
    assert_facts(&tx, super::terminal(&terminal[0]).0);
    assert_eq!(a.retire_complete_blocks_through(block.slot).unwrap(), 2);
    assert_eq!(a.block_cache_usage(), (0, 0));
    assert!(
        feed(&mut a, 3, tx_input(&tx)).is_empty(),
        "retained original identity is still duplicate"
    );
    Ok(())
}

#[test]
fn retirement_preserves_original_float_info_and_late_conflicts_even_for_foreign_signer(
) -> Result<()> {
    for change in 0..3 {
        let (mut tx, mut block) = pair(false, false)?;
        tx_mut(&mut tx)
            .transaction
            .as_mut()
            .unwrap()
            .meta
            .as_mut()
            .unwrap()
            .pre_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .ui_amount = 0.0;
        block.transactions[0] = transaction(&tx).transaction.as_ref().unwrap().clone();
        let source = YellowstoneGrpcSource::new(&config())?;
        let mut a = adapter(&source.runtime_config, limits());
        let wallet = decode_yellowstone_swap_facts(
            transaction(&tx),
            &source.runtime_config.interested_program_ids,
            &source.runtime_config.raydium_program_ids,
            &source.runtime_config.pumpswap_program_ids,
        )
        .facts
        .unwrap()
        .unwrap()
        .signer;
        let scope = HashSet::from([wallet]);
        a.restrict_wallets(&scope);
        feed(&mut a, 0, Input::Block(&block));
        let first = feed(&mut a, 1, tx_input(&tx));
        assert!(matches!(
            super::terminal(&first[0]).1,
            Resolution::ProviderAsserted { .. }
        ));
        a.retire_complete_blocks_through(block.slot).unwrap();
        let mut changed = tx.clone();
        let info = tx_mut(&mut changed).transaction.as_mut().unwrap();
        match change {
            0 => info.meta.as_mut().unwrap().fee += 1,
            1 => {
                info.meta.as_mut().unwrap().pre_token_balances[0]
                    .ui_token_amount
                    .as_mut()
                    .unwrap()
                    .ui_amount = -0.0
            }
            _ => {
                info.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .account_keys[0] = vec![244; 32]
            }
        }
        let notice = feed(&mut a, 2, tx_input(&changed));
        assert!(matches!(
            &notice[..],
            [Outcome::Late {
                evidence: LateEvidence::ConflictingTransaction,
                original: Resolution::ProviderAsserted { .. },
                ..
            }]
        ));
        assert!(feed(&mut a, 3, tx_input(&changed)).is_empty());
        assert!(feed(&mut a, 4, tx_input(&tx)).is_empty());
    }
    // A NEW containing block can still supply late contradictory Info after the
    // old full block was released: original records/Info remain authoritative.
    let (tx, block) = pair(true, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, Input::Block(&block));
    feed(&mut a, 1, tx_input(&tx));
    a.retire_complete_blocks_through(block.slot).unwrap();
    let mut changed = block.clone();
    changed.transactions[0].meta.as_mut().unwrap().fee += 1;
    let notice = feed(&mut a, 2, Input::Block(&changed));
    assert!(matches!(
        &notice[..],
        [Outcome::Late {
            evidence: LateEvidence::Association(AssociationRefusal::InfoMismatch),
            ..
        }]
    ));
    Ok(())
}

#[test]
fn unknown_closed_info_cannot_reenter_via_old_block_not_checked_or_foreign_identity() -> Result<()>
{
    let (tx, block) = pair(false, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let wallet = decode_yellowstone_swap_facts(
        transaction(&tx),
        &source.runtime_config.interested_program_ids,
        &source.runtime_config.raydium_program_ids,
        &source.runtime_config.pumpswap_program_ids,
    )
    .facts
    .unwrap()
    .unwrap()
    .signer;
    let scope = HashSet::from([wallet]);
    let mut a = adapter(&source.runtime_config, limits());
    a.restrict_wallets(&scope);
    let mut unsupported = tx.clone();
    tx_mut(&mut unsupported)
        .transaction
        .as_mut()
        .unwrap()
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .config = Some(Default::default());
    assert!(matches!(
        a.push(context(0), tx_input(&unsupported)),
        Ok(Admission::NotChecked { .. })
    ));
    drain(&mut a);
    feed(&mut a, 1, Input::Block(&block));
    a.retire_complete_blocks_through(block.slot).unwrap();
    assert!(matches!(
        a.push(context(2), tx_input(&unsupported)),
        Ok(Admission::NotChecked { .. })
    ));
    drain(&mut a);
    assert_eq!(
        a.push(context(3), tx_input(&tx)),
        Err(Rejection::ClosedBlockUnknownInfo)
    );
    // Reintroducing an old full block does not reopen its guarded prefix.
    feed(&mut a, 3, Input::Block(&block));
    assert_eq!(
        a.push(context(4), tx_input(&tx)),
        Err(Rejection::ClosedBlockUnknownInfo)
    );
    let mut foreign = tx.clone();
    tx_mut(&mut foreign)
        .transaction
        .as_mut()
        .unwrap()
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .account_keys[0] = vec![244; 32];
    // Keep balances' ownership consistent so this is decoded ForeignSigner,
    // rather than a malformed facts refusal hiding the guard order.
    let original_wallet = scope.iter().next().unwrap();
    let meta = tx_mut(&mut foreign)
        .transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap();
    for row in meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances)
    {
        if &row.owner == original_wallet {
            row.owner = bs58::encode([244; 32]).into_string();
        }
    }
    assert!(matches!(
        a.push(context(4), tx_input(&foreign)),
        Ok(Admission::NotChecked {
            reason: NotCheckedReason::ForeignSigner,
            ..
        })
    ));
    drain(&mut a);
    assert_eq!(
        a.push(context(5), tx_input(&tx)),
        Err(Rejection::ClosedBlockUnknownInfo)
    );
    Ok(())
}

#[test]
fn generic_block_before_transaction_and_reset_generation_remain_unchanged() -> Result<()> {
    let (tx, block) = pair(false, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, Input::Block(&block));
    assert_eq!(
        feed(&mut a, 1, tx_input(&tx)).len(),
        1,
        "generic adapter has no retirement owner"
    );
    a.retire_complete_blocks_through(block.slot).unwrap();
    let next = Session {
        id: [91; 16],
        generation: 2,
    };
    feed(&mut a, 2, Input::Reset(next));
    a.push(
        Context {
            session: next,
            offset: Duration::ZERO,
        },
        tx_input(&tx),
    )
    .unwrap();
    assert!(drain(&mut a).is_empty());
    a.push(
        Context {
            session: next,
            offset: Duration::from_nanos(1),
        },
        Input::Block(&block),
    )
    .unwrap();
    assert_eq!(
        drain(&mut a).len(),
        1,
        "old closed prefix was cleared only by new generation"
    );
    Ok(())
}
