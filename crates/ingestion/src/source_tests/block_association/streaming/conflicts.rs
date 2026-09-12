use super::*;

#[test]
fn not_found_is_only_one_filtered_message_and_later_block_can_match() -> Result<()> {
    let (tx, b) = pair(true, false)?;
    let mut missing = b.clone();
    missing.transactions.clear();
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, tx_input(&tx));
    assert!(feed(&mut a, 1, Input::Block(&missing)).is_empty());
    let out = feed(&mut a, 2, Input::Block(&b));
    assert!(matches!(
        terminal(&out[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    Ok(())
}
#[test]
fn info_mismatch_and_duplicate_signature_preserve_original_facts() -> Result<()> {
    for duplicate in [false, true] {
        let (tx, mut b) = pair(true, true)?;
        let reason = if duplicate {
            b.transactions.push(b.transactions[0].clone());
            AssociationRefusal::DuplicateSignature
        } else {
            b.transactions[0].meta.as_mut().unwrap().fee += 1;
            AssociationRefusal::InfoMismatch
        };
        let source = YellowstoneGrpcSource::new(&config())?;
        let mut a = adapter(&source.runtime_config, limits());
        feed(&mut a, 0, tx_input(&tx));
        let out = feed(&mut a, 1, Input::Block(&b));
        unresolved(&out[0], UnresolvedReason::Association(reason));
        assert_facts(&tx, terminal(&out[0]).0);
    }
    Ok(())
}
#[test]
fn fork_and_time_conflicts_before_and_after_terminal_have_distinct_outcomes() -> Result<()> {
    for fork in [false, true] {
        for before in [false, true] {
            let (tx, b) = pair(true, true)?;
            let mut conflict = b.clone();
            if fork {
                conflict.blockhash = bs58::encode([88; 32]).into_string();
            } else {
                conflict.block_time.as_mut().unwrap().timestamp += 1;
            }
            let source = YellowstoneGrpcSource::new(&config())?;
            let mut a = adapter(&source.runtime_config, limits());
            feed(&mut a, 0, Input::Block(&b));
            let out = if before {
                feed(&mut a, 1, Input::Block(&conflict));
                feed(&mut a, 2, tx_input(&tx))
            } else {
                let first = feed(&mut a, 1, tx_input(&tx));
                assert!(matches!(
                    terminal(&first[0]).1,
                    Resolution::ProviderAsserted { .. }
                ));
                feed(&mut a, 2, Input::Block(&conflict))
            };
            if before {
                unresolved(&out[0], UnresolvedReason::ConflictingAssertions);
            } else {
                assert!(matches!(
                    &out[..],
                    [Outcome::Late {
                        id: ResultId(0),
                        original: Resolution::ProviderAsserted { .. },
                        evidence: LateEvidence::ProviderAssertion { .. },
                        ..
                    }]
                ));
            }
            assert!(feed(&mut a, 3, tx_input(&tx)).is_empty());
            assert!(feed(&mut a, 4, Input::Block(&conflict)).is_empty());
        }
    }
    Ok(())
}
#[test]
fn conflicting_transaction_slot_info_and_float_bits_are_never_last_wins() -> Result<()> {
    for change in [0, 1, 2] {
        for after in [false, true] {
            let (mut tx, mut b) = pair(true, false)?;
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
            b.transactions[0] = transaction(&tx).transaction.as_ref().unwrap().clone();
            let mut conflict = tx.clone();
            match change {
                0 => tx_mut(&mut conflict).slot += 1,
                1 => {
                    tx_mut(&mut conflict)
                        .transaction
                        .as_mut()
                        .unwrap()
                        .meta
                        .as_mut()
                        .unwrap()
                        .fee += 1
                }
                _ => {
                    tx_mut(&mut conflict)
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
                        .ui_amount = -0.0
                }
            }
            let source = YellowstoneGrpcSource::new(&config())?;
            let mut a = adapter(&source.runtime_config, limits());
            feed(&mut a, 0, tx_input(&tx));
            if after {
                assert_eq!(feed(&mut a, 1, Input::Block(&b)).len(), 1);
            }
            let out = feed(&mut a, 2, tx_input(&conflict));
            if after {
                assert!(matches!(
                    &out[..],
                    [Outcome::Late {
                        evidence: LateEvidence::ConflictingTransaction,
                        ..
                    }]
                ));
            } else {
                unresolved(&out[0], UnresolvedReason::ConflictingTransaction);
                assert_facts(&tx, terminal(&out[0]).0);
            }
            assert!(feed(&mut a, 3, tx_input(&conflict)).is_empty());
        }
    }
    Ok(())
}
