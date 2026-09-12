use super::*;

#[test]
fn metadata_and_unchecked_inputs_have_no_hidden_admission() -> Result<()> {
    let (tx, b) = pair(true, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    // Obtain the production output charge to derive the documented per-record
    // metadata charge, then check N/N+1 admission through the public adapter.
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, tx_input(&tx));
    a.push(context(1), Input::End).unwrap();
    let metadata = a.drain().charged_bytes - transaction(&tx).encoded_len() - 512;
    for exact in [true, false] {
        let mut l = limits();
        l.metadata_bytes = metadata - usize::from(!exact);
        let mut a = adapter(&source.runtime_config, l);
        let result = a.push(context(0), tx_input(&tx));
        if exact {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(Rejection::MetadataCapacity));
        }
    }
    let mut l = limits();
    l.metadata_bytes = 512 + b.transactions.len() * 128;
    let mut a = adapter(&source.runtime_config, l);
    a.push(context(0), Input::Block(&b)).unwrap();
    drain(&mut a);
    assert_eq!(
        a.push(context(1), tx_input(&tx)),
        Err(Rejection::MetadataCapacity)
    );
    let mut a = adapter(&source.runtime_config, limits());
    let mut vote = tx.clone();
    tx_mut(&mut vote).transaction.as_mut().unwrap().is_vote = true;
    assert!(matches!(
        a.push(context(0), tx_input(&vote)),
        Ok(Admission::NotChecked { .. })
    ));
    assert!(drain(&mut a).is_empty());
    assert!(feed(&mut a, 1, Input::End).is_empty());
    Ok(())
}
#[test]
fn admission_preserves_api80_bounds_and_invalid_identity() -> Result<()> {
    let (tx, b) = pair(true, true)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    for change in 0..4 {
        let mut damaged = tx.clone();
        let expected = match change {
            0 => {
                tx_mut(&mut damaged).transaction = None;
                Rejection::InputBounds(AssociationRefusal::MissingExpectedInfo)
            }
            1 => {
                tx_mut(&mut damaged)
                    .transaction
                    .as_mut()
                    .unwrap()
                    .signature
                    .pop();
                Rejection::InputBounds(AssociationRefusal::InvalidSignatureLength(63))
            }
            2 => {
                tx_mut(&mut damaged)
                    .transaction
                    .as_mut()
                    .unwrap()
                    .meta
                    .as_mut()
                    .unwrap()
                    .log_messages = vec![String::new(); MAX_INFO_ITEMS + 1];
                Rejection::InputBounds(AssociationRefusal::InfoCardinalityExceeded(
                    InfoSide::Expected,
                ))
            }
            _ => {
                tx_mut(&mut damaged)
                    .transaction
                    .as_mut()
                    .unwrap()
                    .meta
                    .as_mut()
                    .unwrap()
                    .log_messages = vec!["x".repeat(MAX_INFO_BYTES)];
                Rejection::InputBounds(AssociationRefusal::InfoTooLarge(InfoSide::Expected))
            }
        };
        let mut a = adapter(&source.runtime_config, limits());
        assert_eq!(a.push(context(0), tx_input(&damaged)), Err(expected));
        assert!(feed(&mut a, 1, Input::End).is_empty());
    }
    let mut a = adapter(&source.runtime_config, limits());
    let mut large = b;
    large
        .transactions
        .resize(MAX_BLOCK_TRANSACTIONS + 1, Default::default());
    assert_eq!(
        a.push(context(0), Input::Block(&large)),
        Err(Rejection::InputBounds(
            AssociationRefusal::TooManyTransactions(MAX_BLOCK_TRANSACTIONS + 1)
        ))
    );
    Ok(())
}
#[test]
fn late_info_conflict_references_first_assertion_without_second_trade() -> Result<()> {
    let (tx, b) = pair(true, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, tx_input(&tx));
    let first = feed(&mut a, 1, Input::Block(&b));
    let original = terminal(&first[0]).1;
    let mut conflict = b.clone();
    conflict.transactions[0].meta.as_mut().unwrap().fee += 1;
    let out = feed(&mut a, 2, Input::Block(&conflict));
    match &out[..] {
        [Outcome::Late {
            id: ResultId(0),
            session: origin_session,
            signature,
            slot,
            original: prior,
            evidence: LateEvidence::Association(AssociationRefusal::InfoMismatch),
        }] => {
            assert_eq!(prior, original);
            assert_eq!(*origin_session, session());
            assert_eq!(signature, &terminal(&first[0]).0.facts.signature);
            assert_eq!(*slot, transaction(&tx).slot);
        }
        _ => panic!("must be a linked notice: {out:?}"),
    }
    assert!(feed(&mut a, 3, Input::Block(&conflict)).is_empty());
    assert!(feed(&mut a, 4, Input::End).is_empty());
    Ok(())
}
#[test]
fn cross_slot_and_block_meta_supply_no_association_or_time_fallback() -> Result<()> {
    let (mut tx, mut b) = pair(true, true)?;
    tx.created_at = None;
    b.slot += 1;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, tx_input(&tx));
    assert!(feed(&mut a, 1, Input::Block(&b)).is_empty());
    // There is deliberately no BlockMeta/now input variant.
    let out = feed(&mut a, 2, Input::End);
    unresolved(&out[0], UnresolvedReason::EndOfStream);
    assert_facts(&tx, terminal(&out[0]).0);
    Ok(())
}
