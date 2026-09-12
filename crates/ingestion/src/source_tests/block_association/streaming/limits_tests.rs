use super::*;

fn second(tx: &SubscribeUpdate) -> SubscribeUpdate {
    let mut second = tx.clone();
    tx_mut(&mut second).transaction.as_mut().unwrap().signature = vec![89; 64];
    second
}
#[test]
fn zero_and_overflow_limits_are_rejected_without_defaults() -> Result<()> {
    let source = YellowstoneGrpcSource::new(&config())?;
    for index in 0..11 {
        let mut l = limits();
        match index {
            0 => l.pending.count = 0,
            1 => l.pending.encoded_bytes = 0,
            2 => l.blocks.count = 0,
            3 => l.blocks.encoded_bytes = 0,
            4 => l.history.count = 0,
            5 => l.history.encoded_bytes = 0,
            6 => l.outputs.count = 0,
            7 => l.outputs.encoded_bytes = 0,
            8 => l.input_bytes = 0,
            9 => l.metadata_bytes = 0,
            _ => l.pending_ttl = Duration::ZERO,
        }
        let c = &source.runtime_config;
        assert!(matches!(
            YellowstoneAssociation::new(
                session(),
                l,
                Programs {
                    interested: &c.interested_program_ids,
                    raydium: &c.raydium_program_ids,
                    pumpswap: &c.pumpswap_program_ids
                }
            ),
            Err(InvalidLimits::Zero)
        ));
    }
    let mut l = limits();
    l.history.count = usize::MAX;
    let c = &source.runtime_config;
    assert!(matches!(
        YellowstoneAssociation::new(
            session(),
            l,
            Programs {
                interested: &c.interested_program_ids,
                raydium: &c.raydium_program_ids,
                pumpswap: &c.pumpswap_program_ids
            }
        ),
        Err(InvalidLimits::Overflow)
    ));
    Ok(())
}
#[test]
fn pending_count_and_encoded_n_plus_one_return_unresolved_not_loss() -> Result<()> {
    for count in [true, false] {
        let (tx, _) = pair(true, false)?;
        let other = second(&tx);
        let source = YellowstoneGrpcSource::new(&config())?;
        let mut l = limits();
        if count {
            l.pending.count = 1;
        } else {
            l.pending.encoded_bytes = transaction(&tx).encoded_len();
        }
        let mut a = adapter(&source.runtime_config, l);
        assert!(feed(&mut a, 0, tx_input(&tx)).is_empty());
        let out = feed(&mut a, 1, tx_input(&other));
        unresolved(&out[0], UnresolvedReason::PendingCapacity);
        assert_facts(&other, terminal(&out[0]).0);
        assert!(feed(&mut a, 2, tx_input(&other)).is_empty());
        let out = feed(&mut a, 3, Input::End);
        assert_eq!(out.len(), 1);
        unresolved(&out[0], UnresolvedReason::EndOfStream);
        assert_facts(&tx, terminal(&out[0]).0);
    }
    Ok(())
}
#[test]
fn history_count_and_encoded_capacity_reject_unaccepted_input_preserving_prior() -> Result<()> {
    for count in [true, false] {
        let (tx, _) = pair(true, false)?;
        let other = second(&tx);
        let source = YellowstoneGrpcSource::new(&config())?;
        let mut l = limits();
        if count {
            l.history.count = 1;
        } else {
            l.history.encoded_bytes = transaction(&tx).encoded_len();
        }
        let mut a = adapter(&source.runtime_config, l);
        feed(&mut a, 0, tx_input(&tx));
        assert_eq!(
            a.push(context(1), tx_input(&other)),
            Err(Rejection::HistoryCapacity)
        );
        let out = feed(&mut a, 0, Input::End); // failed push did not advance clock
        assert_eq!(out.len(), 1);
        assert_facts(&tx, terminal(&out[0]).0);
    }
    Ok(())
}
#[test]
fn block_count_bytes_and_input_bytes_have_exact_boundaries() -> Result<()> {
    let (tx, b) = pair(true, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut other = b.clone();
    other.blockhash = bs58::encode([89; 32]).into_string();
    for count in [true, false] {
        let mut l = limits();
        if count {
            l.blocks.count = 1;
        } else {
            l.blocks.encoded_bytes = b.encoded_len();
        }
        let mut a = adapter(&source.runtime_config, l);
        feed(&mut a, 0, Input::Block(&b));
        assert!(feed(&mut a, 1, Input::Block(&b)).is_empty()); // duplicate consumes no block capacity
        assert_eq!(
            a.push(context(2), Input::Block(&other)),
            Err(Rejection::BlockCapacity)
        );
        assert!(matches!(
            terminal(&feed(&mut a, 1, tx_input(&tx))[0]).1,
            Resolution::ProviderAsserted { .. }
        ));
    }
    for block in [false, true] {
        for exact in [false, true] {
            let n = if block {
                b.encoded_len()
            } else {
                transaction(&tx).encoded_len()
            };
            let mut l = limits();
            l.input_bytes = n - usize::from(!exact);
            let mut a = adapter(&source.runtime_config, l);
            let result = a.push(
                context(0),
                if block {
                    Input::Block(&b)
                } else {
                    tx_input(&tx)
                },
            );
            if exact {
                assert!(result.is_ok());
            } else {
                assert_eq!(result, Err(Rejection::InputTooLarge));
            }
        }
    }
    Ok(())
}
#[test]
fn output_count_bytes_and_busy_cannot_drop_terminal_results() -> Result<()> {
    let (tx, _) = pair(true, false)?;
    let other = second(&tx);
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut l = limits();
    l.outputs.count = 1;
    let mut a = adapter(&source.runtime_config, l);
    feed(&mut a, 0, tx_input(&tx));
    feed(&mut a, 1, tx_input(&other));
    a.push(context(2), Input::End).unwrap();
    assert_eq!(a.push(context(3), tx_input(&tx)), Err(Rejection::Busy));
    let first = a.drain();
    assert_eq!(first.outcomes.len(), 1);
    assert!(!first.complete);
    assert_eq!(a.push(context(3), Input::Tick), Err(Rejection::Busy));
    let charge = first.charged_bytes;
    let second = drain(&mut a);
    assert_eq!(second.len(), 1);
    for exact in [false, true] {
        let mut l = limits();
        l.outputs.encoded_bytes = charge - usize::from(!exact);
        let mut a = adapter(&source.runtime_config, l);
        let result = a.push(context(0), tx_input(&tx));
        if exact {
            assert!(result.is_ok());
            drain(&mut a);
            feed(&mut a, 1, tx_input(&other));
            a.push(context(2), Input::End).unwrap();
            let first = a.drain();
            assert_eq!(first.outcomes.len(), 1);
            assert_eq!(first.charged_bytes, charge);
            assert_eq!(drain(&mut a).len(), 1);
        } else {
            assert_eq!(result, Err(Rejection::OutputCapacity));
        }
    }
    Ok(())
}
#[test]
fn block_expiry_at_n_and_n_plus_one_and_reset_isolation() -> Result<()> {
    let (tx, b) = pair(false, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    for ns in [10, 11] {
        let mut l = limits();
        l.block_ttl = Duration::from_nanos(10);
        let mut a = adapter(&source.runtime_config, l);
        feed(&mut a, 0, Input::Block(&b));
        let out = feed(&mut a, ns, tx_input(&tx));
        if ns == 10 {
            assert_eq!(out.len(), 1);
        } else {
            assert!(out.is_empty());
        }
    }
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, Input::Block(&b));
    let next = Session {
        generation: 2,
        ..session()
    };
    feed(&mut a, 1, Input::Reset(next));
    a.push(
        Context {
            session: next,
            offset: Duration::ZERO,
        },
        tx_input(&tx),
    )
    .unwrap();
    assert!(drain(&mut a).is_empty());
    Ok(())
}
