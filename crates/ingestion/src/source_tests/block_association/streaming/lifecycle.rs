use super::*;

#[test]
fn both_arrival_orders_keep_buy_sell_exact_facts_and_three_message_times() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for block_first in [false, true] {
                for t in [
                    Some(yellowstone_grpc_proto::prost_types::Timestamp {
                        seconds: 1788868701,
                        nanos: 123,
                    }),
                    None,
                    Some(yellowstone_grpc_proto::prost_types::Timestamp {
                        seconds: 1788868701,
                        nanos: -1,
                    }),
                ] {
                    let (mut tx, b) = pair(sell, native)?;
                    tx.created_at = t;
                    let source = YellowstoneGrpcSource::new(&config())?;
                    let mut a = adapter(&source.runtime_config, limits());
                    let result = if block_first {
                        assert!(feed(&mut a, 0, Input::Block(&b)).is_empty());
                        feed(&mut a, 1, tx_input(&tx))
                    } else {
                        assert!(feed(&mut a, 0, tx_input(&tx)).is_empty());
                        feed(&mut a, 1, Input::Block(&b))
                    };
                    assert_eq!(result.len(), 1);
                    let (checked, resolution) = terminal(&result[0]);
                    assert_facts(&tx, checked);
                    assert_eq!(
                        checked.message_time,
                        YellowstoneMessageTime::from_created_at(t.as_ref())
                    );
                    assert!(matches!(resolution, Resolution::ProviderAsserted { .. }));
                    assert!(feed(&mut a, 2, tx_input(&tx)).is_empty());
                    assert!(feed(&mut a, 3, Input::Block(&b)).is_empty());
                    assert!(feed(&mut a, 4, Input::End).is_empty());
                }
            }
        }
    }
    Ok(())
}

#[test]
fn provider_time_missing_and_out_of_range_are_separate_from_facts() -> Result<()> {
    for timestamp in [None, Some(i64::MAX), Some(1788868700)] {
        let (mut tx, mut b) = pair(true, false)?;
        tx.created_at = None;
        b.block_time = timestamp.map(|timestamp| UnixTimestamp { timestamp });
        let source = YellowstoneGrpcSource::new(&config())?;
        let mut a = adapter(&source.runtime_config, limits());
        feed(&mut a, 0, tx_input(&tx));
        let result = feed(&mut a, 1, Input::Block(&b));
        let (checked, resolution) = terminal(&result[0]);
        assert_facts(&tx, checked);
        assert_eq!(
            checked.message_time,
            YellowstoneMessageTime::UnresolvedCreatedAt(CreatedAtUnavailable::Missing)
        );
        let expected_time = match timestamp {
            None => ProviderBlockTime::Missing,
            Some(i64::MAX) => ProviderBlockTime::OutOfRange(i64::MAX),
            Some(t) => ProviderBlockTime::AvailableBlockTime(
                chrono::DateTime::from_timestamp(t, 0).unwrap(),
            ),
        };
        assert!(
            matches!(resolution, Resolution::ProviderAsserted { block_time, .. } if block_time == &expected_time)
        );
    }
    Ok(())
}

#[test]
fn timeout_is_inclusive_and_late_block_never_upgrades_unresolved() -> Result<()> {
    let (tx, b) = pair(true, true)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut l = limits();
    l.pending_ttl = Duration::from_nanos(10);
    let mut a = adapter(&source.runtime_config, l);
    assert!(feed(&mut a, 2, tx_input(&tx)).is_empty());
    assert!(feed(&mut a, 12, Input::Tick).is_empty());
    let out = feed(&mut a, 13, Input::Tick);
    unresolved(&out[0], UnresolvedReason::Expired);
    assert_facts(&tx, terminal(&out[0]).0);
    let late = feed(&mut a, 14, Input::Block(&b));
    assert!(matches!(
        &late[..],
        [Outcome::Late {
            id: ResultId(0),
            original: Resolution::Unresolved(UnresolvedReason::Expired),
            ..
        }]
    ));
    assert!(feed(&mut a, 15, tx_input(&tx)).is_empty());
    assert!(feed(&mut a, 16, Input::Block(&b)).is_empty());
    Ok(())
}

#[test]
fn reset_flushes_original_session_and_stale_or_regressing_input_is_unconsumed() -> Result<()> {
    let (tx, b) = pair(true, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 5, tx_input(&tx));
    assert_eq!(
        a.push(context(4), Input::Block(&b)),
        Err(Rejection::RegressingOffset)
    );
    let next = Session {
        id: [89; 16],
        generation: 2,
    };
    assert_eq!(
        a.push(context(5), Input::Reset(session())),
        Err(Rejection::InvalidReset)
    );
    let out = feed(&mut a, 6, Input::Reset(next));
    unresolved(&out[0], UnresolvedReason::SessionReset);
    assert_eq!(terminal(&out[0]).0.context.session, session());
    assert_eq!(
        a.push(context(7), Input::Block(&b)),
        Err(Rejection::StaleSession)
    );
    a.push(
        Context {
            session: next,
            offset: Duration::ZERO,
        },
        tx_input(&tx),
    )
    .unwrap();
    assert!(drain(&mut a).is_empty()); // old blocks are not reused
    a.push(
        Context {
            session: next,
            offset: Duration::from_nanos(1),
        },
        Input::End,
    )
    .unwrap();
    let out = drain(&mut a);
    unresolved(&out[0], UnresolvedReason::EndOfStream);
    assert_eq!(terminal(&out[0]).0.context.session, next);
    assert_eq!(
        a.push(
            Context {
                session: next,
                offset: Duration::from_nanos(2)
            },
            tx_input(&tx)
        ),
        Err(Rejection::Ended)
    );
    Ok(())
}

#[test]
fn reset_clears_retained_blocks_and_history_boundary_is_explicit() -> Result<()> {
    let (tx, b) = pair(false, false)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut l = limits();
    l.history_ttl = Duration::from_nanos(10);
    l.block_ttl = Duration::from_nanos(10);
    let mut a = adapter(&source.runtime_config, l);
    feed(&mut a, 0, Input::Block(&b));
    assert_eq!(feed(&mut a, 0, tx_input(&tx)).len(), 1);
    assert!(feed(&mut a, 10, tx_input(&tx)).is_empty());
    assert!(feed(&mut a, 11, tx_input(&tx)).is_empty());
    let out = feed(&mut a, 12, Input::End);
    assert!(matches!(
        &out[..],
        [Outcome::Terminal {
            id: ResultId(1),
            ..
        }]
    ));
    unresolved(&out[0], UnresolvedReason::EndOfStream);
    Ok(())
}
