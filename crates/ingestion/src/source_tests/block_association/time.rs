use super::*;
use chrono::{DateTime, Utc};

#[test]
fn actual_saved79_buy_sell_native_wsol_ignore_both_message_clocks() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            let (mut expected, block) = pair(sell, native)?;
            let source = YellowstoneGrpcSource::new(&config())?;
            let c = &source.runtime_config;
            let facts79 = decode_yellowstone_swap_facts(
                transaction(&expected),
                &c.interested_program_ids,
                &c.raydium_program_ids,
                &c.pumpswap_program_ids,
            )
            .facts?
            .unwrap();
            for (clock, created_at) in cases::times() {
                expected.created_at = created_at;
                let result =
                    observe(&format!("clock-{sell}-{native}-{clock}"), &expected, &block).unwrap();
                assert_eq!(
                    canonical(facts_json(&result.facts)),
                    canonical(facts_json(&facts79))
                );
                assert_eq!(result.provider_assertion.slot, transaction(&expected).slot);
                assert_eq!(result.provider_assertion.blockhash, block.blockhash);
                assert_eq!(
                    result.provider_assertion.signature.as_slice(),
                    block.transactions[0].signature
                );
                assert_eq!(
                    result.provider_assertion.transaction_index,
                    block.transactions[0].index
                );
                assert_eq!(
                    result.block_time,
                    Time::AvailableBlockTime(
                        DateTime::<Utc>::from_timestamp(1788868700, 0).unwrap()
                    )
                );
            }
        }
    }
    Ok(())
}

#[test]
fn only_provider_block_time_changes_and_unknown_retains_facts() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            let (mut expected, mut block) = pair(sell, native)?;
            expected.created_at = None;
            let original = associate(&expected, &block).unwrap();
            for seconds in [
                None,
                Some(0),
                Some(-1),
                Some(1788868800),
                Some(i64::MAX),
                Some(i64::MIN),
            ] {
                block.block_time = seconds.map(|timestamp| UnixTimestamp { timestamp });
                let result = observe(
                    &format!("time-{sell}-{native}-{seconds:?}"),
                    &expected,
                    &block,
                )
                .unwrap();
                assert_eq!(
                    canonical(facts_json(&result.facts)),
                    canonical(facts_json(&original.facts))
                );
                assert_eq!(result.provider_assertion, original.provider_assertion);
                let expected_time = match seconds {
                    None => Time::Missing,
                    Some(n) => DateTime::<Utc>::from_timestamp(n, 0)
                        .map(Time::AvailableBlockTime)
                        .unwrap_or(Time::OutOfRange(n)),
                };
                assert_eq!(result.block_time, expected_time);
                let source = YellowstoneGrpcSource::new(&config())?;
                assert_eq!(
                    super::super::legacy(expected.clone(), &source.runtime_config),
                    json!({"none":true})
                );
            }
        }
    }
    Ok(())
}

#[test]
fn two_forks_are_separate_provider_assertions_without_choice() -> Result<()> {
    let (expected, block_a) = pair(false, false)?;
    let mut block_b = block_a.clone();
    block_b.blockhash = bs58::encode([81u8; 32]).into_string();
    let a = observe("fork-a", &expected, &block_a).unwrap();
    let b = observe("fork-b", &expected, &block_b).unwrap();
    assert_ne!(a.provider_assertion, b.provider_assertion);
    assert_eq!(
        canonical(facts_json(&a.facts)),
        canonical(facts_json(&b.facts))
    );
    assert_eq!(a.block_time, b.block_time);
    // No session state: revisiting A retains precisely A's assertion.
    assert_eq!(
        associate(&expected, &block_a).unwrap().provider_assertion,
        a.provider_assertion
    );
    Ok(())
}
