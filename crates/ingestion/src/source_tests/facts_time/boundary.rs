use super::*;
use crate::source::yellowstone_facts::{decode_yellowstone_swap_facts, YellowstoneSwapFacts};
use crate::source::yellowstone_message_time::{
    CreatedAtUnavailable as Missing, YellowstoneMessageTime as Time,
};

fn facts_fields(facts: &YellowstoneSwapFacts) -> Value {
    json!({"signature":facts.signature,"slot":facts.slot,"signer":facts.signer,
        "token_in":facts.token_in,"token_out":facts.token_out,
        "amount_in":facts.amount_in,"amount_out":facts.amount_out,
        "amount_in_bits":facts.amount_in.to_bits(),"amount_out_bits":facts.amount_out.to_bits(),
        "exact_amounts":facts.exact_amounts,"program_ids":facts.program_ids,
        "dex_hint":facts.dex_hint})
}

fn canonical(mut fields: Value) -> Value {
    // Program IDs already originate in a randomized HashSet in accepted77.
    // Captures retain the actual order; equality here compares membership.
    fields["program_ids"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(Value::to_string);
    fields
}

fn decode(update: &SubscribeUpdate) -> Result<Option<YellowstoneSwapFacts>> {
    let source = YellowstoneGrpcSource::new(&config())?;
    let c = &source.runtime_config;
    let before = c.telemetry.parse_fallback_by_reason.lock().unwrap().clone();
    let result = decode_yellowstone_swap_facts(
        transaction(update),
        &c.interested_program_ids,
        &c.raydium_program_ids,
        &c.pumpswap_program_ids,
    );
    assert_eq!(
        *c.telemetry.parse_fallback_by_reason.lock().unwrap(),
        before
    );
    result.facts
}

#[test]
fn buy_sell_wsol_native_facts_survive_unresolved_message_time() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            let mut healthy = cases::base(sell, native);
            healthy.created_at = cases::times()[0].1;
            let expected = canonical(facts_fields(&decode(&healthy)?.unwrap()));
            let source = YellowstoneGrpcSource::new(&config())?;
            let before = legacy(healthy.clone(), &source.runtime_config);
            let mut raw = before["raw"].clone();
            raw.as_object_mut().unwrap().remove("ts_utc");
            assert_eq!(canonical(raw), expected);
            for (clock, ts) in cases::times() {
                let name = format!("{sell}-{native}-{clock}");
                let mut update = healthy.clone();
                update.created_at = ts;
                let update = replay(&name, update)?;
                let facts = decode(&update)?.expect("facts do not require created_at");
                assert_eq!(canonical(facts_fields(&facts)), expected);
                let time = Time::from_created_at(update.created_at.as_ref());
                let refused = match clock {
                    "missing" => Some(Missing::Missing),
                    "negative_nanos" => Some(Missing::InvalidNanos(-1)),
                    "overflow_nanos" => Some(Missing::InvalidNanos(1_000_000_000)),
                    "overflow_seconds" => Some(Missing::OutOfRangeSeconds(i64::MAX)),
                    "underflow_seconds" => Some(Missing::OutOfRangeSeconds(i64::MIN)),
                    _ => None,
                };
                let result = legacy(update.clone(), &source.runtime_config);
                if let Some(reason) = refused {
                    assert_eq!(time, Time::UnresolvedCreatedAt(reason));
                    assert_eq!(result, json!({"none":true}));
                } else {
                    let Time::AvailableCreatedAt(created_at) = time else {
                        panic!("valid message timestamp")
                    };
                    assert_eq!(result["raw"]["ts_utc"], json!(created_at));
                    let mut raw = result["raw"].clone();
                    raw.as_object_mut().unwrap().remove("ts_utc");
                    assert_eq!(canonical(raw), expected);
                    if clock == "restored" {
                        assert_eq!(result["event"], before["event"]);
                    }
                }
                capture(
                    &format!("facts-{name}"),
                    &update,
                    &json!({
                    "facts":facts_fields(&facts),"message_time_source":"SubscribeUpdate.created_at",
                    "message_time":format!("{time:?}"),"chain_time":"unproven",
                    "containing_block_identity":"unproven","legacy":result}),
                )?;
            }
        }
    }
    Ok(())
}

#[test]
fn malformed_or_unsupported_transactions_never_gain_facts_from_missing_time() -> Result<()> {
    for case in cases::corpus().into_iter().filter(|c| c.refused) {
        if damage::NAMES.iter().all(|name| !case.name.contains(name)) {
            continue;
        }
        let mut source = YellowstoneGrpcSource::new(&config())?;
        let c = std::sync::Arc::get_mut(&mut source.runtime_config).unwrap();
        if case.empty_interest {
            c.interested_program_ids.clear();
        }
        let update = replay(&case.name, case.update)?;
        let decoded = decode_yellowstone_swap_facts(
            transaction(&update),
            &c.interested_program_ids,
            &c.raydium_program_ids,
            &c.pumpswap_program_ids,
        );
        assert!(!matches!(decoded.facts, Ok(Some(_))), "{}", case.name);
        let outcome = match decoded.facts {
            Err(e) => json!({"error":format!("{e:#}")}),
            Ok(None) => json!({"none":true}),
            _ => unreachable!(),
        };
        capture(
            &format!("facts-{}", case.name),
            &update,
            &json!({"facts_outcome":outcome,
            "message_time_source":"SubscribeUpdate.created_at",
            "message_time":format!("{:?}",Time::from_created_at(update.created_at.as_ref()))}),
        )?;
    }
    Ok(())
}

#[test]
fn established_fixtures_keep_facts_and_program_fallback_without_clock_or_side_effects() -> Result<()>
{
    for case in cases::corpus().into_iter().filter(|c| {
        c.name.starts_with("fixture-")
            || c.name.starts_with("native-")
            || c.name.starts_with("fallback-")
    }) {
        let update = replay(&case.name, case.update)?;
        let mut untimed = update.clone();
        untimed.created_at = None;
        let a = decode(&update)
            .map(|f| f.map(|f| canonical(facts_fields(&f))))
            .map_err(|e| e.to_string());
        let b = decode(&untimed)
            .map(|f| f.map(|f| canonical(facts_fields(&f))))
            .map_err(|e| e.to_string());
        assert_eq!(a, b, "{}", case.name);
        if let Ok(dir) = std::env::var("B79_REPLAY_DIR") {
            let baseline: Value = serde_json::from_slice(&std::fs::read(
                std::path::Path::new(&dir).join(format!("{}.json", case.name)),
            )?)?;
            if let Some(raw) = baseline.get("raw") {
                let mut raw = raw.clone();
                raw.as_object_mut().unwrap().remove("ts_utc");
                assert_eq!(a, Ok(Some(canonical(raw))), "{}", case.name);
            } else if matches!(
                Time::from_created_at(update.created_at.as_ref()),
                Time::AvailableCreatedAt(_)
            ) {
                assert!(!matches!(a, Ok(Some(_))), "{}", case.name);
            }
        }
    }
    Ok(())
}
