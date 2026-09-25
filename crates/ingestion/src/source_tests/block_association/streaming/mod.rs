use super::*;
use crate::source::yellowstone_association::limits::Budget;
use crate::source::yellowstone_association::*;
use crate::source::yellowstone_message_time::{CreatedAtUnavailable, YellowstoneMessageTime};
use std::time::Duration;

mod conflicts;
mod lifecycle;
mod limits_tests;
mod resource_tests;
mod scoped_admission_tests;

pub(super) fn limits() -> Limits {
    Limits {
        pending: Budget {
            count: 1024,
            encoded_bytes: 16 * 1024 * 1024,
        },
        blocks: Budget {
            count: 32,
            encoded_bytes: 64 * 1024 * 1024,
        },
        history: Budget {
            count: 2048,
            encoded_bytes: 32 * 1024 * 1024,
        },
        outputs: Budget {
            count: 17,
            encoded_bytes: 4 * 1024 * 1024,
        },
        input_bytes: 8 * 1024 * 1024,
        metadata_bytes: 32 * 1024 * 1024,
        pending_ttl: Duration::from_secs(60),
        block_ttl: Duration::from_secs(60),
        history_ttl: Duration::from_secs(120),
    }
}
fn session() -> Session {
    Session {
        id: [88; 16],
        generation: 1,
    }
}
fn context(ns: u64) -> Context {
    Context {
        session: session(),
        offset: Duration::from_nanos(ns),
    }
}
fn adapter(c: &crate::source::YellowstoneRuntimeConfig, l: Limits) -> YellowstoneAssociation<'_> {
    YellowstoneAssociation::new(
        session(),
        l,
        Programs {
            interested: &c.interested_program_ids,
            raydium: &c.raydium_program_ids,
            pumpswap: &c.pumpswap_program_ids,
        },
    )
    .unwrap()
}
fn tx_input(u: &SubscribeUpdate) -> Input<'_> {
    Input::Transaction(
        transaction(u),
        YellowstoneMessageTime::from_created_at(u.created_at.as_ref()),
    )
}
fn drain(a: &mut YellowstoneAssociation<'_>) -> Vec<Outcome> {
    let mut all = vec![];
    for _ in 0..10000 {
        let batch = a.drain();
        all.extend(batch.outcomes);
        if batch.complete {
            return all;
        }
    }
    panic!("drain must progress")
}
fn feed(a: &mut YellowstoneAssociation<'_>, ns: u64, input: Input<'_>) -> Vec<Outcome> {
    a.push(context(ns), input).unwrap();
    drain(a)
}
fn terminal(out: &Outcome) -> (&CheckedTransaction, &Resolution) {
    match out {
        Outcome::Terminal {
            checked,
            resolution,
            ..
        } => (checked, resolution),
        _ => panic!("expected terminal, got {out:?}"),
    }
}
fn unresolved(out: &Outcome, expected: UnresolvedReason) {
    assert_eq!(terminal(out).1, &Resolution::Unresolved(expected));
}
fn assert_facts(expected: &SubscribeUpdate, checked: &CheckedTransaction) {
    let c = YellowstoneGrpcSource::new(&config()).unwrap();
    let c = &c.runtime_config;
    let original = decode_yellowstone_swap_facts(
        transaction(expected),
        &c.interested_program_ids,
        &c.raydium_program_ids,
        &c.pumpswap_program_ids,
    )
    .facts
    .unwrap()
    .unwrap();
    assert_eq!(
        canonical(facts_json(&original)),
        canonical(facts_json(&checked.facts))
    );
}
