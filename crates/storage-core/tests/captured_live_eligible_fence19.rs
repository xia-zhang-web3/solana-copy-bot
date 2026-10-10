//! Saved live inputs; fresh databases and controlled clocks, never a live launch.
#[path = "common/association.rs"]
mod fixture;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::association_delivery::{
    BlockTime, CandidateGeneration, Delivery, DeliveryEvent, MessageTime, ProviderAssertion,
    Terminal,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{NativeBuyFence, TechnicalCohortAuthority, SPL_TOKEN_PROGRAM},
    SqliteStore,
};
use rusqlite::Connection;
use serde::Deserialize;

struct Corpus {
    authority: TechnicalCohortAuthority,
    epochs: Vec<NativeBuyFence>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    delivery: Delivery,
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
}

fn corpus() -> Result<Corpus> {
    let v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::var("CAPTURED_FENCE19_CASES")?)?)?;
    let a = &v["authority"];
    let s = |v: &serde_json::Value| v.as_str().unwrap().to_owned();
    let authority = TechnicalCohortAuthority {
        run_id: s(&a["run_id"]),
        wallet_ids: serde_json::from_value(a["wallet_ids"].clone())?,
        mint_policy: s(&a["mint_policy"]),
        activated_at: s(&a["activated_at"]).parse()?,
        deadline: s(&a["deadline"]).parse()?,
        max_buy_count: a["max_buy_count"].as_u64().unwrap().try_into()?,
        policy_identity: s(&a["policy_identity"]),
    };
    let epochs = v["epochs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| -> Result<NativeBuyFence> {
            Ok(NativeBuyFence {
                session: s(&e["session"]),
                processed_slot: e["processed_slot"].as_u64().unwrap(),
                sampled_at: s(&e["sampled_at"]).parse()?,
                genesis_hash: s(&e["genesis_hash"]),
                policy_identity: s(&e["policy_identity"]),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Corpus {
        authority,
        epochs,
        cases: serde_json::from_value(v["cases"].clone())?,
    })
}

fn facts(case: &Case) -> &copybot_core_types::association_delivery::AdmissionFacts {
    let DeliveryEvent::Admission(a) = &case.delivery.event else {
        panic!("saved admission")
    };
    a
}

fn created(case: &Case) -> DateTime<Utc> {
    let MessageTime::CreatedAt { seconds, nanos } = facts(case).message_time else {
        panic!("saved live CreatedAt")
    };
    DateTime::from_timestamp(seconds, nanos as u32).unwrap()
}

fn count(sql: &Connection) -> Result<i64> {
    Ok(sql.query_row(
        "SELECT count(*) FROM native_buy_cohort_decisions",
        [],
        |r| r.get(0),
    )?)
}

fn run_case(c: &Corpus, case: &Case, observed: DateTime<Utc>) -> Result<i64> {
    let (_dir, path) = fixture::db();
    let mut inbox = AssociationInbox::open(&path, fixture::limits())?;
    inbox.register_technical_cohort_authority(&c.authority)?;
    for epoch in c.epochs.iter().filter(|e| e.sampled_at <= observed) {
        inbox.record_native_buy_fence_epoch(epoch)?;
    }
    inbox.persist_at(&case.delivery, &CandidateGeneration::Unknown, observed)?;
    let sql = Connection::open(&path)?;
    let saved: String = sql.query_row(
        "SELECT admission FROM association_inbox_identities WHERE signature=?1",
        [&facts(case).facts.signature],
        |r| r.get(0),
    )?;
    assert_eq!(
        serde_json::from_str::<copybot_core_types::association_delivery::AdmissionFacts>(&saved)?,
        *facts(case)
    );
    let decisions = count(&sql)?;
    if decisions == 1 {
        let a = facts(case).clone();
        let terminal = Delivery {
            sequence: case.delivery.sequence + 1,
            event: DeliveryEvent::Terminal {
                signature: a.facts.signature.clone(),
                expected: a.clone(),
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: a.facts.slot,
                    blockhash: "offline-confirmation-only".into(),
                    signature: a.facts.signature.clone(),
                    transaction_index: 0,
                    block_time: BlockTime::Missing,
                }),
            },
            ..case.delivery.clone()
        };
        inbox.persist_at(&terminal, &CandidateGeneration::Unknown, observed)?;
        let store = SqliteStore::open(&path)?;
        assert!(store.native_buy_record_finalized(
            &a.facts.signature,
            a.facts.slot,
            SPL_TOKEN_PROGRAM,
            observed
        )?);
        let ready = store
            .native_buy_ready(&a.facts.signature, observed, 120)?
            .unwrap();
        assert!(store.native_buy_recheck(&ready.signal_id, &ready.decision_id, observed, 120)?);
    }
    Ok(decisions)
}

#[test]
#[ignore = "requires retained private run19 corpus; run explicitly with CAPTURED_FENCE19_CASES"]
fn approved_epoch_policy_admits_only_three_lower_bounds_without_historical_trade_claim(
) -> Result<()> {
    let c = corpus()?;
    assert_eq!(c.cases.len(), 4);
    for (i, case) in c.cases.iter().enumerate() {
        for observed in [case.lower, case.upper] {
            let latest = c
                .epochs
                .iter()
                .rev()
                .find(|e| e.sampled_at <= observed)
                .unwrap();
            assert!(observed - created(case) < Duration::seconds(120));
            assert!(facts(case).facts.slot <= latest.processed_slot);
            let expected = if observed == case.lower && i < 3 {
                1
            } else {
                0
            };
            assert_eq!(run_case(&c, case, observed)?, expected);
            println!(
                "CAPTURED_BOUND seq={} observed={} age_ms={} fence_slot={} decisions={}",
                case.delivery.sequence,
                observed,
                (observed - created(case)).num_milliseconds(),
                latest.processed_slot,
                expected
            );
        }
    }
    Ok(())
}

#[test]
#[ignore = "requires retained private run19 corpus; counterfactual timing control only"]
fn same_inputs_delivered_before_periodic_refresh_create_decision() -> Result<()> {
    let c = corpus()?;
    for case in &c.cases {
        // Counterfactual timing control only; this is not the historical arrival.
        let timely = created(case) + Duration::seconds(1);
        assert!(timely < case.lower);
        let last = c
            .epochs
            .iter()
            .rev()
            .find(|e| e.sampled_at <= timely)
            .unwrap();
        assert!(facts(case).facts.slot > last.processed_slot);
        assert_eq!(run_case(&c, case, timely)?, 1);
        let closing = c
            .epochs
            .iter()
            .find(|e| e.processed_slot >= facts(case).facts.slot)
            .unwrap();
        println!("COUNTERFACTUAL_TIMELY seq={} max_delay_before_refresh_ms={} decisions=1; not a quote or live-copy proof",
            case.delivery.sequence, (closing.sampled_at-created(case)).num_milliseconds());
    }
    Ok(())
}

#[test]
#[ignore = "requires retained private run19 corpus; evaluates approved policy bounds, not exact historical observed_at"]
fn approved_epoch_selection_is_bounded_and_not_a_proven_historical_buy() -> Result<()> {
    let c = corpus()?;
    for (i, case) in c.cases.iter().enumerate() {
        for (label, observed) in [("lower", case.lower), ("upper", case.upper)] {
            // Independent arithmetic for approved selection bounds.
            let eligible = c
                .epochs
                .iter()
                .filter(|e| {
                    e.session == case.delivery.session
                        && e.sampled_at <= observed
                        && observed - e.sampled_at <= Duration::seconds(120)
                        && e.sampled_at >= c.authority.activated_at
                        && e.processed_slot < facts(case).facts.slot
                })
                .count();
            let expected = if label == "lower" && i < 3 { 1 } else { 0 };
            assert_eq!(eligible, expected);
            println!(
                "APPROVED_POLICY_BOUND seq={} bound={} eligible_prior_epochs={}",
                case.delivery.sequence, label, eligible
            );
        }
    }
    Ok(())
}
