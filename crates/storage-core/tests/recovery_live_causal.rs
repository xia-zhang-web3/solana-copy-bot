//! Real admission716 remains old; the control is a separate synthetic observation.
#[path = "common/association.rs"]
mod fixture;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::association_delivery::{
    AdmissionFacts, BlockTime, CandidateGeneration, Delivery, DeliveryEvent, InfoIdentity,
    MessageTime, ProviderAssertion, SessionGap, Terminal,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{NativeBuyCandidate, NativeBuyFence, TechnicalCohortAuthority, SPL_TOKEN_PROGRAM},
    SqliteStore,
};
use rusqlite::Connection;
use serde_json::Value;

fn corpus() -> Value {
    serde_json::from_str(include_str!("fixtures/recovery_06.json")).unwrap()
}
fn time(v: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(v).unwrap().with_timezone(&Utc)
}
fn fence(v: &Value) -> NativeBuyFence {
    NativeBuyFence {
        session: v["session"].as_str().unwrap().into(),
        processed_slot: v["processed_slot"].as_u64().unwrap(),
        sampled_at: time(v["sampled_at"].as_str().unwrap()),
        genesis_hash: v["genesis_hash"].as_str().unwrap().into(),
        policy_identity: v["policy_identity"].as_str().unwrap().into(),
    }
}
fn setup(path: &std::path::Path) -> Result<(AssociationInbox, Delivery, DateTime<Utc>)> {
    let data = corpus();
    let mut inbox = AssociationInbox::open(path, fixture::limits())?;
    let a = &data["authority"];
    inbox.register_technical_cohort_authority(&TechnicalCohortAuthority {
        run_id: a["run_id"].as_str().unwrap().into(),
        wallet_ids: serde_json::from_value(a["wallet_ids"].clone())?,
        mint_policy: a["mint_policy"].as_str().unwrap().into(),
        activated_at: time(a["activated_at"].as_str().unwrap()),
        deadline: time(a["deadline"].as_str().unwrap()),
        max_buy_count: a["max_buy_count"].as_u64().unwrap().try_into()?,
        policy_identity: a["policy_identity"].as_str().unwrap().into(),
    })?;
    for epoch in data["epochs"].as_array().unwrap() {
        inbox.record_native_buy_fence_epoch(&fence(epoch))?;
    }
    Ok((
        inbox,
        serde_json::from_value(data["delivery"].clone())?,
        time(data["observed_after_recovery"].as_str().unwrap()),
    ))
}
fn counts(path: &std::path::Path) -> (i64, i64, i64) {
    let c = Connection::open(path).unwrap();
    let count = |table: &str| {
        c.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap()
    };
    (
        count("native_buy_decisions"),
        count("native_buy_cohort_decisions"),
        count("copy_signals"),
    )
}
fn facts(d: &Delivery) -> &AdmissionFacts {
    let DeliveryEvent::Admission(a) = &d.event else {
        panic!("admission fixture required")
    };
    a
}

/// No saved signature/info/clock is renamed. This typed fixture stands in for a
/// newly decoded stream transaction; it is not evidence of a real market BUY.
fn fresh_control(session: &str, sequence: u64, now: DateTime<Utc>) -> Delivery {
    let mut a = fixture::facts();
    a.facts.signature = format!("mock-fresh-control-{sequence}");
    a.facts.slot = 454492977;
    a.facts.wallet = corpus()["authority"]["wallet_ids"][0]
        .as_str()
        .unwrap()
        .into();
    a.facts.token_in = "So11111111111111111111111111111111111111112".into();
    a.facts.token_out = "mock-classic-spl-mint".into();
    let exact = a.facts.exact_amounts.as_mut().unwrap();
    exact.amount_in_raw = "10000000".into();
    exact.amount_out_raw = "12345".into();
    exact.amount_out_decimals = 6;
    a.facts.amount_in_bits = 0.01f64.to_bits();
    a.facts.amount_out_bits = 0.012345f64.to_bits();
    a.info = InfoIdentity {
        encoded: format!("distinct-mock-info-{sequence}").into_bytes(),
        float_bits: vec![],
    };
    a.message_time = MessageTime::CreatedAt {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos(),
    };
    Delivery {
        session: session.into(),
        sequence,
        arrival_offset_ns: sequence,
        event: DeliveryEvent::Admission(a),
    }
}
fn terminal(d: &Delivery) -> Delivery {
    let a = facts(d).clone();
    Delivery {
        sequence: d.sequence + 1,
        event: DeliveryEvent::Terminal {
            signature: a.facts.signature.clone(),
            expected: a.clone(),
            result: Terminal::ProviderAsserted(ProviderAssertion {
                slot: a.facts.slot,
                blockhash: "mock-request-bound-finalized-hash".into(),
                signature: a.facts.signature,
                transaction_index: 0,
                block_time: BlockTime::Missing,
            }),
        },
        ..d.clone()
    }
}
#[derive(Default)]
struct MockQuoteAdapter {
    calls: Vec<String>,
}
impl MockQuoteAdapter {
    fn quote(
        &mut self,
        store: &SqliteStore,
        c: &NativeBuyCandidate,
        now: DateTime<Utc>,
    ) -> Result<&'static str> {
        assert!(store.native_buy_recheck(&c.signal_id, &c.decision_id, now, 120)?);
        assert!(c.amount_lamports <= 10_000_000);
        assert!(!self.calls.contains(&c.signal_id));
        self.calls.push(c.signal_id.clone());
        // Explicit local refusal: no signing/submission/receipt claim.
        Ok("MOCK_ONLY_NO_SUBMIT")
    }
}

#[test]
fn actual_admission716_after_recorded_recovery_is_behind_fence_without_spending_buy() -> Result<()>
{
    let (_dir, path) = fixture::db();
    let (mut inbox, old, observed) = setup(&path)?;
    assert_eq!(old.sequence, 716);
    assert_eq!(facts(&old).facts.slot, 454492296);
    let exact = facts(&old).facts.exact_amounts.as_ref().unwrap();
    assert_eq!(exact.amount_in_raw, "15219555");
    assert_eq!(exact.amount_out_raw, "81112956");
    assert_eq!(
        facts(&old).message_time,
        MessageTime::CreatedAt {
            seconds: 1791448455,
            nanos: 942898216
        }
    );
    let created = DateTime::from_timestamp(1791448455, 942898216).unwrap();
    assert!(observed - created > Duration::seconds(201));
    inbox.persist_at(&old, &CandidateGeneration::Unknown, observed)?;
    assert_eq!(counts(&path), (0, 0, 0));
    let original = inbox.identity(&facts(&old).facts.signature)?.unwrap();
    assert_eq!(original.admission, *facts(&old));
    let c = Connection::open(&path)?;
    let fence: i64 = c.query_row(
        "SELECT processed_slot FROM native_buy_fence_epochs
        WHERE session=?1 ORDER BY epoch_id DESC LIMIT 1",
        [&old.session],
        |r| r.get(0),
    )?;
    assert_eq!(fence, 454492975);
    assert!(facts(&old).facts.slot <= fence as u64);
    Ok(())
}

#[test]
fn old_and_recovered_stay_refused_then_new_control_reaches_mock_once() -> Result<()> {
    let (_dir, path) = fixture::db();
    let (mut inbox, old, observed) = setup(&path)?;
    inbox.persist_at(&old, &CandidateGeneration::Unknown, observed)?;
    let mut recovered = fresh_control(&old.session, 720, observed);
    let DeliveryEvent::Admission(a) = &mut recovered.event else {
        unreachable!()
    };
    a.message_time = MessageTime::RecoveredBlock {
        block_time: Some(1791448455),
    };
    inbox.persist_at(&recovered, &CandidateGeneration::Unknown, observed)?;
    assert_eq!(counts(&path), (0, 0, 0));
    let live = fresh_control(&old.session, 722, observed + Duration::seconds(1));
    assert_ne!(facts(&live).facts.signature, facts(&old).facts.signature);
    assert_ne!(facts(&live).info, facts(&old).info);
    assert!(facts(&live).facts.slot > 454492975);
    inbox.persist_at(
        &live,
        &CandidateGeneration::Unknown,
        observed + Duration::seconds(1),
    )?;
    inbox.persist_at(
        &live,
        &CandidateGeneration::Unknown,
        observed + Duration::seconds(2),
    )?;
    inbox.persist_at(
        &terminal(&live),
        &CandidateGeneration::Unknown,
        observed + Duration::seconds(2),
    )?;
    let store = SqliteStore::open(&path)?;
    let now = observed + Duration::seconds(2);
    assert!(store.native_buy_record_finalized(
        &facts(&live).facts.signature,
        facts(&live).facts.slot,
        SPL_TOKEN_PROGRAM,
        now
    )?);
    let ready = store
        .native_buy_ready(&facts(&live).facts.signature, now, 120)?
        .unwrap();
    let mut mock = MockQuoteAdapter::default();
    assert_eq!(mock.quote(&store, &ready, now)?, "MOCK_ONLY_NO_SUBMIT");
    assert_eq!(mock.calls.len(), 1);
    assert_eq!(counts(&path), (1, 1, 1));
    let second = fresh_control(&old.session, 725, now);
    inbox.persist_at(&second, &CandidateGeneration::Unknown, now)?;
    assert_eq!(counts(&path), (1, 1, 1));
    assert!(store
        .native_buy_ready(&facts(&old).facts.signature, now, 120)?
        .is_none());
    assert!(store
        .native_buy_ready(
            &facts(&live).facts.signature,
            now + Duration::seconds(121),
            120
        )?
        .is_none());
    Ok(())
}

#[test]
fn dataloss_closes_old_session_until_new_fence_without_reusing_old_buy() -> Result<()> {
    let (_dir, path) = fixture::db();
    let (mut inbox, old, observed) = setup(&path)?;
    inbox.persist_at(
        &Delivery {
            session: old.session.clone(),
            sequence: 717,
            arrival_offset_ns: 1,
            event: DeliveryEvent::Session(SessionGap::Transport),
        },
        &CandidateGeneration::Unknown,
        observed,
    )?;
    inbox.persist_at(
        &fresh_control(&old.session, 720, observed),
        &CandidateGeneration::Unknown,
        observed,
    )?;
    assert_eq!(counts(&path), (0, 0, 0));
    let mut fence = fence(&corpus()["epochs"][3]);
    fence.session = "mock-post-DataLoss-session".into();
    fence.sampled_at = observed;
    inbox.record_native_buy_fence_epoch(&fence)?;
    let live = fresh_control(&fence.session, 1, observed + Duration::seconds(1));
    inbox.persist_at(
        &live,
        &CandidateGeneration::Unknown,
        observed + Duration::seconds(1),
    )?;
    assert_eq!(counts(&path), (1, 1, 0));
    Ok(())
}

#[test]
fn expired_fence_and_deadline_never_create_decision() -> Result<()> {
    for expired_deadline in [false, true] {
        let (_dir, path) = fixture::db();
        let (mut inbox, old, observed) = setup(&path)?;
        let now = if expired_deadline {
            time(corpus()["authority"]["deadline"].as_str().unwrap())
        } else {
            observed + Duration::seconds(121)
        };
        inbox.persist_at(
            &fresh_control(&old.session, 720, now),
            &CandidateGeneration::Unknown,
            now,
        )?;
        assert_eq!(counts(&path), (0, 0, 0));
    }
    Ok(())
}
