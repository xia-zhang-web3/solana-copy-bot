//! Approved selection policy; all decisions flow through durable admission.
#[path = "common/association.rs"]
mod fixture;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::association_delivery::{
    AdmissionFacts, BlockTime, CandidateGeneration, DeliveryEvent, MessageTime, ProviderAssertion,
    Terminal,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{
        NativeBuyCandidate, NativeBuyFence, TechnicalCohortAuthority, CLASSIC_SPL_MINT_POLICY,
        SPL_TOKEN_PROGRAM,
    },
    SqliteStore,
};
use rusqlite::Connection;

fn now() -> DateTime<Utc> {
    "2026-10-10T20:00:00Z".parse().unwrap()
}
fn authority() -> TechnicalCohortAuthority {
    TechnicalCohortAuthority {
        run_id: "eligible-epoch-once".into(),
        wallet_ids: vec!["leader".into()],
        mint_policy: CLASSIC_SPL_MINT_POLICY.into(),
        activated_at: now() - Duration::seconds(300),
        deadline: now() + Duration::seconds(300),
        max_buy_count: 1,
        policy_identity: "policy".into(),
    }
}
fn fence(at: DateTime<Utc>, slot: u64) -> NativeBuyFence {
    NativeBuyFence {
        session: "session-A".into(),
        processed_slot: slot,
        sampled_at: at,
        genesis_hash: "genesis".into(),
        policy_identity: "policy".into(),
    }
}
fn setup(
    samples: &[(i64, u64)],
) -> Result<(tempfile::TempDir, std::path::PathBuf, AssociationInbox)> {
    let (dir, path) = fixture::db();
    let mut inbox = AssociationInbox::open(&path, fixture::limits())?;
    inbox.register_technical_cohort_authority(&authority())?;
    for &(offset, slot) in samples {
        inbox.record_native_buy_fence_epoch(&fence(now() + Duration::seconds(offset), slot))?;
    }
    Ok((dir, path, inbox))
}
fn facts(signature: &str, slot: u64) -> AdmissionFacts {
    let mut a = fixture::facts();
    a.facts.signature = signature.into();
    a.facts.slot = slot;
    a.facts.token_in = "So11111111111111111111111111111111111111112".into();
    a.facts.token_out = "mint".into();
    a.facts.exact_amounts.as_mut().unwrap().amount_in_raw = "10000000".into();
    a.message_time = MessageTime::CreatedAt {
        seconds: (now() - Duration::seconds(70)).timestamp(),
        nanos: 0,
    };
    a
}
fn deliver(
    inbox: &mut AssociationInbox,
    a: &AdmissionFacts,
    seq: u64,
    at: DateTime<Utc>,
) -> Result<()> {
    inbox.persist_at(
        &fixture::event(seq, DeliveryEvent::Admission(a.clone())),
        &CandidateGeneration::Unknown,
        at,
    )?;
    inbox.persist_at(
        &fixture::event(
            seq + 1,
            DeliveryEvent::Terminal {
                signature: a.facts.signature.clone(),
                expected: a.clone(),
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: a.facts.slot,
                    blockhash: "block".into(),
                    signature: a.facts.signature.clone(),
                    transaction_index: 1,
                    block_time: BlockTime::Missing,
                }),
            },
        ),
        &CandidateGeneration::Unknown,
        at,
    )?;
    Ok(())
}
fn ready(
    path: &std::path::Path,
    a: &AdmissionFacts,
    at: DateTime<Utc>,
) -> Result<Option<NativeBuyCandidate>> {
    let store = SqliteStore::open(path)?;
    if !store.native_buy_record_finalized(
        &a.facts.signature,
        a.facts.slot,
        SPL_TOKEN_PROGRAM,
        at,
    )? {
        return Ok(None);
    }
    store.native_buy_ready(&a.facts.signature, at, 120)
}
fn count(path: &std::path::Path) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM native_buy_cohort_decisions",
            [],
            |r| r.get(0),
        )
        .unwrap()
}
fn pinned(path: &std::path::Path) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT fence_epoch_id FROM native_buy_cohort_decisions",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn newest_eligible_epoch_is_pinned_and_refresh_restart_duplicate_do_not_reselect() -> Result<()> {
    let (_dir, path, mut inbox) = setup(&[(-100, 100), (-80, 150), (-1, 300)])?;
    let a = facts("delayed-live", 200);
    deliver(&mut inbox, &a, 1, now())?;
    let candidate = ready(&path, &a, now())?.expect("approved older eligible epoch must admit");
    assert_eq!(pinned(&path), 2);
    inbox.record_native_buy_fence_epoch(&fence(now() + Duration::seconds(1), 400))?;
    drop(inbox);
    let mut reopened = AssociationInbox::open(&path, fixture::limits())?;
    reopened.register_technical_cohort_authority(&authority())?;
    deliver(&mut reopened, &a, 3, now() + Duration::seconds(2))?;
    let second = facts("second-live", 450);
    deliver(&mut reopened, &second, 5, now() + Duration::seconds(2))?;
    assert_eq!(count(&path), 1);
    assert_eq!(pinned(&path), 2);
    assert!(ready(&path, &second, now() + Duration::seconds(2))?.is_none());
    let store = SqliteStore::open(&path)?;
    assert!(store.native_buy_recheck(
        &candidate.signal_id,
        &candidate.decision_id,
        now() + Duration::seconds(2),
        120
    )?);
    assert!(!store.native_buy_recheck(
        &candidate.signal_id,
        &candidate.decision_id,
        now() + Duration::seconds(41),
        120
    )?);
    assert_eq!(pinned(&path), 2);
    Ok(())
}

#[test]
fn epoch_120_seconds_is_inclusive_but_one_nanosecond_more_never_rejuvenates() -> Result<()> {
    for nanos in [0, 1] {
        let (_dir, path, mut inbox) = setup(&[(-120, 100), (-1, 300)])?;
        let observed = now() + Duration::nanoseconds(nanos);
        let a = facts("boundary", 200);
        deliver(&mut inbox, &a, 1, observed)?;
        assert_eq!(count(&path), if nanos == 0 { 1 } else { 0 });
        assert_eq!(ready(&path, &a, observed)?.is_some(), nanos == 0);
        if nanos == 0 {
            inbox.record_native_buy_fence_epoch(&fence(now() + Duration::seconds(1), 400))?;
            assert!(ready(&path, &a, now() + Duration::seconds(1))?.is_none());
            assert_eq!(pinned(&path), 1);
        }
    }
    Ok(())
}

fn corrupt(path: &std::path::Path, row: i64, field: &str) {
    let sql = Connection::open(path).unwrap();
    sql.execute("DROP TRIGGER native_buy_fence_epoch_immutable", [])
        .unwrap();
    let (column, value) = match field {
        "clock" => ("sampled_at", "not-a-clock".to_owned()),
        "future" => ("sampled_at", (now() + Duration::seconds(10)).to_rfc3339()),
        "preactivation" => ("sampled_at", (now() - Duration::seconds(301)).to_rfc3339()),
        "genesis" => ("genesis_hash", "different-genesis".to_owned()),
        "policy" => ("policy_identity", "different-policy".to_owned()),
        "regressed_slot" => ("processed_slot", "99".to_owned()),
        "duplicate_clock" => ("sampled_at", (now() - Duration::seconds(100)).to_rfc3339()),
        _ => panic!("explicit mutation"),
    };
    sql.execute(
        &format!("UPDATE native_buy_fence_epochs SET {column}=?1 WHERE epoch_id=?2"),
        rusqlite::params![value, row],
    )
    .unwrap();
}

#[test]
fn damaged_chain_cannot_fall_back_before_admission_or_financial_recheck() -> Result<()> {
    for row in [1, 2, 3] {
        for field in [
            "clock",
            "future",
            "preactivation",
            "genesis",
            "policy",
            "regressed_slot",
            "duplicate_clock",
        ] {
            if row == 1 && field == "duplicate_clock" {
                continue;
            }
            for after_admission in [false, true] {
                let (_dir, path, mut inbox) = setup(&[(-100, 100), (-80, 150), (-1, 300)])?;
                let a = facts("corrupted-proof", 200);
                if after_admission {
                    deliver(&mut inbox, &a, 1, now())?;
                    assert_eq!(count(&path), 1);
                    assert!(ready(&path, &a, now())?.is_some());
                }
                corrupt(&path, row, field);
                if after_admission {
                    let store = SqliteStore::open(&path)?;
                    assert!(!matches!(
                        store.native_buy_recheck(
                            "native-buy-v1:corrupted-proof",
                            "native-buy-decision-v1:corrupted-proof",
                            now(),
                            120
                        ),
                        Ok(true)
                    ));
                    assert!(
                        !matches!(ready(&path, &a, now()), Ok(Some(_))),
                        "row={row} field={field}"
                    );
                    assert_eq!(pinned(&path), 2);
                } else {
                    let _ = deliver(&mut inbox, &a, 1, now());
                    assert_eq!(count(&path), 0, "row={row} field={field}");
                }
            }
        }
    }
    Ok(())
}

#[test]
fn initial_boundary_deadline_and_inactive_session_remain_refusals() -> Result<()> {
    for mode in [
        "initial_slot",
        "deadline",
        "before_activation",
        "inactive",
        "other_session",
    ] {
        let (_dir, path, mut inbox) = setup(&[(-100, 100), (-1, 300)])?;
        let mut at = now();
        let a = facts(
            "boundary-denied",
            if mode == "initial_slot" { 100 } else { 200 },
        );
        if mode == "deadline" {
            at = authority().deadline;
        }
        if mode == "before_activation" {
            at = authority().activated_at - Duration::nanoseconds(1);
        }
        if mode == "inactive" {
            Connection::open(&path)?.execute(
                "UPDATE native_buy_session_state SET active_session=NULL",
                [],
            )?;
        }
        if mode == "other_session" {
            let mut f = fence(now(), 101);
            f.session = "session-B".into();
            inbox.record_native_buy_fence_epoch(&f)?;
        }
        deliver(&mut inbox, &a, 1, at)?;
        assert_eq!(count(&path), 0, "mode={mode}");
        assert!(ready(&path, &a, at)?.is_none());
    }
    Ok(())
}

#[test]
fn recovered_buy_cannot_consume_epoch_and_live_createdat_is_retained_after_restart() -> Result<()> {
    let (_dir, path, mut inbox) = setup(&[(-100, 100), (-80, 150), (-1, 300)])?;
    let mut old = facts("recovered", 200);
    old.message_time = MessageTime::RecoveredBlock {
        block_time: Some(now().timestamp()),
    };
    deliver(&mut inbox, &old, 1, now())?;
    assert_eq!(count(&path), 0);
    drop(inbox);
    let mut reopened = AssociationInbox::open(&path, fixture::limits())?;
    let live = facts("live", 200);
    deliver(&mut reopened, &live, 3, now())?;
    assert!(ready(&path, &live, now())?.is_some());
    assert_eq!(pinned(&path), 2);
    let saved: String = Connection::open(&path)?.query_row(
        "SELECT admission FROM native_buy_decisions WHERE signature='live'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(serde_json::from_str::<AdmissionFacts>(&saved)?, live);
    Ok(())
}

#[test]
fn unresolved_source_terminal_keeps_original_epoch_and_consumed_slot_after_restart() -> Result<()> {
    use copybot_core_types::association_delivery::Unresolved;
    let (_dir, path, mut inbox) = setup(&[(-100, 100), (-80, 150), (-1, 300)])?;
    let a = facts("unresolved-source", 200);
    inbox.persist_at(
        &fixture::event(1, DeliveryEvent::Admission(a.clone())),
        &CandidateGeneration::Unknown,
        now(),
    )?;
    inbox.persist_at(
        &fixture::event(
            2,
            DeliveryEvent::Terminal {
                signature: a.facts.signature.clone(),
                expected: a.clone(),
                result: Terminal::Unresolved(Unresolved::EndOfStream),
            },
        ),
        &CandidateGeneration::Unknown,
        now(),
    )?;
    assert_eq!(count(&path), 1);
    assert_eq!(pinned(&path), 2);
    drop(inbox);
    let mut reopened = AssociationInbox::open(&path, fixture::limits())?;
    reopened.register_technical_cohort_authority(&authority())?;
    reopened.record_native_buy_fence_epoch(&fence(now() + Duration::seconds(1), 400))?;
    let second = facts("another-source", 450);
    deliver(&mut reopened, &second, 3, now() + Duration::seconds(2))?;
    assert_eq!(count(&path), 1);
    assert_eq!(pinned(&path), 2);
    assert!(ready(&path, &a, now() + Duration::seconds(2))?.is_none());
    assert!(ready(&path, &second, now() + Duration::seconds(2))?.is_none());
    assert!(!SqliteStore::open(&path)?.native_buy_recheck(
        "native-buy-v1:unresolved-source",
        "native-buy-decision-v1:unresolved-source",
        now() + Duration::seconds(2),
        120
    )?);
    // Source association unresolved is not a fabricated financial UNKNOWN.
    Ok(())
}
