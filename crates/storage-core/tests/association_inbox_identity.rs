#[path = "common/association.rs"]
mod f;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::association_inbox::AssociationInbox;
use f::*;
#[test]
fn b89_first_admission_a_survives_terminal_b_reopen_and_duplicate() {
    let (_d, p) = db();
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    i.persist_at(
        &admission(0),
        &candidate("A"),
        chrono::DateTime::from_timestamp(10, 0).unwrap(),
    )
    .unwrap();
    drop(i);
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    assert!(row(&i).recovery);
    i.persist(&admission(0), &candidate("B")).unwrap();
    i.persist(&terminal(1, result()), &candidate("B")).unwrap();
    let mut duplicate = admission(0);
    duplicate.session = "new-session".into();
    i.persist(&duplicate, &candidate("B")).unwrap();
    assert_eq!(row(&i).candidate, candidate("A"));
    assert_eq!(row(&i).admission, facts());
    assert_eq!(row(&i).terminal, Some(result()));
    assert!(row(&i).recovery);
}
#[test]
fn b89_unknown_initial_binding_never_becomes_known() {
    let (_d, p) = db();
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    i.persist(&admission(0), &CandidateGeneration::Unknown)
        .unwrap();
    i.persist(&admission(1), &candidate("B")).unwrap();
    assert_eq!(row(&i).candidate, CandidateGeneration::Unknown);
}
#[test]
fn b89_conflicting_info_and_signed_zero_are_sticky_across_restart() {
    for float_only in [false, true] {
        let (_d, p) = db();
        let mut i = AssociationInbox::open(&p, limits()).unwrap();
        i.persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )
        .unwrap();
        let mut different = facts();
        if float_only {
            different.info.float_bits[0] = Some(0);
        } else {
            different.info.encoded.push(4);
        }
        i.persist(
            &event(1, DeliveryEvent::Admission(different.clone())),
            &candidate("B"),
        )
        .unwrap();
        let different_terminal = event(
            2,
            DeliveryEvent::Terminal {
                signature: facts().facts.signature,
                expected: different,
                result: result(),
            },
        );
        i.persist(&different_terminal, &candidate("B")).unwrap();
        assert!(row(&i).terminal.is_none());
        drop(i);
        let mut i = AssociationInbox::open(&p, limits()).unwrap();
        i.persist(&admission(3), &candidate("B")).unwrap();
        assert!(row(&i).conflict);
        assert_eq!(row(&i).admission, facts());
        assert_eq!(row(&i).candidate, candidate("A"));
    }
}
#[test]
fn b89_terminal_immutable_and_late_retained_after_reopen() {
    for first in [result(), Terminal::Unresolved(Unresolved::EndOfStream)] {
        let (_d, p) = db();
        let mut i = AssociationInbox::open(&p, limits()).unwrap();
        i.persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )
        .unwrap();
        i.persist(&terminal(1, first.clone()), &CandidateGeneration::Unknown)
            .unwrap();
        let notice = event(
            2,
            DeliveryEvent::Late {
                signature: facts().facts.signature,
                original: first.clone(),
                evidence: Late::ConflictingTransaction,
            },
        );
        i.persist(&notice, &CandidateGeneration::Unknown).unwrap();
        drop(i);
        let mut i = AssociationInbox::open(&p, limits()).unwrap();
        i.persist(&notice, &CandidateGeneration::Unknown).unwrap();
        i.persist(&terminal(3, result()), &CandidateGeneration::Unknown)
            .unwrap();
        assert_eq!(row(&i).terminal, Some(first));
        assert!(row(&i).conflict);
    }
}
#[test]
fn b89_terminal_late_and_event_collision_require_durable_admission() {
    let (_d, p) = db();
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    assert!(i.persist(&terminal(0, result()), &candidate("A")).is_err());
    assert_eq!(i.usage().unwrap(), (1, 512));
    i.persist_at(
        &admission(0),
        &candidate("A"),
        chrono::DateTime::from_timestamp(10, 0).unwrap(),
    )
    .unwrap();
    assert!(i.persist(&terminal(0, result()), &candidate("B")).is_err());
    let late = event(
        2,
        DeliveryEvent::Late {
            signature: facts().facts.signature,
            original: result(),
            evidence: Late::ConflictingTransaction,
        },
    );
    assert!(i.persist(&late, &CandidateGeneration::Unknown).is_err());
    assert!(!row(&i).conflict);
}
