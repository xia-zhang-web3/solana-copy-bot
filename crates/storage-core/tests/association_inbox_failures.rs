#[path = "common/association.rs"]
mod f;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::association_inbox::AssociationInbox;
use f::*;
use rusqlite::Connection;
#[test]
fn b89_trigger_ignore_and_abort_never_ack_admission_event_terminal_conflict() {
    for table in ["association_inbox_identities", "association_inbox_events"] {
        for failure in ["IGNORE", "ABORT, 'fixture failure'"] {
            let (_d, p) = db();
            let mut i = AssociationInbox::open(&p, limits()).unwrap();
            let c = Connection::open(&p).unwrap();
            c.execute_batch(&format!("CREATE TRIGGER injected BEFORE INSERT ON {table} BEGIN SELECT RAISE({failure}); END;")).unwrap();
            assert!(i
                .persist_at(
                    &admission(0),
                    &candidate("A"),
                    chrono::DateTime::from_timestamp(10, 0).unwrap()
                )
                .is_err());
            assert_eq!(i.usage().unwrap(), (1, 512));
        }
    }
    for field in ["terminal", "conflict"] {
        let (_d, p) = db();
        let mut i = AssociationInbox::open(&p, limits()).unwrap();
        i.persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )
        .unwrap();
        let c = Connection::open(&p).unwrap();
        c.execute_batch(&format!("CREATE TRIGGER injected BEFORE UPDATE OF {field} ON association_inbox_identities BEGIN SELECT RAISE(IGNORE); END;")).unwrap();
        let d = if field == "terminal" {
            terminal(1, result())
        } else {
            let mut a = facts();
            a.info.encoded.push(9);
            event(1, DeliveryEvent::Admission(a))
        };
        assert!(i.persist(&d, &candidate("B")).is_err());
        assert!(row(&i).terminal.is_none());
        assert!(!row(&i).conflict);
    }
}
#[test]
fn b89_missing_corrupt_unrecorded_schema_fail_closed() {
    for ddl in [
        "DROP TABLE association_inbox_events",
        "ALTER TABLE association_inbox_identities ADD COLUMN injected TEXT",
        "DELETE FROM schema_migrations",
        "DROP TRIGGER association_inbox_identity_immutable",
    ] {
        let (_d, p) = db();
        Connection::open(&p).unwrap().execute_batch(ddl).unwrap();
        assert!(AssociationInbox::open(&p, limits()).is_err());
    }
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("corrupt");
    std::fs::write(&p, b"not a sqlite database").unwrap();
    assert!(AssociationInbox::open(&p, limits()).is_err());
}
#[test]
fn b89_busy_and_fatal_trigger_abort_never_ack() {
    let (_d, p) = db();
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    let c = Connection::open(&p).unwrap();
    c.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(i
        .persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap()
        )
        .is_err());
    c.execute_batch("ROLLBACK").unwrap();
    assert_eq!(i.usage().unwrap(), (1, 512));
    c.execute_batch("CREATE TRIGGER fatal BEFORE INSERT ON association_inbox_events BEGIN SELECT RAISE(ABORT,'fatal write fault'); END;").unwrap();
    assert!(i
        .persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap()
        )
        .is_err());
    assert_eq!(i.usage().unwrap(), (1, 512));
}
#[test]
fn b89_inbox_count_and_bytes_n_n_plus_one_rollback() {
    let (_d, p) = db();
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    i.persist_at(
        &admission(0),
        &candidate("A"),
        chrono::DateTime::from_timestamp(10, 0).unwrap(),
    )
    .unwrap();
    let (count, bytes) = i.usage().unwrap();
    for l in [
        copybot_storage_core::association_inbox::InboxLimits {
            count,
            bytes,
            ..limits()
        },
        copybot_storage_core::association_inbox::InboxLimits {
            count: 1000,
            bytes,
            ..limits()
        },
    ] {
        let (_d, p) = db();
        let mut i = AssociationInbox::open(&p, l).unwrap();
        i.persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(i.usage().unwrap(), (count, bytes));
        assert!(i
            .persist(
                &event(1, DeliveryEvent::Session(SessionGap::End)),
                &CandidateGeneration::Unknown
            )
            .is_err());
        assert_eq!(i.usage().unwrap(), (count, bytes));
    }
    let (_d, p) = db();
    let mut i = AssociationInbox::open(
        &p,
        copybot_storage_core::association_inbox::InboxLimits {
            bytes: bytes - 1,
            ..limits()
        },
    )
    .unwrap();
    assert!(i
        .persist_at(
            &admission(0),
            &candidate("A"),
            chrono::DateTime::from_timestamp(10, 0).unwrap()
        )
        .is_err());
    assert_eq!(i.usage().unwrap(), (1, 512));
}
#[test]
fn b89_recovery_ignore_refuses_startup() {
    let (_d, p) = db();
    let mut i = AssociationInbox::open(&p, limits()).unwrap();
    i.persist_at(
        &admission(0),
        &candidate("A"),
        chrono::DateTime::from_timestamp(10, 0).unwrap(),
    )
    .unwrap();
    drop(i);
    Connection::open(&p).unwrap().execute_batch("CREATE TRIGGER ignored_recovery BEFORE UPDATE OF recovery ON association_inbox_identities BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(AssociationInbox::open(&p, limits()).is_err());
}
